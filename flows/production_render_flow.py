from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from prefect import flow, get_run_logger, task

from app.gpu_leases import LeaseRequest, ReleaseRequest, acquire_lease, read_profile, release_lease
from app.neo4j_assets import ingest_render_manifest
from app.render_assets import preflight_scene_assets
from app.render_ledger import upsert_render_run
from app.wan2gp_cli import run_wan2gp_process

PROJECT_ROOT = Path("/projects/microdramas")
RENDER_TEMPLATE = PROJECT_ROOT / "renders/render_manifest_template.json"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def render_attempt_id(scene_id: str) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"render_{scene_id}_{stamp}"


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: str | Path, data: dict[str, Any]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
    return str(target)


@task
def load_scene_manifest(scene_manifest_path: str) -> dict[str, Any]:
    scene = read_json(scene_manifest_path)
    required = ["scene_id", "episode_id", "target_duration_seconds", "characters"]
    missing = [key for key in required if not scene.get(key)]
    if missing:
        raise ValueError(f"Scene manifest missing required fields: {missing}")
    return scene


@task
def preflight_assets(scene: dict[str, Any]) -> dict[str, Any]:
    return preflight_scene_assets(scene)


@task(retries=2, retry_delay_seconds=5)
def check_http_service(name: str, url: str) -> dict[str, Any]:
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(url)
        return {"name": name, "url": url, "ok": response.status_code < 500, "status_code": response.status_code}
    except Exception as exc:  # noqa: BLE001 - health checks should report instead of hiding service names.
        return {"name": name, "url": url, "ok": False, "error": str(exc)}


@task
def acquire_gpu_role(job_id: str, role: str) -> dict[str, Any]:
    result = acquire_lease(LeaseRequest(job_id=job_id, role=role, ttl_minutes=120, owner="prefect-render-flow"))
    if not result.get("acquired"):
        raise RuntimeError(f"Could not acquire GPU lease for role {role}: {result}")
    return result["lease"]


@task
def release_gpu_role(lease_id: str) -> dict[str, Any]:
    return release_lease(ReleaseRequest(lease_id=lease_id))


@task
def build_render_manifest(
    scene: dict[str, Any],
    scene_manifest_path: str,
    asset_preflight: dict[str, Any],
    lease: dict[str, Any],
    render_id: str,
    out_dir: str,
    note: str,
) -> dict[str, Any]:
    render = deepcopy(read_json(RENDER_TEMPLATE))
    scene_id = scene["scene_id"]
    episode_id = scene["episode_id"]
    render_path = Path(out_dir) / "render_manifest.json"

    render.update(
        {
            "render_id": render_id,
            "scene_id": scene_id,
            "episode_id": episode_id,
            "atlas_task_id": scene.get("atlas_task_id", ""),
            "model": scene.get("video_generation", {}).get("model_type", render["model"]),
            "settings_json": asset_preflight.get("settings_json", ""),
            "prompt": scene.get("video_generation", {}).get("prompt", ""),
            "negative_prompt": scene.get("video_generation", {}).get("negative_prompt", ""),
        }
    )
    render["source_manifests"]["scene_manifest"] = scene_manifest_path
    render["source_manifests"]["keyframe_manifest"] = scene.get("visuals", {}).get("keyframe_manifest", "")
    render["source_manifests"]["dialogue_manifest"] = scene.get("dialogue", {}).get("dialogue_lines_manifest", "")
    render["inputs"]["start_keyframe_path"] = asset_preflight.get("start_keyframe_path", "")
    render["inputs"]["end_keyframe_path"] = asset_preflight.get("end_keyframe_path", "")
    render["inputs"]["audio_guide_path"] = asset_preflight.get("audio_guide_path", "")
    render["runtime"]["settings_summary"] = asset_preflight.get("settings", {})
    render["runtime"].update(
        {
            "host": os.uname().nodename,
            "gpu_ids": lease.get("gpu_ids", []),
            "started_at": utc_now(),
            "finished_at": utc_now(),
        }
    )
    render["review"]["notes"] = note
    path = write_json(render_path, render)
    return {"path": path, "manifest": render}


@task
def record_render_run(
    scene: dict[str, Any],
    render_id: str,
    status: str,
    dry_run: bool,
    gpu_role: str,
    service_checks: list[dict[str, Any]],
    lease: dict[str, Any] | None = None,
    manifest_result: dict[str, Any] | None = None,
    error: str | None = None,
    finished: bool = False,
) -> dict[str, Any]:
    return upsert_render_run(
        render_id=render_id,
        episode_id=scene["episode_id"],
        scene_id=scene["scene_id"],
        atlas_task_id=scene.get("atlas_task_id") or None,
        status=status,
        dry_run=dry_run,
        gpu_role=gpu_role,
        gpu_ids=(lease or {}).get("gpu_ids", []),
        service_checks=service_checks,
        manifest_path=(manifest_result or {}).get("path"),
        manifest=(manifest_result or {}).get("manifest"),
        error=error,
        finished=finished,
    )


@task
def ingest_manifest_into_neo4j(manifest_path: str, scene_manifest_path: str) -> dict[str, Any]:
    try:
        return ingest_render_manifest(manifest_path, scene_manifest_path=scene_manifest_path)
    except Exception as exc:  # noqa: BLE001 - a render artifact should survive graph indexing errors.
        return {"ok": False, "error": str(exc)}


@task
def run_wan2gp_cli(settings_path: str, output_dir: str, validate_only: bool, gpu_ids: list[int]) -> dict[str, Any]:
    profile = read_profile()
    uuid_by_id = {int(gpu["id"]): gpu["uuid"] for gpu in profile.get("gpus", [])}
    cuda_visible_devices = ",".join(uuid_by_id.get(int(gpu_id), str(gpu_id)) for gpu_id in gpu_ids)
    return run_wan2gp_process(
        settings_path=settings_path,
        output_dir=output_dir,
        gpu_ids=gpu_ids,
        cuda_visible_devices=cuda_visible_devices,
        dry_run=validate_only,
    ).as_dict()


@flow(name="microdrama-production-render")
def microdrama_production_render(
    scene_manifest_path: str,
    dry_run: bool = True,
    gpu_role: str = "wan2gp_ltx_30s",
    wan2gp_validate_only: bool = False,
) -> str:
    logger = get_run_logger()
    scene = load_scene_manifest(scene_manifest_path)
    asset_preflight = preflight_assets(scene)
    render_id = render_attempt_id(scene["scene_id"])
    run_dir = PROJECT_ROOT / "orchestrator_runs" / scene["episode_id"] / scene["scene_id"] / render_id

    service_checks = [
        check_http_service("orchestrator", "http://orchestrator-api:8090/health"),
        check_http_service("qwen27b", os.getenv("QWEN27B_BASE_URL", "http://rtx0.python-bull.ts.net:8000/v1") + "/models"),
        check_http_service("comfy", os.getenv("COMFY_URL", "http://host.docker.internal:8188")),
        check_http_service("wan2gp", os.getenv("WAN2GP_URL", "http://host.docker.internal:7860")),
    ]
    failed = [check for check in service_checks if not check.get("ok")]
    if failed:
        record_render_run(
            scene,
            render_id,
            "service_check_failed",
            dry_run,
            gpu_role,
            service_checks,
            error=f"Required service checks failed: {failed}",
            finished=True,
        )
        raise RuntimeError(f"Required service checks failed: {failed}")

    lease = acquire_gpu_role(job_id=render_id, role=gpu_role)
    record_render_run(scene, render_id, "leased", dry_run, gpu_role, service_checks, lease=lease)
    try:
        if not dry_run:
            settings_json = asset_preflight.get("settings_json", "")
            if not settings_json:
                raise ValueError("Live Wan2GP render requires scene.video_generation.settings_json")
            wan_out_dir = run_dir / "wan2gp"
            wan_result = run_wan2gp_cli(settings_json, str(wan_out_dir), wan2gp_validate_only, lease.get("gpu_ids", []))
            if not wan_result.get("ok"):
                raise RuntimeError(f"Wan2GP CLI failed with exit code {wan_result.get('exit_code')}:\n{wan_result.get('output')}")

            manifest_result = build_render_manifest(
                scene,
                scene_manifest_path,
                asset_preflight,
                lease,
                render_id,
                str(run_dir),
                "Wan2GP CLI validation completed." if wan2gp_validate_only else "Live Wan2GP CLI render completed.",
            )
            manifest = manifest_result["manifest"]
            generated_files = wan_result.get("generated_files", [])
            if generated_files:
                manifest["outputs"]["video_path"] = generated_files[0]
                manifest["outputs"]["video_asset_id"] = f"asset_{render_id}_video"
            manifest_result["path"] = write_json(manifest_result["path"], manifest)
            manifest_result["manifest"] = manifest
            record_render_run(
                scene,
                render_id,
                "wan2gp_validated" if wan2gp_validate_only else "render_completed",
                dry_run,
                gpu_role,
                service_checks,
                lease=lease,
                manifest_result=manifest_result,
                finished=True,
            )
            ingest_result = ingest_manifest_into_neo4j(manifest_result["path"], scene_manifest_path)
            if ingest_result.get("ok"):
                record_render_run(
                    scene,
                    render_id,
                    "render_ingested",
                    dry_run,
                    gpu_role,
                    service_checks,
                    lease=lease,
                    manifest_result=manifest_result,
                    finished=True,
                )
            else:
                record_render_run(
                    scene,
                    render_id,
                    "render_ingest_failed",
                    dry_run,
                    gpu_role,
                    service_checks,
                    lease=lease,
                    manifest_result=manifest_result,
                    error=ingest_result.get("error", "Unknown Neo4j ingest error"),
                    finished=True,
                )
            logger.info("Wan2GP CLI completed; wrote render manifest to %s", manifest_result["path"])
            return manifest_result["path"]

        manifest_result = build_render_manifest(
            scene,
            scene_manifest_path,
            asset_preflight,
            lease,
            render_id,
            str(run_dir),
            "Dry run only: service checks and manifest contract validation completed; no GPU render submitted.",
        )
        record_render_run(
            scene,
            render_id,
            "dry_run_completed",
            dry_run,
            gpu_role,
            service_checks,
            lease=lease,
            manifest_result=manifest_result,
            finished=True,
        )
        ingest_result = ingest_manifest_into_neo4j(manifest_result["path"], scene_manifest_path)
        if ingest_result.get("ok"):
            record_render_run(
                scene,
                render_id,
                "dry_run_ingested",
                dry_run,
                gpu_role,
                service_checks,
                lease=lease,
                manifest_result=manifest_result,
                finished=True,
            )
        else:
            record_render_run(
                scene,
                render_id,
                "dry_run_ingest_failed",
                dry_run,
                gpu_role,
                service_checks,
                lease=lease,
                manifest_result=manifest_result,
                error=ingest_result.get("error", "Unknown Neo4j ingest error"),
                finished=True,
            )
        logger.info("Wrote dry-run render manifest to %s", manifest_result["path"])
        return manifest_result["path"]
    except Exception as exc:
        record_render_run(scene, render_id, "failed", dry_run, gpu_role, service_checks, lease=lease, error=str(exc), finished=True)
        raise
    finally:
        release_gpu_role(lease["lease_id"])


if __name__ == "__main__":
    microdrama_production_render(
        scene_manifest_path=str(PROJECT_ROOT / "scenes/smoke_scene_manifest.json"),
        dry_run=True,
    )

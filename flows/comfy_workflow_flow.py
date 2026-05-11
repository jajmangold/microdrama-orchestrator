from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from app.comfy_workflows import (
    collect_output_images,
    prepare_workflow,
    queue_prompt,
    wait_for_outputs,
    write_json,
)

PROJECT_ROOT = Path("/projects/microdramas")


def run_id(workflow_id: str) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"comfy_{workflow_id}_{stamp}"


@task
def prepare_comfy_payload(manifest_path: str, overrides: dict[str, Any], out_dir: str) -> dict[str, Any]:
    prepared = prepare_workflow(manifest_path, overrides)
    payload_path = write_json(Path(out_dir) / "workflow.api.json", prepared.prompt)
    return {
        "workflow_id": prepared.workflow_id,
        "manifest_path": prepared.manifest_path,
        "api_workflow_path": prepared.api_workflow_path,
        "payload_path": payload_path,
        "prompt": prepared.prompt,
    }


@task
def submit_comfy_payload(prompt: dict[str, Any], base_url: str) -> dict[str, Any]:
    queued = queue_prompt(base_url, prompt)
    prompt_id = queued["prompt_id"]
    history = wait_for_outputs(base_url, prompt_id)
    return {
        "prompt_id": prompt_id,
        "outputs": collect_output_images(history),
    }


@flow(name="microdrama-comfy-workflow")
def microdrama_comfy_workflow(
    manifest_path: str,
    overrides: dict[str, Any] | None = None,
    dry_run: bool = True,
) -> str:
    logger = get_run_logger()
    prepared_probe = prepare_workflow(manifest_path, overrides or {})
    out_dir = PROJECT_ROOT / "orchestrator_runs" / "comfy" / run_id(prepared_probe.workflow_id)
    result = prepare_comfy_payload(manifest_path, overrides or {}, str(out_dir))
    if dry_run:
        summary_path = write_json(
            out_dir / "result.json",
            {
                "status": "prepared",
                "workflow_id": result["workflow_id"],
                "manifest_path": result["manifest_path"],
                "api_workflow_path": result["api_workflow_path"],
                "payload_path": result["payload_path"],
            },
        )
        logger.info("Prepared Comfy workflow payload at %s", result["payload_path"])
        return summary_path

    comfy_url = os.getenv("COMFY_URL", "http://comfyui:8188")
    execution = submit_comfy_payload(result["prompt"], comfy_url)
    summary_path = write_json(
        out_dir / "result.json",
        {
            "status": "completed",
            "workflow_id": result["workflow_id"],
            "manifest_path": result["manifest_path"],
            "api_workflow_path": result["api_workflow_path"],
            "payload_path": result["payload_path"],
            "comfy": execution,
        },
    )
    logger.info("Completed Comfy workflow %s", result["workflow_id"])
    return summary_path


if __name__ == "__main__":
    microdrama_comfy_workflow(
        manifest_path="workflows/manifests/z-image-turbo-fast-q5km.json",
        overrides={
            "prompt": "vertical cinematic character reference portrait, neutral expression, clean background",
            "seed": 12345,
            "filename_prefix": "microdrama_assets/orchestrator_probe",
        },
        dry_run=True,
    )

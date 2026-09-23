from __future__ import annotations

import json
import os
import socket
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.comfy_workflows import queue_prompt, read_json, wait_for_outputs


@dataclass(frozen=True)
class QueueWorker:
    worker_id: str
    base_url: str
    enabled: bool = True
    concurrency: int = 1


@dataclass(frozen=True)
class ArtifactQueueConfig:
    queue_name: str
    queue_dir: Path
    output_root: Path
    stage_workflow_path: Path
    output_prefix_base: str
    poll_seconds: float
    prompt_timeout_seconds: int
    claim_stale_seconds: int
    max_attempts: int
    retry_failed: bool
    event_log_path: Path
    workers: tuple[QueueWorker, ...]
    load_node_class_type: str = "LoadLatentArtifactForQueue"
    artifact_input_name: str = "artifact_path"
    output_prefix_input_name: str = "filename_prefix"

    @property
    def stage2_workflow_path(self) -> Path:
        """Compatibility alias for existing two-stage queue configs."""
        return self.stage_workflow_path


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _env_name(prefix: str, name: str) -> str:
    return f"{prefix}_{name}" if prefix else name


def _env_bool(value: object) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def load_config(
    path: str | Path,
    *,
    env_prefix: str = "ARTIFACT_QUEUE",
    default_queue_name: str = "artifact-queue",
    default_load_node_class_type: str = "LoadLatentArtifactForQueue",
) -> ArtifactQueueConfig:
    raw = read_json(Path(path))
    workers = tuple(
        QueueWorker(
            worker_id=item["worker_id"],
            base_url=item["base_url"].rstrip("/"),
            enabled=bool(item.get("enabled", True)),
            concurrency=int(item.get("concurrency", 1)),
        )
        for item in raw.get("workers", [])
    )
    if not workers:
        raise ValueError(f"No artifact queue workers configured in {path}")

    stage_workflow_path = raw.get("stage_workflow_path", raw.get("stage2_workflow_path"))
    if not stage_workflow_path:
        raise ValueError(f"Missing stage_workflow_path in {path}")
    stage_workflow_env = os.getenv(_env_name(env_prefix, "STAGE_WORKFLOW")) or os.getenv(
        _env_name(env_prefix, "STAGE2_WORKFLOW")
    )

    return ArtifactQueueConfig(
        queue_name=os.getenv(_env_name(env_prefix, "QUEUE_NAME"), raw.get("queue_name", default_queue_name)),
        queue_dir=Path(os.getenv(_env_name(env_prefix, "QUEUE_DIR"), raw["queue_dir"])),
        output_root=Path(os.getenv(_env_name(env_prefix, "OUTPUT_ROOT"), raw["output_root"])),
        stage_workflow_path=Path(stage_workflow_env or stage_workflow_path),
        output_prefix_base=os.getenv(
            _env_name(env_prefix, "OUTPUT_PREFIX_BASE"),
            raw.get("output_prefix_base", "artifact_queue/processed"),
        ),
        poll_seconds=float(os.getenv(_env_name(env_prefix, "POLL_SECONDS"), raw.get("poll_seconds", 5.0))),
        prompt_timeout_seconds=int(
            os.getenv(_env_name(env_prefix, "PROMPT_TIMEOUT_SECONDS"), raw.get("prompt_timeout_seconds", 1800))
        ),
        claim_stale_seconds=int(
            os.getenv(_env_name(env_prefix, "CLAIM_STALE_SECONDS"), raw.get("claim_stale_seconds", 7200))
        ),
        max_attempts=int(os.getenv(_env_name(env_prefix, "MAX_ATTEMPTS"), raw.get("max_attempts", 2))),
        retry_failed=_env_bool(os.getenv(_env_name(env_prefix, "RETRY_FAILED"), raw.get("retry_failed", False))),
        event_log_path=Path(os.getenv(_env_name(env_prefix, "EVENT_LOG"), raw["event_log_path"])),
        workers=workers,
        load_node_class_type=os.getenv(
            _env_name(env_prefix, "LOAD_NODE_CLASS_TYPE"),
            raw.get("load_node_class_type", default_load_node_class_type),
        ),
        artifact_input_name=os.getenv(
            _env_name(env_prefix, "ARTIFACT_INPUT_NAME"),
            raw.get("artifact_input_name", "artifact_path"),
        ),
        output_prefix_input_name=os.getenv(
            _env_name(env_prefix, "OUTPUT_PREFIX_INPUT_NAME"),
            raw.get("output_prefix_input_name", "filename_prefix"),
        ),
    )


def sidecar_path(ready_path: Path, suffix: str) -> Path:
    return Path(f"{ready_path.with_suffix('')}.{suffix}.json")


def read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
    tmp.replace(path)


def append_event(config: ArtifactQueueConfig, event: dict[str, Any]) -> None:
    config.event_log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ts": utc_now(), "queue_name": config.queue_name, **event}
    with config.event_log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def artifact_ref(config: ArtifactQueueConfig, ready_path: Path) -> str:
    try:
        return str(ready_path.relative_to(config.output_root))
    except ValueError:
        return str(ready_path)


def artifact_status(config: ArtifactQueueConfig, ready_path: Path) -> dict[str, Any]:
    done = read_json_if_exists(sidecar_path(ready_path, "DONE"))
    failed = read_json_if_exists(sidecar_path(ready_path, "FAILED"))
    claimed = read_json_if_exists(sidecar_path(ready_path, "CLAIMED"))
    status = "ready"
    if done:
        status = "done"
    elif claimed:
        claimed_at = float(claimed.get("claimed_at_unix", 0))
        age = time.time() - claimed_at if claimed_at else None
        status = "stale_claim" if age is not None and age > config.claim_stale_seconds else "claimed"
    elif failed:
        attempts = int(failed.get("attempts", 1))
        status = "failed_retryable" if config.retry_failed and attempts < config.max_attempts else "failed"
    return {
        "artifact": artifact_ref(config, ready_path),
        "ready_path": str(ready_path),
        "status": status,
        "ready_mtime_unix": ready_path.stat().st_mtime,
        "done": done,
        "failed": failed,
        "claimed": claimed,
    }


def list_artifacts(config: ArtifactQueueConfig) -> list[dict[str, Any]]:
    if not config.queue_dir.exists():
        return []
    return [artifact_status(config, path) for path in sorted(config.queue_dir.glob("*.READY"))]


def failed_attempts(ready_path: Path) -> int:
    failed = read_json_if_exists(sidecar_path(ready_path, "FAILED"))
    if not failed:
        return 0
    return int(failed.get("attempts", 1))


def is_claim_stale(config: ArtifactQueueConfig, ready_path: Path) -> bool:
    claimed = read_json_if_exists(sidecar_path(ready_path, "CLAIMED"))
    if not claimed:
        return False
    claimed_at = float(claimed.get("claimed_at_unix", 0))
    return bool(claimed_at and time.time() - claimed_at > config.claim_stale_seconds)


def claim_artifact(config: ArtifactQueueConfig, ready_path: Path, worker_id: str) -> dict[str, Any] | None:
    if sidecar_path(ready_path, "DONE").exists():
        return None
    if sidecar_path(ready_path, "CLAIMED").exists():
        if not is_claim_stale(config, ready_path):
            return None
        sidecar_path(ready_path, "CLAIMED").unlink(missing_ok=True)
    attempts = failed_attempts(ready_path)
    if sidecar_path(ready_path, "FAILED").exists() and (not config.retry_failed or attempts >= config.max_attempts):
        return None

    claim = {
        "artifact": artifact_ref(config, ready_path),
        "ready_path": str(ready_path),
        "worker_id": worker_id,
        "claim_id": uuid.uuid4().hex,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "attempt": attempts + 1,
        "claimed_at": utc_now(),
        "claimed_at_unix": time.time(),
    }
    claimed_path = sidecar_path(ready_path, "CLAIMED")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(claimed_path, flags, 0o644)
    except FileExistsError:
        return None
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(claim, handle, indent=2, sort_keys=True)
        handle.write("\n")
    append_event(config, {"event": "claimed", **claim})
    return claim


def patch_stage_workflow(config: ArtifactQueueConfig, ready_path: Path, worker_id: str) -> dict[str, Any]:
    prompt = read_json(config.stage_workflow_path)
    artifact = artifact_ref(config, ready_path)
    load_nodes = 0
    for node in prompt.values():
        if node.get("class_type") == config.load_node_class_type:
            node.setdefault("inputs", {})[config.artifact_input_name] = artifact
            load_nodes += 1
    if load_nodes != 1:
        raise ValueError(
            f"Expected exactly one {config.load_node_class_type} node, found {load_nodes}"
        )

    prefix = f"{config.output_prefix_base}/{ready_path.with_suffix('').name}/{worker_id}"
    for node in prompt.values():
        inputs = node.setdefault("inputs", {})
        if config.output_prefix_input_name in inputs:
            inputs[config.output_prefix_input_name] = prefix
    return prompt


def collect_media_outputs(config: ArtifactQueueConfig, history_item: dict[str, Any]) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for node_id, node_output in history_item.get("outputs", {}).items():
        for key in ("images", "gifs", "audio"):
            for item in node_output.get(key, []):
                filename = item.get("filename", "")
                if not filename:
                    continue
                subfolder = item.get("subfolder", "")
                relative = Path(subfolder) / filename if subfolder else Path(filename)
                outputs.append(
                    {
                        "node_id": node_id,
                        "kind": key,
                        "filename": filename,
                        "subfolder": subfolder,
                        "type": item.get("type", ""),
                        "container_path": str(config.output_root / relative),
                    }
                )
    return outputs


def assert_history_success(history_item: dict[str, Any], prompt_id: str) -> None:
    status = history_item.get("status", {})
    if status and not status.get("completed", False):
        raise RuntimeError(f"Comfy prompt {prompt_id} did not complete: {status}")


def process_artifact(config: ArtifactQueueConfig, ready_path: Path, worker: QueueWorker) -> dict[str, Any]:
    claim = claim_artifact(config, ready_path, worker.worker_id)
    if not claim:
        return {"status": "skipped", "artifact": artifact_ref(config, ready_path), "worker_id": worker.worker_id}

    started = time.monotonic()
    try:
        prompt = patch_stage_workflow(config, ready_path, worker.worker_id)
        queued = queue_prompt(worker.base_url, prompt, client_id=f"{config.queue_name}-{claim['claim_id']}")
        prompt_id = queued["prompt_id"]
        append_event(config, {"event": "submitted", "artifact": claim["artifact"], "worker_id": worker.worker_id, "prompt_id": prompt_id})
        history_item = wait_for_outputs(worker.base_url, prompt_id, timeout_seconds=config.prompt_timeout_seconds)
        assert_history_success(history_item, prompt_id)
        outputs = collect_media_outputs(config, history_item)
        elapsed = time.monotonic() - started
        done = {
            **claim,
            "status": "done",
            "prompt_id": prompt_id,
            "worker_base_url": worker.base_url,
            "finished_at": utc_now(),
            "elapsed_seconds": round(elapsed, 3),
            "outputs": outputs,
        }
        atomic_write_json(sidecar_path(ready_path, "DONE"), done)
        sidecar_path(ready_path, "CLAIMED").unlink(missing_ok=True)
        sidecar_path(ready_path, "FAILED").unlink(missing_ok=True)
        append_event(config, {"event": "done", "artifact": claim["artifact"], "worker_id": worker.worker_id, "prompt_id": prompt_id, "elapsed_seconds": round(elapsed, 3), "outputs": outputs})
        return done
    except Exception as exc:
        elapsed = time.monotonic() - started
        failed = {
            **claim,
            "status": "failed",
            "worker_base_url": worker.base_url,
            "failed_at": utc_now(),
            "elapsed_seconds": round(elapsed, 3),
            "attempts": claim["attempt"],
            "error": repr(exc),
        }
        atomic_write_json(sidecar_path(ready_path, "FAILED"), failed)
        sidecar_path(ready_path, "CLAIMED").unlink(missing_ok=True)
        append_event(config, {"event": "failed", "artifact": claim["artifact"], "worker_id": worker.worker_id, "elapsed_seconds": round(elapsed, 3), "error": repr(exc)})
        raise


def pick_ready_artifact(config: ArtifactQueueConfig, in_flight: set[Path]) -> Path | None:
    for item in list_artifacts(config):
        ready_path = Path(item["ready_path"])
        if ready_path in in_flight:
            continue
        if item["status"] in {"ready", "stale_claim", "failed_retryable"}:
            return ready_path
    return None


def run_once(config: ArtifactQueueConfig, worker: QueueWorker | None = None) -> dict[str, Any]:
    selected_worker = worker or next((item for item in config.workers if item.enabled), None)
    if not selected_worker:
        raise ValueError("No enabled artifact queue worker")
    ready_path = pick_ready_artifact(config, set())
    if not ready_path:
        return {"status": "idle", "queue_dir": str(config.queue_dir)}
    return process_artifact(config, ready_path, selected_worker)


def run_forever(config: ArtifactQueueConfig, once: bool = False) -> None:
    enabled_workers = [worker for worker in config.workers if worker.enabled]
    if not enabled_workers:
        raise ValueError("No enabled artifact queue workers")
    max_workers = sum(max(1, worker.concurrency) for worker in enabled_workers)
    in_flight: dict[Future[dict[str, Any]], tuple[Path, str]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while True:
            active_by_worker: dict[str, int] = {}
            for _, worker_id in in_flight.values():
                active_by_worker[worker_id] = active_by_worker.get(worker_id, 0) + 1

            for worker in enabled_workers:
                free_slots = max(1, worker.concurrency) - active_by_worker.get(worker.worker_id, 0)
                if free_slots <= 0:
                    continue
                for _ in range(free_slots):
                    if len(in_flight) >= max_workers:
                        break
                    ready_path = pick_ready_artifact(config, {path for path, _ in in_flight.values()})
                    if not ready_path:
                        break
                    future = pool.submit(process_artifact, config, ready_path, worker)
                    in_flight[future] = (ready_path, worker.worker_id)

            if once:
                if not in_flight:
                    return
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            elif in_flight:
                done, _ = wait(in_flight, timeout=config.poll_seconds, return_when=FIRST_COMPLETED)
            else:
                time.sleep(config.poll_seconds)
                done = set()

            for future in done:
                ready_path, _ = in_flight.pop(future)
                try:
                    future.result()
                except Exception as exc:
                    print(f"{config.queue_name} failed for {ready_path}: {exc!r}", flush=True)

            if once and not in_flight:
                return


def queue_summary(config: ArtifactQueueConfig) -> dict[str, Any]:
    artifacts = list_artifacts(config)
    counts: dict[str, int] = {}
    for item in artifacts:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    return {
        "queue_name": config.queue_name,
        "queue_dir": str(config.queue_dir),
        "stage_workflow_path": str(config.stage_workflow_path),
        "load_node_class_type": config.load_node_class_type,
        "workers": [worker.__dict__ for worker in config.workers],
        "counts": counts,
        "artifacts": artifacts,
    }

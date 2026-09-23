from __future__ import annotations

import os
from pathlib import Path

from app.artifact_queue import (
    ArtifactQueueConfig,
    QueueWorker,
    artifact_ref,
    artifact_status,
    claim_artifact,
    collect_media_outputs,
    list_artifacts,
    load_config as load_artifact_queue_config,
    patch_stage_workflow,
    pick_ready_artifact,
    process_artifact,
    queue_summary as artifact_queue_summary,
    read_json_if_exists,
    run_forever,
    run_once,
    sidecar_path,
)

DEFAULT_CONFIG_PATH = Path(os.getenv("LTX_AV_QUEUE_CONFIG", "/app/config/ltx_av_queue.json"))

QueueConfig = ArtifactQueueConfig


def load_config(path: str | Path | None = None) -> ArtifactQueueConfig:
    return load_artifact_queue_config(
        Path(path) if path else DEFAULT_CONFIG_PATH,
        env_prefix="LTX_AV",
        default_queue_name="ltx-av-queue",
        default_load_node_class_type="LoadLTXAVLatentForQueue",
    )


def patch_stage2_workflow(config: ArtifactQueueConfig, ready_path: Path, worker_id: str) -> dict:
    return patch_stage_workflow(config, ready_path, worker_id)


def queue_summary(config: ArtifactQueueConfig) -> dict:
    summary = artifact_queue_summary(config)
    summary["stage2_workflow_path"] = str(config.stage2_workflow_path)
    return summary


__all__ = [
    "ArtifactQueueConfig",
    "DEFAULT_CONFIG_PATH",
    "QueueConfig",
    "QueueWorker",
    "artifact_ref",
    "artifact_status",
    "claim_artifact",
    "collect_media_outputs",
    "list_artifacts",
    "load_config",
    "patch_stage2_workflow",
    "patch_stage_workflow",
    "pick_ready_artifact",
    "process_artifact",
    "queue_summary",
    "read_json_if_exists",
    "run_forever",
    "run_once",
    "sidecar_path",
]

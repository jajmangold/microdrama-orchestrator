from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CONFIG_PATH = Path("/app/config/worker_profiles.json")


def read_worker_profiles(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def list_workers(status: str | None = None, job_role: str | None = None) -> list[dict[str, Any]]:
    profile = read_worker_profiles()
    workers = profile.get("workers", [])
    if status:
        workers = [worker for worker in workers if worker.get("status") == status]
    if job_role:
        workers = [worker for worker in workers if job_role in worker.get("allowed_job_roles", [])]
    return workers

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

PROFILE_PATH = Path(os.getenv("GPU_PROFILE_PATH", "/app/config/gpu_profile.json"))
LEASE_PATH = Path(os.getenv("GPU_LEASE_PATH", "/projects/microdramas/orchestrator_state/gpu_leases.json"))


class LeaseRequest(BaseModel):
    job_id: str = Field(min_length=1)
    role: str = Field(min_length=1)
    gpu_ids: list[int] | None = None
    ttl_minutes: int | None = Field(default=None, ge=1, le=24 * 60)
    owner: str = "orchestrator"


class ReleaseRequest(BaseModel):
    lease_id: str


def now_ts() -> float:
    return time.time()


def read_profile() -> dict[str, Any]:
    with PROFILE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _read_leases_unlocked() -> dict[str, Any]:
    if not LEASE_PATH.exists():
        return {"schema_version": 1, "leases": []}
    with LEASE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_leases_unlocked(data: dict[str, Any]) -> None:
    LEASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LEASE_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
    tmp.replace(LEASE_PATH)


@contextmanager
def lease_lock():
    lock_path = LEASE_PATH.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as lock:
        import fcntl

        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def active_leases(data: dict[str, Any]) -> list[dict[str, Any]]:
    current = now_ts()
    return [lease for lease in data.get("leases", []) if lease.get("expires_at", 0) > current and lease.get("status") == "active"]


def list_leases() -> dict[str, Any]:
    with lease_lock():
        data = _read_leases_unlocked()
        data["leases"] = active_leases(data)
        _write_leases_unlocked(data)
        return data


def acquire_lease(request: LeaseRequest) -> dict[str, Any]:
    profile = read_profile()
    role = profile["roles"].get(request.role)
    if not role:
        raise ValueError(f"Unknown GPU role: {request.role}")

    ttl_minutes = request.ttl_minutes or profile.get("default_ttl_minutes", 180)
    requested_ids = request.gpu_ids
    candidates = requested_ids or role.get("preferred_gpu_ids") or role.get("candidate_gpu_ids", [])
    gpu_count = role["gpu_count"]
    if len(candidates) < gpu_count:
        raise ValueError(f"Not enough candidate GPUs for role {request.role}")

    with lease_lock():
        data = _read_leases_unlocked()
        leases = active_leases(data)
        busy = {gpu for lease in leases for gpu in lease.get("gpu_ids", [])}
        selected = [gpu for gpu in candidates if gpu not in busy][:gpu_count]
        if len(selected) < gpu_count:
            return {
                "acquired": False,
                "reason": "insufficient_free_gpus",
                "busy_gpu_ids": sorted(busy),
                "candidate_gpu_ids": candidates,
                "active_leases": leases,
            }

        lease_id = f"{request.job_id}-{int(now_ts())}"
        lease = {
            "lease_id": lease_id,
            "job_id": request.job_id,
            "owner": request.owner,
            "role": request.role,
            "gpu_ids": selected,
            "exclusive": role.get("exclusive", True),
            "created_at": now_ts(),
            "expires_at": now_ts() + ttl_minutes * 60,
            "ttl_minutes": ttl_minutes,
            "status": "active",
        }
        leases.append(lease)
        data["leases"] = leases
        _write_leases_unlocked(data)
        return {"acquired": True, "lease": lease}


def release_lease(request: ReleaseRequest) -> dict[str, Any]:
    with lease_lock():
        data = _read_leases_unlocked()
        released = None
        remaining = []
        for lease in data.get("leases", []):
            if lease.get("lease_id") == request.lease_id and lease.get("status") == "active":
                lease["status"] = "released"
                lease["released_at"] = now_ts()
                released = lease
            else:
                remaining.append(lease)
        data["leases"] = active_leases({"leases": remaining})
        _write_leases_unlocked(data)
        return {"released": released is not None, "lease": released}

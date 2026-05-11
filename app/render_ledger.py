from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row


def utc_now() -> datetime:
    return datetime.now(UTC)


def database_url() -> str:
    return os.getenv(
        "RENDER_LEDGER_DATABASE_URL",
        os.getenv(
            "PREFECT_API_DATABASE_CONNECTION_URL_SYNC",
            "postgresql://microdrama:microdrama-local@postgres:5432/microdrama",
        ),
    )


@contextmanager
def connect() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(database_url(), row_factory=dict_row) as conn:
        yield conn


def _json(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), sort_keys=True)


def init_render_ledger() -> None:
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS render_runs (
                render_id TEXT PRIMARY KEY,
                episode_id TEXT NOT NULL,
                scene_id TEXT NOT NULL,
                atlas_task_id TEXT,
                status TEXT NOT NULL,
                dry_run BOOLEAN NOT NULL DEFAULT TRUE,
                gpu_role TEXT,
                gpu_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                service_checks JSONB NOT NULL DEFAULT '[]'::jsonb,
                manifest_path TEXT,
                manifest JSONB,
                error TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_render_runs_scene ON render_runs(scene_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_render_runs_status ON render_runs(status)")


def upsert_render_run(
    *,
    render_id: str,
    episode_id: str,
    scene_id: str,
    atlas_task_id: str | None = None,
    status: str,
    dry_run: bool,
    gpu_role: str | None = None,
    gpu_ids: list[int] | None = None,
    service_checks: list[dict[str, Any]] | None = None,
    manifest_path: str | None = None,
    manifest: dict[str, Any] | None = None,
    error: str | None = None,
    finished: bool = False,
) -> dict[str, Any]:
    init_render_ledger()
    finished_at = utc_now() if finished else None
    with connect() as conn:
        row = conn.execute(
            """
            INSERT INTO render_runs (
                render_id, episode_id, scene_id, atlas_task_id, status, dry_run,
                gpu_role, gpu_ids, service_checks, manifest_path, manifest, error,
                finished_at
            )
            VALUES (
                %(render_id)s, %(episode_id)s, %(scene_id)s, %(atlas_task_id)s,
                %(status)s, %(dry_run)s, %(gpu_role)s, %(gpu_ids)s::jsonb,
                %(service_checks)s::jsonb, %(manifest_path)s, %(manifest)s::jsonb,
                %(error)s, %(finished_at)s
            )
            ON CONFLICT (render_id) DO UPDATE SET
                atlas_task_id = EXCLUDED.atlas_task_id,
                status = EXCLUDED.status,
                dry_run = EXCLUDED.dry_run,
                gpu_role = EXCLUDED.gpu_role,
                gpu_ids = EXCLUDED.gpu_ids,
                service_checks = EXCLUDED.service_checks,
                manifest_path = EXCLUDED.manifest_path,
                manifest = EXCLUDED.manifest,
                error = EXCLUDED.error,
                updated_at = now(),
                finished_at = COALESCE(EXCLUDED.finished_at, render_runs.finished_at)
            RETURNING *
            """,
            {
                "render_id": render_id,
                "episode_id": episode_id,
                "scene_id": scene_id,
                "atlas_task_id": atlas_task_id,
                "status": status,
                "dry_run": dry_run,
                "gpu_role": gpu_role,
                "gpu_ids": _json(gpu_ids or []),
                "service_checks": _json(service_checks or []),
                "manifest_path": manifest_path,
                "manifest": _json(manifest) if manifest else None,
                "error": error,
                "finished_at": finished_at,
            },
        ).fetchone()
        return dict(row)


def list_render_runs(limit: int = 50, status: str | None = None) -> list[dict[str, Any]]:
    init_render_ledger()
    limit = max(1, min(limit, 500))
    query = "SELECT * FROM render_runs"
    params: dict[str, Any] = {"limit": limit}
    if status:
        query += " WHERE status = %(status)s"
        params["status"] = status
    query += " ORDER BY updated_at DESC LIMIT %(limit)s"
    with connect() as conn:
        return [dict(row) for row in conn.execute(query, params).fetchall()]


def get_render_run(render_id: str) -> dict[str, Any] | None:
    init_render_ledger()
    with connect() as conn:
        row = conn.execute("SELECT * FROM render_runs WHERE render_id = %s", (render_id,)).fetchone()
        return dict(row) if row else None

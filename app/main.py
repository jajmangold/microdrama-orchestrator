import os
from typing import Any

import httpx
from fastapi import FastAPI
from neo4j import GraphDatabase

from app.gpu_leases import LeaseRequest, ReleaseRequest, acquire_lease, list_leases, read_profile, release_lease
from app.render_ledger import get_render_run, init_render_ledger, list_render_runs
from app.worker_profiles import list_workers, read_worker_profiles

app = FastAPI(title="Microdrama Orchestrator", version="0.1.0")


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/services")
async def services() -> dict[str, str]:
    return {
        "qwen27b": env("QWEN27B_BASE_URL", "http://rtx0.python-bull.ts.net:8000/v1"),
        "wan2gp": env("WAN2GP_URL", "http://host.docker.internal:7860"),
        "comfy": env("COMFY_URL", "http://host.docker.internal:8188"),
        "acestep": env("ACESTEP_URL", "http://host.docker.internal:8081"),
        "neo4j": env("NEO4J_URI", "bolt://host.docker.internal:7687"),
        "prefect": env("PREFECT_API_URL", "http://prefect-server:4200/api"),
    }


@app.get("/qwen27b/models")
async def qwen_models() -> Any:
    base_url = env("QWEN27B_BASE_URL", "http://rtx0.python-bull.ts.net:8000/v1")
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(f"{base_url}/models")
        response.raise_for_status()
        return response.json()


@app.post("/qwen27b/smoke")
async def qwen_smoke() -> Any:
    base_url = env("QWEN27B_BASE_URL", "http://rtx0.python-bull.ts.net:8000/v1")
    model = env("QWEN27B_MODEL", "qwen27b")
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with exactly: orchestrator online"}],
        "max_tokens": 16,
        "temperature": 0,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(f"{base_url}/chat/completions", json=payload)
        response.raise_for_status()
        return response.json()


@app.get("/neo4j/smoke")
def neo4j_smoke() -> dict[str, Any]:
    uri = env("NEO4J_URI", "bolt://host.docker.internal:7687")
    user = env("NEO4J_USER", "neo4j")
    password = env("NEO4J_PASSWORD", "microdrama-local")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            record = session.run("RETURN 1 AS ok").single()
            return {"ok": record["ok"] if record else None}
    finally:
        driver.close()


@app.get("/gpu/profile")
def gpu_profile() -> dict[str, Any]:
    return read_profile()


@app.get("/workers/profile")
def workers_profile() -> dict[str, Any]:
    return read_worker_profiles()


@app.get("/workers")
def workers(status: str | None = None, job_role: str | None = None) -> dict[str, Any]:
    return {"workers": list_workers(status=status, job_role=job_role)}


@app.get("/gpu/leases")
def gpu_leases() -> dict[str, Any]:
    return list_leases()


@app.post("/gpu/leases/acquire")
def gpu_lease_acquire(request: LeaseRequest) -> dict[str, Any]:
    return acquire_lease(request)


@app.post("/gpu/leases/release")
def gpu_lease_release(request: ReleaseRequest) -> dict[str, Any]:
    return release_lease(request)


@app.get("/render-runs")
def render_runs(limit: int = 50, status: str | None = None) -> dict[str, Any]:
    return {"runs": list_render_runs(limit=limit, status=status)}


@app.get("/render-runs/{render_id}")
def render_run(render_id: str) -> dict[str, Any]:
    run = get_render_run(render_id)
    return {"run": run}


@app.post("/render-runs/init")
def render_runs_init() -> dict[str, str]:
    init_render_ledger()
    return {"status": "ok"}

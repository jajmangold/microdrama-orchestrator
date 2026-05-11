# Microdrama Orchestrator

This directory is the glue layer for the microdrama production system.

## Roles

- FastAPI (`orchestrator-api`, port `127.0.0.1:8090`): local control plane and health checks.
- Prefect (`prefect-server`, port `127.0.0.1:4200`): durable workflow spine for long GPU jobs, retries, logs, and schedules.
- Prefect worker (`prefect-worker`): executes local Python flows against the existing ComfyUI, Wan2GP, AceStep, Neo4j, and Qwen endpoints.
- Atlas MCP (`atlas-mcp`, port `127.0.0.1:3010`): project/task/knowledge management for agents, backed by the existing `neo4j-world` database.
- Atlas Web UI (`atlas-webui`, port `127.0.0.1:3011`): lightweight project/task viewer.
- Postgres (`postgres`, port `127.0.0.1:5433`): Prefect metadata and future pipeline ledger tables.
- Render ledger (`render_runs` table): durable run status, service checks, GPU IDs, manifest path, and errors.

## Existing Services Used

- Qwen3.6 27B vLLM: `http://rtx0.python-bull.ts.net:8000/v1`
- Wan2GP: `http://host.docker.internal:7860`
- ComfyUI: `http://host.docker.internal:8188`
- AceStep/music: `http://host.docker.internal:8081`
- Neo4j world graph: `bolt://neo4j-world:7687` from containers, `bolt://127.0.0.1:7687` from the host.

## Start

```bash
cd /srv/nvme-data/containers/microdrama-orchestrator
cp -n .env.example .env
docker compose up -d --build
```

## Smoke Checks

```bash
curl http://127.0.0.1:8090/health
curl http://127.0.0.1:8090/services
curl http://127.0.0.1:8090/qwen27b/models
curl -X POST http://127.0.0.1:8090/qwen27b/smoke
curl http://127.0.0.1:8090/neo4j/smoke
curl http://127.0.0.1:8090/gpu/profile
curl http://127.0.0.1:8090/render-runs
```

Run the first planning flow directly:

```bash
docker compose run --rm prefect-worker python -m flows.microdrama_flow
```

Run the production render contract smoke test:

```bash
docker compose run --rm prefect-worker python -m flows.production_render_flow
```

This validates service reachability and writes a dry-run render manifest without submitting a GPU render.
It also records the run in Postgres under `render_runs`.

Validate the live Wan2GP CLI path without rendering:

```bash
docker compose run --rm prefect-worker python - <<'PY'
from flows.production_render_flow import microdrama_production_render

microdrama_production_render(
    scene_manifest_path="/projects/microdramas/scenes/smoke_scene_manifest.json",
    dry_run=False,
    wan2gp_validate_only=True,
)
PY
```

This uses Docker exec against the running `wan2gp` container and calls `wgp.py --process ... --dry-run`.

Successful dry runs, Wan2GP validation runs, and completed renders write a timestamped render manifest under
`/projects/microdramas/orchestrator_runs/<episode>/<scene>/<render_id>/` and ingest that manifest into Neo4j.
Ledger statuses ending in `_ingested` mean the `RenderRun` and linked `Asset` nodes were written to the world graph.
Before leasing a GPU, the flow preflights the scene and Wan2GP settings, resolves project-relative paths, verifies
required start keyframes and audio guides exist, and records the resolved inputs in the render manifest.

Acquire and release a local GPU lease:

```bash
curl -sS -X POST http://127.0.0.1:8090/gpu/leases/acquire \
  -H 'Content-Type: application/json' \
  -d '{"job_id":"smoke","role":"wan2gp_ltx_30s","ttl_minutes":5}' | jq .

curl -sS http://127.0.0.1:8090/gpu/leases | jq .
```

Prepare a ComfyUI workflow payload without submitting a GPU job:

```bash
docker compose run --rm prefect-worker python - <<'PY'
from flows.comfy_workflow_flow import microdrama_comfy_workflow

print(microdrama_comfy_workflow(
    manifest_path="workflows/manifests/z-image-turbo-fast-q5km.json",
    overrides={
        "prompt": "vertical cinematic character reference portrait, neutral expression",
        "seed": 12345,
        "filename_prefix": "microdrama_assets/orchestrator_probe",
    },
    dry_run=True,
))
PY
```

Live Comfy runs can also hand off their first image output into a scene manifest:

```python
microdrama_comfy_workflow(
    manifest_path="workflows/manifests/flux2-klein-9b-kv-q6-2gpu-consistency-lora-edit.json",
    overrides={...},
    dry_run=False,
    gpu_role="comfy_klein_edit",
    scene_manifest_path="/projects/microdramas/scenes/smoke_scene_manifest.json",
    keyframe_role="start",
)
```

The handoff copies the Comfy output into `/projects/microdramas/assets/generated/<scene_id>/`, updates the scene
visual fields, creates or updates a scene-specific keyframe manifest, and leaves the asset reachable by Wan2GP.

## Project Management With Atlas

Atlas is useful as the agent-facing task layer. Use it for:

- production projects, episodes, and pipeline tasks
- research tasks such as model/workflow evaluation
- knowledge notes with citations and decisions
- dependency tracking between tasks

Keep Neo4j world-sim labels (`Character`, `Scene`, `Episode`, `Asset`, `Event`) for narrative state. Keep Atlas labels (`Project`, `Task`, `Knowledge`) for work management. Link them by stable IDs in properties instead of merging the two schemas.

Atlas MCP is intentionally bound to localhost and started with `NODE_ENV=development`, so local MCP clients can connect without JWT setup. Add `MCP_AUTH_SECRET_KEY` and switch to production mode before exposing it beyond this machine.

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

Acquire and release a local GPU lease:

```bash
curl -sS -X POST http://127.0.0.1:8090/gpu/leases/acquire \
  -H 'Content-Type: application/json' \
  -d '{"job_id":"smoke","role":"wan2gp_ltx_30s","ttl_minutes":5}' | jq .

curl -sS http://127.0.0.1:8090/gpu/leases | jq .
```

## Project Management With Atlas

Atlas is useful as the agent-facing task layer. Use it for:

- production projects, episodes, and pipeline tasks
- research tasks such as model/workflow evaluation
- knowledge notes with citations and decisions
- dependency tracking between tasks

Keep Neo4j world-sim labels (`Character`, `Scene`, `Episode`, `Asset`, `Event`) for narrative state. Keep Atlas labels (`Project`, `Task`, `Knowledge`) for work management. Link them by stable IDs in properties instead of merging the two schemas.

Atlas MCP is intentionally bound to localhost and started with `NODE_ENV=development`, so local MCP clients can connect without JWT setup. Add `MCP_AUTH_SECRET_KEY` and switch to production mode before exposing it beyond this machine.

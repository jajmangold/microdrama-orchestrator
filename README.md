# Microdrama Orchestrator

Orchestration layer for micro-drama video production. Combines a FastAPI control plane, Prefect workflow engine, GPU lease management, ComfyUI artifact queuing, and Neo4j world graph integration.

## License

[MIT](LICENSE)

## Configuration

Key environment variables (set in `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `QWEN27B_BASE_URL` | `http://localhost:8000/v1` | vLLM/OpenAI-compatible planner endpoint |
| `NEO4J_URI` | `bolt://neo4j-world:7687` | Neo4j Bolt URI |
| `HOST_MICRODRAMA_PROJECTS` | `./data/projects` | Host path to project assets |
| `HOST_COMFY_OUTPUT` | `./data/comfy-output` | Host path to ComfyUI output |
| `HOST_COMFY_WORKFLOWS` | `./data/comfy-workflows` | Host path to ComfyUI workflows |
| `HOST_WAN2GP_OUTPUTS` | `./data/wan2gp-outputs` | Host path to Wan2GP outputs |

See `.env.example` for the full list.

## Services

- FastAPI (`orchestrator-api`, port `127.0.0.1:8090`): local control plane and health checks.
- Prefect (`prefect-server`, port `127.0.0.1:4200`): durable workflow spine for long GPU jobs, retries, logs, and schedules.
- Prefect worker (`prefect-worker`): executes local Python flows against the existing ComfyUI, Wan2GP, AceStep, Neo4j, and Qwen endpoints.
- Atlas MCP (`atlas-mcp`, port `127.0.0.1:3010`): project/task/knowledge management for agents, backed by the existing `neo4j-world` database.
- Atlas Web UI (`atlas-webui`, port `127.0.0.1:3011`): lightweight project/task viewer.
- Postgres (`postgres`, port `127.0.0.1:5433`): Prefect metadata and future pipeline ledger tables.
- Render ledger (`render_runs` table): durable run status, service checks, GPU IDs, manifest path, and errors.

## Existing Services Used

- Qwen3.6 27B vLLM: set `QWEN27B_BASE_URL` in `.env`
- Wan2GP: `http://host.docker.internal:7860`
- ComfyUI: `http://host.docker.internal:8188`
- AceStep/music: `http://host.docker.internal:8081`
- Neo4j world graph: `bolt://neo4j-world:7687` from containers, `bolt://127.0.0.1:7687` from the host.

## Start

```bash
cp -n .env.example .env
# Edit .env to set your host-side volume paths (HOST_MICRODRAMA_PROJECTS, HOST_COMFY_OUTPUT, etc.)
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
curl http://127.0.0.1:8090/workers
curl 'http://127.0.0.1:8090/workers?job_role=wan2gp_ltx_30s'
curl http://127.0.0.1:8090/ltx-av-queue
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

Register the production render dry-run as a named Prefect deployment:

```bash
docker compose run --rm prefect-worker bash /app/scripts/deploy_prefect_flows.sh
```

Launch and inspect that deployment:

```bash
docker compose run --rm prefect-worker prefect deployment run 'microdrama-production-render/smoke-dry-run'
docker compose run --rm prefect-worker prefect flow-run inspect <flow-run-uuid>
```

Validation on 2026-05-11 created deployment `microdrama-production-render/smoke-dry-run` and launched flow run
`aeda2816-9017-42f2-8e0c-1769dd6879cf`, which completed through the `microdrama-local` process worker.

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

The first live Comfy-keyframe-to-Wan2GP smoke render completed as
`render_scene_smoke_001_20260511T204421Z`. It produced a `576x1024`, `24 fps`, `10.042s` MP4 with audio and ended
with ledger status `render_ingested`.

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

## Comfy Artifact Queue Framework

The generic artifact queue splits a Comfy workflow at an expensive boundary so different GPU workers can process the
next stage later. Stage 1 writes a small handoff bundle, and queue workers claim `.READY` markers, patch a stage
workflow, submit it to a target Comfy endpoint, and write durable sidecars.

Generic Comfy nodes live in `ComfyUI/custom_nodes/ArtifactQueue`:

- `SaveLatentArtifactForQueue`: writes a `LATENT` object to `<prefix>.latent.safetensors`, a traceable
  `<prefix>.manifest.json`, and an atomic `<prefix>.READY` marker.
- `LoadLatentArtifactForQueue`: loads the artifact from a `.READY`, manifest, safetensors path, or relative Comfy
  output path.

The reusable runner lives in `app/artifact_queue.py`, with a CLI in `app/artifact_queue_worker.py`. A new model queue
needs a config shaped like `config/artifact_queue.example.json`, a stage workflow containing exactly one configured
loader node, and one or more Comfy worker endpoints.

Run a generic queue:

```bash
docker compose run --rm ltx-av-queue python -m app.artifact_queue_worker \
  --config /app/config/artifact_queue.example.json --status
```

Important config knobs:

- `queue_name`: stable event/client prefix for this queue.
- `queue_dir`: directory scanned for `.READY` artifacts.
- `output_root`: Comfy output root used to convert absolute paths into workflow-safe relative paths.
- `stage_workflow_path`: Comfy API workflow submitted by workers.
- `load_node_class_type` and `artifact_input_name`: identify the loader node and input to patch.
- `output_prefix_input_name`: optional output-prefix input patched on any node that has it.

The sidecar contract is intentionally simple:

- `<artifact>.CLAIMED.json`: active worker claim
- `<artifact>.DONE.json`: completed prompt id, outputs, elapsed time
- `<artifact>.FAILED.json`: failed attempt, error, and retry count

## LTX AV Latent Decode Queue

The LTX AV queue is a model-specific compatibility wrapper around the generic artifact queue. It splits Comfy
generation into two stages:

- Stage 1 samples with the LTX transformer and writes a `.READY` latent artifact through `SaveLTXAVLatentForQueue`.
- Stage 2 workers claim `.READY` artifacts, patch the stage-2 workflow, run VAE/audio decode through Comfy, and write `.DONE.json` or `.FAILED.json` sidecars.

The queue runner is configured in `config/ltx_av_queue.json` and runs as the `ltx-av-queue` service. Existing workflows
continue to use `LoadLTXAVLatentForQueue`; new model queues can use `LoadLatentArtifactForQueue`.

Start the production queue watcher:

```bash
docker compose up -d --build ltx-av-queue
```

Inspect queue state:

```bash
curl http://127.0.0.1:8090/ltx-av-queue | jq .
docker compose run --rm ltx-av-queue python -m app.ltx_av_queue_worker --status
```

Process one artifact and exit:

```bash
docker compose run --rm ltx-av-queue python -m app.ltx_av_queue_worker --once
```

Queue state is stored beside the latent artifact:

- `<artifact>.CLAIMED.json`: active worker claim
- `<artifact>.DONE.json`: completed decode result, prompt id, outputs, elapsed time
- `<artifact>.FAILED.json`: failed attempt and error

Events are appended to `/projects/microdramas/orchestrator_state/ltx_av_queue/events.jsonl`.

Optional dedicated decode workers can be started from the Comfy service directory:

```bash
docker compose -f docker-compose.yml -f docker-compose.ltx-av-workers.yml up -d ltx-av-decode-0 ltx-av-decode-1
```

Add those endpoints to `config/ltx_av_queue.json` and restart `ltx-av-queue` when the workers are online.

## Project Management With Atlas

Atlas is useful as the agent-facing task layer. Use it for:

- production projects, episodes, and pipeline tasks
- research tasks such as model/workflow evaluation
- knowledge notes with citations and decisions
- dependency tracking between tasks

Keep Neo4j world-sim labels (`Character`, `Scene`, `Episode`, `Asset`, `Event`) for narrative state. Keep Atlas labels (`Project`, `Task`, `Knowledge`) for work management. Link them by stable IDs in properties instead of merging the two schemas.

Atlas MCP is intentionally bound to localhost and started with `NODE_ENV=development`, so local MCP clients can connect without JWT setup. Add `MCP_AUTH_SECRET_KEY` and switch to production mode before exposing it beyond this machine.

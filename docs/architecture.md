# Microdrama Glue Architecture

## Default Stack

Use Prefect as the durable execution spine, LangGraph inside selected tasks for agent loops, Neo4j for world memory, Atlas MCP for project management, and Postgres for workflow metadata.

## Why This Split

- Prefect owns long-running production jobs, retries, schedules, logs, and worker placement.
- LangGraph owns stateful reasoning loops: planning, critique, human approval, continuity repair, and visual QA.
- Neo4j world graph owns persistent fictional state: characters, relationships, events, locations, secrets, arcs, and assets.
- Atlas MCP owns production management: projects, tasks, research notes, dependencies, and knowledge.
- Qwen3.6 27B vLLM is the high-context planner/critic/vision QA model.
- ComfyUI owns still image generation and editing with Z-Image and Klein.
- Wan2GP owns production LTX2.3 video/audio rendering.

## Worker Profiles

Cluster routing starts from `config/worker_profiles.json`. The file describes local and remote machines, model availability, allowed job roles, service endpoints, data roots, and rough throughput estimates.

Current profiles:

- `beast-sm70-local`: local 16-GPU SM70 render host for Wan2GP, Comfy, Qwen3 TTS through Wan2GP, and AceStep.
- `rtx0-qwen27b`: remote vLLM planner/critic/vision endpoint at `http://rtx0.python-bull.ts.net:8000/v1`.
- `remote-render-template`: template for adding more render machines.

The orchestrator exposes this inventory at:

```bash
curl http://127.0.0.1:8090/workers/profile
curl 'http://127.0.0.1:8090/workers?job_role=wan2gp_ltx_30s'
```

Use worker profiles for route planning. Use GPU leases for actual local mutual exclusion.

## Atlas Integration Rule

Do not make Atlas the canonical story database. Atlas is the work-management lens over the pipeline.

Recommended links:

- Atlas `Project.externalWorldId` -> Neo4j `Project.id` or `Series.id`
- Atlas `Task.externalEpisodeId` -> Neo4j `Episode.id`
- Atlas `Task.assetIds` -> Neo4j `Asset.id` values
- Atlas `Knowledge.citations` -> local files, model cards, benchmark summaries, or workflow URLs

This keeps agents able to plan and track work without corrupting narrative continuity data.

#!/usr/bin/env bash
set -euo pipefail

prefect deploy flows/production_render_flow.py:microdrama_production_render \
  --name smoke-dry-run \
  --pool microdrama-local \
  --work-queue default \
  --params '{"scene_manifest_path":"/projects/microdramas/scenes/smoke_scene_manifest.json","dry_run":true,"gpu_role":"wan2gp_ltx_30s","wan2gp_validate_only":false}' \
  --tag microdrama \
  --tag smoke \
  --tag dry-run \
  --no-prompt

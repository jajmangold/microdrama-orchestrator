from __future__ import annotations

import os
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx
from prefect import flow, get_run_logger, task


@dataclass
class RenderPlan:
    project_id: str
    episode_id: str
    vertical: bool = True
    target_duration_seconds: int = 30
    ltx_budget_seconds_per_30s: int = 4200
    video_engine: str = "wan2gp_ltx23"
    still_engine: str = "comfy_zimage_klein"
    planner_model: str = "qwen27b"


@task(retries=2, retry_delay_seconds=5)
def ask_planner(prompt: str) -> str:
    base_url = os.getenv("QWEN27B_BASE_URL", "http://rtx0.python-bull.ts.net:8000/v1")
    model = os.getenv("QWEN27B_MODEL", "qwen27b")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are planning a short vertical AI microdrama production run."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 600,
        "temperature": 0.2,
    }
    with httpx.Client(timeout=120) as client:
        response = client.post(f"{base_url}/chat/completions", json=payload)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


@task
def write_manifest(plan: RenderPlan, planner_notes: str) -> str:
    out_dir = Path("/projects/microdramas/orchestrator_runs") / plan.project_id / plan.episode_id
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = out_dir / "run_manifest.md"
    manifest.write_text(
        "# Microdrama Run Manifest\n\n"
        f"```json\n{json.dumps(asdict(plan), indent=2)}\n```\n\n"
        "## Planner Notes\n\n"
        f"{planner_notes}\n",
        encoding="utf-8",
    )
    return str(manifest)


@flow(name="microdrama-production-plan")
def microdrama_production_plan(project_id: str, episode_id: str, premise: str) -> str:
    logger = get_run_logger()
    plan = RenderPlan(project_id=project_id, episode_id=episode_id)
    planner_notes = ask_planner(
        "Create a concrete shot/audio/asset plan for this 30 second vertical microdrama. "
        "Use Z-Image/Klein for keyframes and Wan2GP LTX for final video. "
        f"Premise: {premise}"
    )
    manifest_path = write_manifest(plan, planner_notes)
    logger.info("Wrote manifest to %s", manifest_path)
    return manifest_path


if __name__ == "__main__":
    microdrama_production_plan(
        project_id="smoke",
        episode_id="episode_001",
        premise="A tense close-up confrontation between two recurring characters.",
    )

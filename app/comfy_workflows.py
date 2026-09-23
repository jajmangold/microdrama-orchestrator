from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

PROJECT_ROOT = Path("/projects/microdramas")
COMFY_OUTPUT_ROOT = Path(os.getenv("COMFY_OUTPUT_ROOT", "/comfy/output"))
HOST_COMFY_OUTPUT_ROOT = Path(
    os.getenv("HOST_COMFY_OUTPUT", "/srv/nvme-data/containers/comfy/storage-user/output")
)


@dataclass
class ComfyPreparedWorkflow:
    workflow_id: str
    manifest_path: str
    api_workflow_path: str
    prompt: dict[str, Any]


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: str | Path, data: dict[str, Any]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
    return str(target)


def project_path(path: str | Path) -> Path:
    raw = Path(path)
    if raw.is_absolute():
        return raw
    return PROJECT_ROOT / raw


def load_workflow_manifest(manifest_path: str | Path) -> dict[str, Any]:
    manifest = read_json(project_path(manifest_path))
    api_path = project_path(manifest["api_workflow_path"])
    if not api_path.exists():
        raise FileNotFoundError(f"Comfy API workflow not found: {api_path}")
    manifest["_resolved_manifest_path"] = str(project_path(manifest_path))
    manifest["_resolved_api_workflow_path"] = str(api_path)
    return manifest


def prepare_workflow(manifest_path: str | Path, overrides: dict[str, Any] | None = None) -> ComfyPreparedWorkflow:
    manifest = load_workflow_manifest(manifest_path)
    prompt = read_json(manifest["_resolved_api_workflow_path"])
    override_values = overrides or {}
    input_specs = {item["name"]: item for item in manifest.get("inputs", [])}

    unknown = sorted(set(override_values) - set(input_specs))
    if unknown:
        raise ValueError(f"Unknown Comfy workflow override(s) for {manifest['workflow_id']}: {unknown}")

    for name, value in override_values.items():
        spec = input_specs[name]
        node_id = str(spec["node_id"])
        field = spec["field"]
        if node_id not in prompt:
            raise KeyError(f"Workflow node {node_id} not found for input {name}")
        prompt[node_id].setdefault("inputs", {})[field] = value

    return ComfyPreparedWorkflow(
        workflow_id=manifest["workflow_id"],
        manifest_path=manifest["_resolved_manifest_path"],
        api_workflow_path=manifest["_resolved_api_workflow_path"],
        prompt=prompt,
    )


def queue_prompt(base_url: str, prompt: dict[str, Any], client_id: str | None = None) -> dict[str, Any]:
    payload = {"prompt": prompt, "client_id": client_id or str(uuid.uuid4())}
    with httpx.Client(timeout=30) as client:
        response = client.post(f"{base_url.rstrip('/')}/prompt", json=payload)
        response.raise_for_status()
        return response.json()


def get_history(base_url: str, prompt_id: str) -> dict[str, Any]:
    with httpx.Client(timeout=30) as client:
        response = client.get(f"{base_url.rstrip('/')}/history/{prompt_id}")
        response.raise_for_status()
        return response.json()


def wait_for_outputs(base_url: str, prompt_id: str, timeout_seconds: int = 900, poll_seconds: float = 2.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        history = get_history(base_url, prompt_id)
        if prompt_id in history:
            return history[prompt_id]
        time.sleep(poll_seconds)
    raise TimeoutError(f"Timed out waiting for Comfy prompt {prompt_id}")


def collect_output_images(history_item: dict[str, Any]) -> list[dict[str, Any]]:
    images: list[dict[str, Any]] = []
    for node_id, node_output in history_item.get("outputs", {}).items():
        for image in node_output.get("images", []):
            filename = image.get("filename", "")
            subfolder = image.get("subfolder", "")
            relative = Path(subfolder) / filename if subfolder else Path(filename)
            images.append(
                {
                    "node_id": node_id,
                    "filename": filename,
                    "subfolder": subfolder,
                    "type": image.get("type", ""),
                    "container_path": str(COMFY_OUTPUT_ROOT / relative),
                    "host_path": str(HOST_COMFY_OUTPUT_ROOT / relative),
                }
            )
    return images

from __future__ import annotations

import json
import shutil
from copy import deepcopy
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps

PROJECT_ROOT = Path("/projects/microdramas")
KEYFRAME_TEMPLATE = PROJECT_ROOT / "keyframes/keyframe_manifest_template.json"


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


def project_relative(path: str | Path) -> str:
    raw = Path(path)
    try:
        return str(raw.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(raw)


def first_comfy_image(result: dict[str, Any]) -> dict[str, Any]:
    outputs = result.get("comfy", {}).get("outputs", [])
    if not outputs:
        raise ValueError("Comfy result has no image outputs")
    return outputs[0]


def normalize_image_to_resolution(source: Path, width: int, height: int) -> Path:
    if not width or not height:
        return source
    suffix = f"_{width}x{height}"
    target = source.with_name(f"{source.stem}{suffix}{source.suffix}")
    with Image.open(source) as image:
        image = image.convert("RGB")
        if image.size == (width, height):
            if source != target:
                shutil.copy2(source, target)
            return target
        normalized = ImageOps.fit(image, (width, height), method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
        normalized.save(target)
    return target


def copy_comfy_output_to_project(output: dict[str, Any], scene: dict[str, Any]) -> dict[str, str]:
    source = Path(output.get("container_path") or "")
    if not source.exists():
        source = Path(output.get("host_path") or "")
    if not source.exists():
        raise FileNotFoundError(f"Comfy output file not found: {output}")

    target_dir = PROJECT_ROOT / "assets/generated" / scene["scene_id"]
    target = target_dir / source.name
    target_dir.mkdir(parents=True, exist_ok=True)
    if source.resolve() != target.resolve():
        shutil.copy2(source, target)

    resolution = scene.get("target_resolution", {})
    target = normalize_image_to_resolution(
        target,
        int(resolution.get("width") or 0),
        int(resolution.get("height") or 0),
    )
    return {
        "source_path": str(source),
        "project_path": str(target),
        "project_relative_path": project_relative(target),
    }


def ensure_keyframe_manifest(scene: dict[str, Any], scene_manifest_path: str, keyframe_manifest_path: str | None) -> Path:
    requested = keyframe_manifest_path or scene.get("visuals", {}).get("keyframe_manifest", "")
    requested_path = project_path(requested) if requested else Path("")
    template_path = KEYFRAME_TEMPLATE

    if requested_path and requested_path.exists() and requested_path.resolve() != template_path.resolve():
        return requested_path

    target = PROJECT_ROOT / "keyframes/generated" / scene["scene_id"] / "keyframes.json"
    if not target.exists():
        manifest = deepcopy(read_json(template_path))
        manifest.update(
            {
                "keyframe_manifest_id": f"keyframes_{scene['scene_id']}",
                "scene_id": scene["scene_id"],
                "episode_id": scene["episode_id"],
                "atlas_task_id": scene.get("atlas_task_id", ""),
            }
        )
        manifest["location"]["location_id"] = scene.get("location_id", "")
        manifest["location"]["location_plate_path"] = scene.get("visuals", {}).get("location_plate_path", "")
        manifest["characters"] = [
            {
                "character_id": character.get("character_id", ""),
                "character_bible_path": character.get("character_bible_path", ""),
                "visual_reference_asset_ids": [],
                "visual_reference_paths": [],
            }
            for character in scene.get("characters", [])
        ]
        write_json(target, manifest)
    return target


def upsert_keyframe(manifest: dict[str, Any], keyframe: dict[str, Any]) -> dict[str, Any]:
    keyframes = manifest.setdefault("keyframes", [])
    for index, existing in enumerate(keyframes):
        if existing.get("role") == keyframe["role"]:
            merged = {**existing, **keyframe}
            keyframes[index] = merged
            return merged
    keyframes.append(keyframe)
    return keyframe


def register_comfy_keyframe(
    *,
    comfy_result_path: str,
    scene_manifest_path: str,
    role: str = "start",
    keyframe_manifest_path: str | None = None,
) -> dict[str, Any]:
    scene_path = project_path(scene_manifest_path)
    scene = read_json(scene_path)
    result = read_json(comfy_result_path)
    output = first_comfy_image(result)
    copied = copy_comfy_output_to_project(output, scene)

    asset_id = f"asset_{scene['scene_id']}_{role}_keyframe"
    graph_asset_id = asset_id
    keyframe_manifest = ensure_keyframe_manifest(scene, str(scene_path), keyframe_manifest_path)
    keyframes = read_json(keyframe_manifest)
    keyframe = upsert_keyframe(
        keyframes,
        {
            "keyframe_id": f"kf_{role}",
            "role": role,
            "asset_id": asset_id,
            "path": copied["project_relative_path"],
            "workflow_id": result.get("workflow_id", ""),
            "prompt": "",
            "negative_prompt": "",
            "seed": None,
            "model_settings": {
                "manifest_path": result.get("manifest_path", ""),
                "payload_path": result.get("payload_path", ""),
                "prompt_id": result.get("comfy", {}).get("prompt_id", ""),
            },
            "graph_asset_id": graph_asset_id,
            "continuity_notes": [
                f"Imported from Comfy output {copied['source_path']}",
            ],
        },
    )
    write_json(keyframe_manifest, keyframes)

    scene.setdefault("visuals", {})
    scene["visuals"]["keyframe_manifest"] = project_relative(keyframe_manifest)
    if role == "start":
        scene["visuals"]["start_keyframe_asset_id"] = asset_id
        scene["visuals"]["start_keyframe_path"] = copied["project_relative_path"]
    elif role == "end":
        scene["visuals"]["end_keyframe_asset_id"] = asset_id
        scene["visuals"]["end_keyframe_path"] = copied["project_relative_path"]
    elif role == "location_plate":
        scene["visuals"]["location_plate_asset_id"] = asset_id
        scene["visuals"]["location_plate_path"] = copied["project_relative_path"]
    else:
        scene["visuals"].setdefault("continuity_notes", []).append(
            f"{role} keyframe imported at {copied['project_relative_path']}"
        )
    write_json(scene_path, scene)

    return {
        "scene_manifest_path": str(scene_path),
        "keyframe_manifest_path": str(keyframe_manifest),
        "role": role,
        "asset_id": asset_id,
        "keyframe": keyframe,
        **copied,
    }

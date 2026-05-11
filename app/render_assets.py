from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path("/projects/microdramas")
WAN2GP_PROJECT_ROOT = Path("/workspace/projects/microdramas")
HOST_PROJECT_ROOT = Path("/srv/nvme-data/containers/projects/microdramas")


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def project_path(path: str | Path | None) -> str:
    if not path:
        return ""
    raw = Path(str(path))
    raw_text = str(raw)
    for root in (WAN2GP_PROJECT_ROOT, HOST_PROJECT_ROOT):
        try:
            return str(PROJECT_ROOT / raw.relative_to(root))
        except ValueError:
            continue
    if raw.is_absolute():
        return raw_text
    return str(PROJECT_ROOT / raw)


def _first_present(*values: str | None) -> str:
    for value in values:
        if value:
            return value
    return ""


def _missing_required(path: str, label: str, errors: list[str]) -> None:
    if not path:
        errors.append(f"Missing required asset path: {label}")
        return
    if not Path(path).exists():
        errors.append(f"Required asset does not exist: {label}={path}")


def preflight_scene_assets(scene: dict[str, Any]) -> dict[str, Any]:
    video_generation = scene.get("video_generation", {})
    visuals = scene.get("visuals", {})
    dialogue = scene.get("dialogue", {})

    settings_json = project_path(video_generation.get("settings_json"))
    errors: list[str] = []
    settings: dict[str, Any] = {}
    if not settings_json:
        errors.append("Missing required scene.video_generation.settings_json")
    elif Path(settings_json).exists():
        settings = read_json(settings_json)
    else:
        errors.append(f"Wan2GP settings JSON does not exist: {settings_json}")

    start_keyframe = project_path(_first_present(visuals.get("start_keyframe_path"), settings.get("image_start")))
    end_keyframe = project_path(_first_present(visuals.get("end_keyframe_path"), settings.get("image_end")))
    audio_guide = project_path(
        _first_present(
            video_generation.get("audio_guide_path"),
            dialogue.get("dialogue_wav_path"),
            settings.get("audio_guide"),
        )
    )

    if settings.get("image_prompt_type") == "S":
        _missing_required(start_keyframe, "start_keyframe", errors)
    if settings.get("image_end"):
        _missing_required(end_keyframe, "end_keyframe", errors)
    if settings.get("audio_prompt_type") == "A":
        _missing_required(audio_guide, "audio_guide", errors)

    if errors:
        raise ValueError("Scene asset preflight failed: " + "; ".join(errors))

    return {
        "settings_json": settings_json,
        "start_keyframe_path": start_keyframe,
        "end_keyframe_path": end_keyframe,
        "audio_guide_path": audio_guide,
        "settings": {
            "model_type": settings.get("model_type", ""),
            "resolution": settings.get("resolution", ""),
            "video_length": settings.get("video_length"),
            "force_fps": settings.get("force_fps", ""),
            "num_inference_steps": settings.get("num_inference_steps"),
            "image_prompt_type": settings.get("image_prompt_type", ""),
            "audio_prompt_type": settings.get("audio_prompt_type", ""),
            "sliding_window_size": settings.get("sliding_window_size"),
            "sliding_window_overlap": settings.get("sliding_window_overlap"),
        },
    }

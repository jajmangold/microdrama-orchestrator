from __future__ import annotations

import json
import mimetypes
import os
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase

PROJECT_ROOT = Path(os.getenv("MICRODRAMA_PROJECT_ROOT", "/projects/microdramas"))


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        return json.load(handle)


def asset_type(path: str, fallback: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    if mime:
        if mime.startswith("image/"):
            return "image"
        if mime.startswith("audio/"):
            return "audio"
        if mime.startswith("video/"):
            return "video"
    return fallback


def abs_path(path: str) -> str:
    if not path:
        return ""
    expanded = Path(path).expanduser()
    if not expanded.is_absolute():
        expanded = PROJECT_ROOT / expanded
    return str(expanded.absolute())


def collect_assets(render: dict[str, Any]) -> list[dict[str, Any]]:
    render_id = render["render_id"]
    inputs = render.get("inputs", {})
    outputs = render.get("outputs", {})
    source_manifests = render.get("source_manifests", {})
    assets: list[dict[str, Any]] = []

    candidates = [
        ("start_keyframe", inputs.get("start_keyframe_path"), "image"),
        ("end_keyframe", inputs.get("end_keyframe_path"), "image"),
        ("audio_guide", inputs.get("audio_guide_path"), "audio"),
        ("video", outputs.get("video_path"), "video"),
        ("contact_sheet", outputs.get("contact_sheet_path"), "image"),
        ("final_mix", outputs.get("final_mix_path"), "audio"),
        ("final_muxed", outputs.get("final_muxed_path"), "video"),
        ("scene_manifest", source_manifests.get("scene_manifest"), "manifest"),
        ("keyframe_manifest", source_manifests.get("keyframe_manifest"), "manifest"),
        ("dialogue_manifest", source_manifests.get("dialogue_manifest"), "manifest"),
        ("settings_json", render.get("settings_json"), "manifest"),
    ]
    for role, path, fallback_type in candidates:
        if not path:
            continue
        explicit_id = outputs.get("video_asset_id") if role == "video" else ""
        assets.append(
            {
                "id": explicit_id or f"asset_{render_id}_{role}",
                "role": role,
                "path": abs_path(path),
                "type": asset_type(path, fallback_type),
                "render_id": render_id,
                "scene_id": render["scene_id"],
                "episode_id": render["episode_id"],
                "model": render.get("model", ""),
                "engine": render.get("engine", ""),
                "preset_id": render.get("preset_id", ""),
            }
        )
    return assets


def _ingest_manifest(tx: Any, render: dict[str, Any], scene: dict[str, Any] | None, manifest_path: str) -> None:
    render_id = render["render_id"]
    episode_id = render["episode_id"]
    scene_id = render["scene_id"]
    runtime = render.get("runtime", {})
    review = render.get("review", {})

    tx.run(
        """
        MERGE (e:Episode {id: $episode_id})
        ON CREATE SET e.created_at = datetime()
        SET e.updated_at = datetime()
        MERGE (s:Scene {id: $scene_id})
        ON CREATE SET s.created_at = datetime()
        SET s.episode_id = $episode_id,
            s.title = $title,
            s.summary = $summary,
            s.world_time = $world_time,
            s.updated_at = datetime()
        MERGE (s)-[:PART_OF]->(e)
        MERGE (rr:RenderRun {id: $render_id})
        ON CREATE SET rr.created_at = datetime()
        SET rr += $render_props,
            rr.updated_at = datetime()
        MERGE (rr)-[:RENDERS]->(s)
        """,
        episode_id=episode_id,
        scene_id=scene_id,
        title=(scene or {}).get("title", ""),
        summary=(scene or {}).get("summary", ""),
        world_time=(scene or {}).get("world_time", ""),
        render_id=render_id,
        render_props={
            "manifest_path": abs_path(manifest_path),
            "atlas_task_id": render.get("atlas_task_id", ""),
            "engine": render.get("engine", ""),
            "model": render.get("model", ""),
            "preset_id": render.get("preset_id", ""),
            "settings_json": render.get("settings_json", ""),
            "host": runtime.get("host", ""),
            "gpu_ids": runtime.get("gpu_ids", []),
            "started_at": runtime.get("started_at", ""),
            "finished_at": runtime.get("finished_at", ""),
            "duration_seconds": runtime.get("duration_seconds"),
            "usable": review.get("usable"),
            "review_notes": review.get("notes", ""),
        },
    )

    for character in (scene or {}).get("characters", []):
        tx.run(
            """
            MERGE (c:Character {id: $character_id})
            ON CREATE SET c.created_at = datetime()
            SET c.updated_at = datetime()
            WITH c
            MATCH (s:Scene {id: $scene_id})
            MERGE (c)-[r:APPEARS_IN]->(s)
            SET r.role = $role,
                r.updated_at = datetime()
            """,
            character_id=character["character_id"],
            scene_id=scene_id,
            role=character.get("role", "actor"),
        )

        if character.get("voice_id"):
            tx.run(
                """
                MATCH (c:Character {id: $character_id})
                MERGE (v:Voice {id: $voice_id})
                ON CREATE SET v.created_at = datetime()
                SET v.updated_at = datetime()
                MERGE (c)-[r:HAS_VOICE {role: 'canonical'}]->(v)
                SET r.updated_at = datetime()
                """,
                character_id=character["character_id"],
                voice_id=character["voice_id"],
            )

        if character.get("visual_identity_id"):
            tx.run(
                """
                MATCH (c:Character {id: $character_id})
                MERGE (vi:VisualIdentity {id: $visual_identity_id})
                ON CREATE SET vi.created_at = datetime()
                SET vi.updated_at = datetime()
                MERGE (c)-[:HAS_VISUAL_IDENTITY]->(vi)
                """,
                character_id=character["character_id"],
                visual_identity_id=character["visual_identity_id"],
            )

    for asset in collect_assets(render):
        tx.run(
            """
            MERGE (a:Asset {id: $asset_id})
            ON CREATE SET a.created_at = datetime()
            SET a += $props,
                a.updated_at = datetime()
            WITH a
            MATCH (s:Scene {id: $scene_id})
            MATCH (rr:RenderRun {id: $render_id})
            MERGE (s)-[uses:USES_ASSET {role: $role}]->(a)
            SET uses.updated_at = datetime()
            MERGE (rr)-[produced:PRODUCED_ASSET {role: $role}]->(a)
            SET produced.updated_at = datetime()
            """,
            asset_id=asset["id"],
            props=asset,
            scene_id=scene_id,
            render_id=render_id,
            role=asset["role"],
        )


def ingest_render_manifest(
    render_manifest_path: str,
    scene_manifest_path: str | None = None,
    uri: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    render = read_json(render_manifest_path)
    scene_path = scene_manifest_path or render.get("source_manifests", {}).get("scene_manifest", "")
    scene = read_json(scene_path) if scene_path and Path(scene_path).expanduser().exists() else None

    driver = GraphDatabase.driver(
        uri or os.getenv("NEO4J_URI", "bolt://neo4j-world:7687"),
        auth=(user or os.getenv("NEO4J_USER", "neo4j"), password or os.getenv("NEO4J_PASSWORD", "microdrama-local")),
    )
    with driver:
        with driver.session() as session:
            session.execute_write(_ingest_manifest, render, scene, render_manifest_path)
    return {
        "ok": True,
        "render_id": render["render_id"],
        "scene_id": render["scene_id"],
        "episode_id": render["episode_id"],
        "asset_count": len(collect_assets(render)),
    }

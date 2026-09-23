from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_LIBRARY = Path("/projects/microdramas/wan21_lora/prompt_rag/wan21_13b_prompt_examples.md")


@dataclass
class PromptExample:
    example_id: str
    tags: list[str]
    positive: str
    negative: str
    notes: str


def _library_path() -> Path:
    return Path(os.getenv("WAN_PROMPT_EXAMPLE_LIBRARY", str(DEFAULT_LIBRARY)))


def _field(block: str, name: str) -> str:
    single_line = re.search(rf"^{name}:\s*(.+)$", block, flags=re.MULTILINE | re.IGNORECASE)
    if single_line:
        return single_line.group(1).strip()
    pattern = rf"{name}:\n(.*?)(?=\n[a-z_]+:\n|\Z)"
    match = re.search(pattern, block, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def load_prompt_examples(path: Path | None = None) -> list[PromptExample]:
    source = path or _library_path()
    if not source.exists():
        return []
    text = source.read_text(encoding="utf-8")
    examples: list[PromptExample] = []
    for part in re.split(r"\n---+\n", text):
        header = re.search(r"^## example:\s*([a-zA-Z0-9_.-]+)", part, flags=re.MULTILINE)
        if not header:
            continue
        tags = [
            tag.strip().lower()
            for tag in _field(part, "tags").replace("\n", " ").split(",")
            if tag.strip()
        ]
        examples.append(
            PromptExample(
                example_id=header.group(1).strip(),
                tags=tags,
                positive=" ".join(_field(part, "positive").split()),
                negative=" ".join(_field(part, "negative").split()),
                notes=" ".join(_field(part, "notes").split()),
            )
        )
    return examples


def _tokens(value: str) -> set[str]:
    stop = {
        "the",
        "and",
        "with",
        "this",
        "that",
        "image",
        "prompt",
        "adult",
        "woman",
        "wearing",
        "visible",
        "plain",
        "background",
        "reference",
    }
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9]+", value.lower().replace("-", " "))
        if len(token) >= 3 and token not in stop
    }


def retrieve_prompt_examples(query: str, limit: int = 5, examples: list[PromptExample] | None = None) -> list[PromptExample]:
    candidates = examples if examples is not None else load_prompt_examples()
    query_tokens = _tokens(query)
    scored: list[tuple[int, PromptExample]] = []
    for example in candidates:
        haystack = " ".join([example.example_id, " ".join(example.tags), example.positive, example.negative, example.notes])
        example_tokens = _tokens(haystack)
        score = len(query_tokens & example_tokens)
        for tag in example.tags:
            if tag in query.lower():
                score += 4
        if example.example_id in query.lower():
            score += 6
        if score > 0:
            scored.append((score, example))
    scored.sort(key=lambda item: (-item[0], item[1].example_id))
    return [example for _, example in scored[:limit]]


def format_examples_for_prompt(examples: list[PromptExample]) -> str:
    if not examples:
        return "No prompt examples found."
    blocks = []
    for example in examples:
        blocks.append(
            "\n".join(
                [
                    f"Example ID: {example.example_id}",
                    f"Tags: {', '.join(example.tags)}",
                    f"Positive: {example.positive}",
                    f"Negative: {example.negative}",
                    f"Notes: {example.notes}",
                ]
            )
        )
    return "\n\n".join(blocks)


def retrieve_examples_for_repair(request: Any, failure_summary: str, limit: int = 5) -> list[PromptExample]:
    failed_keys = " ".join(str(check.get("key", "")) for check in getattr(request, "failed_checks", []))
    failed_text = " ".join(str(check.get("explanation", "")) for check in getattr(request, "failed_checks", []))
    settings = getattr(request, "generation_settings", {}) or {}
    query = "\n".join(
        [
            str(getattr(request, "asset_type", "")),
            str(getattr(request, "original_prompt", "")),
            str(getattr(request, "original_negative_prompt", "")),
            failed_keys,
            failed_text,
            failure_summary,
            " ".join(str(value) for value in settings.values()),
        ]
    )
    return retrieve_prompt_examples(query, limit=limit)

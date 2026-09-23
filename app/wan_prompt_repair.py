from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal, TypedDict

import httpx
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from app.wan_prompt_rag import format_examples_for_prompt, retrieve_examples_for_repair


WAN_GUIDELINES_PATH = Path("/projects/microdramas/wan21_lora/docs/wan21_13b_prompting_guidelines.md")


class PromptRepairRequest(BaseModel):
    asset_type: str = "clothing_lora_training_plate"
    original_prompt: str
    original_negative_prompt: str = ""
    failed_checks: list[dict[str, Any]] = Field(default_factory=list)
    generation_settings: dict[str, Any] = Field(default_factory=dict)
    attempt: int = 1
    max_attempts: int = 3


class PromptRepairResponse(BaseModel):
    action: Literal["regenerate", "human_review", "accept"] = "regenerate"
    repaired_prompt: str
    repaired_negative_prompt: str
    setting_tweaks: dict[str, Any] = Field(default_factory=dict)
    target_fixes: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    rationale: str = ""
    raw_model_output: dict[str, Any] = Field(default_factory=dict)


class RepairState(TypedDict, total=False):
    request: PromptRepairRequest
    failure_summary: str
    wan_guidelines: str
    retrieved_examples: str
    model_output: dict[str, Any]
    validation_warnings: list[str]
    response: PromptRepairResponse


def _qwen_base_url() -> str:
    return os.getenv("QWEN27B_BASE_URL", "http://localhost:8000/v1").rstrip("/")


def _qwen_model() -> str:
    return os.getenv("QWEN27B_MODEL", "qwen27b")


def _read_wan_guidelines() -> str:
    if WAN_GUIDELINES_PATH.exists():
        return WAN_GUIDELINES_PATH.read_text(encoding="utf-8")[:5000]
    return (
        "Use compact Wan2.1 1.3B clothing prompts: full-body head-to-toe catalog outfit reference, "
        "relaxed A-pose, hands visible outside pockets, feet visible, plain gray background, exact outfit, "
        "realistic fabric texture, clean garment boundaries."
    )


def _summarize_failures(state: RepairState) -> RepairState:
    request = state["request"]
    if not request.failed_checks:
        summary = "No failed checks supplied."
    else:
        parts = []
        for check in request.failed_checks:
            key = check.get("key", "unknown")
            explanation = check.get("explanation") or check.get("reason") or ""
            parts.append(f"{key}: {explanation}")
        summary = "\n".join(parts)
    return {
        **state,
        "failure_summary": summary,
        "wan_guidelines": _read_wan_guidelines(),
        "retrieved_examples": format_examples_for_prompt(retrieve_examples_for_repair(request, summary, limit=6)),
    }


def _repair_with_qwen(state: RepairState) -> RepairState:
    request = state["request"]
    system = (
        "You are a production prompt repair agent for Wan2.1 1.3B / Wan VACE 1.3B dataset generation. "
        "You repair prompts after visual QA failures. Return JSON only. Keep prompts compact and concrete. "
        "Do not add cinematic motion, narrative action, scene complexity, or unrequested accessories for still clothing plates."
    )
    user_payload = {
        "asset_type": request.asset_type,
        "attempt": request.attempt,
        "max_attempts": request.max_attempts,
        "original_prompt": request.original_prompt,
        "original_negative_prompt": request.original_negative_prompt,
        "generation_settings": request.generation_settings,
        "failed_checks": request.failed_checks,
        "failure_summary": state["failure_summary"],
        "wan_guidelines_excerpt": state["wan_guidelines"],
        "retrieved_prompt_examples": state["retrieved_examples"],
        "required_json_shape": {
            "action": "regenerate|human_review|accept",
            "repaired_prompt": "string",
            "repaired_negative_prompt": "string",
            "setting_tweaks": {},
            "target_fixes": ["failed check names this repair addresses"],
            "rationale": "brief explanation",
        },
        "rules": [
            "Use the retrieved prompt examples as few-shot guidance for structure, specificity, and narrow negative prompts.",
            "Do not copy an example blindly; adapt it to the actual outfit, failed checks, and asset type.",
            "Preserve the requested outfit unless the QA says it was wrong.",
            "Do not place a desired garment name in the negative prompt.",
            "If a garment was confused, make the positive prompt more concrete and add only the incorrect alternative to the negative prompt.",
            "Never put broad garment category words like jacket, blazer, trousers, skirt, dress, blouse, coat, or shoes in the negative prompt if the repaired prompt still needs that category.",
            "When blocking an incorrect garment, use a narrow phrase such as single-breasted closure, skirt instead of trousers, cartoon eyes, or hands in pockets.",
            "If hands failed but this is clothing_lora_training_plate, prefer hands visible outside pockets but do not over-focus on fingers.",
            "If face/eyes failed for a realistic dataset, add natural realistic human face/eyes and negative cartoon/surreal eyes.",
            "Prefer stable 480x832, image_mode=1, unipc, 8 steps unless settings already differ for a reason.",
        ],
    }
    payload = {
        "model": _qwen_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_payload, indent=2)},
        ],
        "temperature": 0.2,
        "top_p": 0.8,
        "max_tokens": 1000,
        "response_format": {"type": "json_object"},
        "chat_template_kwargs": {"enable_thinking": False},
    }
    with httpx.Client(timeout=180) as client:
        response = client.post(f"{_qwen_base_url()}/chat/completions", json=payload)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
    try:
        model_output = json.loads(content)
    except json.JSONDecodeError:
        model_output = {"action": "human_review", "raw_content": content}
    return {**state, "model_output": model_output}


def _validate_repair(state: RepairState) -> RepairState:
    request = state["request"]
    output = state["model_output"]
    repaired_prompt = str(output.get("repaired_prompt") or request.original_prompt)
    repaired_negative = str(output.get("repaired_negative_prompt") or request.original_negative_prompt)
    warnings: list[str] = []

    negative_lower = repaired_negative.lower().replace("-", " ")
    garment_terms = [
        "blazer",
        "jacket",
        "trousers",
        "pants",
        "skirt",
        "dress",
        "cardigan",
        "coat",
        "blouse",
        "turtleneck",
        "sweater",
        "heels",
        "loafers",
        "boots",
    ]
    garment_overlap = [term for term in garment_terms if term in repaired_prompt.lower() and term in negative_lower]
    if garment_overlap:
        for term in garment_overlap:
            repaired_negative = repaired_negative.replace(term, "").replace(term.title(), "")
        repaired_negative = " ".join(repaired_negative.replace(" ,", ",").split())

    required_prompt_phrases = [
        "full-body head-to-toe catalog outfit reference",
        "hands visible outside pockets",
        "feet visible",
        "plain light gray studio background",
        "clean garment boundaries",
        "photorealistic still image",
    ]
    for phrase in required_prompt_phrases:
        if phrase not in repaired_prompt.lower():
            if phrase == "hands visible outside pockets" and "hands fully visible" in repaired_prompt.lower():
                repaired_prompt = repaired_prompt.replace("hands fully visible and open", "hands visible outside pockets")
            elif phrase == "feet visible" and "feet fully visible" in repaired_prompt.lower():
                repaired_prompt = repaired_prompt.replace("feet fully visible", "feet visible")
            elif phrase not in repaired_prompt.lower():
                repaired_prompt = repaired_prompt.rstrip(" .") + f", {phrase}"

    if request.attempt >= request.max_attempts and output.get("action", "regenerate") == "regenerate":
        warnings.append("Max attempts reached; route next failure to human_review.")

    response = PromptRepairResponse(
        action=output.get("action", "regenerate"),
        repaired_prompt=repaired_prompt,
        repaired_negative_prompt=repaired_negative,
        setting_tweaks=output.get("setting_tweaks") or {},
        target_fixes=output.get("target_fixes") or [check.get("key", "unknown") for check in request.failed_checks],
        validation_warnings=warnings,
        rationale=output.get("rationale") or output.get("repair_reasoning") or "",
        raw_model_output=output,
    )
    return {**state, "validation_warnings": warnings, "response": response}


def build_wan_prompt_repair_graph():
    graph = StateGraph(RepairState)
    graph.add_node("summarize_failures", _summarize_failures)
    graph.add_node("repair_with_qwen", _repair_with_qwen)
    graph.add_node("validate_repair", _validate_repair)
    graph.set_entry_point("summarize_failures")
    graph.add_edge("summarize_failures", "repair_with_qwen")
    graph.add_edge("repair_with_qwen", "validate_repair")
    graph.add_edge("validate_repair", END)
    return graph.compile()


def repair_wan_prompt(request: PromptRepairRequest) -> PromptRepairResponse:
    graph = build_wan_prompt_repair_graph()
    state = graph.invoke({"request": request})
    return state["response"]

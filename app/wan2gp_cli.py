from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import docker


PROJECT_HOST_ROOT = Path("/projects/microdramas")
PROJECT_WAN2GP_ROOT = Path("/workspace/projects/microdramas")


@dataclass
class Wan2GPCommandResult:
    exit_code: int
    output: str
    generated_files: list[str]

    @property
    def ok(self) -> bool:
        return self.exit_code == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "exit_code": self.exit_code,
            "ok": self.ok,
            "output": self.output,
            "generated_files": self.generated_files,
        }


def to_wan2gp_path(path: str | Path) -> str:
    raw = Path(path)
    if not raw.is_absolute():
        raw = PROJECT_HOST_ROOT / raw
    resolved = raw.resolve()
    try:
        relative = resolved.relative_to(PROJECT_HOST_ROOT)
        return str(PROJECT_WAN2GP_ROOT / relative)
    except ValueError:
        return str(resolved)


def from_wan2gp_path(path: str | Path) -> str:
    raw = Path(path)
    if not raw.is_absolute():
        raw = Path("/workspace") / raw
    try:
        relative = raw.resolve().relative_to(PROJECT_WAN2GP_ROOT)
        return str(PROJECT_HOST_ROOT / relative)
    except ValueError:
        return str(raw)


def parse_generated_files(output: str) -> list[str]:
    files: list[str] = []
    patterns = [
        r"New video saved to Path:\s*(?P<path>.+)",
        r"New image saved to Path:\s*(?P<path>.+)",
        r"New audio saved to Path:\s*(?P<path>.+)",
    ]
    for line in output.splitlines():
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                files.append(from_wan2gp_path(match.group("path").strip()))
    return files


def run_wan2gp_process(
    *,
    settings_path: str | Path,
    output_dir: str | Path | None = None,
    gpu_ids: list[int] | None = None,
    cuda_visible_devices: str | None = None,
    container_name: str = "wan2gp",
    dry_run: bool = False,
    timeout: int | None = None,
) -> Wan2GPCommandResult:
    client = docker.from_env()
    container = client.containers.get(container_name)

    command = ["cd /workspace", f"python3 wgp.py --process {to_wan2gp_path(settings_path)}"]
    if output_dir:
        command[-1] += f" --output-dir {to_wan2gp_path(output_dir)}"
    if dry_run:
        command[-1] += " --dry-run"
    shell_command = " && ".join(command)

    environment = {"PYTORCH_ALLOC_CONF": "expandable_segments:True"}
    if cuda_visible_devices:
        environment["CUDA_VISIBLE_DEVICES"] = cuda_visible_devices
    elif gpu_ids:
        environment["CUDA_VISIBLE_DEVICES"] = ",".join(str(gpu_id) for gpu_id in gpu_ids)
    result = container.exec_run(["bash", "-lc", shell_command], stream=False, demux=False, environment=environment)
    output_bytes = result.output or b""
    output = output_bytes.decode("utf-8", errors="replace") if isinstance(output_bytes, bytes) else str(output_bytes)
    return Wan2GPCommandResult(
        exit_code=int(result.exit_code or 0),
        output=output,
        generated_files=parse_generated_files(output),
    )

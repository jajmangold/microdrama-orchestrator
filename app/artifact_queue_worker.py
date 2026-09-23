from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from app.artifact_queue import load_config, queue_summary, run_forever, run_once

DEFAULT_CONFIG_PATH = Path(os.getenv("ARTIFACT_QUEUE_CONFIG", "/app/config/artifact_queue.json"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a generic Comfy artifact queue worker.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to artifact queue config JSON.")
    parser.add_argument("--once", action="store_true", help="Process one artifact and exit.")
    parser.add_argument("--status", action="store_true", help="Print queue status and exit.")
    args = parser.parse_args()

    config = load_config(args.config)
    if args.status:
        print(json.dumps(queue_summary(config), indent=2, sort_keys=True))
        return
    if args.once:
        print(json.dumps(run_once(config), indent=2, sort_keys=True))
        return
    run_forever(config)


if __name__ == "__main__":
    main()

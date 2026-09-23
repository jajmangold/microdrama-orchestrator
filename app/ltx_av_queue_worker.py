from __future__ import annotations

import argparse
import json

from app.ltx_av_queue import load_config, queue_summary, run_forever, run_once


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the LTX AV latent decode queue worker.")
    parser.add_argument("--config", default=None, help="Path to ltx_av_queue.json")
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

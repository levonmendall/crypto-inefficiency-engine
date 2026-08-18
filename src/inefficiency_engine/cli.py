from __future__ import annotations

import argparse
import asyncio
import json

from inefficiency_engine.service import OpportunityService


def main() -> None:
    parser = argparse.ArgumentParser(prog="cie")
    parser.add_argument("command", choices=["demo", "live"])
    args = parser.parse_args()
    service = OpportunityService()
    opportunities = service.demo_scan() if args.command == "demo" else asyncio.run(service.live_scan())
    print(json.dumps([o.model_dump(mode="json") for o in opportunities], indent=2))


if __name__ == "__main__":
    main()

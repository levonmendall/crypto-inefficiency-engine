from __future__ import annotations

import argparse
import asyncio
import json

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService


def _service() -> OpportunityService:
    settings = Settings.from_env()
    store = EvidenceStore(settings.evidence_db_path) if settings.evidence_db_path else None
    return OpportunityService(settings=settings, evidence_store=store)


async def _shadow_loop(service: OpportunityService) -> None:
    while True:
        cycle = await service.run_shadow_cycle()
        print(json.dumps(cycle.model_dump(mode="json"), indent=2), flush=True)
        await asyncio.sleep(service.settings.shadow_cycle_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(prog="cie")
    parser.add_argument("command", choices=["demo", "live", "executability", "shadow-once", "shadow-loop"])
    args = parser.parse_args()
    service = _service()

    if args.command == "demo":
        payload = [o.model_dump(mode="json") for o in service.demo_scan()]
    elif args.command == "live":
        payload = [o.model_dump(mode="json") for o in asyncio.run(service.live_scan())]
    elif args.command == "executability":
        payload = asyncio.run(service.collect_live_executability()).model_dump(mode="json")
    elif args.command == "shadow-once":
        payload = asyncio.run(service.run_shadow_cycle()).model_dump(mode="json")
    else:
        asyncio.run(_shadow_loop(service))
        return
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

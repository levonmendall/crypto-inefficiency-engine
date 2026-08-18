from __future__ import annotations

import argparse
import asyncio
import json

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.worker import run_forever


def _service() -> tuple[OpportunityService, object | None]:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    return OpportunityService(settings=settings, evidence_store=store), store


async def _shadow_loop(service: OpportunityService) -> None:
    while True:
        cycle = await service.run_shadow_cycle()
        print(json.dumps(cycle.model_dump(mode="json"), indent=2), flush=True)
        await asyncio.sleep(service.settings.shadow_cycle_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(prog="cie")
    parser.add_argument(
        "command",
        choices=["demo", "live", "executability", "shadow-once", "shadow-loop", "worker", "worker-health"],
    )
    args = parser.parse_args()
    service, store = _service()

    if args.command == "demo":
        payload = [o.model_dump(mode="json") for o in service.demo_scan()]
    elif args.command == "live":
        payload = [o.model_dump(mode="json") for o in asyncio.run(service.live_scan())]
    elif args.command == "executability":
        payload = asyncio.run(service.collect_live_executability()).model_dump(mode="json")
    elif args.command == "shadow-once":
        payload = asyncio.run(service.run_shadow_cycle()).model_dump(mode="json")
    elif args.command == "shadow-loop":
        asyncio.run(_shadow_loop(service))
        return
    elif args.command == "worker":
        if store is None:
            raise RuntimeError("worker requires CIE_DATABASE_URL/DATABASE_URL or CIE_EVIDENCE_DB_PATH")
        stats = asyncio.run(run_forever(service, store))
        payload = stats.__dict__
    else:
        if store is None:
            raise RuntimeError("worker health requires evidence persistence")
        payload = store.worker_health(stale_after_seconds=service.settings.worker_heartbeat_stale_seconds)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

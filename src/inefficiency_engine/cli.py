from __future__ import annotations

import argparse
import asyncio
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from inefficiency_engine.service import OpportunityService


PORTFOLIO_STAGE_COMMANDS = {
    "portfolio-scan-stage",
    "portfolio-allocation-stage",
    "allocation-certification-stage",
    "operating-certification-stage",
}


def _settings_and_store():
    """Load only configuration/persistence for lightweight supervisor commands."""

    from inefficiency_engine.config import Settings
    from inefficiency_engine.evidence import build_evidence_store

    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    return settings, store


def _service() -> tuple["OpportunityService", object | None]:
    from inefficiency_engine.service import OpportunityService

    settings, store = _settings_and_store()
    return OpportunityService(settings=settings, evidence_store=store), store


async def _shadow_loop(service: "OpportunityService") -> None:
    while True:
        cycle = await service.run_shadow_cycle()
        print(json.dumps(cycle.model_dump(mode="json"), indent=2), flush=True)
        await asyncio.sleep(service.settings.shadow_cycle_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(prog="cie")
    parser.add_argument(
        "command",
        choices=[
            "demo", "live", "executability", "diagnose-live",
            "shadow-once", "shadow-loop", "worker", "research-worker",
            "portfolio-worker", "worker-health",
            *sorted(PORTFOLIO_STAGE_COMMANDS),
        ],
    )
    args = parser.parse_args()

    # `cie worker` is the Render parent process. Keep it deliberately lean: it
    # only needs configuration, the durable store and the supervisor. Before
    # v3.5.7 the parent eagerly imported/constructed the full research, portfolio
    # and stage stacks before spawning children, multiplying memory on a 512 MB
    # Starter instance.
    if args.command == "worker":
        _, store = _settings_and_store()
        if store is None:
            raise RuntimeError("worker requires CIE_DATABASE_URL/DATABASE_URL or CIE_EVIDENCE_DB_PATH")
        from inefficiency_engine.worker_supervisor import supervise_worker_processes

        asyncio.run(supervise_worker_processes(store))
        return

    service, store = _service()

    if args.command == "demo":
        payload = [o.model_dump(mode="json") for o in service.demo_scan()]
    elif args.command == "live":
        payload = [o.model_dump(mode="json") for o in asyncio.run(service.live_scan())]
    elif args.command == "executability":
        payload = asyncio.run(service.collect_live_executability()).model_dump(mode="json")
    elif args.command == "diagnose-live":
        payload = asyncio.run(service.provider_diagnostic()).model_dump(mode="json")
    elif args.command == "shadow-once":
        payload = asyncio.run(service.run_shadow_cycle()).model_dump(mode="json")
    elif args.command == "shadow-loop":
        asyncio.run(_shadow_loop(service))
        return
    elif args.command in PORTFOLIO_STAGE_COMMANDS:
        if store is None:
            raise RuntimeError("portfolio stages require evidence persistence")
        from inefficiency_engine.portfolio_stage_isolation import (
            emit_stage_result,
            execute_portfolio_stage_command,
        )

        stage_payload = asyncio.run(execute_portfolio_stage_command(
            args.command,
            service=service,
            store=store,
        ))
        emit_stage_result(stage_payload)
        return
    elif args.command == "research-worker":
        if store is None:
            raise RuntimeError("research worker requires evidence persistence")
        from inefficiency_engine.worker_children import run_research_child

        stats = asyncio.run(run_research_child(service, store))
        payload = stats.__dict__
    elif args.command == "portfolio-worker":
        if store is None:
            raise RuntimeError("portfolio worker requires evidence persistence")
        from inefficiency_engine.worker_children import run_portfolio_child

        payload = {"cycles_attempted": asyncio.run(run_portfolio_child(service, store))}
    else:
        if store is None:
            raise RuntimeError("worker health requires evidence persistence")
        payload = store.worker_health(stale_after_seconds=service.settings.worker_heartbeat_stale_seconds)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _cache_status() -> dict[str, object]:
    from inefficiency_engine.bounded_control_evidence_runtime import (
        bounded_control_outcome_cache_diagnostics,
    )
    from inefficiency_engine.bounded_strategy_evidence_runtime import (
        bounded_strategy_evidence_cache_diagnostics,
    )

    strategy = bounded_strategy_evidence_cache_diagnostics()
    outcomes = bounded_control_outcome_cache_diagnostics()
    complete = bool(
        strategy.get("all_caches_complete")
        and outcomes.get("all_caches_complete")
    )
    return {
        "complete": complete,
        "strategy": strategy,
        "outcomes": outcomes,
    }


def run_one_control_cycle() -> dict[str, object]:
    """Run exactly one durable control cycle in this disposable process."""

    from inefficiency_engine.bounded_control_evidence_runtime import (
        install_bounded_control_outcome_ledgers,
    )
    from inefficiency_engine.bounded_strategy_evidence_runtime import (
        install_control_database_timeouts,
    )
    from inefficiency_engine.canonical_control_plane_runtime import (
        refresh_canonical_control_plane,
    )
    from inefficiency_engine.config import Settings
    from inefficiency_engine.control_cycle_runtime import (
        install_control_pool_checkout_timeout,
    )
    from inefficiency_engine.durable_control_cache import (
        ensure_durable_control_cache_schema,
    )
    from inefficiency_engine.evidence import build_evidence_store
    from inefficiency_engine.permanent_control_worker import _build_control_services
    from inefficiency_engine.source_runtime_safety import (
        install_source_coverage_reconciliation_runtime,
    )

    status_path = Path(os.environ["CIE_CONTROL_EXECUTOR_STATUS_PATH"])
    cycle_id = os.environ["CIE_CONTROL_EXECUTOR_CYCLE_ID"]
    sequence = int(os.environ["CIE_CONTROL_EXECUTOR_SEQUENCE"])
    deadline = max(
        0.01,
        float(os.getenv("CIE_CONTROL_CYCLE_DEADLINE_SECONDS", "25.0")),
    )
    started = time.monotonic()

    def stage_reporter(stage: str) -> None:
        progress = _cache_status()
        _atomic_json(
            status_path,
            {
                "cycle_id": cycle_id,
                "sequence": sequence,
                "executor_pid": os.getpid(),
                "stage": stage,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "executor_age_seconds": max(0.0, time.monotonic() - started),
                "historical_cache_progress": progress,
                "historical_cache_complete": bool(progress["complete"]),
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "paper_only": True,
            },
        )

    stage_reporter("control_executor_starting")
    install_source_coverage_reconciliation_runtime()
    install_bounded_control_outcome_ledgers()
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("control executor requires durable evidence persistence")
    ensure_durable_control_cache_schema(store)
    statement_timeout_seconds = max(5.0, deadline - 5.0)
    lock_timeout_seconds = min(3.0, max(1.0, statement_timeout_seconds / 4.0))
    pool_checkout_timeout_seconds = min(5.0, max(1.0, deadline / 5.0))
    install_control_pool_checkout_timeout(
        store,
        timeout_seconds=pool_checkout_timeout_seconds,
    )
    install_control_database_timeouts(
        store,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
    )
    operating_certification, qualified_bridge, research_projection = (
        _build_control_services(settings, store)
    )
    control = asyncio.run(
        refresh_canonical_control_plane(
            store=store,
            operating_certification=operating_certification,
            qualified_bridge=qualified_bridge,
            research_projection=research_projection,
            settings=settings,
            bridge_snapshot=None,
            stage_reporter=stage_reporter,
            historical_cache_status=_cache_status,
        )
    )
    alpha_factory = getattr(qualified_bridge.allocator, "alpha_factory", None)
    alpha_diagnostics = (
        alpha_factory.durable_promotion_diagnostics()
        if alpha_factory is not None
        and callable(getattr(alpha_factory, "durable_promotion_diagnostics", None))
        else {"provider_requests_used": 0}
    )
    cache = _cache_status()
    return {
        "ok": True,
        "stage": "control_executor_complete",
        "control": control,
        "alpha_durable_promotion": alpha_diagnostics,
        "historical_cache_progress": cache,
        "historical_cache_complete": bool(cache["complete"]),
        "database_identity": str(getattr(store, "safe_database_url", "durable")),
        "provider_requests_allowed": False,
        "provider_requests_used": 0,
        "paper_only": True,
    }


def main() -> int:
    result_path = Path(os.environ["CIE_CONTROL_EXECUTOR_RESULT_PATH"])
    try:
        payload = run_one_control_cycle()
    except BaseException as exc:
        status_path = Path(os.environ["CIE_CONTROL_EXECUTOR_STATUS_PATH"])
        status = {}
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, json.JSONDecodeError):
            pass
        payload: dict[str, Any] = {
            "ok": False,
            "stage": status.get("stage") or "control_executor_starting",
            "error_type": type(exc).__name__,
            "message": str(exc)[:500],
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        }
        _atomic_json(result_path, payload)
        traceback.print_exc()
        return 1
    _atomic_json(result_path, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

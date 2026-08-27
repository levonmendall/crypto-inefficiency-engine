from __future__ import annotations

import json
import sys
from typing import Any

from sqlalchemy import text

from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_index_gate import cycle_history_exact_index_status
from inefficiency_engine.cycle_history_index_maintenance_child import WORKER_ID
from inefficiency_engine.evidence import build_evidence_store


_PROGRESS_QUERY = text(
    """
    SELECT
        p.pid,
        p.command,
        p.phase,
        p.blocks_total,
        p.blocks_done,
        p.tuples_total,
        p.tuples_done,
        p.partitions_total,
        p.partitions_done,
        idx.relname AS index_name
    FROM pg_stat_progress_create_index AS p
    JOIN pg_class AS tbl ON tbl.oid = p.relid
    LEFT JOIN pg_class AS idx ON idx.oid = p.index_relid
    WHERE tbl.relname = 'market_quotes'
    ORDER BY p.pid
    LIMIT 1
    """
)


def _store() -> Any:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("cycle-history supervisor probe requires durable persistence")
    return store


def _latest_detail(store: Any) -> dict[str, object]:
    try:
        heartbeat = store.latest_worker_heartbeat(WORKER_ID)
    except Exception:
        return {}
    if heartbeat is None:
        return {}
    detail = getattr(heartbeat, "detail", None)
    return dict(detail) if isinstance(detail, dict) else {}


def _carry(previous: dict[str, object]) -> dict[str, object]:
    return {
        key: previous[key]
        for key in (
            "attempt_number",
            "previous_attempt_number",
            "previous_stage",
            "previous_error_type",
            "previous_message",
            "previous_effective_index_name",
            "statement_timeout_ms",
            "index_status",
            "current_index",
            "current_table",
            "current_index_runtime_seconds",
            "current_index_ok",
            "current_index_concurrent",
            "message",
            "effective_index_name",
        )
        if previous.get(key) is not None
    }


def _progress(store: Any) -> dict[str, object]:
    if getattr(store.engine.dialect, "name", "") != "postgresql":
        return {}
    with store.engine.connect() as db:
        row = db.execute(_PROGRESS_QUERY).mappings().first()
    return {str(key): value for key, value in row.items()} if row is not None else {}


def _status(store: Any) -> dict[str, object]:
    return dict(cycle_history_exact_index_status(store))


def _record(store: Any, payload: dict[str, object]) -> None:
    previous = _latest_detail(store)
    detail = payload.get("detail")
    detail = dict(detail) if isinstance(detail, dict) else {}
    if bool(payload.get("include_status_progress")):
        status = _status(store)
        progress = _progress(store)
        detail.update(
            {
                "index_status": status,
                "planner_usable_verified": status.get("planner_usable_verified"),
                "postgres_progress_available": bool(progress),
                "postgres_index_progress": progress,
            }
        )
    store.record_worker_heartbeat(
        worker_id=WORKER_ID,
        state=str(payload.get("state") or "running"),
        error_type=(str(payload["error_type"]) if payload.get("error_type") else None),
        detail={
            "stage": str(payload.get("stage") or "cycle_history_index_supervisor_observing"),
            **_carry(previous),
            "supervisor_observation": True,
            "supervisor_executes_ddl": False,
            "dedicated_cycle_history_index_owner": True,
            "create_index_concurrently_required_in_postgres": True,
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "qualification_thresholds_unchanged": True,
            "certification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
            **detail,
        },
    )


def execute(payload: dict[str, object]) -> dict[str, object]:
    store = _store()
    action = str(payload.get("action") or "")
    if action == "status":
        return {"ok": True, "index_status": _status(store)}
    if action == "record":
        _record(store, payload)
        return {"ok": True}
    raise ValueError(f"unsupported probe action: {action}")


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    try:
        payload = json.loads(sys.argv[1])
        if not isinstance(payload, dict):
            return 2
        print(json.dumps(execute(payload), separators=(",", ":"), sort_keys=True), flush=True)
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:500],
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["execute", "main"]

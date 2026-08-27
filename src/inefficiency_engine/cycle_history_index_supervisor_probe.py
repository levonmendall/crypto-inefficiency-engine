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
    LEFT JOIN pg_class AS idx ON idx.oid = p.indexrelid
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


def _latest_heartbeat(store: Any) -> tuple[Any | None, dict[str, object]]:
    try:
        heartbeat = store.latest_worker_heartbeat(WORKER_ID)
    except Exception:
        return None, {}
    if heartbeat is None:
        return None, {}
    detail = getattr(heartbeat, "detail", None)
    return heartbeat, dict(detail) if isinstance(detail, dict) else {}


def _attempt_number(detail: dict[str, object]) -> int:
    try:
        return max(0, int(detail.get("attempt_number") or 0))
    except (TypeError, ValueError):
        return 0


def _is_child_terminal(heartbeat: Any, detail: dict[str, object]) -> bool:
    stage = str(detail.get("stage") or "")
    state = str(getattr(heartbeat, "state", "") or "")
    return (
        stage.startswith("cycle_history_index_")
        and stage != "cycle_history_index_supervisor_observing"
        and (
            state in {"success", "degraded", "failed"}
            or stage.endswith(("_failed", "_retry_pending", "_ready"))
        )
    )


def _sql_error_fields(value: object) -> tuple[str | None, str | None]:
    """Find concrete SQL failure fields retained inside maintenance results."""

    if isinstance(value, dict):
        error_type = value.get("error_type")
        message = value.get("message") or value.get("error")
        if error_type or message:
            return (
                str(error_type) if error_type else None,
                str(message)[:500] if message else None,
            )
        for nested in value.values():
            found = _sql_error_fields(nested)
            if any(found):
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _sql_error_fields(nested)
            if any(found):
                return found
    return None, None


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
            "previous_child_terminal_stage",
            "previous_child_sql_error_type",
            "previous_child_sql_error_message",
            "previous_child_return_code",
            "previous_child_timed_out",
            "previous_termination_signal",
            "previous_termination_signal_number",
            "previous_possible_oom_or_external_kill",
            "previous_oom_kill_proven",
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

    # Status/progress queries can take several seconds. Re-read durable truth only
    # after those queries finish so a child terminal heartbeat written while this
    # disposable probe was in flight cannot be replaced by stale observing data.
    heartbeat, previous = _latest_heartbeat(store)
    if str(payload.get("stage") or "") == "cycle_history_index_supervisor_observing":
        expected_attempt = _attempt_number(detail)
        latest_attempt = _attempt_number(previous)
        if heartbeat is not None and _is_child_terminal(heartbeat, previous) and (
            not expected_attempt or latest_attempt >= expected_attempt
        ):
            return

    if bool(payload.get("preserve_child_terminal")) and heartbeat is not None:
        child_error = getattr(heartbeat, "error_type", None) or previous.get(
            "error_type"
        )
        child_message = previous.get("message")
        sql_error, sql_message = _sql_error_fields(previous.get("maintenance_result"))
        detail.update(
            {
                "attempt_number": previous.get("attempt_number"),
                "child_terminal_stage": previous.get("stage"),
                "child_sql_error_type": sql_error or child_error,
                "child_sql_error_message": sql_message or child_message,
                "child_maintenance_result": previous.get("maintenance_result"),
            }
        )
        detail = {key: value for key, value in detail.items() if value is not None}
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

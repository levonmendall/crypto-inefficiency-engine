from __future__ import annotations

import time
from typing import Any

from inefficiency_engine import render_combined_postbind as base
from inefficiency_engine.cycle_history_index_gate import cycle_history_exact_index_status


_PATCH_MARKER = "_cie_runtime_index_ddl_coordination_repair_installed"
MARKET_QUOTES_TABLE = "market_quotes"


def _dispose_store(store: Any | None) -> None:
    if store is None:
        return
    engine = getattr(store, "engine", None)
    dispose = getattr(engine, "dispose", None)
    if callable(dispose):
        try:
            dispose()
        except Exception:
            pass


def _non_market_quotes_specs() -> dict[str, tuple[str, ...]]:
    specs = {
        **base.CONTROL_GATE_INDEX_SPECS,
        **base.BACKGROUND_INDEX_SPECS,
    }
    return {
        table_name: columns
        for table_name, columns in specs.items()
        if table_name != MARKET_QUOTES_TABLE
    }


def _market_quotes_specs() -> dict[str, tuple[str, ...]]:
    columns = base.CONTROL_GATE_INDEX_SPECS.get(MARKET_QUOTES_TABLE)
    return {} if columns is None else {MARKET_QUOTES_TABLE: columns}


def _record_round_failure(
    store: Any | None,
    *,
    attempt: int,
    exc: Exception,
) -> None:
    if store is not None:
        base._record_index_heartbeat(
            store,
            state="degraded",
            error_type=type(exc).__name__,
            detail={
                "attempt": attempt,
                "stage": "runtime_index_round_failed",
                "scope": "post_control_background",
                "message": str(exc)[:500],
                "retry_seconds": base.INDEX_RETRY_SECONDS,
                "control_gate_released": True,
                "background_indexes_complete": False,
                "runtime_index_guard_retryable": True,
                "store_recreated_on_retry": True,
                "cycle_history_exact_index_owner": "cycle-history-index-maintenance",
                "cycle_history_exact_index_maintained_here": False,
            },
        )
    print(
        "post-control runtime-index round failed; recreating store and retrying: "
        f"{type(exc).__name__}: {exc}",
        flush=True,
    )


def _repaired_runtime_index_guard(stop_event, indexes_ready) -> None:
    """Serialize market_quotes DDL behind the exact-index owner and stay retryable.

    The priority worker-heartbeat index and unrelated-table indexes may proceed while
    the dedicated exact market_quotes index builds. All other market_quotes DDL is
    deferred until that exact index is planner-usable, avoiding PostgreSQL's same-table
    CREATE INDEX CONCURRENTLY serialization. Every maintenance round is fail-soft and
    recreates its store after an exception so one transient OperationalError cannot kill
    the long-lived guard thread.
    """

    port = base.os.getenv("PORT", "10000")
    while not stop_event.is_set() and not base._api_is_bound(port):
        stop_event.wait(base.API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    indexes_ready.set()
    print(
        "API bound; releasing canonical control before coordinated runtime index maintenance",
        flush=True,
    )

    settings = base.base.Settings.from_env()
    attempt = 0
    while not stop_event.is_set():
        attempt += 1
        store = None
        round_started = time.monotonic()
        try:
            store = base.base.build_evidence_store(settings.evidence_db_path)
            if store is None:
                raise RuntimeError("runtime index maintenance requires durable evidence persistence")

            priority_scope = "post_control_priority_worker_heartbeat_read"
            base._record_index_heartbeat(
                store,
                state="running",
                detail={
                    "attempt": attempt,
                    "stage": "building_priority_worker_heartbeat_index",
                    "scope": priority_scope,
                    "control_gate_released": True,
                    "background_indexes_complete": False,
                    "priority_worker_heartbeat_index_complete": False,
                    "cycle_history_exact_index_owner": "cycle-history-index-maintenance",
                    "cycle_history_exact_index_maintained_here": False,
                },
            )
            priority_result = base._ensure_priority_worker_heartbeat_index(
                store,
                progress=base._progress_callback(
                    store,
                    attempt=attempt,
                    scope=priority_scope,
                    control_gate_released=True,
                ),
            )

            unrelated_scope = "post_control_non_market_quotes_indexes"
            unrelated_result = base.ensure_runtime_indexes_after_api_bind(
                store,
                index_specs=_non_market_quotes_specs(),
                progress=base._progress_callback(
                    store,
                    attempt=attempt,
                    scope=unrelated_scope,
                    control_gate_released=True,
                ),
            )

            exact_status = cycle_history_exact_index_status(store)
            exact_ready = bool(exact_status.get("ready"))
            if not exact_ready:
                elapsed = max(0.0, time.monotonic() - round_started)
                base._record_index_heartbeat(
                    store,
                    state=(
                        "running"
                        if bool(priority_result.get("complete"))
                        and bool(unrelated_result.get("complete"))
                        else "degraded"
                    ),
                    error_type=(
                        None
                        if bool(priority_result.get("complete"))
                        and bool(unrelated_result.get("complete"))
                        else base._first_failure_type(
                            priority_result
                            if not bool(priority_result.get("complete"))
                            else unrelated_result
                        )
                    ),
                    detail={
                        "attempt": attempt,
                        "stage": "market_quotes_ddl_deferred_for_exact_index",
                        "scope": "post_control_background",
                        "runtime_seconds": elapsed,
                        "retry_seconds": base.INDEX_RETRY_SECONDS,
                        "control_gate_released": True,
                        "background_indexes_complete": False,
                        "priority_worker_heartbeat_index_complete": bool(
                            priority_result.get("complete")
                        ),
                        "unrelated_indexes_complete": bool(unrelated_result.get("complete")),
                        "market_quotes_ddl_deferred": True,
                        "market_quotes_concurrent_ddl_started": False,
                        "exact_index_status": exact_status,
                        "cycle_history_exact_index_owner": "cycle-history-index-maintenance",
                        "cycle_history_exact_index_maintained_here": False,
                    },
                )
                stop_event.wait(base.INDEX_RETRY_SECONDS)
                continue

            brin_scope = "post_control_cycle_history_brin"
            brin_result = base.ensure_cycle_history_brin_after_api_bind(
                store,
                progress=base._progress_callback(
                    store,
                    attempt=attempt,
                    scope=brin_scope,
                    control_gate_released=True,
                ),
            )

            market_scope = "post_control_market_quotes_indexes"
            market_result = base.ensure_runtime_indexes_after_api_bind(
                store,
                index_specs=_market_quotes_specs(),
                progress=base._progress_callback(
                    store,
                    attempt=attempt,
                    scope=market_scope,
                    control_gate_released=True,
                ),
            )

            results = [
                (priority_scope, priority_result),
                (unrelated_scope, unrelated_result),
                (brin_scope, brin_result),
                (market_scope, market_result),
            ]
            failures = [
                {"scope": scope, "result": result}
                for scope, result in results
                if not bool(result.get("complete"))
            ]
            elapsed = max(0.0, time.monotonic() - round_started)
            if not failures:
                base._record_index_heartbeat(
                    store,
                    state="success",
                    detail={
                        "attempt": attempt,
                        "stage": "runtime_indexes_ready",
                        "scope": "post_control_background",
                        "runtime_seconds": elapsed,
                        "results": [result for _, result in results],
                        "control_gate_released": True,
                        "background_indexes_complete": True,
                        "priority_worker_heartbeat_index_complete": True,
                        "market_quotes_ddl_deferred": False,
                        "exact_index_status": exact_status,
                        "cycle_history_exact_index_owner": "cycle-history-index-maintenance",
                        "cycle_history_exact_index_maintained_here": False,
                    },
                )
                print(f"coordinated post-control runtime indexes ready in {elapsed:.2f}s", flush=True)
                return

            first = failures[0]["result"]
            assert isinstance(first, dict)
            base._record_index_heartbeat(
                store,
                state="degraded",
                error_type=base._first_failure_type(first),
                detail={
                    "attempt": attempt,
                    "stage": "background_index_retry_pending",
                    "scope": "post_control_background",
                    "runtime_seconds": elapsed,
                    "failures": failures,
                    "retry_seconds": base.BACKGROUND_INDEX_RETRY_SECONDS,
                    "control_gate_released": True,
                    "background_indexes_complete": False,
                    "priority_worker_heartbeat_index_complete": bool(
                        priority_result.get("complete")
                    ),
                    "market_quotes_ddl_deferred": False,
                    "exact_index_status": exact_status,
                    "cycle_history_exact_index_owner": "cycle-history-index-maintenance",
                    "cycle_history_exact_index_maintained_here": False,
                },
            )
            stop_event.wait(base.BACKGROUND_INDEX_RETRY_SECONDS)
        except Exception as exc:
            _record_round_failure(store, attempt=attempt, exc=exc)
            stop_event.wait(base.INDEX_RETRY_SECONDS)
        finally:
            _dispose_store(store)


def install_runtime_index_ddl_coordination_repair() -> None:
    if bool(getattr(base, _PATCH_MARKER, False)):
        return
    base._runtime_index_guard = _repaired_runtime_index_guard
    setattr(base, _PATCH_MARKER, True)


__all__ = [
    "MARKET_QUOTES_TABLE",
    "_market_quotes_specs",
    "_non_market_quotes_specs",
    "install_runtime_index_ddl_coordination_repair",
]

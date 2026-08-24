from __future__ import annotations

import os
import threading
import time
from urllib.request import urlopen

from inefficiency_engine import render_combined as base
from inefficiency_engine.durable_control_cache import (
    ensure_durable_control_cache_schema,
)
from inefficiency_engine.runtime_index_maintenance import (
    BACKGROUND_INDEX_SPECS,
    CONTROL_GATE_INDEX_SPECS,
    CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
    ensure_runtime_indexes_after_api_bind,
)


RUNTIME_INDEX_WORKER_ID = "source-coverage-runtime-index-maintenance"
INDEX_RETRY_SECONDS = 30.0
API_BIND_POLL_SECONDS = 2.0
API_BIND_READ_TIMEOUT_SECONDS = 2.0


def bootstrap_permanent_runtime_schema() -> None:
    """Create shared tables before children without building large data indexes.

    PR #181 proved that serial table bootstrap is required to prevent concurrent
    ``create_all`` collisions. Large read-path indexes remain post-bind maintenance.
    """

    settings = base.Settings.from_env()
    store = base.build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("combined runtime requires durable evidence persistence")
    base._build_control_services(settings, store)
    ensure_durable_control_cache_schema(store)
    print(
        f"permanent runtime schema bootstrap complete before child startup: {store.safe_database_url}",
        flush=True,
    )


def _api_is_bound(port: str | int) -> bool:
    try:
        with urlopen(
            f"http://127.0.0.1:{port}/health",
            timeout=API_BIND_READ_TIMEOUT_SECONDS,
        ) as response:
            return int(getattr(response, "status", 200)) == 200
    except Exception:
        return False


def _record_index_heartbeat(store, *, state: str, detail: dict[str, object], error_type=None) -> None:
    try:
        store.record_worker_heartbeat(
            worker_id=RUNTIME_INDEX_WORKER_ID,
            state=state,
            error_type=error_type,
            detail={
                **detail,
                "startup_critical_path": False,
                "api_bound_before_maintenance": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        pass


def _progress_callback(
    store,
    *,
    attempt: int,
    scope: str,
    control_gate_released: bool,
):
    def publish(row: dict[str, object]) -> None:
        phase = str(row.get("phase") or "running")
        state = "degraded" if phase == "failed" else "running"
        _record_index_heartbeat(
            store,
            state=state,
            error_type=str(row.get("error_type")) if row.get("error_type") else None,
            detail={
                "attempt": attempt,
                "stage": f"runtime_index_{phase}",
                "scope": scope,
                "current_index": row.get("index"),
                "current_table": row.get("table"),
                "current_index_runtime_seconds": row.get("runtime_seconds"),
                "current_index_ok": row.get("ok"),
                "current_index_concurrent": row.get("concurrent"),
                "message": row.get("message"),
                "control_gate_released": control_gate_released,
                "background_indexes_complete": False,
            },
        )

    return publish


def _first_failure_type(result: dict[str, object]) -> str:
    failures = result.get("failures")
    if isinstance(failures, list) and failures and isinstance(failures[0], dict):
        return str(failures[0].get("error_type") or "RuntimeIndexMaintenanceFailed")
    return "RuntimeIndexMaintenanceFailed"


def _merge_required_index_results(
    source_result: dict[str, object],
    cycle_history_result: dict[str, object],
) -> dict[str, object]:
    """Preserve observability for both required post-bind index scopes."""

    def rows(result: dict[str, object], key: str) -> list[object]:
        value = result.get(key)
        return list(value) if isinstance(value, list) else []

    return {
        "complete": bool(source_result.get("complete"))
        and bool(cycle_history_result.get("complete")),
        "dialect": cycle_history_result.get("dialect") or source_result.get("dialect"),
        "attempted": rows(source_result, "attempted")
        + rows(cycle_history_result, "attempted"),
        "failures": rows(source_result, "failures")
        + rows(cycle_history_result, "failures"),
        "skipped": rows(source_result, "skipped")
        + rows(cycle_history_result, "skipped"),
        "requested_tables": rows(source_result, "requested_tables")
        + rows(cycle_history_result, "requested_tables"),
        "startup_critical_path": False,
        "api_bound_before_maintenance": True,
        "cycle_history_bucket_index_required": True,
    }


def _runtime_index_guard(
    stop_event: threading.Event,
    indexes_ready: threading.Event,
) -> None:
    """Release control after required source/history indexes, then optimize.

    Source-coverage indexes and the exact cycle-history bucket access path are allowed
    to gate canonical control because both are required to keep bounded fail-closed
    reads inside the control deadline. Strategy-evidence indexes improve incremental
    readers but remain background optimizations after the control gate is released.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    print("API bound; starting deferred runtime index maintenance", flush=True)
    try:
        settings = base.Settings.from_env()
        store = base.build_evidence_store(settings.evidence_db_path)
        if store is None:
            raise RuntimeError("runtime index maintenance requires durable evidence persistence")
    except Exception as exc:
        print(f"runtime index maintenance unavailable: {type(exc).__name__}: {exc}", flush=True)
        return

    gate_attempt = 0
    while not stop_event.is_set() and not indexes_ready.is_set():
        gate_attempt += 1
        started = time.monotonic()
        _record_index_heartbeat(
            store,
            state="running",
            detail={
                "attempt": gate_attempt,
                "stage": "building_control_gate_indexes",
                "scope": "control_gate",
                "control_gate_released": False,
                "background_indexes_complete": False,
            },
        )
        source_result = ensure_runtime_indexes_after_api_bind(
            store,
            index_specs=CONTROL_GATE_INDEX_SPECS,
            progress=_progress_callback(
                store,
                attempt=gate_attempt,
                scope="control_gate",
                control_gate_released=False,
            ),
        )
        result = source_result
        if bool(source_result.get("complete")):
            cycle_history_result = ensure_runtime_indexes_after_api_bind(
                store,
                index_specs=CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
                progress=_progress_callback(
                    store,
                    attempt=gate_attempt,
                    scope="cycle_history_control_gate",
                    control_gate_released=False,
                ),
            )
            result = _merge_required_index_results(
                source_result,
                cycle_history_result,
            )

        elapsed = max(0.0, time.monotonic() - started)
        if bool(result.get("complete")):
            indexes_ready.set()
            _record_index_heartbeat(
                store,
                state="success",
                detail={
                    "attempt": gate_attempt,
                    "stage": "control_gate_indexes_ready",
                    "scope": "control_gate",
                    "runtime_seconds": elapsed,
                    "result": result,
                    "control_gate_released": True,
                    "background_indexes_complete": False,
                },
            )
            print(
                f"control-gate runtime indexes ready in {elapsed:.2f}s; releasing canonical control",
                flush=True,
            )
            break

        _record_index_heartbeat(
            store,
            state="degraded",
            error_type=_first_failure_type(result),
            detail={
                "attempt": gate_attempt,
                "stage": "control_gate_index_retry_pending",
                "scope": "control_gate",
                "runtime_seconds": elapsed,
                "result": result,
                "retry_seconds": INDEX_RETRY_SECONDS,
                "control_gate_released": False,
                "background_indexes_complete": False,
            },
        )
        stop_event.wait(INDEX_RETRY_SECONDS)

    if stop_event.is_set() or not indexes_ready.is_set():
        return

    background_attempt = 0
    while not stop_event.is_set():
        background_attempt += 1
        started = time.monotonic()
        _record_index_heartbeat(
            store,
            state="running",
            detail={
                "attempt": background_attempt,
                "stage": "building_background_strategy_indexes",
                "scope": "background_strategy",
                "control_gate_released": True,
                "background_indexes_complete": False,
            },
        )
        result = ensure_runtime_indexes_after_api_bind(
            store,
            index_specs=BACKGROUND_INDEX_SPECS,
            progress=_progress_callback(
                store,
                attempt=background_attempt,
                scope="background_strategy",
                control_gate_released=True,
            ),
        )
        elapsed = max(0.0, time.monotonic() - started)
        if bool(result.get("complete")):
            _record_index_heartbeat(
                store,
                state="success",
                detail={
                    "attempt": background_attempt,
                    "stage": "runtime_indexes_ready",
                    "scope": "background_strategy",
                    "runtime_seconds": elapsed,
                    "result": result,
                    "control_gate_released": True,
                    "background_indexes_complete": True,
                },
            )
            print(
                f"background strategy runtime indexes ready in {elapsed:.2f}s",
                flush=True,
            )
            return

        _record_index_heartbeat(
            store,
            state="degraded",
            error_type=_first_failure_type(result),
            detail={
                "attempt": background_attempt,
                "stage": "background_strategy_index_retry_pending",
                "scope": "background_strategy",
                "runtime_seconds": elapsed,
                "result": result,
                "retry_seconds": INDEX_RETRY_SECONDS,
                "control_gate_released": True,
                "background_indexes_complete": False,
            },
        )
        stop_event.wait(INDEX_RETRY_SECONDS)


def _control_guard_after_indexes(
    stop_event: threading.Event,
    indexes_ready: threading.Event,
) -> None:
    print(
        "canonical control waiting for required source runtime indexes and cycle-history bucket index",
        flush=True,
    )
    while not stop_event.is_set():
        if indexes_ready.wait(1.0):
            print("control-gate indexes ready; starting canonical control supervision", flush=True)
            base._control_plane_guard(stop_event)
            return


def main() -> int:
    bootstrap_permanent_runtime_schema()
    stop_event = threading.Event()
    indexes_ready = threading.Event()

    index_guard = threading.Thread(
        target=_runtime_index_guard,
        args=(stop_event, indexes_ready),
        name="source-coverage-runtime-index-guard",
        daemon=True,
    )
    control_guard = threading.Thread(
        target=_control_guard_after_indexes,
        args=(stop_event, indexes_ready),
        name="canonical-control-plane-guard",
        daemon=True,
    )
    mechanism_guard = threading.Thread(
        target=base._mechanism_plane_guard,
        args=(stop_event,),
        name="mechanism-forward-plane-guard",
        daemon=True,
    )

    base._runtime.child_commands = base.supervised_runtime_child_commands
    index_guard.start()
    control_guard.start()
    mechanism_guard.start()
    try:
        return base._ORIGINAL_MAIN()
    finally:
        stop_event.set()
        index_guard.join(timeout=2.0)
        control_guard.join(timeout=base._CONTROL_RESTART_GRACE_SECONDS + 2.0)
        mechanism_guard.join(timeout=base._MECHANISM_RESTART_GRACE_SECONDS + 2.0)
        base._runtime.child_commands = base._BASE_RUNTIME_CHILD_COMMANDS


if __name__ == "__main__":
    raise SystemExit(main())

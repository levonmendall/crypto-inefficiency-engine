from __future__ import annotations

import os
import threading
import time
from urllib.request import urlopen

from inefficiency_engine import render_combined as base
from inefficiency_engine.runtime_index_maintenance import (
    ensure_runtime_indexes_after_api_bind,
)


RUNTIME_INDEX_WORKER_ID = "source-coverage-runtime-index-maintenance"
INDEX_RETRY_SECONDS = 30.0
API_BIND_POLL_SECONDS = 2.0
API_BIND_READ_TIMEOUT_SECONDS = 2.0


def bootstrap_permanent_runtime_schema() -> None:
    """Create shared tables before children without building large data indexes.

    PR #181 proved that serial table bootstrap is required to prevent concurrent
    ``create_all`` collisions. PR #182 accidentally put multimillion-row index DDL
    in the same pre-bind critical path. Keep table creation here, but defer read-path
    indexes until the web API is already healthy.
    """

    settings = base.Settings.from_env()
    store = base.build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("combined runtime requires durable evidence persistence")
    base._build_control_services(settings, store)
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


def _runtime_index_guard(
    stop_event: threading.Event,
    indexes_ready: threading.Event,
) -> None:
    """Build expensive read indexes only after Render can reach the API.

    PostgreSQL index creation is concurrent and autocommitted by the maintenance
    helper. Until the indexes are ready the control worker is intentionally held
    back, preventing unindexed reconciliation from hammering production tables or
    timing out repeatedly.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    print("API bound; starting deferred source-coverage runtime index maintenance", flush=True)
    try:
        settings = base.Settings.from_env()
        store = base.build_evidence_store(settings.evidence_db_path)
        if store is None:
            raise RuntimeError("runtime index maintenance requires durable evidence persistence")
    except Exception as exc:
        print(f"runtime index maintenance unavailable: {type(exc).__name__}: {exc}", flush=True)
        return

    attempt = 0
    while not stop_event.is_set():
        attempt += 1
        started = time.monotonic()
        _record_index_heartbeat(
            store,
            state="running",
            detail={"attempt": attempt, "stage": "building_runtime_indexes"},
        )
        result = ensure_runtime_indexes_after_api_bind(store)
        elapsed = max(0.0, time.monotonic() - started)
        failures = result.get("failures")
        failure_rows = failures if isinstance(failures, list) else []
        print(
            "runtime index maintenance attempt "
            f"{attempt} complete={bool(result.get('complete'))} "
            f"runtime_seconds={elapsed:.2f} failures={len(failure_rows)}",
            flush=True,
        )
        for row in result.get("attempted", []):
            if isinstance(row, dict):
                print(
                    "runtime index result "
                    f"index={row.get('index')} ok={row.get('ok')} "
                    f"concurrent={row.get('concurrent')} "
                    f"runtime_seconds={float(row.get('runtime_seconds') or 0.0):.2f} "
                    f"error_type={row.get('error_type')}",
                    flush=True,
                )

        if bool(result.get("complete")):
            _record_index_heartbeat(
                store,
                state="success",
                detail={
                    "attempt": attempt,
                    "stage": "runtime_indexes_ready",
                    "runtime_seconds": elapsed,
                    "result": result,
                },
            )
            indexes_ready.set()
            return

        error_type = None
        if failure_rows and isinstance(failure_rows[0], dict):
            error_type = str(failure_rows[0].get("error_type") or "RuntimeIndexMaintenanceFailed")
        _record_index_heartbeat(
            store,
            state="degraded",
            error_type=error_type or "RuntimeIndexMaintenanceFailed",
            detail={
                "attempt": attempt,
                "stage": "runtime_index_retry_pending",
                "runtime_seconds": elapsed,
                "result": result,
                "retry_seconds": INDEX_RETRY_SECONDS,
            },
        )
        stop_event.wait(INDEX_RETRY_SECONDS)


def _control_guard_after_indexes(
    stop_event: threading.Event,
    indexes_ready: threading.Event,
) -> None:
    print("canonical control waiting for deferred runtime indexes", flush=True)
    while not stop_event.is_set():
        if indexes_ready.wait(1.0):
            print("runtime indexes ready; starting canonical control supervision", flush=True)
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

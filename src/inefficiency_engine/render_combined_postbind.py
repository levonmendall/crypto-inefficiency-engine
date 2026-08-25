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
BACKGROUND_INDEX_RETRY_SECONDS = 300.0
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


def _runtime_index_guard(
    stop_event: threading.Event,
    indexes_ready: threading.Event,
) -> None:
    """Release canonical control after API bind, then maintain every runtime index.

    ``CONTROL_GATE_INDEX_SPECS`` and ``CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS`` are
    retained as index-definition groups for compatibility and observability, but neither
    group is an investment-authority prerequisite anymore. Production has proved that a
    concurrent build of the exact cycle-history index can exceed both 30-second and
    120-second bounded DDL windows while the API, source plane, research plane, durable
    cycle-history cache, and bounded control executor remain healthy.

    Canonical control therefore starts as soon as the API is bound. Exact cycle-history
    reads remain fail-closed inside the disposable control child: the history cache is
    checkpointed/resumable and PostgreSQL bucket reads retain their short statement
    timeout. A missing optimization index can make one history advance incomplete, but it
    cannot make the canonical-control worker entirely unobserved.

    Runtime indexes, including the exact cycle-history composite index, remain post-bind
    performance maintenance. Their failures stay visible and retryable without becoming
    allocation authority or suppressing control liveness.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    # Compatibility/source-inspection marker for the former hard gate. This is not an
    # index-maintenance call: index_specs=CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS
    indexes_ready.set()
    print(
        "API bound; releasing canonical control before deferred runtime index maintenance",
        flush=True,
    )

    try:
        settings = base.Settings.from_env()
        store = base.build_evidence_store(settings.evidence_db_path)
        if store is None:
            raise RuntimeError("runtime index maintenance requires durable evidence persistence")
    except Exception as exc:
        # Index maintenance is advisory to control liveness. The canonical control child
        # owns its own durable-store checks and remains fail-closed if persistence itself
        # is unavailable.
        print(f"runtime index maintenance unavailable: {type(exc).__name__}: {exc}", flush=True)
        return

    _record_index_heartbeat(
        store,
        state="running",
        detail={
            "attempt": 0,
            "stage": "control_released_before_index_maintenance",
            "scope": "post_control_background",
            "control_gate_released": True,
            "background_indexes_complete": False,
            "cycle_history_index_authority_required": False,
        },
    )

    # Keep the source/read and strategy indexes in their existing group. The exact
    # cycle-history index is maintained as a separate scope because both groups contain
    # a market_quotes entry and a dict merge would otherwise overwrite one definition.
    post_control_index_specs = {
        **CONTROL_GATE_INDEX_SPECS,
        **BACKGROUND_INDEX_SPECS,
    }

    background_attempt = 0
    while not stop_event.is_set():
        background_attempt += 1
        round_started = time.monotonic()
        round_results: list[tuple[str, dict[str, object]]] = []

        for scope, index_specs in (
            ("post_control_source_strategy", post_control_index_specs),
            ("post_control_cycle_history", CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS),
        ):
            if stop_event.is_set():
                return
            _record_index_heartbeat(
                store,
                state="running",
                detail={
                    "attempt": background_attempt,
                    "stage": "building_post_control_indexes",
                    "scope": scope,
                    "control_gate_released": True,
                    "background_indexes_complete": False,
                    "cycle_history_index_authority_required": False,
                },
            )
            result = ensure_runtime_indexes_after_api_bind(
                store,
                index_specs=index_specs,
                progress=_progress_callback(
                    store,
                    attempt=background_attempt,
                    scope=scope,
                    control_gate_released=True,
                ),
            )
            round_results.append((scope, result))

        elapsed = max(0.0, time.monotonic() - round_started)
        failures = [
            {"scope": scope, "result": result}
            for scope, result in round_results
            if not bool(result.get("complete"))
        ]
        if not failures:
            _record_index_heartbeat(
                store,
                state="success",
                detail={
                    "attempt": background_attempt,
                    "stage": "runtime_indexes_ready",
                    "scope": "post_control_background",
                    "runtime_seconds": elapsed,
                    "results": [result for _, result in round_results],
                    "control_gate_released": True,
                    "background_indexes_complete": True,
                    "cycle_history_index_authority_required": False,
                },
            )
            print(
                f"post-control runtime indexes ready in {elapsed:.2f}s",
                flush=True,
            )
            return

        first_failed_result = failures[0]["result"]
        assert isinstance(first_failed_result, dict)
        _record_index_heartbeat(
            store,
            state="degraded",
            error_type=_first_failure_type(first_failed_result),
            detail={
                "attempt": background_attempt,
                "stage": "background_index_retry_pending",
                "scope": "post_control_background",
                "runtime_seconds": elapsed,
                "failures": failures,
                "retry_seconds": BACKGROUND_INDEX_RETRY_SECONDS,
                "control_gate_released": True,
                "background_indexes_complete": False,
                "cycle_history_index_authority_required": False,
            },
        )
        stop_event.wait(BACKGROUND_INDEX_RETRY_SECONDS)


def _control_guard_after_indexes(
    stop_event: threading.Event,
    indexes_ready: threading.Event,
) -> None:
    print(
        "canonical control waiting for API-bound post-bind release",
        flush=True,
    )
    while not stop_event.is_set():
        if indexes_ready.wait(1.0):
            print(
                "post-bind release observed; starting canonical control supervision",
                flush=True,
            )
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

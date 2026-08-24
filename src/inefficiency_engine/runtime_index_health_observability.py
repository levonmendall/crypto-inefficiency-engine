from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from inefficiency_engine.evidence import WorkerHeartbeat


RUNTIME_INDEX_LABEL = "runtime_index_maintenance"
RUNTIME_INDEX_WORKER_ID = "source-coverage-runtime-index-maintenance"
RUNTIME_INDEX_STALE_AFTER_SECONDS = 1800.0
SOURCE_COVERAGE_REFRESH_LABEL = "source_coverage_refresh"
SOURCE_COVERAGE_REFRESH_WORKER_ID = "canonical-source-coverage-refresh"
SOURCE_COVERAGE_SNAPSHOT_LABEL = "source_coverage_snapshot"
SOURCE_COVERAGE_SNAPSHOT_WORKER_ID = "canonical-source-coverage-snapshot"
SOURCE_COVERAGE_EXECUTOR_WORKER_ID = "canonical-source-coverage-executor"
SOURCE_COVERAGE_HANDOFF_STALE_AFTER_SECONDS = 90.0
_INSTALL_MARKER = "_cie_runtime_index_health_observability_installed"


def _store(base: Any):
    try:
        return base._store()  # noqa: SLF001 - deploy-layer observability hook
    except Exception:
        return None


def _heartbeat(base: Any, worker_id: str) -> WorkerHeartbeat | None:
    store = _store(base)
    if store is None:
        return None
    try:
        return store.latest_worker_heartbeat(worker_id)
    except Exception:
        return None


def _detail_payload(base: Any, worker_id: str) -> dict[str, object]:
    heartbeat = _heartbeat(base, worker_id)
    if heartbeat is None:
        return {}
    detail = getattr(heartbeat, "detail", None)
    return dict(detail) if isinstance(detail, dict) else {}


def _latest_terminal_refresh(base: Any) -> WorkerHeartbeat | None:
    """Read the newest completed refresh even while the next attempt is running."""

    store = _store(base)
    if store is None:
        return None
    try:
        query = (
            select(store.worker_heartbeats.c.payload_json)
            .where(
                store.worker_heartbeats.c.worker_id == SOURCE_COVERAGE_REFRESH_WORKER_ID,
                store.worker_heartbeats.c.state != "running",
            )
            .order_by(store.worker_heartbeats.c.id.desc())
            .limit(1)
        )
        with store.engine.connect() as db:
            payload = db.execute(query).scalar_one_or_none()
        return WorkerHeartbeat.model_validate_json(payload) if payload else None
    except Exception:
        return None


def _executor_stage_for_pid(base: Any, pid: object | None) -> WorkerHeartbeat | None:
    """Find the latest durable executor stage belonging to one OS process."""

    try:
        expected_pid = int(pid) if pid is not None else None
    except (TypeError, ValueError):
        expected_pid = None
    if expected_pid is None:
        return None

    store = _store(base)
    if store is None:
        return None
    try:
        query = (
            select(store.worker_heartbeats.c.payload_json)
            .where(store.worker_heartbeats.c.worker_id == SOURCE_COVERAGE_EXECUTOR_WORKER_ID)
            .order_by(store.worker_heartbeats.c.id.desc())
            .limit(64)
        )
        with store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
    except Exception:
        return None

    for payload in payloads:
        try:
            heartbeat = WorkerHeartbeat.model_validate_json(payload)
        except Exception:
            continue
        detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
        try:
            observed_pid = int(detail.get("executor_pid"))
        except (TypeError, ValueError):
            continue
        if observed_pid == expected_pid:
            return heartbeat
    return None


def _stage_fields(heartbeat: WorkerHeartbeat | None) -> dict[str, object]:
    if heartbeat is None:
        return {
            "stage": None,
            "observed_at": None,
            "age_seconds": None,
            "error_type": None,
            "message": None,
            "timings_seconds": None,
        }
    detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
    observed_at = heartbeat.observed_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (datetime.now(timezone.utc) - observed_at).total_seconds())
    timings = detail.get("stage_timings_seconds")
    return {
        "stage": detail.get("stage"),
        "observed_at": observed_at.isoformat(),
        "age_seconds": age_seconds,
        "error_type": heartbeat.error_type,
        "message": detail.get("message"),
        "timings_seconds": dict(timings) if isinstance(timings, dict) else None,
    }


def install_runtime_index_health_observability(base: Any) -> None:
    """Expose post-bind index and durable source-coverage state through public health.

    These supervisors already persist their own progress. This hook makes current and
    previous terminal source-refresh state visible without changing startup, freshness,
    provider, control, qualification, or trading behavior.
    """

    if bool(getattr(base, _INSTALL_MARKER, False)):
        return

    base._RUNTIME_HEARTBEATS[RUNTIME_INDEX_LABEL] = RUNTIME_INDEX_WORKER_ID  # noqa: SLF001
    base._RUNTIME_STALE_AFTER_SECONDS[RUNTIME_INDEX_LABEL] = (  # noqa: SLF001
        RUNTIME_INDEX_STALE_AFTER_SECONDS
    )
    base._RUNTIME_HEARTBEATS[SOURCE_COVERAGE_REFRESH_LABEL] = (  # noqa: SLF001
        SOURCE_COVERAGE_REFRESH_WORKER_ID
    )
    base._RUNTIME_HEARTBEATS[SOURCE_COVERAGE_SNAPSHOT_LABEL] = (  # noqa: SLF001
        SOURCE_COVERAGE_SNAPSHOT_WORKER_ID
    )

    original = base._runtime_heartbeats  # noqa: SLF001

    def runtime_heartbeats_with_index_gate() -> dict[str, object]:
        payload = original()
        workers = payload.get("workers")
        if not isinstance(workers, dict):
            return payload

        index_worker = workers.get(RUNTIME_INDEX_LABEL)
        if isinstance(index_worker, dict) and bool(index_worker.get("available")):
            detail = _detail_payload(base, RUNTIME_INDEX_WORKER_ID)
            result = detail.get("result") if isinstance(detail.get("result"), dict) else {}
            failures = result.get("failures") if isinstance(result, dict) else None
            index_worker.update(
                {
                    "attempt": detail.get("attempt"),
                    "scope": detail.get("scope"),
                    "current_index": detail.get("current_index"),
                    "current_table": detail.get("current_table"),
                    "current_index_runtime_seconds": detail.get(
                        "current_index_runtime_seconds"
                    ),
                    "current_index_ok": detail.get("current_index_ok"),
                    "current_index_concurrent": detail.get("current_index_concurrent"),
                    "message": detail.get("message"),
                    "control_gate_released": detail.get("control_gate_released"),
                    "background_indexes_complete": detail.get(
                        "background_indexes_complete"
                    ),
                    "maintenance_runtime_seconds": detail.get("runtime_seconds"),
                    "maintenance_result_complete": (
                        result.get("complete") if isinstance(result, dict) else None
                    ),
                    "maintenance_dialect": (
                        result.get("dialect") if isinstance(result, dict) else None
                    ),
                    "maintenance_failures": (
                        failures if isinstance(failures, list) else []
                    ),
                }
            )
            workers[RUNTIME_INDEX_LABEL] = index_worker

        refresh_worker = workers.get(SOURCE_COVERAGE_REFRESH_LABEL)
        if isinstance(refresh_worker, dict) and bool(refresh_worker.get("available")):
            detail = _detail_payload(base, SOURCE_COVERAGE_REFRESH_WORKER_ID)
            terminal = _latest_terminal_refresh(base)
            terminal_detail = (
                terminal.detail
                if terminal is not None and isinstance(terminal.detail, dict)
                else {}
            )
            current_stage = _stage_fields(
                _executor_stage_for_pid(base, detail.get("executor_pid"))
            )
            terminal_stage = _stage_fields(
                _executor_stage_for_pid(base, terminal_detail.get("executor_pid"))
            )
            snapshot_heartbeat = _heartbeat(base, SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
            refresh_worker.update(
                {
                    "ok": detail.get("ok"),
                    "return_code": detail.get("return_code"),
                    "executor_pid": detail.get("executor_pid"),
                    "executor_runtime_seconds": detail.get("executor_runtime_seconds"),
                    "executor_deadline_seconds": detail.get("executor_deadline_seconds"),
                    "executor_terminated": detail.get("executor_terminated"),
                    "executor_killed": detail.get("executor_killed"),
                    "executor_current_stage": current_stage["stage"],
                    "executor_stage_observed_at": current_stage["observed_at"],
                    "executor_stage_age_seconds": current_stage["age_seconds"],
                    "executor_stage_error_type": current_stage["error_type"],
                    "executor_stage_error_message": current_stage["message"],
                    "executor_stage_timings_seconds": current_stage["timings_seconds"],
                    "last_refresh_result": (
                        None
                        if terminal is None
                        else "success"
                        if terminal.state == "success"
                        else "failed"
                    ),
                    "last_refresh_error_type": (
                        terminal.error_type if terminal is not None else None
                    ),
                    "last_refresh_error_message": (
                        terminal_detail.get("message") or terminal_stage["message"]
                    ),
                    "last_refresh_runtime_seconds": terminal_detail.get(
                        "executor_runtime_seconds"
                    ),
                    "last_refresh_return_code": terminal_detail.get("return_code"),
                    "last_refresh_completed_at": (
                        terminal.observed_at.isoformat() if terminal is not None else None
                    ),
                    "last_refresh_executor_pid": terminal_detail.get("executor_pid"),
                    "last_refresh_executor_stage": terminal_stage["stage"],
                    "last_refresh_executor_stage_observed_at": terminal_stage[
                        "observed_at"
                    ],
                    "last_refresh_executor_error_type": terminal_stage["error_type"],
                    "last_refresh_executor_error_message": terminal_stage["message"],
                    "last_refresh_executor_stage_timings_seconds": terminal_stage[
                        "timings_seconds"
                    ],
                    "last_successful_publication_at": (
                        snapshot_heartbeat.observed_at.isoformat()
                        if snapshot_heartbeat is not None
                        else None
                    ),
                    "independent_publication_cadence": detail.get(
                        "independent_publication_cadence"
                    ),
                    "provider_requests_allowed": detail.get(
                        "provider_requests_allowed"
                    ),
                    "provider_requests_used": detail.get("provider_requests_used"),
                    "qualification_thresholds_unchanged": detail.get(
                        "qualification_thresholds_unchanged"
                    ),
                    "paper_only": detail.get("paper_only"),
                }
            )
            workers[SOURCE_COVERAGE_REFRESH_LABEL] = refresh_worker

        snapshot_worker = workers.get(SOURCE_COVERAGE_SNAPSHOT_LABEL)
        if isinstance(snapshot_worker, dict) and bool(snapshot_worker.get("available")):
            detail = _detail_payload(base, SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
            publication_age_seconds = snapshot_worker.get("age_seconds")
            try:
                handoff_stale = (
                    float(publication_age_seconds)
                    > SOURCE_COVERAGE_HANDOFF_STALE_AFTER_SECONDS
                )
            except (TypeError, ValueError):
                handoff_stale = None
            snapshot_worker.update(
                {
                    "snapshot_observed_at": detail.get("snapshot_observed_at"),
                    "publication_owner": detail.get("publication_owner"),
                    "persisted_complete_snapshot": detail.get(
                        "persisted_complete_snapshot"
                    ),
                    "lane_count": detail.get("lane_count"),
                    "sufficient_lane_count": detail.get("sufficient_lane_count"),
                    "forward_test_eligible_lane_count": detail.get(
                        "forward_test_eligible_lane_count"
                    ),
                    "allocation_source_qualified_lane_count": detail.get(
                        "allocation_source_qualified_lane_count"
                    ),
                    "publication_age_seconds": publication_age_seconds,
                    "handoff_stale_after_seconds": (
                        SOURCE_COVERAGE_HANDOFF_STALE_AFTER_SECONDS
                    ),
                    "handoff_stale": handoff_stale,
                    "qualification_thresholds_unchanged": detail.get(
                        "qualification_thresholds_unchanged"
                    ),
                    "paper_only": detail.get("paper_only"),
                }
            )
            workers[SOURCE_COVERAGE_SNAPSHOT_LABEL] = snapshot_worker

        payload["runtime_index_gate_observability"] = True
        payload["source_coverage_refresh_observability"] = True
        payload["source_coverage_snapshot_observability"] = True
        payload["source_coverage_executor_stage_observability"] = True
        payload["source_coverage_executor_timing_observability"] = True
        return payload

    base._runtime_heartbeats = runtime_heartbeats_with_index_gate  # type: ignore[attr-defined]  # noqa: SLF001
    setattr(base, _INSTALL_MARKER, True)

from __future__ import annotations

from typing import Any


RUNTIME_INDEX_LABEL = "runtime_index_maintenance"
RUNTIME_INDEX_WORKER_ID = "source-coverage-runtime-index-maintenance"
RUNTIME_INDEX_STALE_AFTER_SECONDS = 1800.0
SOURCE_COVERAGE_REFRESH_LABEL = "source_coverage_refresh"
SOURCE_COVERAGE_REFRESH_WORKER_ID = "canonical-source-coverage-refresh"
SOURCE_COVERAGE_SNAPSHOT_LABEL = "source_coverage_snapshot"
SOURCE_COVERAGE_SNAPSHOT_WORKER_ID = "canonical-source-coverage-snapshot"
SOURCE_COVERAGE_HANDOFF_STALE_AFTER_SECONDS = 90.0
_INSTALL_MARKER = "_cie_runtime_index_health_observability_installed"


def _detail_payload(base: Any, worker_id: str) -> dict[str, object]:
    try:
        store = base._store()  # noqa: SLF001 - deploy-layer observability hook
    except Exception:
        return {}
    if store is None:
        return {}
    try:
        heartbeat = store.latest_worker_heartbeat(worker_id)
    except Exception:
        return {}
    if heartbeat is None:
        return {}
    detail = getattr(heartbeat, "detail", None)
    return dict(detail) if isinstance(detail, dict) else {}


def install_runtime_index_health_observability(base: Any) -> None:
    """Expose post-bind index and durable source-coverage state through public health.

    These supervisors already persist their own progress. This hook only makes that
    durable state visible through the existing runtime heartbeat contract; it does not
    change startup, freshness, provider, control, qualification, or trading behavior.
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
            refresh_worker.update(
                {
                    "ok": detail.get("ok"),
                    "return_code": detail.get("return_code"),
                    "executor_pid": detail.get("executor_pid"),
                    "executor_runtime_seconds": detail.get("executor_runtime_seconds"),
                    "executor_deadline_seconds": detail.get("executor_deadline_seconds"),
                    "executor_terminated": detail.get("executor_terminated"),
                    "executor_killed": detail.get("executor_killed"),
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
        return payload

    base._runtime_heartbeats = runtime_heartbeats_with_index_gate  # type: ignore[attr-defined]  # noqa: SLF001
    setattr(base, _INSTALL_MARKER, True)

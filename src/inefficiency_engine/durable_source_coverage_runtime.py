from __future__ import annotations

import contextvars
import os
from datetime import datetime, timezone
from typing import Any


SOURCE_COVERAGE_SNAPSHOT_WORKER_ID = "canonical-source-coverage-snapshot"
_DEFAULT_MAX_SNAPSHOT_AGE_SECONDS = 90.0
_PUBLISH_CONTEXT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "cie_source_coverage_snapshot_publish_context",
    default=False,
)
_PRIORITY_PATCH_MARKER = "_cie_durable_source_coverage_priority_context"
_SNAPSHOT_PUBLISH_PATCH_MARKER = "_cie_durable_source_coverage_snapshot_publisher"
_CONTROL_READ_PATCH_MARKER = "_cie_durable_source_coverage_control_reader"


class DurableSourceCoverageSnapshotMissing(RuntimeError):
    """No persisted canonical source-coverage snapshot is available yet."""


class DurableSourceCoverageSnapshotStale(RuntimeError):
    """The latest persisted source-coverage snapshot is too old for control use."""


class DurableSourceCoverageSnapshotInvalid(RuntimeError):
    """The persisted source-coverage payload cannot be validated."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def source_coverage_snapshot_max_age_seconds() -> float:
    raw = os.getenv(
        "CIE_CONTROL_SOURCE_COVERAGE_SNAPSHOT_MAX_AGE_SECONDS",
        str(_DEFAULT_MAX_SNAPSHOT_AGE_SECONDS),
    )
    try:
        value = float(raw)
    except ValueError:
        value = _DEFAULT_MAX_SNAPSHOT_AGE_SECONDS
    return max(30.0, value)


def persist_source_coverage_snapshot(store: Any, snapshot: Any) -> bool:
    """Publish one complete source-coverage snapshot through the durable heartbeat ledger."""

    try:
        payload = snapshot.model_dump(mode="json")
        store.record_worker_heartbeat(
            worker_id=SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
            state="success",
            detail={
                "snapshot": payload,
                "snapshot_observed_at": snapshot.observed_at.isoformat(),
                "lane_count": int(snapshot.lane_count),
                "sufficient_lane_count": int(snapshot.sufficient_lane_count),
                "forward_test_eligible_lane_count": int(
                    snapshot.forward_test_eligible_lane_count
                ),
                "allocation_source_qualified_lane_count": int(
                    snapshot.allocation_source_qualified_lane_count
                ),
                "publication_owner": "priority-source-coverage",
                "persisted_complete_snapshot": True,
                "qualification_thresholds_unchanged": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        return False
    return True


def _revalidated_snapshot(snapshot: Any, *, now: datetime) -> Any:
    """Age persisted source truth in memory without ever promoting prior eligibility.

    A persisted snapshot may be tens of seconds old. Recompute freshness from each
    source's original evidence-class TTL before canonical control consumes it. A row
    that was not admitted when the source owner created the snapshot can never become
    admitted here; this reader can only preserve or demote prior source eligibility.
    """

    from inefficiency_engine.source_coverage import LaneSourceCoverage

    lanes: list[LaneSourceCoverage] = []
    for lane in snapshot.lanes:
        source_rows: list[dict[str, object]] = []
        for original in list(lane.sources or []):
            row = dict(original)
            observed = _time(row.get("observed_at"))
            try:
                ttl_seconds = float(row.get("freshness_ttl_seconds") or 0.0)
            except (TypeError, ValueError):
                ttl_seconds = 0.0
            age_seconds = (
                max(0.0, (now - observed).total_seconds())
                if observed is not None
                else None
            )
            healthy = bool(row.get("healthy"))
            fresh = bool(
                healthy
                and age_seconds is not None
                and ttl_seconds > 0.0
                and age_seconds <= ttl_seconds
            )
            # Fail closed: time can revoke prior admission, never create a new one.
            admitted = bool(row.get("admitted")) and fresh
            if observed is not None:
                state = "healthy" if fresh else "stale" if healthy else "failed"
            else:
                state = str(row.get("state") or "unobserved")
            row.update(
                {
                    "state": state,
                    "fresh": fresh,
                    "admitted": admitted,
                    "age_seconds": age_seconds,
                    "age_hours": (
                        age_seconds / 3600.0 if age_seconds is not None else None
                    ),
                }
            )
            source_rows.append(row)

        admitted_rows = [row for row in source_rows if bool(row.get("admitted"))]
        covered = sorted(
            {
                str(cls)
                for row in admitted_rows
                for cls in list(row.get("classes") or [])
            }
        )
        required = [str(value) for value in lane.required_evidence_classes]
        missing = [value for value in required if value not in covered]
        groups = {
            str(row.get("group") or "")
            for row in admitted_rows
            if bool(row.get("authoritative")) and str(row.get("group") or "")
        }
        redundancy = len(groups) >= 2
        class_ok = not missing
        research_eligible = bool(admitted_rows)
        forward_test_eligible = research_eligible and class_ok
        allocation_source_qualified = forward_test_eligible and redundancy
        state = (
            "sufficient"
            if allocation_source_qualified
            else "provider_gap"
            if not admitted_rows
            else "evidence_class_gap"
            if missing
            else "concentration_risk"
        )
        lanes.append(
            lane.model_copy(
                update={
                    "covered_evidence_classes": covered,
                    "missing_evidence_classes": missing,
                    "healthy_source_count": len(admitted_rows),
                    "independent_authoritative_source_count": len(groups),
                    "source_redundancy_satisfied": redundancy,
                    "evidence_class_coverage_satisfied": class_ok,
                    "research_eligible": research_eligible,
                    "forward_test_eligible": forward_test_eligible,
                    "allocation_source_qualified": allocation_source_qualified,
                    "source_layer_sufficient": allocation_source_qualified,
                    "source_state": state,
                    "sources": source_rows,
                }
            )
        )

    return snapshot.model_copy(
        update={
            "lane_count": len(lanes),
            "sufficient_lane_count": sum(row.source_layer_sufficient for row in lanes),
            "insufficient_lane_count": sum(
                not row.source_layer_sufficient for row in lanes
            ),
            "research_eligible_lane_count": sum(row.research_eligible for row in lanes),
            "forward_test_eligible_lane_count": sum(
                row.forward_test_eligible for row in lanes
            ),
            "allocation_source_qualified_lane_count": sum(
                row.allocation_source_qualified for row in lanes
            ),
            "lanes": lanes,
        }
    )


def load_persisted_source_coverage_snapshot(
    store: Any,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
):
    """Read one persisted complete source snapshot and re-age it fail-closed in memory."""

    from inefficiency_engine.source_coverage import SourceCoverageSnapshot

    heartbeat = store.latest_worker_heartbeat(SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
    if heartbeat is None:
        raise DurableSourceCoverageSnapshotMissing(
            "canonical source-coverage snapshot has not been published"
        )
    detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
    payload = detail.get("snapshot")
    if not isinstance(payload, dict):
        raise DurableSourceCoverageSnapshotInvalid(
            "canonical source-coverage heartbeat does not contain a complete snapshot"
        )
    try:
        snapshot = SourceCoverageSnapshot.model_validate(payload)
    except Exception as exc:
        raise DurableSourceCoverageSnapshotInvalid(
            "canonical source-coverage snapshot payload is invalid"
        ) from exc

    current = now or _now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    observed = snapshot.observed_at
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    age_seconds = max(
        0.0,
        (current.astimezone(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds(),
    )
    allowed_age = (
        source_coverage_snapshot_max_age_seconds()
        if max_age_seconds is None
        else max(1.0, float(max_age_seconds))
    )
    if age_seconds > allowed_age:
        raise DurableSourceCoverageSnapshotStale(
            f"canonical source-coverage snapshot is {age_seconds:.1f}s old "
            f"(limit {allowed_age:.1f}s)"
        )
    return _revalidated_snapshot(snapshot, now=current)


def install_source_coverage_snapshot_publisher_runtime() -> None:
    """Persist snapshots computed inside real priority-source cycles without recomputing them."""

    from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService
    from inefficiency_engine.source_coverage import SourceCoveragePlane

    if not bool(getattr(PrioritySourceCollectionService, _PRIORITY_PATCH_MARKER, False)):
        original_run_cycle = PrioritySourceCollectionService.run_cycle

        async def run_cycle_with_snapshot_publication(self, *args, **kwargs):
            token = _PUBLISH_CONTEXT.set(True)
            try:
                return await original_run_cycle(self, *args, **kwargs)
            finally:
                _PUBLISH_CONTEXT.reset(token)

        PrioritySourceCollectionService.run_cycle = run_cycle_with_snapshot_publication  # type: ignore[method-assign]
        setattr(PrioritySourceCollectionService, _PRIORITY_PATCH_MARKER, True)

    if not bool(getattr(SourceCoveragePlane, _SNAPSHOT_PUBLISH_PATCH_MARKER, False)):
        original_snapshot = SourceCoveragePlane.snapshot

        def snapshot_with_publication(self, *args, **kwargs):
            snapshot = original_snapshot(self, *args, **kwargs)
            if _PUBLISH_CONTEXT.get():
                persist_source_coverage_snapshot(self.store, snapshot)
            return snapshot

        SourceCoveragePlane.snapshot = snapshot_with_publication  # type: ignore[method-assign]
        setattr(SourceCoveragePlane, _SNAPSHOT_PUBLISH_PATCH_MARKER, True)


def install_control_source_coverage_snapshot_reader_runtime() -> None:
    """Make canonical control consume persisted source truth instead of rebuilding it."""

    from inefficiency_engine.source_coverage import SourceCoveragePlane

    if bool(getattr(SourceCoveragePlane, _CONTROL_READ_PATCH_MARKER, False)):
        return

    def persisted_snapshot(self, *, now: datetime | None = None):
        return load_persisted_source_coverage_snapshot(self.store, now=now)

    SourceCoveragePlane.snapshot = persisted_snapshot  # type: ignore[method-assign]
    setattr(SourceCoveragePlane, _CONTROL_READ_PATCH_MARKER, True)

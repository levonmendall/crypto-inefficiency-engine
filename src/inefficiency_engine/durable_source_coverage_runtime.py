from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable


SOURCE_COVERAGE_SNAPSHOT_WORKER_ID = "canonical-source-coverage-snapshot"
SOURCE_COVERAGE_REFRESH_WORKER_ID = "canonical-source-coverage-refresh"
_DEFAULT_MAX_SNAPSHOT_AGE_SECONDS = 90.0
_DEFAULT_REFRESH_INTERVAL_SECONDS = 30.0
_DEFAULT_REFRESH_DEADLINE_SECONDS = 45.0
_DEFAULT_TERMINATE_GRACE_SECONDS = 2.0
_SNAPSHOT_PUBLISH_PATCH_MARKER = "_cie_durable_source_coverage_snapshot_publisher"
_CONTROL_READ_PATCH_MARKER = "_cie_durable_source_coverage_control_reader"
_PERMANENT_SOURCE_REFRESH_PATCH_MARKER = "_cie_durable_source_coverage_refresh_cadence"
_SOURCE_COVERAGE_REFRESH_TASK: asyncio.Task[None] | None = None


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


def _env_float(name: str, default: float, *, minimum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value)


def source_coverage_snapshot_max_age_seconds() -> float:
    return _env_float(
        "CIE_CONTROL_SOURCE_COVERAGE_SNAPSHOT_MAX_AGE_SECONDS",
        _DEFAULT_MAX_SNAPSHOT_AGE_SECONDS,
        minimum=30.0,
    )


def source_coverage_refresh_interval_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_COVERAGE_REFRESH_INTERVAL_SECONDS",
        _DEFAULT_REFRESH_INTERVAL_SECONDS,
        minimum=10.0,
    )


def source_coverage_refresh_deadline_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_COVERAGE_REFRESH_DEADLINE_SECONDS",
        _DEFAULT_REFRESH_DEADLINE_SECONDS,
        minimum=5.0,
    )


def source_coverage_terminate_grace_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_COVERAGE_TERMINATE_GRACE_SECONDS",
        _DEFAULT_TERMINATE_GRACE_SECONDS,
        minimum=0.1,
    )


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
                "publication_owner": "source-coverage-reconciliation",
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

    The durable handoff may be newer than the point-in-time calculation timestamp.
    Every individual source observation is therefore re-aged against its unchanged
    evidence-class TTL. Time can revoke an admission, never create a new one.
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
    """Read one complete persisted source snapshot and re-age it fail closed.

    Handoff freshness is measured from the durable publication heartbeat, not the
    snapshot's calculation timestamp. The calculation timestamp is still preserved,
    while every source observation is re-aged independently against its original TTL.
    """

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
    published_at = heartbeat.observed_at
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    handoff_age_seconds = max(
        0.0,
        (
            current.astimezone(timezone.utc)
            - published_at.astimezone(timezone.utc)
        ).total_seconds(),
    )
    allowed_age = (
        source_coverage_snapshot_max_age_seconds()
        if max_age_seconds is None
        else max(1.0, float(max_age_seconds))
    )
    if handoff_age_seconds > allowed_age:
        raise DurableSourceCoverageSnapshotStale(
            f"canonical source-coverage publication is {handoff_age_seconds:.1f}s old "
            f"(limit {allowed_age:.1f}s)"
        )
    return _revalidated_snapshot(snapshot, now=current)


async def _terminate_snapshot_process(
    process: asyncio.subprocess.Process,
    *,
    grace_seconds: float,
) -> tuple[bool, bool]:
    if process.returncode is not None:
        return False, False
    terminated = True
    killed = False
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=max(0.1, grace_seconds))
    except TimeoutError:
        if process.returncode is None:
            killed = True
            process.kill()
        await process.wait()
    return terminated, killed


def _snapshot_child_preexec() -> Callable[[], None] | None:
    if not sys.platform.startswith("linux"):
        return None
    from inefficiency_engine.control_cycle_runtime import _linux_parent_death_signal

    return _linux_parent_death_signal


async def _run_one_source_coverage_refresh(
    store: Any,
    *,
    sequence: int,
    deadline_seconds: float | None = None,
    process_factory: Callable[..., Awaitable[asyncio.subprocess.Process]] | None = None,
) -> dict[str, object]:
    """Rebuild one source snapshot in a disposable OS process with a real deadline."""

    deadline = (
        source_coverage_refresh_deadline_seconds()
        if deadline_seconds is None
        else max(0.01, float(deadline_seconds))
    )
    factory = process_factory or asyncio.create_subprocess_exec
    started = time.monotonic()
    process = await factory(
        sys.executable,
        "-m",
        "inefficiency_engine.source_coverage_snapshot_executor",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
        preexec_fn=_snapshot_child_preexec(),
    )
    try:
        try:
            store.record_worker_heartbeat(
                worker_id=SOURCE_COVERAGE_REFRESH_WORKER_ID,
                state="running",
                detail={
                    "sequence": int(sequence),
                    "stage": "source_coverage_snapshot_executor",
                    "executor_pid": int(process.pid),
                    "executor_deadline_seconds": deadline,
                    "independent_publication_cadence": True,
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception:
            pass

        try:
            return_code = await asyncio.wait_for(process.wait(), timeout=deadline)
        except TimeoutError:
            terminated, killed = await _terminate_snapshot_process(
                process,
                grace_seconds=source_coverage_terminate_grace_seconds(),
            )
            runtime = max(0.0, time.monotonic() - started)
            result = {
                "ok": False,
                "error_type": "SourceCoverageSnapshotRefreshDeadlineExceeded",
                "return_code": process.returncode,
                "executor_pid": int(process.pid),
                "executor_runtime_seconds": runtime,
                "executor_deadline_seconds": deadline,
                "executor_terminated": terminated,
                "executor_killed": killed,
            }
        else:
            runtime = max(0.0, time.monotonic() - started)
            result = {
                "ok": int(return_code or 0) == 0,
                "error_type": (
                    None
                    if int(return_code or 0) == 0
                    else "SourceCoverageSnapshotRefreshExitedNonzero"
                ),
                "return_code": return_code,
                "executor_pid": int(process.pid),
                "executor_runtime_seconds": runtime,
                "executor_deadline_seconds": deadline,
                "executor_terminated": False,
                "executor_killed": False,
            }
    except asyncio.CancelledError:
        await _terminate_snapshot_process(
            process,
            grace_seconds=source_coverage_terminate_grace_seconds(),
        )
        raise

    try:
        store.record_worker_heartbeat(
            worker_id=SOURCE_COVERAGE_REFRESH_WORKER_ID,
            state="success" if bool(result["ok"]) else "degraded",
            error_type=(
                None if bool(result["ok"]) else str(result.get("error_type") or "")
            ),
            detail={
                "sequence": int(sequence),
                "stage": (
                    "source_coverage_snapshot_published"
                    if bool(result["ok"])
                    else "source_coverage_snapshot_refresh_failed"
                ),
                **result,
                "independent_publication_cadence": True,
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "qualification_thresholds_unchanged": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        pass
    return result


async def _source_coverage_snapshot_refresh_loop(store: Any) -> None:
    """Refresh durable coverage independently of slow priority-provider collection."""

    sequence = 0
    while True:
        sequence += 1
        started = time.monotonic()
        try:
            await _run_one_source_coverage_refresh(store, sequence=sequence)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=SOURCE_COVERAGE_REFRESH_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "sequence": int(sequence),
                        "stage": "source_coverage_snapshot_refresh_failed",
                        "message": str(exc)[:500],
                        "retrying": True,
                        "independent_publication_cadence": True,
                        "provider_requests_allowed": False,
                        "provider_requests_used": 0,
                        "qualification_thresholds_unchanged": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
        elapsed = max(0.0, time.monotonic() - started)
        delay = max(0.0, source_coverage_refresh_interval_seconds() - elapsed)
        await asyncio.sleep(delay)


def _ensure_source_coverage_snapshot_refresh_loop(store: Any) -> None:
    global _SOURCE_COVERAGE_REFRESH_TASK

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = _SOURCE_COVERAGE_REFRESH_TASK
    if task is not None and not task.done():
        return
    _SOURCE_COVERAGE_REFRESH_TASK = loop.create_task(
        _source_coverage_snapshot_refresh_loop(store),
        name="source-coverage-snapshot-refresh",
    )


def install_source_coverage_snapshot_publisher_runtime() -> None:
    """Persist complete source snapshots and maintain an independent refresh cadence.

    Existing source calculations still publish at their natural boundaries. In the
    permanent executable-source process, the first market/L2 refresh also starts one
    process-local background supervisor. Every refresh calculation runs in a separate
    OS process, so a slow SQLAlchemy/DBAPI/native call cannot block market acquisition.
    """

    from inefficiency_engine.source_coverage import SourceCoveragePlane

    if not bool(getattr(SourceCoveragePlane, _SNAPSHOT_PUBLISH_PATCH_MARKER, False)):
        original_snapshot = SourceCoveragePlane.snapshot

        def snapshot_with_publication(self, *args, **kwargs):
            snapshot = original_snapshot(self, *args, **kwargs)
            persist_source_coverage_snapshot(self.store, snapshot)
            return snapshot

        SourceCoveragePlane.snapshot = snapshot_with_publication  # type: ignore[method-assign]
        setattr(SourceCoveragePlane, _SNAPSHOT_PUBLISH_PATCH_MARKER, True)

    from inefficiency_engine.permanent_source_plane import PermanentSourcePlane

    if not bool(
        getattr(PermanentSourcePlane, _PERMANENT_SOURCE_REFRESH_PATCH_MARKER, False)
    ):
        original_refresh = PermanentSourcePlane.refresh_market_l2_snapshot

        async def refresh_with_coverage_cadence(self, *args, **kwargs):
            _ensure_source_coverage_snapshot_refresh_loop(self.store)
            return await original_refresh(self, *args, **kwargs)

        PermanentSourcePlane.refresh_market_l2_snapshot = refresh_with_coverage_cadence  # type: ignore[method-assign]
        setattr(
            PermanentSourcePlane,
            _PERMANENT_SOURCE_REFRESH_PATCH_MARKER,
            True,
        )


def install_control_source_coverage_snapshot_reader_runtime() -> None:
    """Make canonical control consume persisted source truth instead of rebuilding it."""

    from inefficiency_engine.source_coverage import SourceCoveragePlane

    if bool(getattr(SourceCoveragePlane, _CONTROL_READ_PATCH_MARKER, False)):
        return

    def persisted_snapshot(self, *, now: datetime | None = None):
        return load_persisted_source_coverage_snapshot(self.store, now=now)

    SourceCoveragePlane.snapshot = persisted_snapshot  # type: ignore[method-assign]
    setattr(SourceCoveragePlane, _CONTROL_READ_PATCH_MARKER, True)

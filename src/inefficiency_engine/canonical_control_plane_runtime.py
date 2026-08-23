from __future__ import annotations

import asyncio
import time

from inefficiency_engine.canonical_paper_portfolio import CANONICAL_INITIAL_CAPITAL_USD
from inefficiency_engine.dashboard_projection import DASHBOARD_RESEARCH_PROJECTION_WORKER_ID


PORTFOLIO_WORKER_ID = "canonical-portfolio-operating-loop"


def portfolio_nav(store) -> float:
    """Read the latest canonical NAV without granting allocation authority."""

    try:
        heartbeat = store.latest_worker_heartbeat(PORTFOLIO_WORKER_ID)
    except Exception:
        heartbeat = None
    detail = getattr(heartbeat, "detail", {}) or {}
    value = detail.get("portfolio_nav_usd") if isinstance(detail, dict) else None
    if isinstance(value, (int, float)) and float(value) > 0:
        return float(value)
    return float(CANONICAL_INITIAL_CAPITAL_USD)


async def refresh_canonical_control_plane(
    *,
    store,
    operating_certification,
    qualified_bridge,
    research_projection,
    settings,
    bridge_snapshot=None,
) -> dict[str, object]:
    """Advance operating truth -> qualified bridge -> dashboard from durable evidence.

    The caller owns no market acquisition. Reconciliation reads append-only durable
    state, bridge publication consumes persisted source/research evidence, and the
    dashboard projection is published only after reconciliation succeeds. The helper
    therefore remains safe to run in a dedicated control process with provider work
    explicitly disabled.
    """

    result: dict[str, object] = {
        "canonical_control_plane_refresh": True,
        "operating_reconciliation_complete": False,
        "qualified_bridge_publication_complete": False,
        "research_projection_publication_complete": False,
        "control_plane_errors": {},
        "control_stage_timings_seconds": {},
    }
    errors: dict[str, str] = {}
    timings: dict[str, float] = {}
    cycle_started = time.monotonic()

    stage_started = time.monotonic()
    try:
        reconciled = await asyncio.to_thread(
            operating_certification.reconcile_latest_runtime_truth
        )
        if reconciled is None:
            errors["operating_reconciliation"] = "OperatingSnapshotUnavailable"
        else:
            result["operating_reconciliation_complete"] = True
            result["operating_snapshot_id"] = reconciled.snapshot_id
            result["operating_observed_at"] = reconciled.observed_at.isoformat()
    except Exception as exc:
        errors["operating_reconciliation"] = type(exc).__name__
    finally:
        timings["operating_reconciliation"] = max(0.0, time.monotonic() - stage_started)

    if result["operating_reconciliation_complete"]:
        original_latest_scan = getattr(qualified_bridge, "_latest_scan", None)
        stage_started = time.monotonic()
        try:
            if bridge_snapshot is not None and callable(original_latest_scan):
                qualified_bridge._latest_scan = lambda: bridge_snapshot
            bridge = await qualified_bridge.publish_latest(
                total_capital_usd=portfolio_nav(store)
            )
            result["qualified_bridge_publication_complete"] = True
            result["qualified_bridge_published"] = bridge is not None
            result["qualified_bridge_candidate_count"] = (
                len(bridge.candidates) if bridge is not None else 0
            )
            result["qualified_bridge_observed_at"] = (
                bridge.observed_at.isoformat() if bridge is not None else None
            )
            result["qualified_bridge_used_current_bounded_snapshot"] = bridge_snapshot is not None
        except Exception as exc:
            errors["qualified_bridge_publication"] = type(exc).__name__
        finally:
            timings["qualified_bridge_publication"] = max(
                0.0,
                time.monotonic() - stage_started,
            )
            if bridge_snapshot is not None and callable(original_latest_scan):
                qualified_bridge._latest_scan = original_latest_scan

        stage_started = time.monotonic()
        try:
            payload = await asyncio.to_thread(
                research_projection.publish,
                forward_target=max(1, int(settings.alpha_min_forward_samples)),
                settled_target=max(
                    5,
                    int(
                        getattr(
                            settings,
                            "operating_certification_min_settled_trials",
                            20,
                        )
                    ),
                ),
                shadow_horizons_seconds=tuple(
                    getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,)
                ),
                shadow_cycle_interval_seconds=float(settings.shadow_cycle_interval_seconds),
                alpha_evidence_every_cycles=max(1, int(settings.alpha_evidence_every_cycles)),
                heartbeat_stale_seconds=float(settings.worker_heartbeat_stale_seconds),
            )
            result["research_projection_publication_complete"] = True
            result["research_projection_observed_at"] = payload.get("observed_at")
            store.record_worker_heartbeat(
                worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                state="success",
                detail={
                    "projection_observed_at": payload.get("observed_at"),
                    "publication_stage": "permanent_control_plane_refresh",
                    "operating_reconciled_first": True,
                    "disposable_research_dependency": False,
                    "presentation_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            errors["research_projection_publication"] = type(exc).__name__
            try:
                store.record_worker_heartbeat(
                    worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "publication_stage": "permanent_control_plane_refresh",
                        "operating_reconciled_first": True,
                        "retrying": True,
                        "disposable_research_dependency": False,
                        "presentation_only": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
        finally:
            timings["research_projection_publication"] = max(
                0.0,
                time.monotonic() - stage_started,
            )

    timings["total"] = max(0.0, time.monotonic() - cycle_started)
    result["control_stage_timings_seconds"] = timings
    result["control_plane_errors"] = errors
    result["control_plane_healthy"] = not errors
    return result

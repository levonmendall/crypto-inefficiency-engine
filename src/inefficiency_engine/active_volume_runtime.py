from __future__ import annotations

import asyncio
import signal
from datetime import datetime, timezone
from typing import Any

from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_runtime import (
    CYCLE_HISTORY_MAINTENANCE_SECONDS,
    CYCLE_HISTORY_WORKER_ID,
    maintain_cycle_history_once,
    read_cycle_history_status,
)
from inefficiency_engine.cycle_probation import CycleHistoricalResearch
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.volume_universe import (
    TOP_VOLUME_ASSET_COUNT,
    VOLUME_UNIVERSE_REFRESH_SECONDS,
    read_latest_volume_universe,
    resolve_top_volume_assets,
    validated_volume_assets,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_observed_at(raw: object) -> datetime | None:
    if not raw:
        return None
    try:
        return _aware(datetime.fromisoformat(str(raw)))
    except (TypeError, ValueError):
        return None


def _unavailable_volume_status(
    *,
    reason: str,
    age_seconds: float | None = None,
    snapshot: dict[str, Any] | None = None,
    last_known_good_observed_at: datetime | None = None,
    last_known_good_asset_count: int = 0,
) -> dict[str, Any]:
    """Return no assets only when there is no complete validated cohort to show."""

    snapshot = snapshot or {}
    return {
        "available": False,
        "current_membership_available": False,
        "asset_count": 0,
        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
        "observed_at": None,
        "age_seconds": age_seconds,
        "stale": True,
        "unavailable_reason": reason,
        "refresh_interval_seconds": VOLUME_UNIVERSE_REFRESH_SECONDS,
        "method": snapshot.get("method") or "marketwide_24h_trading_volume_usd",
        "ranking_metric": snapshot.get("ranking_metric") or "reported_24h_trading_volume_usd",
        "ranking_source": snapshot.get("ranking_source"),
        "ranking_scope": snapshot.get("ranking_scope"),
        "volume_is_defining_metric": True,
        "stable_value_assets_excluded": bool(snapshot.get("stable_value_assets_excluded")),
        "fresh_snapshot_required_for_current_membership": True,
        "last_known_good_retained": bool(last_known_good_asset_count),
        "last_known_good_observed_at": (
            last_known_good_observed_at.isoformat()
            if last_known_good_observed_at is not None
            else None
        ),
        "last_known_good_age_seconds": age_seconds,
        "last_known_good_asset_count": int(last_known_good_asset_count),
        "assets": [],
        "paper_only": True,
        "allocation_authority": False,
    }


def read_active_volume_universe_status(
    store: EvidenceStore,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the latest complete validated market-wide top-volume cohort.

    The API remains database-only. A lightweight permanent task refreshes membership
    independently of historical/research jobs. If that source is briefly delayed,
    the last complete validated cohort remains visible with ``stale=true`` and
    ``current_membership_available=false`` rather than making the entire dashboard
    disappear. No stale cohort is silently represented as fresh/current.
    """

    current = _aware(now or _now())
    snapshot = read_latest_volume_universe(store)
    assets = validated_volume_assets(snapshot)
    if len(assets) != TOP_VOLUME_ASSET_COUNT or not isinstance(snapshot, dict):
        return _unavailable_volume_status(reason="no_validated_snapshot")

    observed_at = _parse_observed_at(snapshot.get("observed_at"))
    age_seconds = (
        max(0.0, (current - observed_at).total_seconds())
        if observed_at is not None
        else None
    )
    source_rows = snapshot.get("assets")
    rows: list[dict[str, Any]] = []
    if isinstance(source_rows, list):
        for row in source_rows:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "rank": int(row.get("rank") or 0),
                    "asset": str(row.get("asset") or "").upper(),
                    "reported_24h_volume_usd": float(
                        row.get("reported_24h_volume_usd")
                        or row.get("aggregate_24h_notional_usd")
                        or 0.0
                    ),
                    "source_asset_id": str(row.get("source_asset_id") or ""),
                }
            )

    if len(rows) != TOP_VOLUME_ASSET_COUNT:
        return _unavailable_volume_status(
            reason="validated_snapshot_projection_incomplete",
            age_seconds=age_seconds,
            snapshot=snapshot,
            last_known_good_observed_at=observed_at,
            last_known_good_asset_count=len(rows),
        )

    stale = bool(age_seconds is None or age_seconds > VOLUME_UNIVERSE_REFRESH_SECONDS)
    return {
        "available": True,
        "current_membership_available": not stale,
        "asset_count": len(rows),
        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
        "observed_at": observed_at.isoformat() if observed_at is not None else None,
        "age_seconds": age_seconds,
        "stale": stale,
        "unavailable_reason": "stale_snapshot" if stale else None,
        "refresh_interval_seconds": VOLUME_UNIVERSE_REFRESH_SECONDS,
        "method": snapshot.get("method"),
        "ranking_metric": snapshot.get("ranking_metric"),
        "ranking_source": snapshot.get("ranking_source"),
        "ranking_scope": snapshot.get("ranking_scope"),
        "volume_is_defining_metric": bool(snapshot.get("volume_is_defining_metric")),
        "stable_value_assets_excluded": bool(snapshot.get("stable_value_assets_excluded")),
        "fresh_snapshot_required_for_current_membership": True,
        "last_known_good_retained": True,
        "last_known_good_observed_at": observed_at.isoformat() if observed_at is not None else None,
        "last_known_good_age_seconds": age_seconds,
        "last_known_good_asset_count": len(rows),
        "assets": rows,
        "paper_only": True,
        "allocation_authority": False,
    }


def _pending_history_row(volume_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "asset": str(volume_row.get("asset") or "").upper(),
        "status": "pending",
        "complete": False,
        "quote_count": 0,
        "expected_quote_count": 0,
        "coverage_fraction": 0.0,
        "walk_forward_ready": False,
        "historical_replay_long_qualified": False,
        "last_error_type": None,
        "historical_counts_as_forward": False,
        "live_execution_authority": False,
    }


def read_active_cycle_history_status(
    store: EvidenceStore,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Project historical status onto the latest complete top-volume cohort only.

    Historical status rows remain durable if an asset leaves the cohort. When the
    latest validated volume snapshot is temporarily stale, its history cards remain
    visible but the payload explicitly marks the active universe stale/not-current.
    """

    volume = read_active_volume_universe_status(store, now=now)
    archived = read_cycle_history_status(store)
    archived_rows = archived.get("assets") if isinstance(archived, dict) else []
    if not isinstance(archived_rows, list):
        archived_rows = []

    if not volume.get("available"):
        return {
            "available": False,
            "active_universe_available": False,
            "active_universe_stale": bool(volume.get("stale")),
            "active_universe_current": False,
            "active_universe_unavailable_reason": volume.get("unavailable_reason"),
            "active_universe_observed_at": None,
            "active_universe_last_known_good_observed_at": volume.get(
                "last_known_good_observed_at"
            ),
            "active_universe_last_known_good_asset_count": int(
                volume.get("last_known_good_asset_count") or 0
            ),
            "universe_target_count": TOP_VOLUME_ASSET_COUNT,
            "maintenance_worker_id": CYCLE_HISTORY_WORKER_ID,
            "asset_count": 0,
            "complete_asset_count": 0,
            "walk_forward_ready_asset_count": 0,
            "historical_replay_qualified_asset_count": 0,
            "all_complete": False,
            "total_quote_count": 0,
            "expected_quote_count": 0,
            "overall_coverage_fraction": 0.0,
            "archived_status_asset_count": len(archived_rows),
            "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
            "fresh_snapshot_required_for_current_membership": True,
            "historical_counts_as_forward": False,
            "full_forward_promotion_gate_unchanged": True,
            "live_execution_authority": False,
            "assets": [],
        }

    by_asset = {
        str(row.get("asset") or "").upper(): row
        for row in archived_rows
        if isinstance(row, dict) and row.get("asset")
    }
    active_rows: list[dict[str, Any]] = []
    active_assets: set[str] = set()
    stale = bool(volume.get("stale"))
    for volume_row in volume.get("assets", []):
        if not isinstance(volume_row, dict):
            continue
        asset = str(volume_row.get("asset") or "").upper()
        if not asset:
            continue
        active_assets.add(asset)
        history_row = dict(by_asset.get(asset) or _pending_history_row(volume_row))
        history_row["asset"] = asset
        history_row["active_rank"] = int(volume_row.get("rank") or 0)
        history_row["reported_24h_volume_usd"] = float(
            volume_row.get("reported_24h_volume_usd") or 0.0
        )
        history_row["latest_validated_top_volume"] = True
        history_row["current_top_volume"] = not stale
        history_row["active_top25"] = bool(not stale and TOP_VOLUME_ASSET_COUNT == 25)
        active_rows.append(history_row)

    complete_count = sum(bool(row.get("complete")) for row in active_rows)
    replay_ready_count = sum(bool(row.get("walk_forward_ready")) for row in active_rows)
    replay_qualified_count = sum(
        bool(row.get("historical_replay_long_qualified")) for row in active_rows
    )
    total_expected = sum(int(row.get("expected_quote_count") or 0) for row in active_rows)
    total_quotes = sum(int(row.get("quote_count") or 0) for row in active_rows)
    archived_only = sum(
        1
        for row in archived_rows
        if isinstance(row, dict)
        and str(row.get("asset") or "").upper() not in active_assets
    )

    return {
        "available": bool(active_rows),
        "active_universe_available": True,
        "active_universe_observed_at": volume.get("observed_at"),
        "active_universe_stale": stale,
        "active_universe_current": not stale,
        "active_universe_unavailable_reason": volume.get("unavailable_reason"),
        "active_universe_last_known_good_observed_at": volume.get(
            "last_known_good_observed_at"
        ),
        "active_universe_last_known_good_asset_count": int(
            volume.get("last_known_good_asset_count") or 0
        ),
        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
        "maintenance_worker_id": CYCLE_HISTORY_WORKER_ID,
        "asset_count": len(active_rows),
        "complete_asset_count": complete_count,
        "walk_forward_ready_asset_count": replay_ready_count,
        "historical_replay_qualified_asset_count": replay_qualified_count,
        "all_complete": bool(active_rows) and complete_count == len(active_rows),
        "total_quote_count": total_quotes,
        "expected_quote_count": total_expected,
        "overall_coverage_fraction": (
            min(1.0, total_quotes / total_expected) if total_expected > 0 else 0.0
        ),
        "archived_status_asset_count": archived_only,
        "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
        "fresh_snapshot_required_for_current_membership": True,
        "historical_counts_as_forward": False,
        "full_forward_promotion_gate_unchanged": True,
        "live_execution_authority": False,
        "assets": active_rows,
    }


async def maintain_active_cycle_history_once(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
    research: CycleHistoricalResearch | None = None,
) -> dict[str, Any]:
    """Resolve the configured volume cohort, then maintain history for exactly it."""

    observed_at = _aware(now or _now())
    active_assets = await resolve_top_volume_assets(store, now=observed_at)
    if len(active_assets) != TOP_VOLUME_ASSET_COUNT:
        raise RuntimeError(
            f"active volume universe did not resolve exactly {TOP_VOLUME_ASSET_COUNT} assets"
        )
    await maintain_cycle_history_once(
        store,
        settings=settings,
        assets=active_assets,
        now=observed_at,
        research=research,
    )
    return read_active_cycle_history_status(store, now=observed_at)


async def maintenance_loop() -> None:
    store = build_evidence_store()
    if store is None:
        raise RuntimeError("active cycle-history maintenance requires durable evidence persistence")
    settings = Settings.from_env()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    store.record_worker_heartbeat(
        worker_id=CYCLE_HISTORY_WORKER_ID,
        state="starting",
        detail={
            "active_top_volume_universe": True,
            "universe_target_count": TOP_VOLUME_ASSET_COUNT,
            "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
            "historical_counts_as_forward": False,
        },
    )

    while not stop.is_set():
        try:
            await maintain_active_cycle_history_once(store, settings=settings)
        except Exception as exc:
            store.record_worker_heartbeat(
                worker_id=CYCLE_HISTORY_WORKER_ID,
                state="error",
                error_type=type(exc).__name__,
                detail={
                    "active_top_volume_universe": True,
                    "universe_target_count": TOP_VOLUME_ASSET_COUNT,
                    "retrying": True,
                    "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
                    "historical_counts_as_forward": False,
                },
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=CYCLE_HISTORY_MAINTENANCE_SECONDS)
        except TimeoutError:
            continue

    store.record_worker_heartbeat(
        worker_id=CYCLE_HISTORY_WORKER_ID,
        state="stopped",
        detail={
            "active_top_volume_universe": True,
            "universe_target_count": TOP_VOLUME_ASSET_COUNT,
            "historical_counts_as_forward": False,
        },
    )


def main() -> int:
    asyncio.run(maintenance_loop())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

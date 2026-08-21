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


def read_active_volume_universe_status(
    store: EvidenceStore,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the exact validated market-wide ranking used by live research.

    This is deliberately a database-only read. The API never performs a public
    market request. Refresh authority remains with the worker/history processes.
    """

    current = _aware(now or _now())
    snapshot = read_latest_volume_universe(store)
    assets = validated_volume_assets(snapshot)
    if len(assets) != TOP_VOLUME_ASSET_COUNT or not isinstance(snapshot, dict):
        return {
            "available": False,
            "asset_count": 0,
            "observed_at": None,
            "age_seconds": None,
            "stale": True,
            "refresh_interval_seconds": VOLUME_UNIVERSE_REFRESH_SECONDS,
            "method": "marketwide_24h_trading_volume_usd",
            "ranking_metric": "reported_24h_trading_volume_usd",
            "volume_is_defining_metric": True,
            "assets": [],
            "paper_only": True,
            "allocation_authority": False,
        }

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

    return {
        "available": len(rows) == TOP_VOLUME_ASSET_COUNT,
        "asset_count": len(rows),
        "observed_at": observed_at.isoformat() if observed_at is not None else None,
        "age_seconds": age_seconds,
        "stale": bool(
            age_seconds is None or age_seconds > VOLUME_UNIVERSE_REFRESH_SECONDS
        ),
        "refresh_interval_seconds": VOLUME_UNIVERSE_REFRESH_SECONDS,
        "method": snapshot.get("method"),
        "ranking_metric": snapshot.get("ranking_metric"),
        "ranking_source": snapshot.get("ranking_source"),
        "ranking_scope": snapshot.get("ranking_scope"),
        "volume_is_defining_metric": bool(snapshot.get("volume_is_defining_metric")),
        "stable_value_assets_excluded": bool(snapshot.get("stable_value_assets_excluded")),
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
    """Project historical status onto current top-40 membership only.

    Historical status rows are retained durably when an asset leaves the universe
    because they are useful if it later re-enters. They are never allowed to
    masquerade as current membership in the read plane.
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
        history_row["active_top40"] = True
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
        "active_universe_stale": bool(volume.get("stale")),
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
    """Refresh the canonical top 40 first, then maintain history for exactly it."""

    observed_at = _aware(now or _now())
    active_assets = await resolve_top_volume_assets(store, now=observed_at)
    if len(active_assets) != TOP_VOLUME_ASSET_COUNT:
        raise RuntimeError("active volume universe did not resolve exactly 40 assets")
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
            "active_top40_volume_universe": True,
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
                    "active_top40_volume_universe": True,
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
            "active_top40_volume_universe": True,
            "historical_counts_as_forward": False,
        },
    )


def main() -> int:
    asyncio.run(maintenance_loop())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

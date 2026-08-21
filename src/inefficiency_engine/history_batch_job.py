from __future__ import annotations

import os
from datetime import datetime, timezone

from inefficiency_engine.batched_cycle_history import BatchedCycleHistoricalResearch
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_runtime import (
    CYCLE_HISTORY_WORKER_ID,
    maintain_cycle_history_once,
    read_cycle_history_status,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.volume_universe import TOP_VOLUME_ASSET_COUNT, resolve_top_volume_assets


DEFAULT_HISTORY_BATCH_SIZE = 4
MAX_HISTORY_BATCH_SIZE = 8


def _parse_time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def select_history_batch(
    active_assets: tuple[str, ...],
    status: dict[str, object],
    *,
    batch_size: int,
) -> tuple[str, ...]:
    """Choose the least-recently-maintained active assets, incomplete first.

    A new top-volume entrant has no archived status row, so its epoch fallback makes
    it naturally outrank old incomplete rows and begin backfill on the first batch
    after membership changes.
    """

    rows = status.get("assets") if isinstance(status, dict) else []
    if not isinstance(rows, list):
        rows = []
    by_asset = {
        str(row.get("asset") or "").upper(): row
        for row in rows
        if isinstance(row, dict) and row.get("asset")
    }
    ordered = sorted(
        (str(asset).upper() for asset in active_assets),
        key=lambda asset: (
            bool((by_asset.get(asset) or {}).get("complete")),
            _parse_time((by_asset.get(asset) or {}).get("observed_at")),
            asset,
        ),
    )
    return tuple(ordered[: max(1, min(MAX_HISTORY_BATCH_SIZE, int(batch_size)))])


async def maintain_history_batch_once(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    batch_size: int | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Maintain a bounded slice of the authoritative top-volume cohort and exit.

    Membership now has an independent lightweight refresher, but this disposable
    history job still force-refreshes once before choosing its batch so research and
    the persisted ranking converge quickly after membership changes.
    """

    settings = settings or Settings.from_env()
    observed_at = now or datetime.now(timezone.utc)
    active_assets = await resolve_top_volume_assets(
        store,
        now=observed_at,
        force_refresh=True,
    )
    if len(active_assets) != TOP_VOLUME_ASSET_COUNT:
        raise RuntimeError(
            f"history batch requires an exact validated top-{TOP_VOLUME_ASSET_COUNT} universe"
        )

    if batch_size is None:
        try:
            batch_size = int(os.getenv("CIE_HISTORY_BATCH_SIZE", str(DEFAULT_HISTORY_BATCH_SIZE)))
        except ValueError:
            batch_size = DEFAULT_HISTORY_BATCH_SIZE
    batch = select_history_batch(
        active_assets,
        read_cycle_history_status(store),
        batch_size=batch_size,
    )
    if not batch:
        raise RuntimeError("history batch selection returned no active assets")

    research = BatchedCycleHistoricalResearch(store, active_assets=batch)
    summary = await maintain_cycle_history_once(
        store,
        settings=settings,
        assets=batch,
        now=observed_at,
        research=research,
    )
    store.record_worker_heartbeat(
        worker_id=CYCLE_HISTORY_WORKER_ID,
        state="success" if summary.get("all_complete") else "degraded",
        detail={
            "disposable_process": True,
            "batch_size": len(batch),
            "batch_assets": list(batch),
            "active_top_volume": True,
            "universe_target_count": TOP_VOLUME_ASSET_COUNT,
            "top_volume_force_refreshed": True,
            "asset_count": summary.get("asset_count", 0),
            "complete_asset_count": summary.get("complete_asset_count", 0),
            "overall_coverage_fraction": summary.get("overall_coverage_fraction", 0.0),
            "historical_counts_as_forward": False,
        },
        observed_at=observed_at,
    )
    return {
        **summary,
        "batch_assets": list(batch),
        "batch_size": len(batch),
        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
        "top_volume_force_refreshed": True,
        "disposable_process": True,
    }

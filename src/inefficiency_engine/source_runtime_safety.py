from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from sqlalchemy import insert, select

from inefficiency_engine.provider_gap_collection import (
    ProviderCatalogLedger,
    _deterministic_id,
)
from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService


_BULK_PATCH_MARKER = "_cie_bulk_catalog_runtime"
_RESEARCH_DELEGATION_MARKER = "_cie_research_source_delegation"


def _bulk_catalog_observe(
    self: ProviderCatalogLedger,
    *,
    provider: str,
    items: list[dict[str, object]],
    observed_at: datetime,
    source_reference: str,
) -> tuple[bool, list[dict[str, object]]]:
    """Persist an exchange catalog in O(1) database round trips, not O(n).

    Production source acquisition runs against remote PostgreSQL. The original
    implementation issued one existence query per catalog item, which can block an
    async provider loop for minutes on a several-hundred-product exchange catalog.
    This implementation loads existing keys once and inserts all new rows with one
    executemany operation while preserving first-seen semantics.
    """

    normalized: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    for item in items:
        category = str(item["category"])
        symbol = str(item["symbol"])
        asset = str(item["asset"]).upper()
        key = _deterministic_id(provider, category, symbol)
        if key in normalized:
            continue
        normalized[key] = (
            {
                "catalog_key": key,
                "provider": provider,
                "category": category,
                "symbol": symbol,
                "asset": asset,
                "first_seen_at": observed_at.isoformat(),
                "source_reference": source_reference,
            },
            dict(item),
        )

    with self.store.engine.begin() as db:
        existing_keys = set(
            db.execute(
                select(self.rows.c.catalog_key).where(self.rows.c.provider == provider)
            ).scalars()
        )
        is_baseline = not existing_keys
        pending = [
            pair
            for key, pair in normalized.items()
            if key not in existing_keys
        ]
        if pending:
            db.execute(insert(self.rows), [row for row, _ in pending])

    return is_baseline, [item for _, item in pending]


def install_bulk_provider_catalog_runtime() -> None:
    """Install the bounded catalog persistence implementation once per process."""

    if bool(getattr(ProviderCatalogLedger, _BULK_PATCH_MARKER, False)):
        return
    ProviderCatalogLedger.observe = _bulk_catalog_observe  # type: ignore[method-assign]
    setattr(ProviderCatalogLedger, _BULK_PATCH_MARKER, True)


def _research_source_owner_current(store: Any) -> bool:
    """Read the durable permanent-source ownership heartbeat lazily.

    The lazy import avoids introducing a module cycle between the source plane and
    the priority collector. A current degraded heartbeat still means the permanent
    source process owns retries; research must consume persisted evidence rather than
    duplicating the same provider calls.
    """

    from inefficiency_engine.permanent_source_plane import permanent_source_plane_current

    try:
        configured = float(os.getenv("CIE_WORKER_HEARTBEAT_STALE_SECONDS", "180"))
    except ValueError:
        configured = 180.0
    return permanent_source_plane_current(
        store,
        max_age_seconds=max(180.0, configured),
    )


def _delegated_source_payload(service: PrioritySourceCollectionService) -> dict[str, object]:
    coverage = service.source_coverage.snapshot()
    return {
        "mechanisms": {},
        "priority_sources": {},
        "source_coverage": {
            "lane_count": coverage.lane_count,
            "sufficient_lane_count": coverage.sufficient_lane_count,
            "insufficient_lane_count": coverage.insufficient_lane_count,
            "research_eligible_lane_count": coverage.research_eligible_lane_count,
            "forward_test_eligible_lane_count": coverage.forward_test_eligible_lane_count,
            "allocation_source_qualified_lane_count": (
                coverage.allocation_source_qualified_lane_count
            ),
            "priority_order": coverage.priority_order,
        },
        "source_refresh": {
            "state": "delegated_to_permanent_source",
            "delegated": True,
            "permanent_source_owner_current": True,
            "refreshed_sources": [],
            "fresh_cached_sources": [],
            "memory_deferred_sources": [],
            "failed_sources": [],
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        },
        "paper_only": True,
        "live_execution_authority": False,
    }


def install_research_source_delegation() -> None:
    """Keep disposable research from duplicating a live permanent source owner.

    This patch is installed only inside the disposable research process. If the
    canonical source owner is current, research receives persisted coverage state and
    proceeds directly to analysis/mechanism-forward work. If ownership is missing or
    stale, the original fail-safe collector still runs unchanged.
    """

    if bool(getattr(PrioritySourceCollectionService, _RESEARCH_DELEGATION_MARKER, False)):
        return

    original_run_cycle = PrioritySourceCollectionService.run_cycle

    async def delegated_run_cycle(self: PrioritySourceCollectionService) -> dict[str, object]:
        if _research_source_owner_current(self.store):
            return _delegated_source_payload(self)
        return await original_run_cycle(self)

    PrioritySourceCollectionService.run_cycle = delegated_run_cycle  # type: ignore[method-assign]
    setattr(PrioritySourceCollectionService, _RESEARCH_DELEGATION_MARKER, True)

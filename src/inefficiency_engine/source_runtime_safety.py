from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, insert, inspect, select, text

from inefficiency_engine.bounded_strategy_evidence_runtime import (
    install_bounded_strategy_evidence_runtime,
)
from inefficiency_engine.provider_gap_collection import (
    ProviderCatalogLedger,
    _deterministic_id,
)
from inefficiency_engine.priority_source_collection import (
    SOURCE_REFRESH_WORKER_ID,
    PrioritySourceCollectionService,
)
from inefficiency_engine.source_coverage import (
    SourceCoverageLedger,
    SourceCoverageObservation,
    SourceCoveragePlane,
)


_BULK_PATCH_MARKER = "_cie_bulk_catalog_runtime"
_RESEARCH_DELEGATION_MARKER = "_cie_research_source_delegation"
_COVERAGE_PATCH_MARKER = "_cie_source_coverage_runtime"
_TABLE_CACHE_ATTR = "_cie_snapshot_table_candidate_cache"

_ORIGINAL_TABLE_CANDIDATE = SourceCoveragePlane._table_candidate
_ORIGINAL_SNAPSHOT = SourceCoveragePlane.snapshot


def _bulk_catalog_observe(
    self: ProviderCatalogLedger,
    *,
    provider: str,
    items: list[dict[str, object]],
    observed_at: datetime,
    source_reference: str,
) -> tuple[bool, list[dict[str, object]]]:
    """Persist an exchange catalog in O(1) database round trips, not O(n)."""

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
        pending = [pair for key, pair in normalized.items() if key not in existing_keys]
        if pending:
            db.execute(insert(self.rows), [row for row, _ in pending])

    return is_baseline, [item for _, item in pending]


def install_bulk_provider_catalog_runtime() -> None:
    """Install the bounded catalog persistence implementation once per process."""

    if bool(getattr(ProviderCatalogLedger, _BULK_PATCH_MARKER, False)):
        return
    ProviderCatalogLedger.observe = _bulk_catalog_observe  # type: ignore[method-assign]
    setattr(ProviderCatalogLedger, _BULK_PATCH_MARKER, True)


def _latest_source_coverage_rows(
    self: SourceCoverageLedger,
) -> dict[tuple[str, str], SourceCoverageObservation]:
    """Read one durable row per source/lane instead of thousands of history rows."""

    latest_ids = (
        select(func.max(self.rows.c.id).label("id"))
        .group_by(self.rows.c.source_id, self.rows.c.lane_id)
        .subquery()
    )
    with self.store.engine.connect() as db:
        payloads = list(
            db.execute(
                select(self.rows.c.payload_json).join(
                    latest_ids,
                    self.rows.c.id == latest_ids.c.id,
                )
            ).scalars()
        )
    result: dict[tuple[str, str], SourceCoverageObservation] = {}
    for payload in payloads:
        row = SourceCoverageObservation.model_validate_json(payload)
        result[(row.source_id, row.lane_id)] = row
    return result


def _latest_provider_rows(
    self: SourceCoveragePlane,
    available: set[str],
) -> list[dict[str, object]]:
    """Read exactly the latest durable status for each public provider."""

    if "provider_statuses" not in available:
        return []
    query = text(
        "SELECT p.provider,p.ok,p.item_count,p.error_type,p.observed_at "
        "FROM provider_statuses p "
        "JOIN (SELECT provider,MAX(id) AS id FROM provider_statuses GROUP BY provider) latest "
        "ON p.id=latest.id"
    )
    with self.store.engine.connect() as db:
        return [dict(row) for row in db.execute(query).mappings()]


def _latest_admissions(
    self: SourceCoveragePlane,
    available: set[str],
) -> list[dict[str, object]]:
    """Read exactly the latest durable admission for each mechanism/provider pair."""

    if "provider_gap_admissions" not in available:
        return []
    query = text(
        "SELECT a.payload_json FROM provider_gap_admissions a "
        "JOIN ("
        "SELECT mechanism_id,provider,MAX(id) AS id "
        "FROM provider_gap_admissions GROUP BY mechanism_id,provider"
        ") latest ON a.id=latest.id"
    )
    result: list[dict[str, object]] = []
    with self.store.engine.connect() as db:
        raws = list(db.execute(query).scalars())
    for raw in raws:
        try:
            payload = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            result.append(payload)
    return result


def _cached_table_candidate(
    self: SourceCoveragePlane,
    spec: dict[str, object],
    available: set[str],
) -> dict[str, object] | None:
    """Reuse identical latest-table probes within one coverage snapshot."""

    cache = getattr(self, _TABLE_CACHE_ATTR, None)
    probe = spec.get("table")
    if not isinstance(cache, dict) or not isinstance(probe, tuple):
        return _ORIGINAL_TABLE_CANDIDATE(self, spec, available)
    key = tuple(probe)
    if key not in cache:
        cache[key] = _ORIGINAL_TABLE_CANDIDATE(self, spec, available)
    value = cache[key]
    return dict(value) if isinstance(value, dict) else None


def _snapshot_with_table_cache(
    self: SourceCoveragePlane,
    *args: object,
    **kwargs: object,
):
    previous = getattr(self, _TABLE_CACHE_ATTR, None)
    setattr(self, _TABLE_CACHE_ATTR, {})
    try:
        return _ORIGINAL_SNAPSHOT(self, *args, **kwargs)
    finally:
        if previous is None:
            try:
                delattr(self, _TABLE_CACHE_ATTR)
            except AttributeError:
                pass
        else:
            setattr(self, _TABLE_CACHE_ATTR, previous)


def install_source_coverage_reconciliation_runtime() -> None:
    """Install bounded latest-state reads for source and strategy reconciliation."""

    # API and canonical control already call this installer before constructing their
    # read graphs. Keep that stable hook and also replace the append-only strategy
    # history reader with its exact aggregate/incremental implementation.
    install_bounded_strategy_evidence_runtime()
    if bool(getattr(SourceCoveragePlane, _COVERAGE_PATCH_MARKER, False)):
        return
    SourceCoverageLedger.latest = _latest_source_coverage_rows  # type: ignore[method-assign]
    SourceCoveragePlane._provider_rows = _latest_provider_rows  # type: ignore[method-assign]
    SourceCoveragePlane._admissions = _latest_admissions  # type: ignore[method-assign]
    SourceCoveragePlane._table_candidate = _cached_table_candidate  # type: ignore[method-assign]
    SourceCoveragePlane.snapshot = _snapshot_with_table_cache  # type: ignore[method-assign]
    setattr(SourceCoveragePlane, _COVERAGE_PATCH_MARKER, True)


def ensure_source_coverage_runtime_indexes(store: Any) -> None:
    """Create indexes used by latest-evidence reconciliation when their tables exist."""

    available = set(inspect(store.engine).get_table_names())
    index_specs: dict[str, tuple[str, ...]] = {
        "market_quotes": ("venue", "observed_at"),
        "funding_quotes": ("venue", "observed_at"),
        "order_books": ("venue", "observed_at"),
        "opportunities": ("observed_at",),
        "provider_statuses": ("provider", "id"),
        "source_coverage_observations": ("source_id", "lane_id", "id"),
        "provider_gap_admissions": ("mechanism_id", "provider", "id"),
        "maker_shadow_outcomes": ("observed_at",),
        "capital_transfer_outcomes": ("observed_at",),
    }
    statements: list[str] = []
    for table_name, columns in index_specs.items():
        if table_name not in available:
            continue
        safe_suffix = "_".join(columns)
        index_name = f"ix_runtime_{table_name}_{safe_suffix}"
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {index_name} "
            f"ON {table_name} ({','.join(columns)})"
        )
    if not statements:
        return
    with store.engine.begin() as db:
        for statement in statements:
            db.execute(text(statement))


def _heartbeat_current(
    store: Any,
    worker_id: str,
    *,
    max_age_seconds: float,
) -> bool:
    """Return whether one durable owner heartbeat is current enough to delegate work."""

    try:
        heartbeat = store.latest_worker_heartbeat(worker_id)
    except Exception:
        return False
    if heartbeat is None:
        return False
    observed_at = heartbeat.observed_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    age = max(
        0.0,
        (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
    )
    return bool(
        age <= max(30.0, float(max_age_seconds))
        and str(heartbeat.state or "") in {"running", "success", "degraded"}
    )


def _research_source_owner_current(store: Any) -> bool:
    """Require both the market owner and slow priority-source owner to be current.

    Market/L2 and priority provider work now run on independent schedules in the same
    isolated source process. A fresh L2 heartbeat therefore no longer proves that the
    priority source tail is advancing. Disposable research delegates provider work
    only while both durable ownership signals are current; otherwise the existing
    fail-safe recovery path remains available.
    """

    from inefficiency_engine.permanent_source_plane import permanent_source_plane_current

    try:
        configured = float(os.getenv("CIE_WORKER_HEARTBEAT_STALE_SECONDS", "180"))
    except ValueError:
        configured = 180.0
    max_age = max(180.0, configured)
    if not permanent_source_plane_current(store, max_age_seconds=max_age):
        return False
    return _heartbeat_current(
        store,
        SOURCE_REFRESH_WORKER_ID,
        max_age_seconds=max_age,
    )


async def _delegated_source_payload(
    service: PrioritySourceCollectionService,
) -> dict[str, object]:
    # Coverage reconciliation contains synchronous SQLAlchemy reads. Keep them off
    # the research event loop so a slow database read cannot suppress research
    # liveness or delay unrelated async work.
    coverage = await asyncio.to_thread(service.source_coverage.snapshot)
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
            "priority_source_owner_current": True,
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

    If either the independent market/L2 cadence or the slow priority-source cadence
    stops advancing, delegation is withdrawn and disposable research may use the
    original fail-safe collector. No source/economic/qualification threshold changes.
    """

    if bool(getattr(PrioritySourceCollectionService, _RESEARCH_DELEGATION_MARKER, False)):
        return

    original_run_cycle = PrioritySourceCollectionService.run_cycle

    async def delegated_run_cycle(self: PrioritySourceCollectionService) -> dict[str, object]:
        if _research_source_owner_current(self.store):
            return await _delegated_source_payload(self)
        return await original_run_cycle(self)

    PrioritySourceCollectionService.run_cycle = delegated_run_cycle  # type: ignore[method-assign]
    setattr(PrioritySourceCollectionService, _RESEARCH_DELEGATION_MARKER, True)

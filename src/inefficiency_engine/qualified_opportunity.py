from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind
from inefficiency_engine.portfolio_risk import PortfolioRiskOverlay
from inefficiency_engine.unified_allocation import (
    UnifiedPaperAllocation,
    UnifiedPaperAllocationPlan,
    UnifiedPaperAllocatorService,
    UnifiedPaperCandidate,
    _core_candidates,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class QualifiedOpportunitySnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    expires_at: datetime
    source_scan_id: str
    total_capital_usd: float = Field(gt=0)
    candidates: list[UnifiedPaperCandidate] = Field(default_factory=list)
    family_failures: list[dict[str, object]] = Field(default_factory=list)
    paper_only: bool = True
    live_execution_authority: bool = False


class QualifiedOpportunityLedger:
    """Append-only bridge from heavy research into the canonical portfolio hot path."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.snapshots = Table(
            "qualified_opportunity_snapshots",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_id", String(64), nullable=False, unique=True),
            Column("observed_at", Text, nullable=False),
            Column("expires_at", Text, nullable=False),
            Column("source_scan_id", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_qualified_opportunity_time", self.snapshots.c.observed_at)
        Index("ix_qualified_opportunity_expiry", self.snapshots.c.expires_at)
        metadata.create_all(store.engine)

    def record(self, snapshot: QualifiedOpportunitySnapshot) -> str:
        raw = json.dumps(snapshot.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        lineage = hashlib.sha256(raw.encode()).hexdigest()
        with self.store.engine.begin() as db:
            existing = db.execute(
                select(self.snapshots.c.snapshot_id).where(
                    self.snapshots.c.snapshot_id == snapshot.snapshot_id
                )
            ).scalar_one_or_none()
            if existing is None:
                db.execute(
                    insert(self.snapshots),
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "observed_at": snapshot.observed_at.isoformat(),
                        "expires_at": snapshot.expires_at.isoformat(),
                        "source_scan_id": snapshot.source_scan_id,
                        "payload_json": raw,
                        "lineage_hash": lineage,
                    },
                )
        return snapshot.snapshot_id

    def latest(self) -> QualifiedOpportunitySnapshot | None:
        with self.store.engine.connect() as db:
            raw = db.execute(
                select(self.snapshots.c.payload_json).order_by(self.snapshots.c.id.desc()).limit(1)
            ).scalar_one_or_none()
        return QualifiedOpportunitySnapshot.model_validate_json(raw) if raw else None

    def latest_active(self, *, now: datetime | None = None) -> QualifiedOpportunitySnapshot | None:
        snapshot = self.latest()
        if snapshot is None:
            return None
        current = now or _now()
        return snapshot if snapshot.observed_at <= current <= snapshot.expires_at else None


def _candidate_has_canonical_settlement(item: UnifiedPaperCandidate) -> bool:
    if item.family == "alpha":
        if (
            item.exposure_kind == "directional_long"
            and item.instrument_market_kind == MarketKind.SPOT.value
            and len(item.venues) == 1
            and item.instrument_symbol
            and item.entry_reference_price is not None
            and item.modeled_roundtrip_cost_return is not None
            and item.modeled_holding_hours is not None
        ):
            return True
        if (
            item.exposure_kind == "directional_short"
            and item.instrument_market_kind == MarketKind.PERPETUAL.value
            and len(item.venues) == 1
            and item.instrument_symbol
            and item.entry_reference_price is not None
            and item.modeled_roundtrip_cost_return is not None
            and item.modeled_holding_hours is not None
        ):
            return True
        return False
    if item.family == "core_cex":
        return bool(
            item.exposure_kind == "market_neutral"
            and item.modeled_holding_hours is not None
            and len(item.settlement_legs) == 2
            and item.modeled_non_slippage_cost_bps is not None
            and item.capital_multiple is not None
        )
    return False


def allocate_prequalified_candidates(
    settings,
    *,
    candidates: list[UnifiedPaperCandidate],
    family_failures: list[dict[str, object]],
    total_capital_usd: float,
    max_venue_fraction: float | None = None,
    max_asset_fraction: float | None = None,
    max_allocations: int | None = None,
    observed_at: datetime | None = None,
) -> UnifiedPaperAllocationPlan:
    """Apply the existing portfolio/risk policy without doing any provider or research work."""

    if total_capital_usd <= 0:
        raise ValueError("total_capital_usd must be positive")
    venue_fraction = max_venue_fraction or settings.allocator_max_venue_fraction
    asset_fraction = max_asset_fraction or settings.allocator_max_asset_fraction
    allocation_limit = max_allocations or settings.allocator_max_allocations
    if not 0 < venue_fraction <= 1 or not 0 < asset_fraction <= 1 or allocation_limit <= 0:
        raise ValueError("invalid allocation constraints")

    ordered = sorted(
        candidates,
        key=lambda item: (
            item.expected_return_on_reserved_capital,
            item.expected_profit_usd_per_deployment,
            -item.capital_required_usd,
        ),
        reverse=True,
    )
    venue_cap = total_capital_usd * venue_fraction
    asset_cap = total_capital_usd * asset_fraction
    venue_used: dict[str, float] = defaultdict(float)
    asset_used: dict[str, float] = defaultdict(float)
    used_conflicts: set[str] = set()
    risk_overlay = PortfolioRiskOverlay(settings, total_capital_usd=total_capital_usd)
    allocated = 0.0
    allocations: list[UnifiedPaperAllocation] = []
    skipped: list[dict[str, object]] = []

    for item in ordered:
        if len(allocations) >= allocation_limit:
            skipped.append({"candidate_id": item.candidate_id, "reason": "allocation count limit"})
            continue
        if used_conflicts.intersection(item.conflict_keys):
            skipped.append(
                {"candidate_id": item.candidate_id, "reason": "shared instrument or route conflict"}
            )
            continue
        capital = item.capital_required_usd
        if allocated + capital > total_capital_usd + 1e-9:
            skipped.append({"candidate_id": item.candidate_id, "reason": "total capital constraint"})
            continue
        venue_share = capital / max(1, len(item.venues))
        if any(venue_used[venue] + venue_share > venue_cap + 1e-9 for venue in item.venues):
            skipped.append({"candidate_id": item.candidate_id, "reason": "venue concentration cap"})
            continue
        if asset_used[item.asset] + capital > asset_cap + 1e-9:
            skipped.append({"candidate_id": item.candidate_id, "reason": "asset concentration cap"})
            continue
        risk_decision = risk_overlay.decision(item)
        if not risk_decision.accepted:
            skipped.append(
                {
                    "candidate_id": item.candidate_id,
                    "reason": risk_decision.reason or "portfolio risk budget",
                }
            )
            continue

        allocations.append(
            UnifiedPaperAllocation(
                candidate_id=item.candidate_id,
                family=item.family,
                strategy=item.strategy,
                asset=item.asset,
                venues=item.venues,
                capital_required_usd=capital,
                notional_usd_per_leg=item.notional_usd_per_leg,
                expected_profit_usd_per_deployment=item.expected_profit_usd_per_deployment,
                expected_return_on_reserved_capital=item.expected_return_on_reserved_capital,
                modeled_holding_hours=item.modeled_holding_hours,
                source_return_metric=item.source_return_metric,
                source_return_value=item.source_return_value,
                exposure_kind=item.exposure_kind,
                source_observed_at=item.source_observed_at,
                instrument_symbol=item.instrument_symbol,
                instrument_market_kind=item.instrument_market_kind,
                entry_reference_price=item.entry_reference_price,
                modeled_roundtrip_cost_return=item.modeled_roundtrip_cost_return,
                settlement_legs=item.settlement_legs,
                modeled_non_slippage_cost_bps=item.modeled_non_slippage_cost_bps,
                modeled_safety_buffer_bps=item.modeled_safety_buffer_bps,
                capital_multiple=item.capital_multiple,
                evidence_id=item.evidence_id,
                opportunity_id=item.opportunity_id,
                capacity_claimed=False,
                authorizes_execution=False,
                paper_only=True,
            )
        )
        risk_overlay.register(item)
        allocated += capital
        asset_used[item.asset] += capital
        for venue in item.venues:
            venue_used[venue] += venue_share
        used_conflicts.update(item.conflict_keys)

    profit = sum(item.expected_profit_usd_per_deployment for item in allocations)
    weighted_return = (
        sum(
            item.capital_required_usd * item.expected_return_on_reserved_capital
            for item in allocations
        )
        / total_capital_usd
    )
    return UnifiedPaperAllocationPlan(
        observed_at=observed_at or _now(),
        total_capital_usd=total_capital_usd,
        allocated_capital_usd=allocated,
        unused_cash_usd=max(0.0, total_capital_usd - allocated),
        expected_profit_usd_current_deployments=profit,
        weighted_expected_return_on_reserved_capital=weighted_return,
        candidate_count=len(ordered),
        allocations=allocations,
        skipped=skipped,
        family_failures=family_failures,
        portfolio_risk_budget=risk_overlay.snapshot(),
        authorizes_execution=False,
        live_execution_eligible=False,
        paper_only=True,
    )


class QualifiedOpportunityBridgePublisher:
    """Project already-collected research into a fresh canonical decision envelope.

    The publisher never launches a broad market scan. It loads the newest persisted
    executable research scan, reuses its structural executability, and asks the
    bounded alpha factory to promote only statistically qualified candidates. Any
    direct alpha L2 fallback remains candidate-level and time-bounded by the public
    adapter registry. Families without a canonical settlement contract remain in
    research/certification and are intentionally absent from portfolio authority.
    """

    def __init__(
        self,
        core,
        store: EvidenceStore,
        allocator: UnifiedPaperAllocatorService,
    ):
        self.core = core
        self.store = store
        self.allocator = allocator
        self.ledger = QualifiedOpportunityLedger(store)

    def _latest_scan(self) -> ScanSnapshot | None:
        with self.store.engine.connect() as db:
            scan_id = db.execute(
                select(self.store.scans.c.scan_id)
                .order_by(self.store.scans.c.completed_at.desc())
                .limit(1)
            ).scalar_one_or_none()
        if scan_id is None:
            return None
        return self.store.load_scan(str(scan_id))

    async def publish_latest(
        self,
        *,
        total_capital_usd: float,
    ) -> QualifiedOpportunitySnapshot | None:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        snapshot = self._latest_scan()
        if snapshot is None:
            return None

        now = _now()
        freshness_seconds = max(
            30.0,
            float(getattr(self.core.settings, "max_quote_age_seconds", 120.0)),
        )
        age = max(0.0, (now - snapshot.completed_at).total_seconds())
        if age > freshness_seconds:
            return None

        rows: list[UnifiedPaperCandidate] = []
        failures: list[dict[str, object]] = []

        try:
            rows.extend(_core_candidates(snapshot.opportunities, snapshot.executability))
        except Exception as exc:
            failures.append(
                {
                    "family": "core_cex",
                    "error_type": type(exc).__name__,
                    "reason": "core CEX bridge projection failed closed",
                }
            )

        if self.allocator.alpha_factory is not None:
            try:
                rows.extend(
                    await self.allocator._alpha_family_candidates(
                        snapshot=snapshot,
                        total_capital_usd=total_capital_usd,
                    )
                )
            except Exception as exc:
                failures.append(
                    {
                        "family": "alpha",
                        "error_type": type(exc).__name__,
                        "reason": "alpha bridge projection failed closed",
                    }
                )

        deployable = [
            item
            for item in rows
            if item.allocation_eligible and _candidate_has_canonical_settlement(item)
        ]
        deployable.sort(
            key=lambda item: (
                item.expected_return_on_reserved_capital,
                item.expected_profit_usd_per_deployment,
                -item.capital_required_usd,
            ),
            reverse=True,
        )
        result = QualifiedOpportunitySnapshot(
            observed_at=snapshot.completed_at,
            expires_at=snapshot.completed_at + timedelta(seconds=freshness_seconds),
            source_scan_id=snapshot.scan_id,
            total_capital_usd=total_capital_usd,
            candidates=deployable,
            family_failures=failures,
        )
        self.ledger.record(result)
        return result


class QualifiedOpportunityAllocatorService(UnifiedPaperAllocatorService):
    """Canonical allocator that reads research-qualified candidates from durable state only."""

    def __init__(self, core, cex_dex, alpha_factory):
        super().__init__(core, cex_dex, alpha_factory)
        store = getattr(alpha_factory, "store", None)
        if store is None:
            raise RuntimeError("qualified opportunity allocator requires a durable evidence store")
        self.qualified_ledger = QualifiedOpportunityLedger(store)

    async def _candidates_with_failures(
        self, *, total_capital_usd: float
    ) -> tuple[list[UnifiedPaperCandidate], list[dict[str, object]]]:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        snapshot = self.qualified_ledger.latest_active()
        if snapshot is None:
            return [], [
                {
                    "family": "qualified_opportunity_bridge",
                    "error_type": "QualifiedOpportunitySnapshotUnavailableOrStale",
                    "reason": (
                        "canonical portfolio requires a fresh research-qualified decision envelope; "
                        "research is never rerun inside the accounting hot path"
                    ),
                }
            ]
        deployable = [item for item in snapshot.candidates if _candidate_has_canonical_settlement(item)]
        return deployable, list(snapshot.family_failures)

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        candidates, failures = await self._candidates_with_failures(
            total_capital_usd=total_capital_usd
        )
        return allocate_prequalified_candidates(
            self.settings,
            candidates=candidates,
            family_failures=failures,
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )

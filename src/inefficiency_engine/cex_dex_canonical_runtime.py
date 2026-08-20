from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select

from inefficiency_engine.allocation_certification import (
    AllocationForwardCertificationService,
    PaperAllocationOutcome,
    PaperAllocationTrial,
)
from inefficiency_engine.canonical_paper_portfolio import CanonicalPortfolioEvent
from inefficiency_engine.cex_dex_shadow import (
    CexDexCompositeEdgeLedger,
    CexDexCompositeEdgeShadowCycle,
)
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import MarketKind
from inefficiency_engine.qualified_opportunity import (
    QualifiedOpportunitySnapshot,
    _candidate_has_canonical_settlement as _base_candidate_has_canonical_settlement,
)
from inefficiency_engine.qualified_opportunity_freshness import (
    FreshnessSeparatedQualifiedOpportunityAllocatorService,
    FreshnessSeparatedQualifiedOpportunityBridgePublisher,
    _bridge_control_freshness_seconds,
    _candidate_freshness_seconds,
    _now,
)
from inefficiency_engine.unified_allocation import (
    UnifiedPaperAllocation,
    UnifiedPaperCandidate,
    _core_candidates,
)
from inefficiency_engine.universal_paper_portfolio import (
    UniversalOperationallyResilientPaperPortfolioService,
)


CEX_DEX_SETTLEMENT_METHOD = "verified_amount_specific_cex_dex_composite_capture"


def _cex_dex_holding_hours(settings) -> float:
    horizons = [
        float(value)
        for value in (getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,))
        if float(value) > 0.0
    ]
    if horizons:
        seconds = min(horizons)
    else:
        seconds = max(1.0, float(getattr(settings, "shadow_delay_seconds", 60.0)))
    return max(1.0, seconds) / 3600.0


def _cex_symbol_from_candidate(item: UnifiedPaperCandidate) -> str | None:
    if not item.venues:
        return None
    venue = item.venues[0]
    prefixes = (f"venue-symbol:{venue}:", f"cex:{venue}:")
    for key in item.conflict_keys:
        for prefix in prefixes:
            if key.startswith(prefix):
                symbol = key[len(prefix) :]
                if symbol:
                    return symbol
    return None


def prepare_cex_dex_candidate(
    item: UnifiedPaperCandidate,
    *,
    settings,
    observed_at: datetime,
) -> UnifiedPaperCandidate:
    if item.family != "cex_dex":
        return item
    return item.model_copy(
        update={
            "modeled_holding_hours": _cex_dex_holding_hours(settings),
            "source_observed_at": observed_at,
            "instrument_symbol": item.instrument_symbol or _cex_symbol_from_candidate(item),
            "instrument_market_kind": item.instrument_market_kind or MarketKind.SPOT.value,
        }
    )


def candidate_has_canonical_settlement(item: UnifiedPaperCandidate) -> bool:
    if _base_candidate_has_canonical_settlement(item):
        return True
    return bool(
        item.family == "cex_dex"
        and item.exposure_kind == "market_neutral"
        and item.candidate_id.startswith("cex-dex:")
        and item.evidence_id
        and len(item.venues) == 2
        and item.instrument_symbol
        and item.instrument_market_kind == MarketKind.SPOT.value
        and item.modeled_holding_hours is not None
        and item.source_observed_at is not None
        and item.expected_profit_usd_per_deployment > 0.0
        and item.expected_return_on_reserved_capital > 0.0
    )


class CexDexFreshnessSeparatedQualifiedOpportunityBridgePublisher(
    FreshnessSeparatedQualifiedOpportunityBridgePublisher
):
    """Publish every currently canonical-settleable qualified family, including CEX↔DEX."""

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
        candidate_freshness = _candidate_freshness_seconds(self.core.settings)
        source_age = max(0.0, (now - snapshot.completed_at).total_seconds())
        if source_age > candidate_freshness:
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

        if getattr(self.allocator, "cex_dex", None) is not None:
            try:
                cex_dex_rows = await self.allocator._cex_dex_family_candidates(
                    total_capital_usd=total_capital_usd
                )
                qualified_at = _now()
                rows.extend(
                    prepare_cex_dex_candidate(
                        item,
                        settings=self.core.settings,
                        observed_at=qualified_at,
                    )
                    for item in cex_dex_rows
                )
            except Exception as exc:
                failures.append(
                    {
                        "family": "cex_dex",
                        "error_type": type(exc).__name__,
                        "reason": "CEX↔DEX bridge projection failed closed",
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
            if item.allocation_eligible and candidate_has_canonical_settlement(item)
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
            expires_at=now
            + timedelta(seconds=_bridge_control_freshness_seconds(self.core.settings)),
            source_scan_id=snapshot.scan_id,
            total_capital_usd=total_capital_usd,
            candidates=deployable,
            family_failures=failures,
        )
        self.ledger.record(result)
        return result


class CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService(
    FreshnessSeparatedQualifiedOpportunityAllocatorService
):
    """Consume the bridge with the same freshness policy while admitting CEX↔DEX contracts."""

    def _active_candidates_with_diagnostics(
        self,
    ) -> tuple[
        list[UnifiedPaperCandidate],
        list[dict[str, object]],
        list[dict[str, object]],
    ]:
        bridge_failure = self._bridge_failure()
        if bridge_failure is not None:
            return [], [bridge_failure], []

        snapshot = self.qualified_ledger.latest_active()
        if snapshot is None:
            return [], [
                {
                    "family": "qualified_opportunity_bridge",
                    "error_type": "QualifiedOpportunitySnapshotUnavailableOrStale",
                    "reason": (
                        "canonical portfolio requires a recent successful bridge control envelope; "
                        "research is never rerun inside the accounting hot path"
                    ),
                }
            ], []

        current = _now()
        max_age = _candidate_freshness_seconds(self.settings)
        deployable: list[UnifiedPaperCandidate] = []
        stale_skips: list[dict[str, object]] = []
        for item in snapshot.candidates:
            if not candidate_has_canonical_settlement(item):
                continue
            source_at = item.source_observed_at
            age = (current - source_at).total_seconds() if source_at is not None else None
            if age is None or age < 0.0 or age > max_age:
                stale_skips.append(
                    {
                        "candidate_id": item.candidate_id,
                        "family": item.family,
                        "reason": "candidate evidence stale; awaiting fresh research qualification",
                        "source_observed_at": source_at.isoformat() if source_at is not None else None,
                        "max_age_seconds": max_age,
                    }
                )
                continue
            deployable.append(item)

        return deployable, list(snapshot.family_failures), stale_skips


class CexDexAwareAllocationForwardCertificationService(
    AllocationForwardCertificationService
):
    """Extend forward paper certification to amount-specific CEX↔DEX composite capture."""

    CEX_DEX_SETTLEMENT_METHOD = CEX_DEX_SETTLEMENT_METHOD

    @classmethod
    def trial_from_allocation(
        cls,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        trial = AllocationForwardCertificationService.trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )
        if allocation.family != "cex_dex":
            return trial

        supported = bool(
            allocation.exposure_kind == "market_neutral"
            and allocation.candidate_id.startswith("cex-dex:")
            and allocation.evidence_id
            and len(allocation.venues) == 2
            and allocation.instrument_symbol
            and allocation.instrument_market_kind == MarketKind.SPOT.value
            and allocation.modeled_holding_hours is not None
            and allocation.expected_profit_usd_per_deployment > 0.0
            and allocation.expected_return_on_reserved_capital > 0.0
        )
        if not supported:
            return trial.model_copy(
                update={
                    "settlement_supported": False,
                    "settlement_method": None,
                    "settlement_blocker": (
                        "CEX-DEX allocation lacks the exact route identity, qualification lineage, "
                        "or verification horizon required for canonical paper settlement"
                    ),
                }
            )

        source_at = allocation.source_observed_at or plan_observed_at
        due_at = source_at + timedelta(hours=float(allocation.modeled_holding_hours))
        return trial.model_copy(
            update={
                "source_observed_at": source_at,
                "due_at": due_at,
                "settlement_supported": True,
                "settlement_method": cls.CEX_DEX_SETTLEMENT_METHOD,
                "settlement_blocker": None,
            }
        )

    def _cex_dex_verification(
        self,
        trial: PaperAllocationTrial,
    ) -> tuple[datetime, float | None, bool] | None:
        if trial.due_at is None or not trial.candidate_id.startswith("cex-dex:"):
            return None
        composite_key = trial.candidate_id.split(":", 1)[1]
        ledger = CexDexCompositeEdgeLedger(self.store)
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(ledger.cycles.c.payload_json)
                    .where(ledger.cycles.c.completed_at >= trial.due_at.isoformat())
                    .order_by(ledger.cycles.c.completed_at)
                    .limit(50)
                ).scalars()
            )

        for payload in payloads:
            cycle = CexDexCompositeEdgeShadowCycle.model_validate_json(payload)
            matches = sorted(
                (
                    row
                    for row in cycle.observations
                    if row.composite_key == composite_key and row.verified_at >= trial.due_at
                ),
                key=lambda row: row.verified_at,
            )
            if matches:
                row = matches[0]
                return row.verified_at, row.verification_net_edge_bps, bool(row.survived)
            if cycle.started_at >= trial.due_at:
                # A complete post-horizon reconstruction did not rediscover this exact
                # route/notional signature. Treat that as zero captured edge, never as
                # invented profit and never as live-execution evidence.
                return cycle.completed_at, None, False
        return None

    @classmethod
    def _cex_dex_outcome(
        cls,
        trial: PaperAllocationTrial,
        *,
        matured_at: datetime,
        verification_net_edge_bps: float | None,
        survived: bool,
    ) -> PaperAllocationOutcome:
        predicted_edge_bps = (
            trial.predicted_profit_usd / trial.notional_usd * 10_000.0
            if trial.notional_usd > 0
            else 0.0
        )
        if survived and verification_net_edge_bps is not None:
            realized_edge_bps = min(predicted_edge_bps, verification_net_edge_bps)
        else:
            realized_edge_bps = 0.0
        realized_profit = trial.notional_usd * realized_edge_bps / 10_000.0
        realized_net = realized_profit / trial.capital_required_usd
        error = realized_profit - trial.predicted_profit_usd
        capture = (
            realized_profit / trial.predicted_profit_usd
            if trial.predicted_profit_usd > 0
            else None
        )
        return PaperAllocationOutcome(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate_id,
            family=trial.family,
            strategy=trial.strategy,
            asset=trial.asset,
            matured_at=matured_at,
            due_at=trial.due_at or matured_at,
            realized_gross_return=realized_net,
            realized_net_return=realized_net,
            realized_profit_usd=realized_profit,
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0.0,
            settlement_method=cls.CEX_DEX_SETTLEMENT_METHOD,
            settlement_evidence_complete=True,
            live_execution_authority=False,
            paper_only=True,
        )

    def _settle_cex_dex(
        self,
        trial: PaperAllocationTrial,
    ) -> PaperAllocationOutcome | None:
        verification = self._cex_dex_verification(trial)
        if verification is None:
            return None
        matured_at, verification_edge, survived = verification
        return self._cex_dex_outcome(
            trial,
            matured_at=matured_at,
            verification_net_edge_bps=verification_edge,
            survived=survived,
        )

    def _settle_trial(
        self,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if trial.settlement_method == self.CEX_DEX_SETTLEMENT_METHOD:
            return self._settle_cex_dex(trial)
        return super()._settle_trial(trial, snapshot)


class CexDexUniversalOperationallyResilientPaperPortfolioService(
    UniversalOperationallyResilientPaperPortfolioService
):
    """Canonical paper account with CEX↔DEX settlement added to existing universal contracts."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.settlement = CexDexAwareAllocationForwardCertificationService(core, allocator, store)

    @classmethod
    def _trial_for_allocation(
        cls,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        return CexDexAwareAllocationForwardCertificationService.trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )

    @classmethod
    def _support_reason(
        cls,
        allocation: UnifiedPaperAllocation,
    ) -> tuple[bool, str | None]:
        trial = cls._trial_for_allocation(
            allocation,
            plan_observed_at=allocation.source_observed_at or _now(),
        )
        if trial.settlement_supported:
            return True, None
        return False, trial.settlement_blocker or "allocation lacks a canonical settlement contract"

    def _mark_universal(
        self,
        position,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> CanonicalPortfolioEvent | None:
        if trial.settlement_method != CEX_DEX_SETTLEMENT_METHOD:
            return super()._mark_universal(position, trial, snapshot)
        return CanonicalPortfolioEvent(
            event_type="mark",
            observed_at=snapshot.completed_at,
            position_id=position.position_id,
            candidate_id=position.candidate_id,
            family=position.family,
            strategy=position.strategy,
            asset=position.asset,
            venue=position.venue,
            symbol=position.symbol,
            market_kind=position.market_kind,
            exposure_kind=position.exposure_kind,
            entry_reference_price=position.entry_reference_price,
            reference_price=position.current_reference_price,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            unrealized_pnl_usd=0.0,
            details={
                "valuation_method": "pending_amount_specific_cex_dex_composite_verification",
                "paper_only": True,
                "live_execution_authority": False,
            },
        )

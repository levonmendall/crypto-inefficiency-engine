from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.allocation_certification import (
    AllocationCertificationCycle,
    AllocationForwardCertificationService,
    PaperAllocationOutcome,
    PaperAllocationTrial,
)
from inefficiency_engine.canonical_paper_portfolio import CanonicalPortfolioEvent
from inefficiency_engine.cex_dex_shadow import composite_edge_key
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.qualified_opportunity import (
    QualifiedOpportunitySnapshot,
    _candidate_has_canonical_settlement,
    _core_candidates,
    allocate_prequalified_candidates,
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
    UnifiedPaperAllocationPlan,
    UnifiedPaperCandidate,
)
from inefficiency_engine.universal_paper_portfolio import (
    UniversalOperationallyResilientPaperPortfolioService,
)


CEX_DEX_MARKET_KIND = "cex_dex_composite"


def _cex_dex_candidate_has_canonical_settlement(item: UnifiedPaperCandidate) -> bool:
    if _candidate_has_canonical_settlement(item):
        return True
    return bool(
        item.family == "cex_dex"
        and item.strategy == "cex_dex"
        and item.exposure_kind == "market_neutral"
        and item.modeled_holding_hours is not None
        and item.modeled_holding_hours > 0
        and len(item.venues) == 2
        and item.instrument_symbol
        and item.instrument_market_kind == CEX_DEX_MARKET_KIND
        and item.entry_reference_price is not None
        and item.evidence_id
        and item.source_observed_at is not None
        and item.capital_multiple is not None
        and item.capital_multiple > 0
    )


class CexDexCanonicalQualifiedOpportunityBridgePublisher(
    FreshnessSeparatedQualifiedOpportunityBridgePublisher
):
    """Publish only fully qualified, canonically settleable CEX↔DEX candidates."""

    async def _cex_dex_candidates(
        self,
        *,
        total_capital_usd: float,
    ) -> list[UnifiedPaperCandidate]:
        promotion = self.allocator.cex_dex
        if promotion is None:
            return []

        qualification_probe = await promotion.live_qualification(
            paper_inventory_usd_per_side=total_capital_usd / 2.0
        )
        # Re-quote once at the bridge boundary so the canonical entry carries exact,
        # current amount-specific CEX+DEX evidence rather than only a qualification id.
        evidence_probe = await promotion.composite_service.probe()
        evidence_by_key = {
            composite_edge_key(evidence): evidence
            for evidence in evidence_probe.evidence
            if evidence.evidence_complete and evidence.route_contiguous_acceptable
        }

        rows: list[UnifiedPaperCandidate] = []
        settings = self.core.settings
        holding_seconds = max(
            0.001,
            float(getattr(settings, "dex_statistical_reference_horizon_seconds", 5.0)),
        )
        min_edge_bps = float(getattr(settings, "dex_statistical_min_net_edge_bps", 12.0))
        reserve_bps = float(getattr(promotion.hedge_policy, "reserve_buffer_bps", 0.0))
        freshness = _candidate_freshness_seconds(settings)
        now = _now()

        for qualification in qualification_probe.qualifications:
            if not qualification.paper_allocation_eligible:
                continue
            evidence = evidence_by_key.get(qualification.composite_key)
            if evidence is None:
                continue
            age = max(0.0, (now - evidence.observed_at).total_seconds())
            if age > freshness:
                continue
            if evidence.cex_venue != qualification.cex_venue:
                continue
            if evidence.cex_symbol != qualification.cex_symbol:
                continue

            # The new entry re-quote may be worse than the qualification probe. Never
            # promote on stale economics and never increase the statistically haircutted
            # edge because of this second observation.
            requote_capture_edge_bps = max(
                0.0,
                evidence.net_research_edge_bps - reserve_bps,
            )
            conservative_edge_bps = min(
                qualification.conservative_capture_edge_bps,
                requote_capture_edge_bps,
            )
            if conservative_edge_bps < min_edge_bps:
                continue

            capital = qualification.paper_capital_required_usd
            notional = qualification.target_notional_usd
            if capital <= 0 or notional <= 0:
                continue
            profit = notional * conservative_edge_bps / 10_000.0
            rows.append(
                UnifiedPaperCandidate(
                    candidate_id=f"cex-dex:{qualification.composite_key}",
                    family="cex_dex",
                    strategy="cex_dex",
                    asset=qualification.asset,
                    venues=[qualification.cex_venue, qualification.dex_venue],
                    capital_required_usd=capital,
                    notional_usd_per_leg=notional,
                    expected_profit_usd_per_deployment=profit,
                    expected_return_on_reserved_capital=profit / capital,
                    modeled_holding_hours=holding_seconds / 3600.0,
                    source_return_metric="conservative_capture_edge_bps",
                    source_return_value=conservative_edge_bps,
                    exposure_kind="market_neutral",
                    source_observed_at=evidence.observed_at,
                    instrument_symbol=qualification.cex_symbol,
                    instrument_market_kind=CEX_DEX_MARKET_KIND,
                    entry_reference_price=evidence.cex_reference_price,
                    modeled_non_slippage_cost_bps=max(
                        0.0,
                        evidence.gross_edge_after_conversion_depth_bps
                        - evidence.net_research_edge_bps
                        + reserve_bps,
                    ),
                    modeled_safety_buffer_bps=reserve_bps,
                    capital_multiple=capital / notional,
                    conflict_keys=[
                        f"cex:{qualification.cex_venue}:{qualification.cex_symbol}",
                        f"venue-symbol:{qualification.cex_venue}:{qualification.cex_symbol}",
                        f"dex:ethereum:{qualification.asset}:{qualification.route_direction}",
                        f"cex-dex-composite:{qualification.composite_key}",
                    ],
                    evidence_id=evidence.evidence_id,
                    capacity_reference_usd=notional,
                    capacity_claimed=False,
                    allocation_eligible=True,
                    executable_eligible=False,
                    paper_only=True,
                )
            )
        return rows

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

        try:
            rows.extend(await self._cex_dex_candidates(total_capital_usd=total_capital_usd))
        except Exception as exc:
            failures.append(
                {
                    "family": "cex_dex",
                    "error_type": type(exc).__name__,
                    "reason": "CEX↔DEX canonical bridge projection failed closed",
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
            if item.allocation_eligible
            and _cex_dex_candidate_has_canonical_settlement(item)
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


class CexDexCanonicalQualifiedOpportunityAllocatorService(
    FreshnessSeparatedQualifiedOpportunityAllocatorService
):
    """Consume the durable bridge without rerunning research in allocation."""

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
            if not _cex_dex_candidate_has_canonical_settlement(item):
                continue
            source_at = item.source_observed_at
            age = (current - source_at).total_seconds() if source_at is not None else None
            if age is None or age < 0.0 or age > max_age:
                stale_skips.append(
                    {
                        "candidate_id": item.candidate_id,
                        "family": item.family,
                        "reason": "candidate evidence stale; awaiting fresh research qualification",
                        "source_observed_at": (
                            source_at.isoformat() if source_at is not None else None
                        ),
                        "max_age_seconds": max_age,
                    }
                )
                continue
            deployable.append(item)
        return deployable, list(snapshot.family_failures), stale_skips

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        candidates, failures, stale_skips = self._active_candidates_with_diagnostics()
        plan = allocate_prequalified_candidates(
            self.settings,
            candidates=candidates,
            family_failures=failures,
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )
        plan.skipped = [*stale_skips, *plan.skipped]
        return plan


class CexDexCanonicalAllocationCertificationService(
    AllocationForwardCertificationService
):
    """Forward-settle CEX↔DEX from a fresh amount-specific composite re-quote."""

    CEX_DEX_SETTLEMENT_METHOD = (
        "cex_dex_amount_specific_requote_plus_cex_hedge_minus_embedded_costs"
    )

    @classmethod
    def trial_from_allocation(
        cls,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        trial = super().trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )
        if trial.settlement_supported or allocation.family != "cex_dex":
            return trial
        if not (
            allocation.strategy == "cex_dex"
            and allocation.exposure_kind == "market_neutral"
            and allocation.modeled_holding_hours is not None
            and allocation.modeled_holding_hours > 0
            and len(allocation.venues) == 2
            and allocation.instrument_symbol
            and allocation.instrument_market_kind == CEX_DEX_MARKET_KIND
            and allocation.entry_reference_price is not None
            and allocation.evidence_id
            and allocation.source_observed_at is not None
            and allocation.capital_multiple is not None
        ):
            return trial
        due_at = allocation.source_observed_at + timedelta(
            hours=allocation.modeled_holding_hours
        )
        return trial.model_copy(
            update={
                "due_at": due_at,
                "settlement_supported": True,
                "settlement_method": cls.CEX_DEX_SETTLEMENT_METHOD,
                "settlement_blocker": None,
                "cohort_key": f"cex_dex|{allocation.candidate_id}",
            }
        )

    @staticmethod
    def _composite_key(trial: PaperAllocationTrial) -> str | None:
        prefix = "cex-dex:"
        return (
            trial.candidate_id[len(prefix):]
            if trial.candidate_id.startswith(prefix)
            else None
        )

    async def settle_cex_dex_trial(
        self,
        trial: PaperAllocationTrial,
    ) -> PaperAllocationOutcome | None:
        if (
            trial.settlement_method != self.CEX_DEX_SETTLEMENT_METHOD
            or trial.due_at is None
        ):
            return None
        promotion = getattr(self.allocator, "cex_dex", None)
        if promotion is None:
            return None
        key = self._composite_key(trial)
        if not key:
            return None

        probe = await promotion.composite_service.probe()
        matching = [
            evidence
            for evidence in probe.evidence
            if composite_edge_key(evidence) == key
            and evidence.evidence_complete
            and evidence.route_contiguous_acceptable
            and evidence.observed_at >= trial.due_at
        ]
        if not matching:
            return None
        evidence = min(matching, key=lambda item: item.observed_at)
        if trial.venues and evidence.cex_venue != trial.venues[0]:
            return None
        if trial.instrument_symbol and evidence.cex_symbol != trial.instrument_symbol:
            return None

        reserve_bps = float(getattr(promotion.hedge_policy, "reserve_buffer_bps", 0.0))
        realized_edge_bps = evidence.net_research_edge_bps - reserve_bps
        gross_capture_usd = (
            trial.notional_usd
            * evidence.gross_edge_after_conversion_depth_bps
            / 10_000.0
        )
        realized_profit = trial.notional_usd * realized_edge_bps / 10_000.0
        realized_net = realized_profit / trial.capital_required_usd
        gross_return = gross_capture_usd / trial.capital_required_usd
        cost_usd = max(0.0, gross_capture_usd - realized_profit)
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
            matured_at=evidence.observed_at,
            due_at=trial.due_at,
            entry_reference_price=trial.entry_reference_price,
            exit_reference_price=evidence.cex_reference_price,
            realized_gross_return=gross_return,
            realized_net_return=realized_net,
            realized_profit_usd=realized_profit,
            realized_price_pnl_usd=gross_capture_usd,
            realized_funding_pnl_usd=0.0,
            modeled_non_slippage_cost_usd=cost_usd,
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0,
            settlement_method=self.CEX_DEX_SETTLEMENT_METHOD,
            settlement_evidence_complete=True,
        )

    async def run_cycle(
        self,
        *,
        total_capital_usd: float = 100000.0,
    ) -> AllocationCertificationCycle:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        snapshot = await self.core.collect_live_executability()
        matured = 0
        for trial in self.ledger.pending_supported_trials(now=snapshot.completed_at):
            if trial.settlement_method == self.CEX_DEX_SETTLEMENT_METHOD:
                outcome = await self.settle_cex_dex_trial(trial)
            else:
                outcome = self._settle_trial(trial, snapshot)
            if outcome is not None:
                self.ledger.record_outcome(outcome)
                matured += 1

        plan = await self.allocator.allocate(total_capital_usd=total_capital_usd)
        recorded = supported = unsupported = 0
        for allocation in plan.allocations:
            trial = self.trial_from_allocation(
                allocation,
                plan_observed_at=plan.observed_at,
            )
            if (
                trial.settlement_supported
                and self.ledger.has_unsettled_supported_cohort(trial.cohort_key)
            ):
                continue
            self.ledger.record_trial(trial)
            recorded += 1
            if trial.settlement_supported:
                supported += 1
            else:
                unsupported += 1
        return AllocationCertificationCycle(
            observed_at=plan.observed_at,
            plan_allocation_count=len(plan.allocations),
            trials_recorded=recorded,
            supported_trials_recorded=supported,
            unsupported_trials_recorded=unsupported,
            outcomes_matured=matured,
        )


class CexDexCanonicalPaperPortfolioService(
    UniversalOperationallyResilientPaperPortfolioService
):
    """Canonical paper account with amount-specific CEX↔DEX settlement support."""

    def _init_universal_settlement(self, core, allocator, store) -> None:
        self.settlement = CexDexCanonicalAllocationCertificationService(
            core,
            allocator,
            store,
        )

    @staticmethod
    def _trial_for_allocation(
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        return CexDexCanonicalAllocationCertificationService.trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )

    @staticmethod
    def _support_reason(
        allocation: UnifiedPaperAllocation,
    ) -> tuple[bool, str | None]:
        trial = CexDexCanonicalAllocationCertificationService.trial_from_allocation(
            allocation,
            plan_observed_at=(
                allocation.source_observed_at or datetime.now(timezone.utc)
            ),
        )
        if trial.settlement_supported:
            return True, None
        return False, trial.settlement_blocker or "allocation lacks a canonical settlement contract"

    @staticmethod
    def _trial_conflicts(trial: PaperAllocationTrial) -> set[str]:
        if (
            trial.settlement_method
            == CexDexCanonicalAllocationCertificationService.CEX_DEX_SETTLEMENT_METHOD
        ):
            rows = {f"candidate:{trial.candidate_id}"}
            if trial.venues and trial.instrument_symbol:
                rows.add(f"venue-symbol:{trial.venues[0]}:{trial.instrument_symbol}")
            if len(trial.venues) > 1:
                rows.add(f"dex-asset:{trial.venues[1]}:{trial.asset.upper()}")
            return rows
        return UniversalOperationallyResilientPaperPortfolioService._trial_conflicts(trial)

    def _mark_universal(
        self,
        position,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> CanonicalPortfolioEvent | None:
        if (
            trial.settlement_method
            != CexDexCanonicalAllocationCertificationService.CEX_DEX_SETTLEMENT_METHOD
        ):
            return super()._mark_universal(position, trial, snapshot)
        # Until the committed capture horizon arrives, reserve capital without
        # manufacturing interim arbitrage P&L from an incomplete one-sided mark.
        reference_price = position.current_reference_price
        if trial.venues and trial.instrument_symbol:
            quote = self._quote_index(snapshot).get(
                (
                    trial.venues[0],
                    trial.asset.upper(),
                    "spot",
                    trial.instrument_symbol,
                )
            )
            if quote is not None and quote.mid > 0:
                reference_price = quote.mid
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
            reference_price=reference_price,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            unrealized_pnl_usd=0.0,
            details={
                "valuation_method": "cex_dex_amount_specific_requote_pending",
                "interim_pnl_claimed": False,
                "paper_only": True,
            },
        )

    async def _settle_universal_position(
        self,
        position,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if (
            trial.settlement_method
            == CexDexCanonicalAllocationCertificationService.CEX_DEX_SETTLEMENT_METHOD
        ):
            return await self.settlement.settle_cex_dex_trial(trial)
        return await super()._settle_universal_position(position, trial, snapshot)

    def _open_from_allocation(
        self,
        allocation: UnifiedPaperAllocation,
        trial: PaperAllocationTrial,
        *,
        observed_at: datetime,
    ) -> CanonicalPortfolioEvent:
        event = super()._open_from_allocation(
            allocation,
            trial,
            observed_at=observed_at,
        )
        if (
            trial.settlement_method
            != CexDexCanonicalAllocationCertificationService.CEX_DEX_SETTLEMENT_METHOD
        ):
            return event
        details = dict(event.details)
        details.update(
            {
                "valuation_method": "cex_dex_amount_specific_entry_then_requote",
                "entry_evidence_id": allocation.evidence_id,
                "interim_pnl_claimed": False,
                "canonical_cex_dex_settlement": True,
                "paper_only": True,
            }
        )
        return event.model_copy(
            update={
                "unrealized_pnl_usd": 0.0,
                "details": details,
            }
        )

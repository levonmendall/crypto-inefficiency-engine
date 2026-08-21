from __future__ import annotations

import statistics
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from inefficiency_engine.allocation_certification import PaperAllocationOutcome, PaperAllocationTrial
from inefficiency_engine.canonical_paper_portfolio import CanonicalPortfolioEvent
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexAwareAllocationForwardCertificationService,
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService,
    CexDexUniversalOperationallyResilientPaperPortfolioService,
)
from inefficiency_engine.mechanism_execution import (
    MECHANISM_IDS,
    MechanismExecutionService,
    MechanismSettlementResult,
)
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.qualified_opportunity import allocate_prequalified_candidates
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperAllocationPlan


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ExecutableMechanismExecutionService(MechanismExecutionService):
    """Corrected production settlement helpers for the five mechanism lanes."""

    def _settle_yield(self, trial):
        exit_observation = self._next_yield_observation(trial)
        if exit_observation is None:
            return None
        entry_net = float(trial.settlement_payload.get("entry_net_apy") or 0.0)
        holding_hours = max(
            1.0,
            (trial.due_at - trial.source_observed_at).total_seconds() / 3600.0,
        )
        annualized_cost = (
            exit_observation.entry_exit_cost_bps / 10_000.0
        ) * 8760.0 / holding_hours
        risk = (
            exit_observation.credit_or_protocol_risk_haircut_apy
            + exit_observation.slashing_or_liquidation_risk_haircut_apy
            + exit_observation.incentive_decay_haircut_apy
        )
        exit_net = exit_observation.gross_apy - annualized_cost - risk
        gross = statistics.fmean([entry_net, exit_net]) * holding_hours / 8760.0
        if exit_observation.capacity_usd < trial.capital_usd:
            net = -max(
                0.0,
                float(trial.settlement_payload.get("entry_exit_cost_bps") or 0.0),
            ) / 10_000.0
            exit_ok = False
        else:
            net = gross
            exit_ok = True
        return MechanismSettlementResult(
            matured_at=exit_observation.observed_at,
            gross_return=gross,
            net_return=net,
            settlement_method="realized_yield_accrual_plus_exit_liquidity",
            detail={
                "exit_capacity_usd": exit_observation.capacity_usd,
                "exit_liquidity_sufficient": exit_ok,
                "exit_net_apy": exit_net,
                "holding_hours": holding_hours,
            },
        )

    def _any_spot_quote_after(self, *, asset: str, due_at: datetime) -> MarketQuote | None:
        table = self.store.market_quotes
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(table.c.payload_json)
                    .where(table.c.asset == asset.upper())
                    .where(table.c.observed_at >= due_at.isoformat())
                    .order_by(table.c.id)
                    .limit(500)
                ).scalars()
            )
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            if quote.market_kind == MarketKind.SPOT and quote.mid > 0:
                return quote
        return None

    def _settle_volatility(self, trial):
        option = self._next_option(trial)
        if option is None:
            return None
        payload = trial.settlement_payload
        entry_mid = float(payload.get("entry_mid") or 0.0)
        if entry_mid <= 0:
            return None
        exit_mid = (option.bid + option.ask) / 2.0
        option_return = exit_mid / entry_mid - 1.0
        direction = str(payload.get("direction") or "")
        if direction not in {"long_volatility", "short_volatility"}:
            return None
        directional = option_return if direction == "long_volatility" else -option_return
        underlying_entry = float(payload.get("underlying_entry_price") or 0.0)
        underlying_exit = self._any_spot_quote_after(asset=trial.asset, due_at=trial.due_at)
        if underlying_exit is None or underlying_entry <= 0:
            return None
        underlying_move = underlying_exit.mid / underlying_entry - 1.0
        hedge_cost = float(payload.get("hedge_cost_return") or 0.0)
        spread = float(payload.get("spread_fraction") or 0.0)
        residual_delta_penalty = (
            abs(underlying_move)
            * abs(float(payload.get("entry_delta") or 0.0))
            * 0.25
        )
        net = directional - hedge_cost - spread - residual_delta_penalty
        return MechanismSettlementResult(
            matured_at=max(option.observed_at, underlying_exit.observed_at),
            gross_return=directional,
            net_return=net,
            settlement_method=(
                "option_mark_forward_with_delta_hedge_cost_and_residual_penalty"
            ),
            detail={
                "entry_option_mid": entry_mid,
                "exit_option_mid": exit_mid,
                "option_mark_return": option_return,
                "underlying_return": underlying_move,
                "underlying_exit_venue": underlying_exit.venue,
                "residual_delta_penalty": residual_delta_penalty,
                "hedge_cost_return": hedge_cost,
                "spread_fraction": spread,
            },
        )


class AllLaneQualifiedOpportunityAllocatorService(
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService
):
    """Read both the established bridge and forward-qualified mechanism candidates."""

    def __init__(self, core, cex_dex, alpha_factory):
        super().__init__(core, cex_dex, alpha_factory)
        store = getattr(alpha_factory, "store", None)
        if store is None:
            raise RuntimeError("all-lane allocator requires durable evidence")
        self.mechanisms = ExecutableMechanismExecutionService(core, store)

    def _mechanism_proxy_candidates(self, *, total_capital_usd: float):
        now = _now()
        rows = []
        stale = []
        for item in self.mechanisms.promoted_proxy_candidates(
            total_capital_usd=total_capital_usd
        ):
            # Mechanism evidence has its own committed horizon. It does not share the
            # market-quote TTL used by spot/perp alpha, but it must still be recent
            # relative to that horizon and never older than 24 hours.
            horizon_seconds = max(300.0, float(item.modeled_holding_hours or 1.0) * 3600.0)
            max_age = min(86_400.0, horizon_seconds)
            source_at = item.source_observed_at
            age = (now - source_at).total_seconds() if source_at is not None else None
            if age is None or age < 0.0 or age > max_age:
                stale.append(
                    {
                        "candidate_id": item.candidate_id,
                        "family": "mechanism",
                        "reason": "mechanism candidate evidence stale; awaiting fresh forward-qualified observation",
                        "source_observed_at": source_at.isoformat() if source_at else None,
                        "max_age_seconds": max_age,
                    }
                )
                continue
            rows.append(item)
        return rows, stale

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
        bridge_candidates, failures, bridge_stale = self._active_candidates_with_diagnostics()
        mechanism_candidates, mechanism_stale = self._mechanism_proxy_candidates(
            total_capital_usd=total_capital_usd
        )
        plan = allocate_prequalified_candidates(
            self.settings,
            candidates=[*bridge_candidates, *mechanism_candidates],
            family_failures=failures,
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )
        converted: list[UnifiedPaperAllocation] = []
        for allocation in plan.allocations:
            if allocation.opportunity_id in MECHANISM_IDS:
                converted.append(allocation.model_copy(update={"family": "mechanism"}))
            else:
                converted.append(allocation)
        return plan.model_copy(
            update={
                "allocations": converted,
                "skipped": [*bridge_stale, *mechanism_stale, *plan.skipped],
            }
        )


class AllLaneAllocationForwardCertificationService(
    CexDexAwareAllocationForwardCertificationService
):
    """Extend canonical forward settlement to every mechanism lane."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.mechanisms = ExecutableMechanismExecutionService(core, store)

    def trial_from_allocation(
        self,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        if allocation.family != "mechanism":
            return super().trial_from_allocation(
                allocation,
                plan_observed_at=plan_observed_at,
            )
        candidate = self.mechanisms.ledger.candidate(allocation.candidate_id)
        if candidate is None:
            base = super().trial_from_allocation(
                allocation.model_copy(update={"family": "alpha"}),
                plan_observed_at=plan_observed_at,
            )
            return base.model_copy(
                update={
                    "family": "mechanism",
                    "settlement_supported": False,
                    "settlement_method": None,
                    "settlement_blocker": "durable mechanism candidate lineage is unavailable",
                }
            )
        due = candidate.observed_at + timedelta(hours=candidate.holding_hours)
        return PaperAllocationTrial(
            plan_observed_at=plan_observed_at,
            candidate_id=allocation.candidate_id,
            family="mechanism",
            strategy=allocation.strategy,
            asset=allocation.asset,
            venues=allocation.venues,
            exposure_kind=allocation.exposure_kind,
            capital_required_usd=allocation.capital_required_usd,
            notional_usd=allocation.notional_usd_per_leg,
            predicted_profit_usd=allocation.expected_profit_usd_per_deployment,
            predicted_return_on_reserved_capital=allocation.expected_return_on_reserved_capital,
            source_observed_at=candidate.observed_at,
            due_at=due,
            instrument_symbol=allocation.instrument_symbol or candidate.asset,
            instrument_market_kind="mechanism",
            entry_reference_price=1.0,
            modeled_roundtrip_cost_return=0.0,
            settlement_supported=True,
            settlement_method=f"mechanism:{candidate.mechanism_id}",
            settlement_blocker=None,
            cohort_key=f"mechanism|{candidate.cohort_key}",
            live_execution_authority=False,
            paper_only=True,
        )

    def _settle_mechanism(
        self,
        trial: PaperAllocationTrial,
    ) -> PaperAllocationOutcome | None:
        result = self.mechanisms.settle_canonical_candidate(
            trial.candidate_id,
            due_at=trial.due_at,
        )
        if result is None:
            return None
        realized_profit = trial.capital_required_usd * result.net_return
        error = realized_profit - trial.predicted_profit_usd
        capture = (
            realized_profit / trial.predicted_profit_usd
            if trial.predicted_profit_usd > 0
            else None
        )
        return PaperAllocationOutcome(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate_id,
            family="mechanism",
            strategy=trial.strategy,
            asset=trial.asset,
            matured_at=result.matured_at,
            due_at=trial.due_at or result.matured_at,
            realized_gross_return=result.gross_return,
            realized_net_return=result.net_return,
            realized_profit_usd=realized_profit,
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0,
            settlement_method=result.settlement_method,
            settlement_evidence_complete=True,
            live_execution_authority=False,
            paper_only=True,
        )

    def _settle_trial(self, trial, snapshot):
        if str(trial.settlement_method or "").startswith("mechanism:"):
            return self._settle_mechanism(trial)
        return super()._settle_trial(trial, snapshot)


class AllLaneOperationallyResilientPaperPortfolioService(
    CexDexUniversalOperationallyResilientPaperPortfolioService
):
    """Canonical paper account with all thirteen lane settlement contracts enabled."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.settlement = AllLaneAllocationForwardCertificationService(
            core,
            allocator,
            store,
        )

    def _trial_for_allocation(
        self,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        return self.settlement.trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )

    def _support_reason(
        self,
        allocation: UnifiedPaperAllocation,
    ) -> tuple[bool, str | None]:
        trial = self._trial_for_allocation(
            allocation,
            plan_observed_at=allocation.source_observed_at or _now(),
        )
        if trial.settlement_supported:
            return True, None
        return False, trial.settlement_blocker or "allocation lacks a canonical settlement contract"

    def _mark_universal(self, position, trial, snapshot):
        if not str(trial.settlement_method or "").startswith("mechanism:"):
            return super()._mark_universal(position, trial, snapshot)
        # Non-price mechanisms are carried at reserved capital until their native
        # forward settlement becomes observable. This avoids fabricated interim P&L.
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
                "valuation_method": "pending_native_mechanism_forward_settlement",
                "settlement_method": trial.settlement_method,
                "paper_only": True,
                "live_execution_authority": False,
            },
        )

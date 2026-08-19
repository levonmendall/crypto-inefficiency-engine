from __future__ import annotations

import uuid
from datetime import timedelta

from inefficiency_engine.canonical_paper_portfolio import (
    CanonicalPaperPortfolioCycle,
    CanonicalPaperPortfolioService,
    CanonicalPortfolioEvent,
)
from inefficiency_engine.models import MarketKind
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger, PortfolioIntegritySnapshot, ValuationStatus


class OperationallyResilientPaperPortfolioService(CanonicalPaperPortfolioService):
    """Canonical portfolio cycle with failure containment and valuation provenance.

    The accounting/event model is unchanged. This service adds an independent,
    append-only integrity record for every completed cycle and prevents allocator
    or mechanism-family degradation from erasing valid marks. New positions are
    fail-closed whenever an existing open position cannot be freshly valued.
    """

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.integrity = PortfolioIntegrityLedger(store)

    async def run_cycle(self) -> CanonicalPaperPortfolioCycle:
        self.ledger.ensure_genesis()
        snapshot = await self.core.collect_live_executability()
        quote_index = self._quote_index(snapshot)
        state = self.ledger.current_state(observed_at=snapshot.completed_at)
        closed = marked = opened = skipped = stale_positions = 0

        for position in state.positions:
            quote = self._quote_for(position, quote_index)
            if quote is None:
                stale_positions += 1
                continue
            if position.due_at <= snapshot.completed_at:
                self.ledger.record_event(self._close_event(position, quote, observed_at=snapshot.completed_at))
                closed += 1
            else:
                self.ledger.record_event(self._mark_event(position, quote, observed_at=snapshot.completed_at))
                marked += 1

        after_close = self.ledger.current_state(observed_at=snapshot.completed_at)
        cash_available = max(0.0, after_close.cash_usd)
        open_keys = {self._position_key(position) for position in after_close.positions}
        allocation_error_type: str | None = None
        family_failures: list[dict[str, object]] = []
        market_evidence_at = snapshot.completed_at

        # If even one existing position lacks a current quote, NAV/risk is not
        # decision-grade. Preserve marks/accounting, but do not deploy more cash.
        allow_new_allocations = stale_positions == 0
        if stale_positions:
            allocation_error_type = "StaleOpenPositionValuation"

        if cash_available > 0 and allow_new_allocations:
            try:
                plan = await self.allocator.allocate(total_capital_usd=cash_available)
            except Exception as exc:
                plan = None
                allocation_error_type = type(exc).__name__
            if plan is not None:
                family_failures = [dict(item) for item in getattr(plan, "family_failures", [])]
                remaining_cash = cash_available
                for allocation in plan.allocations:
                    supported, reason = self._support_reason(allocation)
                    venue = allocation.venues[0] if allocation.venues else None
                    symbol = allocation.instrument_symbol
                    if not supported:
                        self.ledger.record_event(CanonicalPortfolioEvent(
                            event_type="skip",
                            observed_at=plan.observed_at,
                            candidate_id=allocation.candidate_id,
                            family=allocation.family,
                            strategy=allocation.strategy,
                            asset=allocation.asset,
                            venue=venue,
                            symbol=symbol,
                            market_kind=allocation.instrument_market_kind,
                            exposure_kind=allocation.exposure_kind,
                            reason=reason,
                        ))
                        skipped += 1
                        continue
                    assert venue is not None and symbol is not None
                    if (venue, symbol) in open_keys:
                        self.ledger.record_event(CanonicalPortfolioEvent(
                            event_type="skip",
                            observed_at=plan.observed_at,
                            candidate_id=allocation.candidate_id,
                            family=allocation.family,
                            strategy=allocation.strategy,
                            asset=allocation.asset,
                            venue=venue,
                            symbol=symbol,
                            market_kind=allocation.instrument_market_kind,
                            exposure_kind=allocation.exposure_kind,
                            reason="canonical portfolio already has an open position in this venue/symbol",
                        ))
                        skipped += 1
                        continue
                    capital = allocation.capital_required_usd
                    if capital > remaining_cash + 1e-9:
                        self.ledger.record_event(CanonicalPortfolioEvent(
                            event_type="skip",
                            observed_at=plan.observed_at,
                            candidate_id=allocation.candidate_id,
                            family=allocation.family,
                            strategy=allocation.strategy,
                            asset=allocation.asset,
                            venue=venue,
                            symbol=symbol,
                            reason="insufficient canonical paper cash after existing open positions",
                        ))
                        skipped += 1
                        continue
                    assert allocation.entry_reference_price is not None
                    assert allocation.modeled_roundtrip_cost_return is not None
                    assert allocation.modeled_holding_hours is not None
                    position_id = uuid.uuid4().hex
                    due_at = (allocation.source_observed_at or plan.observed_at) + timedelta(
                        hours=allocation.modeled_holding_hours
                    )
                    initial_unrealized = (
                        -allocation.notional_usd_per_leg * allocation.modeled_roundtrip_cost_return
                    )
                    self.ledger.record_event(CanonicalPortfolioEvent(
                        event_type="open",
                        observed_at=plan.observed_at,
                        position_id=position_id,
                        candidate_id=allocation.candidate_id,
                        family=allocation.family,
                        strategy=allocation.strategy,
                        asset=allocation.asset,
                        venue=venue,
                        symbol=symbol,
                        market_kind=allocation.instrument_market_kind,
                        exposure_kind=allocation.exposure_kind,
                        cash_delta_usd=-capital,
                        capital_reserved_usd=capital,
                        notional_usd=allocation.notional_usd_per_leg,
                        entry_reference_price=allocation.entry_reference_price,
                        reference_price=allocation.entry_reference_price,
                        due_at=due_at,
                        modeled_roundtrip_cost_return=allocation.modeled_roundtrip_cost_return,
                        unrealized_pnl_usd=initial_unrealized,
                        reason="qualified allocator decision opened in canonical paper portfolio",
                        details={
                            "expected_profit_usd": allocation.expected_profit_usd_per_deployment,
                            "expected_return_on_reserved_capital": allocation.expected_return_on_reserved_capital,
                            "source_return_metric": allocation.source_return_metric,
                            "source_return_value": allocation.source_return_value,
                        },
                    ))
                    if allocation.source_observed_at is not None:
                        market_evidence_at = max(market_evidence_at, allocation.source_observed_at)
                    remaining_cash -= capital
                    open_keys.add((venue, symbol))
                    opened += 1

        final_state = self.ledger.current_state(observed_at=snapshot.completed_at)
        self.ledger.record_snapshot(final_state)

        if final_state.open_position_count == 0:
            valuation_status: ValuationStatus = "cash_only"
        elif stale_positions == 0:
            valuation_status = "fresh"
        elif stale_positions < final_state.open_position_count:
            valuation_status = "partial"
        else:
            valuation_status = "stale"
        degraded = bool(stale_positions or allocation_error_type or family_failures)
        integrity = PortfolioIntegritySnapshot(
            observed_at=snapshot.completed_at,
            account_snapshot_at=final_state.observed_at,
            market_evidence_at=market_evidence_at,
            valuation_status=valuation_status,
            cycle_status="degraded" if degraded else "success",
            fallback_snapshot=False,
            cycle_error_type=allocation_error_type,
            stale_position_count=stale_positions,
            open_position_count=final_state.open_position_count,
            allocation_family_failures=family_failures,
            market_snapshot_id=snapshot.scan_id,
        )
        self.integrity.record(integrity)

        return CanonicalPaperPortfolioCycle(
            observed_at=snapshot.completed_at,
            opened_position_count=opened,
            closed_position_count=closed,
            marked_position_count=marked,
            skipped_allocation_count=skipped,
            nav_usd=final_state.nav_usd,
            cash_usd=final_state.cash_usd,
        )

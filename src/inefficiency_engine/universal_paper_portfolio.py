from __future__ import annotations

from datetime import datetime

from inefficiency_engine.canonical_paper_portfolio import (
    CanonicalPaperPortfolioCycle,
    CanonicalPortfolioEvent,
)
from inefficiency_engine.portfolio_integrity import PortfolioIntegritySnapshot, ValuationStatus
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService
from inefficiency_engine.universal_settlement import UniversalSettlementMixin


class UniversalOperationallyResilientPaperPortfolioService(
    UniversalSettlementMixin,
    OperationallyResilientPaperPortfolioService,
):
    """Canonical account that can express every currently settlement-supported family.

    Heavy discovery never runs here. The service consumes the qualified-opportunity
    bridge, reuses the canonical settlement contracts, and fetches only bounded
    settlement L2 for positions that have actually reached their committed horizon.
    """

    def __init__(self, core, allocator, store):
        OperationallyResilientPaperPortfolioService.__init__(self, core, allocator, store)
        self._init_universal_settlement(core, allocator, store)

    async def run_cycle(self) -> CanonicalPaperPortfolioCycle:
        self.ledger.ensure_genesis()
        previous_integrity = self.integrity.latest()
        snapshot = await self._collect_canonical_market_snapshot()
        quote_index = self._quote_index(snapshot)
        state = self.ledger.current_state(observed_at=snapshot.completed_at)
        prior_evidence_times = self._latest_position_evidence_times()
        open_events = self._open_event_map()
        fresh_position_evidence: dict[str, datetime] = {}
        closed = marked = opened = skipped = stale_positions = settlement_blocked = 0

        for position in state.positions:
            event = open_events.get(position.position_id)
            trial = self._event_trial(event)
            if trial is not None:
                # The portfolio event embeds the immutable trial for recovery; the
                # allocation ledger is the operating-certification source of truth.
                # Re-recording is idempotent and repairs a crash between the event
                # append and the original trial append.
                self.settlement.ledger.record_trial(trial)
            is_universal = bool(
                trial is not None
                and trial.settlement_method
                != self.settlement.SETTLEMENT_METHOD
            )

            if is_universal:
                assert trial is not None
                if position.due_at <= snapshot.completed_at:
                    outcome = await self._settle_universal_position(
                        position,
                        trial,
                        snapshot,
                    )
                    if outcome is None:
                        stale_positions += 1
                        settlement_blocked += 1
                        continue
                    self.settlement.ledger.record_outcome(outcome)
                    self.ledger.record_event(
                        self._close_from_outcome(position, outcome)
                    )
                    closed += 1
                    continue

                mark = self._mark_universal(position, trial, snapshot)
                if mark is None:
                    stale_positions += 1
                    continue
                prior_evidence = prior_evidence_times.get(
                    position.position_id,
                    position.opened_at,
                )
                if mark.observed_at > prior_evidence:
                    self.ledger.record_event(mark)
                    marked += 1
                fresh_position_evidence[position.position_id] = mark.observed_at
                continue

            quote = self._quote_for(position, quote_index)
            if quote is None:
                stale_positions += 1
                continue
            prior_evidence = prior_evidence_times.get(
                position.position_id,
                position.opened_at,
            )
            if quote.observed_at < prior_evidence:
                stale_positions += 1
                continue
            if (
                position.due_at <= snapshot.completed_at
                and quote.observed_at < position.due_at
            ):
                if quote.observed_at > prior_evidence:
                    self.ledger.record_event(
                        self._mark_event(position, quote, observed_at=quote.observed_at)
                    )
                    marked += 1
                stale_positions += 1
                settlement_blocked += 1
                continue
            if position.due_at <= quote.observed_at:
                self.ledger.record_event(
                    self._close_event(position, quote, observed_at=quote.observed_at)
                )
                closed += 1
            else:
                if quote.observed_at > prior_evidence:
                    self.ledger.record_event(
                        self._mark_event(position, quote, observed_at=quote.observed_at)
                    )
                    marked += 1
                fresh_position_evidence[position.position_id] = quote.observed_at

        after_close = self.ledger.current_state(observed_at=snapshot.completed_at)
        cash_available = max(0.0, after_close.cash_usd)
        open_events = self._open_event_map()
        used_conflicts: set[str] = set()
        for position in after_close.positions:
            trial = self._event_trial(open_events.get(position.position_id))
            if trial is not None:
                used_conflicts.update(self._trial_conflicts(trial))
            else:
                used_conflicts.add(f"venue-symbol:{position.venue}:{position.symbol}")

        if after_close.open_position_count == 0:
            checkpoint_valuation: ValuationStatus = "cash_only"
            checkpoint_market_evidence = snapshot.completed_at
        elif stale_positions == 0:
            checkpoint_valuation = "fresh"
            checkpoint_market_evidence = (
                min(fresh_position_evidence.values())
                if fresh_position_evidence
                else snapshot.completed_at
            )
        elif stale_positions < after_close.open_position_count:
            checkpoint_valuation = "partial"
            checkpoint_market_evidence = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None
                else None
            )
        else:
            checkpoint_valuation = "stale"
            checkpoint_market_evidence = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None
                else None
            )

        self.integrity.record(
            PortfolioIntegritySnapshot(
                observed_at=snapshot.completed_at,
                account_snapshot_at=after_close.observed_at,
                market_evidence_at=checkpoint_market_evidence,
                valuation_status=checkpoint_valuation,
                cycle_status="accounting_only",
                fallback_snapshot=False,
                stale_position_count=stale_positions,
                settlement_evidence_blocked_count=settlement_blocked,
                open_position_count=after_close.open_position_count,
                market_snapshot_id=snapshot.scan_id,
            )
        )

        allocation_error_type: str | None = None
        family_failures: list[dict[str, object]] = []
        if settlement_blocked:
            allocation_error_type = "SettlementEvidenceUnavailable"
        elif stale_positions:
            allocation_error_type = "StaleOpenPositionValuation"

        if cash_available > 0 and stale_positions == 0:
            plan, bounded_error = await self._bounded_allocation_plan(
                total_capital_usd=cash_available
            )
            if bounded_error is not None:
                allocation_error_type = bounded_error
                family_failures = [
                    {
                        "family": "canonical_allocator",
                        "error_type": bounded_error,
                        "reason": (
                            "universal canonical accounting remained live while the durable "
                            "allocation stage failed its bounded liveness contract"
                        ),
                    }
                ]
            if plan is not None:
                family_failures = [
                    dict(item) for item in getattr(plan, "family_failures", [])
                ]
                remaining_cash = cash_available
                for allocation in plan.allocations:
                    trial = self._trial_for_allocation(
                        allocation,
                        plan_observed_at=plan.observed_at,
                    )
                    if not trial.settlement_supported:
                        self.ledger.record_event(
                            CanonicalPortfolioEvent(
                                event_type="skip",
                                observed_at=plan.observed_at,
                                candidate_id=allocation.candidate_id,
                                family=allocation.family,
                                strategy=allocation.strategy,
                                asset=allocation.asset,
                                venue=allocation.venues[0] if allocation.venues else None,
                                symbol=allocation.instrument_symbol,
                                market_kind=allocation.instrument_market_kind,
                                exposure_kind=allocation.exposure_kind,
                                reason=trial.settlement_blocker,
                            )
                        )
                        skipped += 1
                        continue
                    conflicts = self._trial_conflicts(trial)
                    if used_conflicts.intersection(conflicts):
                        self.ledger.record_event(
                            CanonicalPortfolioEvent(
                                event_type="skip",
                                observed_at=plan.observed_at,
                                candidate_id=allocation.candidate_id,
                                family=allocation.family,
                                strategy=allocation.strategy,
                                asset=allocation.asset,
                                reason="canonical portfolio already has a conflicting open instrument",
                            )
                        )
                        skipped += 1
                        continue
                    capital = allocation.capital_required_usd
                    if capital > remaining_cash + 1e-9:
                        self.ledger.record_event(
                            CanonicalPortfolioEvent(
                                event_type="skip",
                                observed_at=plan.observed_at,
                                candidate_id=allocation.candidate_id,
                                family=allocation.family,
                                strategy=allocation.strategy,
                                asset=allocation.asset,
                                reason="insufficient canonical paper cash after existing open positions",
                            )
                        )
                        skipped += 1
                        continue

                    event = self._open_from_allocation(
                        allocation,
                        trial,
                        observed_at=plan.observed_at,
                    )
                    self.ledger.record_event(event)
                    self.settlement.ledger.record_trial(trial)
                    fresh_position_evidence[event.position_id or ""] = (
                        allocation.source_observed_at or plan.observed_at
                    )
                    remaining_cash -= capital
                    used_conflicts.update(conflicts)
                    opened += 1

        final_state = self.ledger.current_state(observed_at=snapshot.completed_at)
        self.ledger.record_snapshot(final_state)

        if final_state.open_position_count == 0:
            valuation_status: ValuationStatus = "cash_only"
            market_evidence_at = snapshot.completed_at
        elif stale_positions == 0:
            valuation_status = "fresh"
            market_evidence_at = (
                min(fresh_position_evidence.values())
                if fresh_position_evidence
                else snapshot.completed_at
            )
        elif stale_positions < final_state.open_position_count:
            valuation_status = "partial"
            market_evidence_at = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None
                else None
            )
        else:
            valuation_status = "stale"
            market_evidence_at = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None
                else None
            )

        degraded = bool(stale_positions or allocation_error_type or family_failures)
        self.integrity.record(
            PortfolioIntegritySnapshot(
                observed_at=snapshot.completed_at,
                account_snapshot_at=final_state.observed_at,
                market_evidence_at=market_evidence_at,
                valuation_status=valuation_status,
                cycle_status="degraded" if degraded else "success",
                fallback_snapshot=False,
                cycle_error_type=allocation_error_type,
                stale_position_count=stale_positions,
                settlement_evidence_blocked_count=settlement_blocked,
                open_position_count=final_state.open_position_count,
                allocation_family_failures=family_failures,
                market_snapshot_id=snapshot.scan_id,
            )
        )

        return CanonicalPaperPortfolioCycle(
            observed_at=snapshot.completed_at,
            opened_position_count=opened,
            closed_position_count=closed,
            marked_position_count=marked,
            skipped_allocation_count=skipped,
            nav_usd=final_state.nav_usd,
            cash_usd=final_state.cash_usd,
        )

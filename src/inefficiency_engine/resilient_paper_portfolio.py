from __future__ import annotations

import asyncio
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from inefficiency_engine.canonical_paper_portfolio import (
    CanonicalPaperPortfolioCycle,
    CanonicalPaperPortfolioService,
    CanonicalPortfolioEvent,
)
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger, PortfolioIntegritySnapshot, ValuationStatus


CANONICAL_ALLOCATION_TIMEOUT_SECONDS = 30.0


class OperationallyResilientPaperPortfolioService(CanonicalPaperPortfolioService):
    """Canonical portfolio cycle with failure containment and valuation provenance.

    Canonical accounting only requires fresh point-in-time market quotes to value
    and settle existing positions. It deliberately avoids both broad opportunity
    analysis and all-opportunity L2 executability fanout: research owns those heavy
    transforms, while paper promotion fetches bounded L2 only after a candidate has
    already cleared statistical qualification. This keeps accounting live without
    weakening any deployment gate.
    """

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.integrity = PortfolioIntegrityLedger(store)
        # Release-D/all-lane allocation is durable DB + CPU work. Keep exactly one
        # isolated worker so that synchronous qualification/ranking can never pin the
        # canonical asyncio loop and defeat its outer stage deadline. A timed-out
        # allocation is allowed to finish in the background, but no replacement is
        # submitted until that single worker completes, preventing thread buildup.
        self._allocation_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="canonical-allocation",
        )
        self._allocation_future: Future | None = None

    def _latest_position_evidence_times(self) -> dict[str, datetime]:
        latest: dict[str, datetime] = {}
        for event in self.ledger.events_all():
            if not event.position_id:
                continue
            if event.event_type == "open":
                raw = event.details.get("valuation_evidence_observed_at")
                if isinstance(raw, str):
                    try:
                        latest[event.position_id] = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    except ValueError:
                        latest[event.position_id] = event.observed_at
                else:
                    latest[event.position_id] = event.observed_at
            elif event.event_type == "mark":
                latest[event.position_id] = event.observed_at
            elif event.event_type == "close":
                latest.pop(event.position_id, None)
        return latest

    async def _bounded_allocation_plan(self, *, total_capital_usd: float):
        """Return a plan without allowing sync allocator work to block accounting.

        Production's lane-success allocator exposes ``allocate_sync`` because its
        work is synchronous durable-state analysis despite an async compatibility
        surface. Run that core in one dedicated thread and enforce a 30-second
        liveness budget. If an older/test allocator has no sync core, preserve the
        existing async behavior.
        """

        allocate_sync = getattr(self.allocator, "allocate_sync", None)
        if not callable(allocate_sync):
            try:
                return await self.allocator.allocate(total_capital_usd=total_capital_usd), None
            except Exception as exc:
                return None, type(exc).__name__

        previous = self._allocation_future
        if previous is not None:
            if not previous.done():
                return None, "AllocationStageStillRunning"
            # A result that completed after the prior deadline is intentionally
            # discarded: its point-in-time candidate evidence may already be stale.
            try:
                previous.result()
            except Exception:
                pass
            self._allocation_future = None

        future = self._allocation_executor.submit(
            allocate_sync,
            total_capital_usd=total_capital_usd,
        )
        self._allocation_future = future
        try:
            wrapped = asyncio.wrap_future(future)
            plan = await asyncio.wait_for(
                asyncio.shield(wrapped),
                timeout=CANONICAL_ALLOCATION_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return None, "AllocationStageTimeout"
        except Exception as exc:
            self._allocation_future = None
            return None, type(exc).__name__

        self._allocation_future = None
        return plan, None

    async def _collect_canonical_market_snapshot(self) -> ScanSnapshot:
        """Persist the public quote/funding surface without synchronous opportunity analysis.

        `OpportunityService.collect_live_evidence()` also constructs the global
        opportunity graph. With broad exchange surfaces that CPU-bound transform can
        consume the canonical 120-second stage budget even though provider I/O itself
        is independently bounded. Canonical valuation and alpha history require the
        raw quotes, not a completed global opportunity graph, so persist that surface
        directly and leave full analysis to the auxiliary research worker.
        """

        # Keep the service usable with deliberately minimal test/injected cores.
        # Production OpportunityService always exposes adapter_registry and therefore
        # always takes the raw bounded-provider path below.
        if not hasattr(self.core, "adapter_registry"):
            return await self.core.collect_live_evidence()

        started_at = datetime.now(timezone.utc)
        funding_quotes, market_quotes, providers = await self.core.adapter_registry.collect_inputs()
        completed_at = datetime.now(timezone.utc)
        if not market_quotes:
            raise RuntimeError("canonical public market surface returned no market quotes")

        analysis_config = asdict(self.core.settings)
        scan_id = self.store.record_scan(
            funding_quotes=funding_quotes,
            market_quotes=market_quotes,
            opportunities=[],
            providers=providers,
            started_at=started_at,
            completed_at=completed_at,
            analysis_config=analysis_config,
        )
        return ScanSnapshot(
            scan_id=scan_id,
            started_at=started_at,
            completed_at=completed_at,
            providers=providers,
            funding_quotes=funding_quotes,
            market_quotes=market_quotes,
            opportunities=[],
            analysis_config=analysis_config,
        )

    async def run_cycle(self) -> CanonicalPaperPortfolioCycle:
        self.ledger.ensure_genesis()
        previous_integrity = self.integrity.latest()

        # Production showed advancing fallback account timestamps while market
        # evidence stayed frozen. Canonical collection must therefore stop before
        # global opportunity discovery as well as before L2 fanout. Persist the raw
        # bounded provider surface first; candidate-level L2 remains mandatory later.
        snapshot = await self._collect_canonical_market_snapshot()
        quote_index = self._quote_index(snapshot)
        state = self.ledger.current_state(observed_at=snapshot.completed_at)
        prior_evidence_times = self._latest_position_evidence_times()
        fresh_position_evidence: dict[str, datetime] = {}
        closed = marked = opened = skipped = stale_positions = settlement_blocked = 0

        for position in state.positions:
            quote = self._quote_for(position, quote_index)
            if quote is None:
                stale_positions += 1
                continue
            prior_evidence = prior_evidence_times.get(position.position_id, position.opened_at)
            if quote.observed_at < prior_evidence:
                stale_positions += 1
                continue

            # Settlement is only legitimate when the price observation itself is
            # at/after the committed holding horizon. Scan completion time is not
            # a substitute for post-horizon market evidence.
            if position.due_at <= snapshot.completed_at and quote.observed_at < position.due_at:
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
        open_keys = {self._position_key(position) for position in after_close.positions}
        allocation_error_type: str | None = None
        family_failures: list[dict[str, object]] = []

        # Checkpoint truthful market provenance before allocation. If a downstream
        # allocator/provider call later consumes the outer stage deadline, the
        # fallback snapshot can preserve the fresh market timestamp instead of
        # making a successful public-data collection look frozen for hours.
        if after_close.open_position_count == 0:
            checkpoint_valuation: ValuationStatus = "cash_only"
            checkpoint_market_evidence = snapshot.completed_at
        elif stale_positions == 0:
            checkpoint_valuation = "fresh"
            checkpoint_market_evidence = (
                min(fresh_position_evidence.values())
                if fresh_position_evidence else snapshot.completed_at
            )
        elif stale_positions < after_close.open_position_count:
            checkpoint_valuation = "partial"
            checkpoint_market_evidence = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None else None
            )
        else:
            checkpoint_valuation = "stale"
            checkpoint_market_evidence = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None else None
            )

        checkpoint = PortfolioIntegritySnapshot(
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
        self.integrity.record(checkpoint)

        # Preserve marks and account history, but never deploy new capital if the
        # current portfolio itself is not decision-grade.
        allow_new_allocations = stale_positions == 0
        if settlement_blocked:
            allocation_error_type = "SettlementEvidencePreHorizon"
        elif stale_positions:
            allocation_error_type = "StaleOpenPositionValuation"

        if cash_available > 0 and allow_new_allocations:
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
                            "canonical accounting remained live while the durable "
                            "allocation stage failed its bounded liveness contract"
                        ),
                    }
                ]
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
                    evidence_at = allocation.source_observed_at or plan.observed_at
                    due_at = evidence_at + timedelta(hours=allocation.modeled_holding_hours)
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
                            "valuation_evidence_observed_at": evidence_at.isoformat(),
                        },
                    ))
                    fresh_position_evidence[position_id] = evidence_at
                    remaining_cash -= capital
                    open_keys.add((venue, symbol))
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
                if fresh_position_evidence else snapshot.completed_at
            )
        elif stale_positions < final_state.open_position_count:
            valuation_status = "partial"
            market_evidence_at = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None else None
            )
        else:
            valuation_status = "stale"
            market_evidence_at = (
                previous_integrity.market_evidence_at
                if previous_integrity is not None else None
            )

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
            settlement_evidence_blocked_count=settlement_blocked,
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

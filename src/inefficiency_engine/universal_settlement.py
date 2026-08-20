from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

from inefficiency_engine.allocation_certification import (
    AllocationForwardCertificationService,
    PaperAllocationOutcome,
    PaperAllocationTrial,
)
from inefficiency_engine.canonical_paper_portfolio import CanonicalPortfolioEvent
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import MarketKind, OpportunityLeg, Side
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation


class UniversalSettlementMixin:
    """Family-neutral canonical settlement helpers backed by certified paper contracts."""

    def _init_universal_settlement(self, core, allocator, store) -> None:
        self.settlement = AllocationForwardCertificationService(core, allocator, store)

    @staticmethod
    def _trial_for_allocation(
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        return AllocationForwardCertificationService.trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )

    @staticmethod
    def _support_reason(
        allocation: UnifiedPaperAllocation,
    ) -> tuple[bool, str | None]:
        trial = AllocationForwardCertificationService.trial_from_allocation(
            allocation,
            plan_observed_at=allocation.source_observed_at or datetime.now(timezone.utc),
        )
        if trial.settlement_supported:
            return True, None
        return False, trial.settlement_blocker or "allocation lacks a canonical settlement contract"

    @staticmethod
    def _trial_conflicts(trial: PaperAllocationTrial) -> set[str]:
        if trial.settlement_legs:
            return {
                f"venue-symbol:{leg.venue}:{leg.symbol}"
                for leg in trial.settlement_legs
            }
        if trial.venues and trial.instrument_symbol:
            return {f"venue-symbol:{trial.venues[0]}:{trial.instrument_symbol}"}
        return {f"candidate:{trial.candidate_id}"}

    def _open_event_map(self) -> dict[str, CanonicalPortfolioEvent]:
        opened: dict[str, CanonicalPortfolioEvent] = {}
        closed: set[str] = set()
        for event in self.ledger.events_all():
            if not event.position_id:
                continue
            if event.event_type == "open":
                opened[event.position_id] = event
            elif event.event_type == "close":
                closed.add(event.position_id)
        return {key: value for key, value in opened.items() if key not in closed}

    @staticmethod
    def _event_trial(event: CanonicalPortfolioEvent | None) -> PaperAllocationTrial | None:
        if event is None:
            return None
        raw = event.details.get("settlement_trial")
        if not isinstance(raw, dict):
            return None
        try:
            return PaperAllocationTrial.model_validate(raw)
        except Exception:
            return None

    def _mark_universal(
        self,
        position,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> CanonicalPortfolioEvent | None:
        if trial.source_observed_at is None:
            return None
        quote_index = self._quote_index(snapshot)
        leg_quotes = []
        for leg in trial.settlement_legs:
            quote = quote_index.get(
                (leg.venue, leg.asset.upper(), leg.market_kind, leg.symbol)
            )
            if quote is None or quote.mid <= 0:
                return None
            leg_quotes.append((leg, quote))
        if not leg_quotes:
            return None

        evidence_at = min(quote.observed_at for _, quote in leg_quotes)
        end_at = min(evidence_at, trial.due_at or evidence_at)
        price_pnl = 0.0
        funding_pnl = 0.0
        reference_price: float | None = None
        for leg, quote in leg_quotes:
            reference_price = reference_price or quote.mid
            price_pnl += (
                leg.base_quantity * (quote.mid - leg.entry_price)
                if leg.side == "long"
                else leg.base_quantity * (leg.entry_price - quote.mid)
            )
            funding = self.settlement._funding_for_leg(
                leg,
                source_at=trial.source_observed_at,
                due_at=end_at,
            )
            if funding is None:
                return None
            funding_pnl += funding[0]

        if trial.settlement_method == self.settlement.PERP_SHORT_SETTLEMENT_METHOD:
            modeled_cost = trial.notional_usd * float(
                trial.modeled_roundtrip_cost_return or 0.0
            )
        else:
            modeled_cost = trial.notional_usd * float(
                trial.modeled_non_slippage_cost_bps or 0.0
            ) / 10_000.0
        unrealized = price_pnl + funding_pnl - modeled_cost
        return CanonicalPortfolioEvent(
            event_type="mark",
            observed_at=evidence_at,
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
            reference_price=reference_price or position.current_reference_price,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            unrealized_pnl_usd=unrealized,
            details={
                "valuation_method": "multi_leg_mid_plus_observed_funding_minus_precommitted_cost",
                "price_pnl_usd": price_pnl,
                "funding_pnl_usd": funding_pnl,
                "modeled_cost_usd": modeled_cost,
            },
        )

    async def _settlement_books(
        self,
        trial: PaperAllocationTrial,
    ) -> list:
        if trial.settlement_method != self.settlement.MULTI_LEG_SETTLEMENT_METHOD:
            return []
        registry = getattr(self.core, "adapter_registry", None)
        if registry is None:
            return []
        timeout = max(0.05, float(getattr(registry, "order_book_timeout_seconds", 8.0)))

        async def fetch(leg):
            request = registry.book_request(
                OpportunityLeg(
                    venue=leg.venue,
                    asset=leg.asset,
                    market_kind=MarketKind(leg.market_kind),
                    side=Side(leg.side),
                    symbol=leg.symbol,
                    quote_currency=leg.quote_currency,
                    contract_key=leg.contract_key,
                    reference_price=leg.entry_price,
                )
            )
            if request is None:
                return None
            try:
                return await asyncio.wait_for(request.awaitable, timeout=timeout)
            except Exception:
                return None

        rows = await asyncio.gather(*(fetch(leg) for leg in trial.settlement_legs))
        if any(row is None for row in rows):
            return []
        return list(rows)

    async def _settle_universal_position(
        self,
        position,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if trial.due_at is None:
            return None

        books = await self._settlement_books(trial)
        if trial.settlement_method == self.settlement.MULTI_LEG_SETTLEMENT_METHOD:
            if (
                len(books) != len(trial.settlement_legs)
                or any(book.observed_at < trial.due_at for book in books)
            ):
                return None
        elif trial.settlement_method == self.settlement.PERP_SHORT_SETTLEMENT_METHOD:
            if not trial.venues or not trial.instrument_symbol or not trial.instrument_market_kind:
                return None
            quote = self._quote_index(snapshot).get(
                (
                    trial.venues[0],
                    trial.asset.upper(),
                    trial.instrument_market_kind,
                    trial.instrument_symbol,
                )
            )
            if quote is None or quote.observed_at < trial.due_at:
                return None

        settle_snapshot = snapshot.model_copy(update={"order_books": books})
        return self.settlement._settle_trial(trial, settle_snapshot)

    @staticmethod
    def _close_from_outcome(
        position,
        outcome: PaperAllocationOutcome,
    ) -> CanonicalPortfolioEvent:
        return CanonicalPortfolioEvent(
            event_type="close",
            observed_at=outcome.matured_at,
            position_id=position.position_id,
            candidate_id=position.candidate_id,
            family=position.family,
            strategy=position.strategy,
            asset=position.asset,
            venue=position.venue,
            symbol=position.symbol,
            market_kind=position.market_kind,
            exposure_kind=position.exposure_kind,
            cash_delta_usd=position.capital_reserved_usd + outcome.realized_profit_usd,
            realized_pnl_delta_usd=outcome.realized_profit_usd,
            modeled_cost_usd=max(0.0, outcome.modeled_non_slippage_cost_usd),
            capital_reserved_usd=position.capital_reserved_usd,
            notional_usd=position.notional_usd,
            entry_reference_price=position.entry_reference_price,
            reference_price=outcome.exit_reference_price or position.current_reference_price,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            reason="qualified opportunity reached canonical paper settlement horizon",
            details={
                "settlement_method": outcome.settlement_method,
                "realized_gross_return": outcome.realized_gross_return,
                "realized_net_return": outcome.realized_net_return,
                "realized_price_pnl_usd": outcome.realized_price_pnl_usd,
                "realized_funding_pnl_usd": outcome.realized_funding_pnl_usd,
                "leg_outcomes": [
                    row.model_dump(mode="json") for row in outcome.leg_outcomes
                ],
            },
        )

    def _open_from_allocation(
        self,
        allocation: UnifiedPaperAllocation,
        trial: PaperAllocationTrial,
        *,
        observed_at: datetime,
    ) -> CanonicalPortfolioEvent:
        venue = allocation.venues[0] if allocation.venues else "multi"
        first_leg = trial.settlement_legs[0] if trial.settlement_legs else None
        symbol = (
            allocation.instrument_symbol
            or (first_leg.symbol if first_leg is not None else f"{allocation.strategy}:{allocation.asset}")
        )
        market_kind = (
            allocation.instrument_market_kind
            or (first_leg.market_kind if len(trial.settlement_legs) == 1 else "multi_leg")
        )
        entry_price = (
            allocation.entry_reference_price
            or (first_leg.entry_price if first_leg is not None else 1.0)
        )
        roundtrip = float(allocation.modeled_roundtrip_cost_return or 0.0)
        if trial.settlement_method == self.settlement.MULTI_LEG_SETTLEMENT_METHOD:
            initial_unrealized = -allocation.notional_usd_per_leg * float(
                allocation.modeled_non_slippage_cost_bps or 0.0
            ) / 10_000.0
        else:
            initial_unrealized = -allocation.notional_usd_per_leg * roundtrip
        return CanonicalPortfolioEvent(
            event_type="open",
            observed_at=observed_at,
            position_id=uuid.uuid4().hex,
            candidate_id=allocation.candidate_id,
            family=allocation.family,
            strategy=allocation.strategy,
            asset=allocation.asset,
            venue=venue,
            symbol=symbol,
            market_kind=market_kind,
            exposure_kind=allocation.exposure_kind,
            cash_delta_usd=-allocation.capital_required_usd,
            capital_reserved_usd=allocation.capital_required_usd,
            notional_usd=allocation.notional_usd_per_leg,
            entry_reference_price=entry_price,
            reference_price=entry_price,
            due_at=trial.due_at,
            modeled_roundtrip_cost_return=roundtrip,
            unrealized_pnl_usd=initial_unrealized,
            reason="fresh qualified-opportunity bridge decision opened in canonical paper portfolio",
            details={
                "expected_profit_usd": allocation.expected_profit_usd_per_deployment,
                "expected_return_on_reserved_capital": allocation.expected_return_on_reserved_capital,
                "source_return_metric": allocation.source_return_metric,
                "source_return_value": allocation.source_return_value,
                "valuation_evidence_observed_at": (
                    allocation.source_observed_at or observed_at
                ).isoformat(),
                "settlement_trial": trial.model_dump(mode="json"),
                "conflict_keys": sorted(self._trial_conflicts(trial)),
                "qualified_opportunity_bridge": True,
            },
        )

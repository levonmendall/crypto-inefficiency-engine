from __future__ import annotations

import asyncio

from inefficiency_engine.alpha_factory import AlphaCandidate
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.execution import estimate_market_order
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.models import OrderBookSnapshot, TradeSide


class BoundedExpandedAlphaFactoryService(ExpandedAlphaFactoryService):
    """Expanded alpha promotion that cannot stall canonical accounting on L2 I/O."""

    def _snapshot_book(
        self,
        candidate: AlphaCandidate,
        snapshot: ScanSnapshot,
    ) -> OrderBookSnapshot | None:
        matches = [
            book
            for book in snapshot.order_books
            if (
                book.venue == candidate.venue
                and book.asset.upper() == candidate.asset.upper()
                and book.market_kind == candidate.market_kind
                and book.symbol == candidate.symbol
                and book.observed_at <= snapshot.completed_at
            )
        ]
        if not matches:
            return None
        book = max(matches, key=lambda item: item.observed_at)
        age = max(0.0, (snapshot.completed_at - book.observed_at).total_seconds())
        if age > max(0.05, float(self.settings.max_order_book_age_seconds)):
            return None
        return book

    def _cost_from_book(
        self,
        candidate: AlphaCandidate,
        book: OrderBookSnapshot,
    ) -> float | None:
        try:
            side = TradeSide.BUY if candidate.direction == "long" else TradeSide.SELL
            estimate = estimate_market_order(book, side, candidate.notional_usd)
        except Exception:
            return None
        fee_bps = self._one_way_fee_bps(candidate.venue, candidate.market_kind)
        if fee_bps is None:
            return None
        total_bps = (
            2.0 * fee_bps
            + estimate.slippage_bps * (1.0 + self.settings.exit_slippage_multiplier)
            + self.settings.alpha_execution_risk_floor_bps
        )
        return max(0.0, total_bps / 10_000.0)

    @staticmethod
    def _holding_carry_cost(candidate: AlphaCandidate) -> float:
        raw = candidate.features.get("holding_carry_cost_return", 0.0)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 0.0

    async def _bounded_current_l2_cost(self, candidate: AlphaCandidate) -> float | None:
        timeout = max(
            0.05,
            float(getattr(self.core.adapter_registry, "order_book_timeout_seconds", 8.0)),
        )
        try:
            return await asyncio.wait_for(
                super()._current_l2_cost(candidate),
                timeout=timeout,
            )
        except TimeoutError:
            return None

    async def promoted_candidates(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        statistically_promoted: list[AlphaCandidate] = []
        for candidate in self.discover(snapshot, total_capital_usd=total_capital_usd):
            qualification = self.qualification(candidate)
            if not qualification.statistically_qualified:
                continue

            book = self._snapshot_book(candidate, snapshot)
            current_cost = (
                self._cost_from_book(candidate, book)
                if book is not None
                else await self._bounded_current_l2_cost(candidate)
            )
            if current_cost is None:
                continue
            current_cost += self._holding_carry_cost(candidate)

            net = candidate.expected_gross_return - current_cost
            conservative_forward = qualification.mean_realized_net_return_ci_lower or 0.0
            conservative = min(net, conservative_forward)
            if conservative <= self.settings.alpha_min_current_net_return:
                continue

            candidate.estimated_cost_return = current_cost
            candidate.expected_net_return = conservative
            candidate.expected_profit_usd = candidate.notional_usd * conservative
            candidate.stage = "paper_qualified"
            candidate.paper_allocation_eligible = True
            statistically_promoted.append(candidate)

        healthy: list[AlphaCandidate] = []
        for candidate in statistically_promoted:
            health = self.strategy_health(candidate)
            if not health.healthy_for_paper_allocation or health.capital_multiplier <= 0:
                continue
            scaled_notional = candidate.notional_usd * health.capital_multiplier
            if scaled_notional < self.settings.alpha_min_notional_usd:
                continue
            candidate.notional_usd = scaled_notional
            candidate.capital_required_usd *= health.capital_multiplier
            candidate.expected_profit_usd = candidate.expected_net_return * scaled_notional
            candidate.features.update({
                "health_score": health.health_score,
                "health_capital_multiplier": health.capital_multiplier,
                "health_recent_mean_net_return": health.recent_mean_net_return or 0.0,
                "health_recent_hit_rate": health.recent_hit_rate or 0.0,
                "health_capture_ratio_median": health.forecast_capture_ratio_median or 0.0,
                "health_recent_to_long_run_ratio": health.recent_to_long_run_ratio or 0.0,
                "health_max_compounded_drawdown": health.max_compounded_drawdown or 0.0,
                "health_trailing_loss_streak": health.trailing_loss_streak,
            })
            healthy.append(candidate)

        healthy.sort(
            key=lambda item: (item.expected_net_return, item.expected_profit_usd),
            reverse=True,
        )
        return healthy

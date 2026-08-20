from __future__ import annotations

from collections import defaultdict
from typing import Literal

from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, Opportunity, OrderBookSnapshot, Strategy
from inefficiency_engine.research_closure import RejectionFunnelSnapshot, ResearchClosureService


CandidateEconomics = tuple[float, float, float, str]


class MemoryBoundedResearchClosureService(ResearchClosureService):
    """Research closure diagnostics whose working set does not grow quadratically.

    The dashboard needs the best observed near-miss and the number of opportunities
    considered, not every pair materialized in memory. Cross-venue pair counts are
    computed algebraically and only the best candidate economics are retained.
    """

    @staticmethod
    def _better(current: CandidateEconomics | None, candidate: CandidateEconomics) -> CandidateEconomics:
        if current is None or candidate[2] > current[2]:
            return candidate
        return current

    @staticmethod
    def _different_venue_ordered_pair_count(rows: list[object]) -> int:
        venue_counts: dict[str, int] = defaultdict(int)
        for row in rows:
            venue_counts[str(getattr(row, "venue"))] += 1
        total = len(rows)
        return max(0, total * (total - 1) - sum(n * (n - 1) for n in venue_counts.values()))

    def _snapshot(
        self,
        mechanism_id: str,
        *,
        observed_at,
        raw_count: int,
        emitted_count: int,
        best: CandidateEconomics | None,
        required: float,
        unit: Literal["annualized_return", "horizon_return"],
        no_candidate_gate: str,
    ) -> RejectionFunnelSnapshot:
        if raw_count <= 0 or best is None:
            gate = no_candidate_gate
        elif emitted_count > 0:
            gate = "detector_emitted"
        elif best[0] <= 0:
            gate = "gross_edge_not_positive"
        elif best[2] <= required:
            gate = "net_return_hurdle"
        else:
            # This is intentionally loud: if the best raw economics clear the same
            # hurdle but the detector emits nothing, the implementation path needs
            # inspection rather than a lower threshold.
            gate = "detector_output_mismatch"
        return RejectionFunnelSnapshot(
            mechanism_id=mechanism_id,
            observed_at=observed_at,
            raw_candidate_count=max(0, raw_count),
            emitted_candidate_count=max(0, emitted_count),
            best_gross_economics=best[0] if best else None,
            best_cost_economics=best[1] if best else None,
            best_net_economics=best[2] if best else None,
            required_net_economics=required,
            gap_to_hurdle=(best[2] - required) if best else None,
            economics_unit=unit,
            dominant_rejection_gate=gate,
            rejection_gate_counts={gate: max(0, raw_count)} if raw_count else {},
            best_candidate_reference=best[3] if best else None,
        )

    def record_rejection_funnels(
        self,
        *,
        market_quotes: list[MarketQuote],
        funding_quotes: list[FundingQuote],
        opportunities: list[Opportunity],
        order_books: list[OrderBookSnapshot],
        microstructure_emitted_count: int,
        observed_at,
    ) -> dict[str, RejectionFunnelSnapshot]:
        annual_factor = 24.0 * 365.0
        required_annual = float(self.settings.min_net_annualized_return)

        # Price discrepancy: group by economically comparable instrument. Raw pair
        # counts are exact; only two best asks/bids are retained per group because
        # the best valid cross-venue pair can only require a second choice when the
        # absolute best bid and ask are on the same venue.
        spot_groups: dict[tuple[str, str], list[MarketQuote]] = defaultdict(list)
        for quote in market_quotes:
            if quote.market_kind == MarketKind.SPOT and quote.quote_currency and quote.bid and quote.ask:
                spot_groups[(quote.asset.upper(), quote.quote_currency.upper())].append(quote)
        price_raw = 0
        price_best: CandidateEconomics | None = None
        holding = max(1e-6, float(self.settings.spot_dislocation_holding_hours))
        price_cost_hour = ((float(self.settings.pair_roundtrip_cost_bps) / 10_000.0) / holding) + (
            float(self.settings.safety_buffer_bps_per_hour) / 10_000.0
        )
        for (asset, _quote_ccy), rows in spot_groups.items():
            price_raw += self._different_venue_ordered_pair_count(rows)
            buys = sorted(rows, key=lambda row: float(row.ask))[:2]
            sells = sorted(rows, key=lambda row: float(row.bid), reverse=True)[:2]
            for buy in buys:
                for sell in sells:
                    if buy.venue == sell.venue:
                        continue
                    gross_hour = ((float(sell.bid) / float(buy.ask)) - 1.0) / holding
                    candidate = (
                        gross_hour * annual_factor,
                        price_cost_hour * annual_factor,
                        (gross_hour - price_cost_hour) * annual_factor,
                        f"{asset}:{buy.venue}->{sell.venue}",
                    )
                    price_best = self._better(price_best, candidate)

        # Carry: process funding pairs and basis pairs per asset without ever
        # materializing the cross-product. The pair count can grow; memory does not.
        carry_raw = 0
        carry_best: CandidateEconomics | None = None
        funding_groups: dict[tuple[str, str], list[FundingQuote]] = defaultdict(list)
        for quote in funding_quotes:
            funding_groups[(quote.asset.upper(), (quote.quote_currency or "").upper())].append(quote)
        funding_holding = max(1e-6, float(self.settings.default_holding_hours))
        funding_cost_hour = ((float(self.settings.pair_roundtrip_cost_bps) / 10_000.0) / funding_holding) + (
            float(self.settings.safety_buffer_bps_per_hour) / 10_000.0
        )
        for (asset, _quote_ccy), rows in funding_groups.items():
            carry_raw += self._different_venue_ordered_pair_count(rows)
            longs = sorted(rows, key=lambda row: row.hourly_rate)[:2]
            shorts = sorted(rows, key=lambda row: row.hourly_rate, reverse=True)[:2]
            for long_quote in longs:
                for short_quote in shorts:
                    if long_quote.venue == short_quote.venue:
                        continue
                    gross_hour = short_quote.hourly_rate - long_quote.hourly_rate
                    carry_best = self._better(carry_best, (
                        gross_hour * annual_factor,
                        funding_cost_hour * annual_factor,
                        (gross_hour - funding_cost_hour) * annual_factor,
                        f"funding:{asset}:{long_quote.venue}->{short_quote.venue}",
                    ))

        market_groups: dict[tuple[str, str], list[MarketQuote]] = defaultdict(list)
        for quote in market_quotes:
            market_groups[(quote.asset.upper(), (quote.quote_currency or "").upper())].append(quote)
        for (asset, _quote_ccy), rows in market_groups.items():
            spots = [row for row in rows if row.market_kind == MarketKind.SPOT]
            derivatives = [row for row in rows if row.market_kind in {MarketKind.PERPETUAL, MarketKind.FUTURE}]
            carry_raw += len(spots) * len(derivatives)
            # Group size is venue/symbol bounded. We retain only the running best,
            # so even a broad universe cannot create a quadratic in-memory list.
            for spot in spots:
                if spot.mid <= 0:
                    continue
                for derivative in derivatives:
                    observed = min(spot.observed_at, derivative.observed_at)
                    if derivative.market_kind == MarketKind.FUTURE:
                        if derivative.expires_at is None or derivative.expires_at <= observed:
                            continue
                        basis_holding = max(1e-6, (derivative.expires_at - observed).total_seconds() / 3600.0)
                    else:
                        basis_holding = funding_holding
                    gross_hour = ((derivative.mid / spot.mid) - 1.0) / basis_holding
                    cost_hour = ((float(self.settings.pair_roundtrip_cost_bps) / 10_000.0) / basis_holding) + (
                        float(self.settings.safety_buffer_bps_per_hour) / 10_000.0
                    )
                    carry_best = self._better(carry_best, (
                        gross_hour * annual_factor,
                        cost_hour * annual_factor,
                        (gross_hour - cost_hour) * annual_factor,
                        f"basis:{asset}:{spot.venue}->{derivative.venue}:{derivative.market_kind.value}",
                    ))

        # Microstructure books are already bounded by the production L2 working set.
        micro_raw = 0
        micro_best: CandidateEconomics | None = None
        levels = max(1, int(getattr(self.settings, "alpha_microstructure_depth_levels", 5)))
        min_imbalance = float(getattr(self.settings, "alpha_microstructure_min_abs_imbalance", 0.20))
        return_scale = float(getattr(self.settings, "alpha_microstructure_return_scale", 0.012))
        max_return = float(getattr(self.settings, "alpha_microstructure_max_expected_return", 0.006))
        min_current = float(getattr(self.settings, "alpha_min_current_net_return", 0.0005))
        cost_floor = float(getattr(self.settings, "alpha_research_cost_floor_bps", 25.0)) / 10_000.0
        for book in order_books:
            if not book.bids or not book.asks:
                continue
            bids = sorted(book.bids, key=lambda item: item.price, reverse=True)[:levels]
            asks = sorted(book.asks, key=lambda item: item.price)[:levels]
            bid_depth = sum(item.price * item.size for item in bids)
            ask_depth = sum(item.price * item.size for item in asks)
            total = bid_depth + ask_depth
            if total <= 0:
                continue
            micro_raw += 1
            imbalance = (bid_depth - ask_depth) / total
            best_bid = max(item.price for item in book.bids)
            best_ask = min(item.price for item in book.asks)
            spread = (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
            gross = min(max_return, abs(imbalance) * return_scale)
            cost = max(cost_floor, spread)
            net = gross - cost
            if abs(imbalance) < min_imbalance:
                net = min(net, min_current - abs(min_imbalance - abs(imbalance)) * return_scale)
            micro_best = self._better(micro_best, (
                gross,
                cost,
                net,
                f"{book.asset}:{book.venue}:{book.symbol}:imbalance={imbalance:.4f}",
            ))

        price_emitted = sum(o.strategy == Strategy.CEX_SPOT_DISLOCATION for o in opportunities)
        carry_emitted = sum(
            o.strategy in {Strategy.FUNDING_DISPERSION, Strategy.SPOT_PERP_BASIS, Strategy.FUTURES_BASIS}
            for o in opportunities
        )
        rows = {
            "price_discrepancy": self._snapshot(
                "price_discrepancy", observed_at=observed_at, raw_count=price_raw,
                emitted_count=price_emitted, best=price_best, required=required_annual,
                unit="annualized_return", no_candidate_gate="no_comparable_cross_venue_spot_pair",
            ),
            "carry": self._snapshot(
                "carry", observed_at=observed_at, raw_count=carry_raw,
                emitted_count=carry_emitted, best=carry_best, required=required_annual,
                unit="annualized_return", no_candidate_gate="no_comparable_carry_pair",
            ),
            "microstructure": self._snapshot(
                "microstructure", observed_at=observed_at, raw_count=micro_raw,
                emitted_count=microstructure_emitted_count, best=micro_best, required=min_current,
                unit="horizon_return", no_candidate_gate="no_usable_order_book",
            ),
        }
        for row in rows.values():
            self.ledger.record_rejection(row)
        return rows

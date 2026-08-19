from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from hashlib import sha256

from inefficiency_engine.config import Settings
from inefficiency_engine.models import MarketKind, MarketQuote, Opportunity, OpportunityLeg, Side, Strategy


class CexSpotDislocationDetector:
    """Detect same-asset, same-quote spot dislocations across centralized venues.

    Discovery may surface candidates even when the downstream executable model
    rejects them because short-spot borrow is unavailable. That fail-closed
    behavior is intentional until inventory/rebalancing economics are modeled.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def detect(self, quotes: list[MarketQuote]) -> list[Opportunity]:
        groups: dict[tuple[str, str], list[MarketQuote]] = defaultdict(list)
        for quote in quotes:
            if quote.market_kind != MarketKind.SPOT or not quote.quote_currency:
                continue
            if quote.bid is None or quote.ask is None:
                continue
            groups[(quote.asset, quote.quote_currency.upper())].append(quote)

        results: list[Opportunity] = []
        holding_hours = max(1e-6, self.settings.spot_dislocation_holding_hours)
        for (asset, quote_currency), group in groups.items():
            for buy in group:
                for sell in group:
                    if buy.venue == sell.venue:
                        continue
                    if buy.ask is None or sell.bid is None or sell.bid <= buy.ask:
                        continue
                    gross_fraction = (sell.bid / buy.ask) - 1.0
                    gross_bps_hour = gross_fraction * 10_000.0 / holding_hours
                    screening_bps_hour = self.settings.pair_roundtrip_cost_bps / holding_hours
                    net_bps_hour = gross_bps_hour - screening_bps_hour - self.settings.safety_buffer_bps_per_hour
                    annualized = (net_bps_hour / 10_000.0) * 24.0 * 365.0
                    if annualized < self.settings.min_net_annualized_return:
                        continue
                    observed = min(buy.observed_at, sell.observed_at)
                    raw_id = (
                        f"cex-spot:{asset}:{quote_currency}:{buy.venue}:{sell.venue}:"
                        f"{observed.isoformat()}"
                    )
                    results.append(
                        Opportunity(
                            id=sha256(raw_id.encode()).hexdigest()[:20],
                            strategy=Strategy.CEX_SPOT_DISLOCATION,
                            asset=asset,
                            legs=[
                                OpportunityLeg(
                                    venue=buy.venue,
                                    asset=asset,
                                    market_kind=MarketKind.SPOT,
                                    side=Side.LONG,
                                    symbol=buy.symbol,
                                    quote_currency=quote_currency,
                                    contract_key=buy.contract_key,
                                    reference_price=buy.ask,
                                ),
                                OpportunityLeg(
                                    venue=sell.venue,
                                    asset=asset,
                                    market_kind=MarketKind.SPOT,
                                    side=Side.SHORT,
                                    symbol=sell.symbol,
                                    quote_currency=quote_currency,
                                    contract_key=sell.contract_key,
                                    reference_price=sell.bid,
                                ),
                            ],
                            gross_edge_bps_per_hour=gross_bps_hour,
                            modeled_cost_bps=self.settings.pair_roundtrip_cost_bps,
                            holding_hours=holding_hours,
                            safety_buffer_bps_per_hour=self.settings.safety_buffer_bps_per_hour,
                            net_edge_bps_per_hour=net_bps_hour,
                            net_annualized_return=annualized,
                            observed_at=observed,
                            expires_at=observed + timedelta(seconds=self.settings.max_quote_age_seconds),
                            confidence="low",
                            evidence={
                                "quote_currency": quote_currency,
                                "buy_ask": buy.ask,
                                "sell_bid": sell.bid,
                                "raw_cross_venue_spread": gross_fraction,
                                "inventory_model": "borrow_required_until_inventory_allocator_exists",
                            },
                        )
                    )
        return sorted(results, key=lambda item: item.net_annualized_return, reverse=True)

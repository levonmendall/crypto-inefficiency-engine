from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from hashlib import sha256

from inefficiency_engine.config import Settings
from inefficiency_engine.models import FundingQuote, MarketKind, Opportunity, OpportunityLeg, Side, Strategy


class FundingDispersionDetector:
    def __init__(self, settings: Settings):
        self.settings = settings

    def detect(self, quotes: list[FundingQuote]) -> list[Opportunity]:
        by_asset: dict[str, list[FundingQuote]] = defaultdict(list)
        for quote in quotes:
            by_asset[quote.asset].append(quote)

        results: list[Opportunity] = []
        for asset, asset_quotes in by_asset.items():
            if len(asset_quotes) < 2:
                continue
            for long_quote in asset_quotes:
                for short_quote in asset_quotes:
                    if long_quote.venue == short_quote.venue:
                        continue
                    gross_hourly = short_quote.hourly_rate - long_quote.hourly_rate
                    if gross_hourly <= 0:
                        continue
                    gross_bps_hour = gross_hourly * 10_000
                    amortized_cost_bps_hour = self.settings.pair_roundtrip_cost_bps / self.settings.default_holding_hours
                    net_bps_hour = gross_bps_hour - amortized_cost_bps_hour - self.settings.safety_buffer_bps_per_hour
                    annualized = (net_bps_hour / 10_000) * 24 * 365
                    if annualized < self.settings.min_net_annualized_return:
                        continue
                    observed = min(long_quote.observed_at, short_quote.observed_at)
                    expires = observed + timedelta(seconds=self.settings.max_quote_age_seconds)
                    raw_id = f"funding:{asset}:{long_quote.venue}:{short_quote.venue}:{observed.isoformat()}"
                    results.append(
                        Opportunity(
                            id=sha256(raw_id.encode()).hexdigest()[:20],
                            strategy=Strategy.FUNDING_DISPERSION,
                            asset=asset,
                            legs=[
                                OpportunityLeg(venue=long_quote.venue, asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.LONG),
                                OpportunityLeg(venue=short_quote.venue, asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
                            ],
                            gross_edge_bps_per_hour=gross_bps_hour,
                            modeled_cost_bps=self.settings.pair_roundtrip_cost_bps,
                            holding_hours=self.settings.default_holding_hours,
                            safety_buffer_bps_per_hour=self.settings.safety_buffer_bps_per_hour,
                            net_edge_bps_per_hour=net_bps_hour,
                            net_annualized_return=annualized,
                            observed_at=observed,
                            expires_at=expires,
                            confidence="medium",
                            evidence={
                                "long_funding_rate": long_quote.rate,
                                "long_interval_hours": long_quote.interval_hours,
                                "short_funding_rate": short_quote.rate,
                                "short_interval_hours": short_quote.interval_hours,
                                "source": "normalized predicted funding dispersion",
                            },
                        )
                    )
        return sorted(results, key=lambda x: x.net_annualized_return, reverse=True)

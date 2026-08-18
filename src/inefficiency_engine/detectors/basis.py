from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from hashlib import sha256

from inefficiency_engine.config import Settings
from inefficiency_engine.models import MarketKind, MarketQuote, Opportunity, OpportunityLeg, Side, Strategy


class SpotPerpBasisDetector:
    """Detect simple positive cash-and-carry basis: long spot, short perp."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def detect(self, quotes: list[MarketQuote]) -> list[Opportunity]:
        by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in quotes:
            by_asset[quote.asset].append(quote)

        results: list[Opportunity] = []
        for asset, asset_quotes in by_asset.items():
            spots = [q for q in asset_quotes if q.market_kind == MarketKind.SPOT]
            perps = [q for q in asset_quotes if q.market_kind == MarketKind.PERPETUAL]
            for spot in spots:
                for perp in perps:
                    if perp.mid <= spot.mid:
                        continue
                    basis = (perp.mid / spot.mid) - 1.0
                    gross_bps_hour = basis * 10_000 / self.settings.default_holding_hours
                    amortized_cost_bps_hour = self.settings.pair_roundtrip_cost_bps / self.settings.default_holding_hours
                    net_bps_hour = gross_bps_hour - amortized_cost_bps_hour - self.settings.safety_buffer_bps_per_hour
                    annualized = (net_bps_hour / 10_000) * 24 * 365
                    if annualized < self.settings.min_net_annualized_return:
                        continue
                    observed = min(spot.observed_at, perp.observed_at)
                    raw_id = f"basis:{asset}:{spot.venue}:{perp.venue}:{observed.isoformat()}"
                    results.append(
                        Opportunity(
                            id=sha256(raw_id.encode()).hexdigest()[:20],
                            strategy=Strategy.SPOT_PERP_BASIS,
                            asset=asset,
                            legs=[
                                OpportunityLeg(venue=spot.venue, asset=asset, market_kind=MarketKind.SPOT, side=Side.LONG, reference_price=spot.mid),
                                OpportunityLeg(venue=perp.venue, asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.SHORT, reference_price=perp.mid),
                            ],
                            gross_edge_bps_per_hour=gross_bps_hour,
                            modeled_cost_bps=self.settings.pair_roundtrip_cost_bps,
                            holding_hours=self.settings.default_holding_hours,
                            safety_buffer_bps_per_hour=self.settings.safety_buffer_bps_per_hour,
                            net_edge_bps_per_hour=net_bps_hour,
                            net_annualized_return=annualized,
                            observed_at=observed,
                            expires_at=observed + timedelta(seconds=self.settings.max_quote_age_seconds),
                            confidence="low",
                            evidence={"spot_mid": spot.mid, "perp_mid": perp.mid, "raw_basis": basis},
                        )
                    )
        return sorted(results, key=lambda x: x.net_annualized_return, reverse=True)

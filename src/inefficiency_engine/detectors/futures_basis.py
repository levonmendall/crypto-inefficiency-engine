from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from hashlib import sha256

from inefficiency_engine.config import Settings
from inefficiency_engine.models import MarketKind, MarketQuote, Opportunity, OpportunityLeg, Side, Strategy


class FuturesBasisDetector:
    """Detect positive cash-and-carry basis between spot and dated futures."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def detect(self, quotes: list[MarketQuote]) -> list[Opportunity]:
        groups: dict[tuple[str, str], list[MarketQuote]] = defaultdict(list)
        for quote in quotes:
            if not quote.quote_currency:
                continue
            groups[(quote.asset, quote.quote_currency.upper())].append(quote)

        results: list[Opportunity] = []
        for (asset, quote_currency), group in groups.items():
            spots = [q for q in group if q.market_kind == MarketKind.SPOT]
            futures = [q for q in group if q.market_kind == MarketKind.FUTURE and q.expires_at is not None]
            for spot in spots:
                for future in futures:
                    observed = min(spot.observed_at, future.observed_at)
                    if future.expires_at is None or future.expires_at <= observed:
                        continue
                    if future.mid <= spot.mid:
                        continue
                    holding_hours = (future.expires_at - observed).total_seconds() / 3600.0
                    if holding_hours <= 0:
                        continue
                    basis = (future.mid / spot.mid) - 1.0
                    gross_bps_hour = basis * 10_000.0 / holding_hours
                    screening_bps_hour = self.settings.pair_roundtrip_cost_bps / holding_hours
                    net_bps_hour = gross_bps_hour - screening_bps_hour - self.settings.safety_buffer_bps_per_hour
                    annualized = (net_bps_hour / 10_000.0) * 24.0 * 365.0
                    if annualized < self.settings.min_net_annualized_return:
                        continue
                    contract_key = future.contract_key or future.symbol
                    raw_id = (
                        f"future-basis:{asset}:{quote_currency}:{spot.venue}:{future.venue}:"
                        f"{contract_key}:{observed.isoformat()}"
                    )
                    results.append(
                        Opportunity(
                            id=sha256(raw_id.encode()).hexdigest()[:20],
                            strategy=Strategy.FUTURES_BASIS,
                            asset=asset,
                            legs=[
                                OpportunityLeg(
                                    venue=spot.venue,
                                    asset=asset,
                                    market_kind=MarketKind.SPOT,
                                    side=Side.LONG,
                                    symbol=spot.symbol,
                                    quote_currency=quote_currency,
                                    contract_key=spot.contract_key,
                                    reference_price=spot.mid,
                                ),
                                OpportunityLeg(
                                    venue=future.venue,
                                    asset=asset,
                                    market_kind=MarketKind.FUTURE,
                                    side=Side.SHORT,
                                    symbol=future.symbol,
                                    quote_currency=quote_currency,
                                    contract_key=future.contract_key,
                                    expires_at=future.expires_at,
                                    reference_price=future.mid,
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
                                "spot_mid": spot.mid,
                                "future_mid": future.mid,
                                "raw_basis": basis,
                                "future_expiry": future.expires_at.isoformat(),
                                "contract_key": future.contract_key,
                            },
                        )
                    )
        return sorted(results, key=lambda item: item.net_annualized_return, reverse=True)

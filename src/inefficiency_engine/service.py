from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timezone

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.adapters.hyperliquid import HyperliquidAdapter
from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.basis import SpotPerpBasisDetector
from inefficiency_engine.detectors.funding import FundingDispersionDetector
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus, ScanSnapshot
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, Opportunity
from inefficiency_engine.risk import RiskGate


class OpportunityService:
    def __init__(self, settings: Settings | None = None, evidence_store: EvidenceStore | None = None):
        self.settings = settings or Settings.from_env()
        self.evidence_store = evidence_store
        self.funding_detector = FundingDispersionDetector(self.settings)
        self.basis_detector = SpotPerpBasisDetector(self.settings)
        self.risk_gate = RiskGate(self.settings)

    def analyze(self, funding_quotes: list[FundingQuote], market_quotes: list[MarketQuote]) -> list[Opportunity]:
        candidates = self.funding_detector.detect(funding_quotes) + self.basis_detector.detect(market_quotes)
        return self.risk_gate.filter(candidates)

    async def collect_live_evidence(self) -> ScanSnapshot:
        started_at = datetime.now(timezone.utc)
        hyperliquid = HyperliquidAdapter()
        coinbase = CoinbaseSpotAdapter()

        async def capture(provider: str, awaitable):
            try:
                items = await awaitable
                return items, ProviderStatus(provider=provider, ok=True, item_count=len(items))
            except Exception as exc:
                return [], ProviderStatus(provider=provider, ok=False, error_type=type(exc).__name__)

        results = await asyncio.gather(
            capture("hyperliquid:predictedFundings", hyperliquid.funding_quotes()),
            capture("hyperliquid:metaAndAssetCtxs", hyperliquid.market_quotes()),
            capture("coinbase-exchange:ticker", coinbase.market_quotes()),
        )
        funding_quotes, funding_status = results[0]
        perp_quotes, perp_status = results[1]
        spot_quotes, spot_status = results[2]
        market_quotes = [*perp_quotes, *spot_quotes]
        opportunities = self.analyze(funding_quotes, market_quotes)
        completed_at = datetime.now(timezone.utc)
        providers = [funding_status, perp_status, spot_status]

        scan_id = "unpersisted"
        if self.evidence_store is not None:
            scan_id = self.evidence_store.record_scan(
                funding_quotes=funding_quotes,
                market_quotes=market_quotes,
                opportunities=opportunities,
                providers=providers,
                started_at=started_at,
                completed_at=completed_at,
                analysis_config=asdict(self.settings),
            )
        return ScanSnapshot(
            scan_id=scan_id,
            started_at=started_at,
            completed_at=completed_at,
            providers=providers,
            funding_quotes=funding_quotes,
            market_quotes=market_quotes,
            opportunities=opportunities,
            analysis_config=asdict(self.settings),
        )

    async def live_scan(self) -> list[Opportunity]:
        return (await self.collect_live_evidence()).opportunities

    def demo_scan(self) -> list[Opportunity]:
        now = datetime.now(timezone.utc)
        funding_quotes = [
            FundingQuote(venue="HlPerp", asset="BTC", rate=0.00002, interval_hours=1, observed_at=now, source="demo"),
            FundingQuote(venue="VenueB", asset="BTC", rate=0.0012, interval_hours=8, observed_at=now, source="demo"),
            FundingQuote(venue="VenueC", asset="BTC", rate=-0.0004, interval_hours=8, observed_at=now, source="demo"),
        ]
        market_quotes = [
            MarketQuote(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD", bid=3998, ask=4002, mid=4000, observed_at=now, source="demo"),
            MarketQuote(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH", mid=4040, observed_at=now, source="demo"),
        ]
        return self.analyze(funding_quotes, market_quotes)

from __future__ import annotations

import asyncio
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.adapters.hyperliquid import HyperliquidAdapter
from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.basis import SpotPerpBasisDetector
from inefficiency_engine.detectors.funding import FundingDispersionDetector
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus, ScanSnapshot
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, Opportunity, OrderBookSnapshot, ShadowCycle, ShadowObservation, ShadowOutcome
from inefficiency_engine.risk import RiskGate
from inefficiency_engine.shadow import opportunity_signature


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

    async def _collect_live_inputs(self):
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
        providers = [funding_status, perp_status, spot_status]
        return started_at, funding_quotes, market_quotes, opportunities, providers

    async def collect_live_evidence(self) -> ScanSnapshot:
        started_at, funding_quotes, market_quotes, opportunities, providers = await self._collect_live_inputs()
        completed_at = datetime.now(timezone.utc)
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

    async def collect_live_executability(self) -> ScanSnapshot:
        started_at, funding_quotes, market_quotes, opportunities, providers = await self._collect_live_inputs()
        hyperliquid = HyperliquidAdapter()
        coinbase = CoinbaseSpotAdapter()

        requests: dict[tuple[str, str, str], tuple[str, object]] = {}
        for opportunity in opportunities:
            for leg in opportunity.legs:
                key = (leg.venue, leg.asset, leg.market_kind.value)
                if key in requests:
                    continue
                if leg.venue == "HlPerp" and leg.market_kind == MarketKind.PERPETUAL:
                    requests[key] = (f"hyperliquid:l2Book:{leg.asset}", hyperliquid.order_book(leg.asset))
                elif leg.venue == "Coinbase" and leg.market_kind == MarketKind.SPOT:
                    requests[key] = (f"coinbase-exchange:book-level2:{leg.asset}", coinbase.order_book(leg.asset))

        async def capture_book(provider: str, awaitable):
            try:
                book = await awaitable
                return book, ProviderStatus(provider=provider, ok=True, item_count=1)
            except Exception as exc:
                return None, ProviderStatus(provider=provider, ok=False, error_type=type(exc).__name__)

        order_books: list[OrderBookSnapshot] = []
        if requests:
            book_results = await asyncio.gather(*(capture_book(provider, awaitable) for provider, awaitable in requests.values()))
            for book, status in book_results:
                providers.append(status)
                if book is not None:
                    order_books.append(book)

        qualification_time = datetime.now(timezone.utc)
        executability = [
            qualify_opportunity(opportunity, order_books, self.settings, notionals_usd=self.settings.capital_tiers_usd, now=qualification_time)
            for opportunity in opportunities
        ]
        completed_at = datetime.now(timezone.utc)

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
                order_books=order_books,
                executability=executability,
            )
        return ScanSnapshot(
            scan_id=scan_id,
            started_at=started_at,
            completed_at=completed_at,
            providers=providers,
            funding_quotes=funding_quotes,
            market_quotes=market_quotes,
            opportunities=opportunities,
            order_books=order_books,
            executability=executability,
            analysis_config=asdict(self.settings),
        )

    async def run_shadow_cycle(self, *, delay_seconds: float | None = None) -> ShadowCycle:
        delay = self.settings.shadow_delay_seconds if delay_seconds is None else max(0.0, delay_seconds)
        started_at = datetime.now(timezone.utc)
        initial = await self.collect_live_executability()
        initial_by_id = {op.id: op for op in initial.opportunities}

        candidates: list[tuple[Opportunity, object, float, float]] = []
        for executability in initial.executability:
            opportunity = initial_by_id.get(executability.opportunity_id)
            if opportunity is None or executability.estimated_capacity_notional_usd <= 0:
                continue
            target = min(self.settings.shadow_notional_usd, executability.estimated_capacity_notional_usd)
            initial_exact = qualify_opportunity(opportunity, initial.order_books, self.settings, notionals_usd=(target,), now=executability.observed_at)
            tier = initial_exact.tiers[0]
            if not (tier.executable and tier.passes_return_hurdle):
                continue
            candidates.append((opportunity, executability, target, tier.net_annualized_return))

        candidates.sort(key=lambda item: item[3], reverse=True)
        candidates = candidates[: max(0, self.settings.shadow_max_candidates)]

        if delay > 0:
            await asyncio.sleep(delay)
        verification = await self.collect_live_executability()
        verification_ops = {opportunity_signature(op): op for op in verification.opportunities}

        observations: list[ShadowObservation] = []
        verified_at = verification.completed_at
        for opportunity, initial_exec, target, initial_return in candidates:
            signature = opportunity_signature(opportunity)
            current = verification_ops.get(signature)
            if current is None:
                observations.append(ShadowObservation(
                    shadow_id=uuid.uuid4().hex,
                    initial_scan_id=initial.scan_id,
                    verification_scan_id=verification.scan_id,
                    opportunity_signature=signature,
                    opportunity_id=opportunity.id,
                    strategy=opportunity.strategy,
                    asset=opportunity.asset,
                    notional_usd_per_leg=target,
                    started_at=started_at,
                    verified_at=verified_at,
                    delay_seconds=delay,
                    initial_net_annualized_return=initial_return,
                    initial_capacity_notional_usd=initial_exec.estimated_capacity_notional_usd,
                    survived=False,
                    outcome=ShadowOutcome.SIGNAL_DISAPPEARED,
                    reason="economic opportunity was not rediscovered at verification",
                ))
                continue

            current_exec = qualify_opportunity(current, verification.order_books, self.settings, notionals_usd=(target,), now=verified_at)
            tier = current_exec.tiers[0]
            survived = tier.executable and tier.passes_return_hurdle
            observations.append(ShadowObservation(
                shadow_id=uuid.uuid4().hex,
                initial_scan_id=initial.scan_id,
                verification_scan_id=verification.scan_id,
                opportunity_signature=signature,
                opportunity_id=opportunity.id,
                strategy=opportunity.strategy,
                asset=opportunity.asset,
                notional_usd_per_leg=target,
                started_at=started_at,
                verified_at=verified_at,
                delay_seconds=delay,
                initial_net_annualized_return=initial_return,
                initial_capacity_notional_usd=initial_exec.estimated_capacity_notional_usd,
                survived=survived,
                verification_net_annualized_return=tier.net_annualized_return,
                outcome=ShadowOutcome.SURVIVED if survived else ShadowOutcome.EXECUTABILITY_FAILED,
                reason=None if survived else tier.rejection_reason,
            ))

        cycle = ShadowCycle(
            cycle_id=uuid.uuid4().hex,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            delay_seconds=delay,
            initial_scan_id=initial.scan_id,
            verification_scan_id=verification.scan_id,
            observations=observations,
        )
        if self.evidence_store is not None:
            self.evidence_store.record_shadow_cycle(cycle)
        return cycle

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

from __future__ import annotations

import asyncio
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.adapters.hyperliquid import HyperliquidAdapter
from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.registry import DetectorContext, DetectorManifest, OpportunityDetectorRegistry
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus, ScanSnapshot
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.fill_model import reconstruct_partial_fill_state
from inefficiency_engine.latency import EmpiricalLatencyResolver
from inefficiency_engine.market_graph import MarketGraphSnapshot, build_market_graph
from inefficiency_engine.models import (
    EmpiricalLatencyModel,
    FundingQuote,
    MarketKind,
    MarketQuote,
    Opportunity,
    OrderBookSnapshot,
    ShadowCycle,
    ShadowObservation,
    ShadowOutcome,
    Strategy,
)
from inefficiency_engine.ranking import RankedOpportunity, rank_qualified_opportunities
from inefficiency_engine.risk import RiskGate
from inefficiency_engine.shadow import (
    build_leg_attribution,
    classify_shadow_failure,
    expected_return_bucket,
    opportunity_signature,
    time_of_day_bucket,
    venue_pair,
)


def _l2_data_path_latency_ms(snapshot: ScanSnapshot) -> float | None:
    values = [
        book.request_latency_ms
        for book in snapshot.order_books
        if book.request_latency_ms is not None
    ]
    return max(values) if values else None


class OpportunityService:
    def __init__(self, settings: Settings | None = None, evidence_store: EvidenceStore | None = None):
        self.settings = settings or Settings.from_env()
        self.evidence_store = evidence_store
        self.detector_registry = OpportunityDetectorRegistry.default(self.settings)
        self.risk_gate = RiskGate(self.settings)

    def empirical_latency_resolver(self) -> EmpiricalLatencyResolver:
        return EmpiricalLatencyResolver(self.evidence_store, self.settings)

    def empirical_latency_model(
        self,
        opportunity: Opportunity | None = None,
        notional_usd_per_leg: float | None = None,
        *,
        strategy: Strategy | str | None = None,
        venue_pair_name: str | None = None,
        asset: str | None = None,
    ) -> EmpiricalLatencyModel:
        return self.empirical_latency_resolver().resolve(
            opportunity,
            notional_usd_per_leg,
            strategy=strategy,
            venue_pair=venue_pair_name,
            asset=asset,
        )

    def market_graph(
        self,
        funding_quotes: list[FundingQuote],
        market_quotes: list[MarketQuote],
    ) -> MarketGraphSnapshot:
        return build_market_graph(funding_quotes, market_quotes)

    def detector_manifests(self) -> list[DetectorManifest]:
        return self.detector_registry.manifests()

    def analyze(self, funding_quotes: list[FundingQuote], market_quotes: list[MarketQuote]) -> list[Opportunity]:
        graph = self.market_graph(funding_quotes, market_quotes)
        candidates = self.detector_registry.discover(
            DetectorContext(
                funding_quotes=funding_quotes,
                market_quotes=market_quotes,
                graph=graph,
            )
        )
        return self.risk_gate.filter(candidates)

    def rank_snapshot(self, snapshot: ScanSnapshot) -> list[RankedOpportunity]:
        return rank_qualified_opportunities(snapshot.opportunities, snapshot.executability)

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

    async def collect_live_graph(self) -> tuple[MarketGraphSnapshot, list[ProviderStatus], list[Opportunity]]:
        _, funding_quotes, market_quotes, opportunities, providers = await self._collect_live_inputs()
        return self.market_graph(funding_quotes, market_quotes), providers, opportunities

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
            book_results = await asyncio.gather(
                *(capture_book(provider, awaitable) for provider, awaitable in requests.values())
            )
            for book, status in book_results:
                providers.append(status)
                if book is not None:
                    order_books.append(book)

        qualification_time = datetime.now(timezone.utc)
        latency_resolver = self.empirical_latency_resolver()
        executability = [
            qualify_opportunity(
                opportunity,
                order_books,
                self.settings,
                notionals_usd=self.settings.capital_tiers_usd,
                now=qualification_time,
                latency_model_resolver=latency_resolver.resolve,
            )
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
        horizons = (
            (max(0.0, delay_seconds),)
            if delay_seconds is not None
            else tuple(sorted(set(max(0.0, value) for value in self.settings.shadow_horizons_seconds)))
        )
        if not horizons:
            horizons = (max(0.0, self.settings.shadow_delay_seconds),)

        started_at = datetime.now(timezone.utc)
        initial = await self.collect_live_executability()
        latency_resolver = self.empirical_latency_resolver()
        initial_by_id = {op.id: op for op in initial.opportunities}
        initial_scan_latency_ms = max(0.0, (initial.completed_at - initial.started_at).total_seconds() * 1000.0)
        initial_data_path_latency_ms = _l2_data_path_latency_ms(initial)

        candidates: list[tuple[Opportunity, object, float, object]] = []
        for executability in initial.executability:
            opportunity = initial_by_id.get(executability.opportunity_id)
            if opportunity is None or executability.estimated_capacity_notional_usd <= 0:
                continue
            qualified_tiers = [
                tier for tier in executability.tiers
                if tier.executable and tier.passes_return_hurdle
            ]
            if qualified_tiers:
                for tier in qualified_tiers:
                    candidates.append((opportunity, executability, tier.notional_usd_per_leg, tier))
            else:
                target = min(self.settings.shadow_notional_usd, executability.estimated_capacity_notional_usd)
                if target > 0:
                    exact = qualify_opportunity(
                        opportunity,
                        initial.order_books,
                        self.settings,
                        notionals_usd=(target,),
                        now=executability.observed_at,
                        latency_model_resolver=latency_resolver.resolve,
                    )
                    tier = exact.tiers[0]
                    if tier.executable and tier.passes_return_hurdle:
                        candidates.append((opportunity, executability, target, tier))

        candidates.sort(key=lambda item: item[3].net_annualized_return, reverse=True)
        if self.settings.shadow_max_candidates > 0:
            candidates = candidates[: self.settings.shadow_max_candidates]

        observations: list[ShadowObservation] = []
        verification_scan_ids: list[str] = []
        elapsed = 0.0
        final_verification = initial

        for horizon in horizons:
            wait = max(0.0, horizon - elapsed)
            if wait > 0:
                await asyncio.sleep(wait)
            elapsed = horizon

            verification = await self.collect_live_executability()
            final_verification = verification
            verification_scan_ids.append(verification.scan_id)
            verification_ops = {opportunity_signature(op): op for op in verification.opportunities}
            provider_failed = any(not status.ok for status in verification.providers)
            verified_at = verification.completed_at
            verification_scan_latency_ms = max(
                0.0, (verification.completed_at - verification.started_at).total_seconds() * 1000.0
            )
            verification_data_path_latency_ms = _l2_data_path_latency_ms(verification)

            for opportunity, initial_exec, target, initial_tier in candidates:
                signature = opportunity_signature(opportunity)
                current = verification_ops.get(signature)
                current_exec = None
                current_tier = None
                if current is not None:
                    current_exec = qualify_opportunity(
                        current,
                        verification.order_books,
                        self.settings,
                        notionals_usd=(target,),
                        now=verified_at,
                        latency_model_resolver=latency_resolver.resolve,
                    )
                    current_tier = current_exec.tiers[0]

                target_quantity = initial_tier.target_base_quantity
                leg_attribution, divergence = build_leg_attribution(
                    opportunity,
                    initial.order_books,
                    verification.order_books,
                    target_quantity=target_quantity,
                )
                fill_state = reconstruct_partial_fill_state(
                    leg_attribution,
                    reserve_ratio=self.settings.hedge_liquidity_reserve_ratio,
                )
                survived = bool(
                    current is not None
                    and current_tier is not None
                    and current_tier.executable
                    and current_tier.passes_return_hurdle
                    and not provider_failed
                )

                failure_cause = None
                if not survived:
                    failure_cause = classify_shadow_failure(
                        current_present=current is not None,
                        provider_failed=provider_failed,
                        verification_tier=current_tier,
                        initial_tier=initial_tier,
                        hedge_leg_divergence_bps=divergence,
                        slippage_expansion_threshold_bps=self.settings.shadow_slippage_expansion_bps,
                        hedge_divergence_threshold_bps=self.settings.shadow_hedge_divergence_bps,
                    )

                verification_return = current_tier.net_annualized_return if current_tier is not None else None
                verification_capacity = current_exec.estimated_capacity_notional_usd if current_exec is not None else None
                verification_gross = current.gross_edge_bps_per_hour if current is not None else None
                initial_cost = initial_tier.total_modeled_cost_bps
                verification_cost = current_tier.total_modeled_cost_bps if current_tier is not None else None
                initial_slippage = initial_tier.observed_entry_slippage_bps
                verification_slippage = current_tier.observed_entry_slippage_bps if current_tier is not None else None

                if survived:
                    outcome = ShadowOutcome.SURVIVED
                    reason = None
                elif current is None and not provider_failed:
                    outcome = ShadowOutcome.SIGNAL_DISAPPEARED
                    reason = "economic opportunity was not rediscovered at verification"
                else:
                    outcome = ShadowOutcome.EXECUTABILITY_FAILED
                    reason = (
                        "provider/data failure during verification"
                        if provider_failed
                        else (current_tier.rejection_reason if current_tier is not None else "verification failed closed")
                    )

                observations.append(
                    ShadowObservation(
                        shadow_id=uuid.uuid4().hex,
                        initial_scan_id=initial.scan_id,
                        verification_scan_id=verification.scan_id,
                        opportunity_signature=signature,
                        opportunity_id=opportunity.id,
                        strategy=opportunity.strategy,
                        asset=opportunity.asset,
                        notional_usd_per_leg=target,
                        target_base_quantity=target_quantity,
                        started_at=started_at,
                        verified_at=verified_at,
                        delay_seconds=horizon,
                        initial_scan_latency_ms=initial_scan_latency_ms,
                        verification_scan_latency_ms=verification_scan_latency_ms,
                        initial_data_path_latency_ms=initial_data_path_latency_ms,
                        verification_data_path_latency_ms=verification_data_path_latency_ms,
                        initial_net_annualized_return=initial_tier.net_annualized_return,
                        initial_capacity_notional_usd=initial_exec.estimated_capacity_notional_usd,
                        survived=survived,
                        pair_fillable=fill_state.pair_fillable,
                        pair_fillable_with_reserve=fill_state.pair_fillable_with_reserve,
                        hedge_recovery_required=fill_state.hedge_recovery_required,
                        pair_fill_fraction=fill_state.pair_fill_fraction,
                        max_leg_fill_fraction=fill_state.max_leg_fill_fraction,
                        unhedged_fraction=fill_state.unhedged_fraction,
                        partial_fill_state=fill_state.partial_fill_state,
                        hedge_recovery_loss_proxy_bps=fill_state.recovery_loss_proxy_bps,
                        queue_position_supported=False,
                        verification_net_annualized_return=verification_return,
                        outcome=outcome,
                        reason=reason,
                        venue_pair=venue_pair(opportunity),
                        time_of_day_bucket=time_of_day_bucket(initial.completed_at),
                        initial_expected_return_bucket=expected_return_bucket(initial_tier.net_annualized_return),
                        initial_gross_edge_bps_per_hour=opportunity.gross_edge_bps_per_hour,
                        verification_gross_edge_bps_per_hour=verification_gross,
                        gross_edge_decay_bps_per_hour=(
                            opportunity.gross_edge_bps_per_hour - verification_gross
                            if verification_gross is not None else None
                        ),
                        initial_total_modeled_cost_bps=initial_cost,
                        verification_total_modeled_cost_bps=verification_cost,
                        cost_expansion_bps=(verification_cost - initial_cost) if verification_cost is not None else None,
                        initial_entry_slippage_bps=initial_slippage,
                        verification_entry_slippage_bps=verification_slippage,
                        slippage_expansion_bps=(verification_slippage - initial_slippage) if verification_slippage is not None else None,
                        verification_capacity_notional_usd=verification_capacity,
                        capacity_deterioration_usd=(
                            initial_exec.estimated_capacity_notional_usd - verification_capacity
                            if verification_capacity is not None else None
                        ),
                        edge_decay_annualized=(
                            initial_tier.net_annualized_return - verification_return
                            if verification_return is not None else None
                        ),
                        hedge_leg_divergence_bps=divergence,
                        failure_cause=failure_cause,
                        leg_attribution=leg_attribution,
                    )
                )

        cycle = ShadowCycle(
            cycle_id=uuid.uuid4().hex,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            delay_seconds=max(horizons),
            horizons_seconds=list(horizons),
            initial_scan_id=initial.scan_id,
            verification_scan_id=final_verification.scan_id,
            verification_scan_ids=verification_scan_ids,
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
            MarketQuote(
                venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
                bid=3998, ask=4002, mid=4000, observed_at=now, source="demo",
            ),
            MarketQuote(
                venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
                mid=4040, observed_at=now, source="demo",
            ),
        ]
        return self.analyze(funding_quotes, market_quotes)

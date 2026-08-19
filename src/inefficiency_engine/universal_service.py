from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from statistics import median
from typing import Awaitable, Callable

from pydantic import BaseModel, Field

from inefficiency_engine.adapters.universal_public import CoinbaseStablecoinAdapter, DeribitOptionsAdapter, DexScreenerAdapter
from inefficiency_engine.adapters.velora import VeloraPriceRouteAdapter
from inefficiency_engine.allocation import AllocationConstraintSet, AllocationPlan, allocate_qualified_opportunities
from inefficiency_engine.dex_frontier import DexRouteSizeFrontier, build_size_frontier
from inefficiency_engine.dex_routes import DexRouteQuote, detect_route_quoted_cex_dex
from inefficiency_engine.dex_shadow import (
    DexRouteQuoteRecord,
    DexRouteShadowCycle,
    build_shadow_observation,
    route_signature,
)
from inefficiency_engine.evidence import ProviderStatus
from inefficiency_engine.models import MarketKind
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.universal import (
    StablecoinConversionModel, build_conversion_edges, build_universal_graph,
    detect_dex_routes, detect_option_relative_value, detect_stablecoin_dislocations,
)
from inefficiency_engine.universal_models import (
    DexPoolSnapshot, OptionQuote, StablecoinConversionEdge, StablecoinConversionObservation,
    UniversalCandidate, UniversalGraphSnapshot,
)


SleepFn = Callable[[float], Awaitable[None]]


class UniversalSurfaceSnapshot(BaseModel):
    observed_at: datetime
    graph: UniversalGraphSnapshot
    providers: list[ProviderStatus] = Field(default_factory=list)
    conversion_observations: list[StablecoinConversionObservation] = Field(default_factory=list)
    conversion_edges: list[StablecoinConversionEdge] = Field(default_factory=list)
    dex_pools: list[DexPoolSnapshot] = Field(default_factory=list)
    dex_route_quotes: list[DexRouteQuote] = Field(default_factory=list)
    option_quotes: list[OptionQuote] = Field(default_factory=list)
    candidates: list[UniversalCandidate] = Field(default_factory=list)
    core_opportunity_count: int = 0
    paper_only: bool = True


class UniversalOpportunityService:
    def __init__(
        self,
        core_service: OpportunityService,
        *,
        velora_adapter: VeloraPriceRouteAdapter | None = None,
    ):
        self.core = core_service
        self.settings = core_service.settings
        self.velora = velora_adapter or VeloraPriceRouteAdapter()

    @staticmethod
    def _reference_prices(market_quotes) -> dict[str, float]:
        by_asset: dict[str, list[float]] = {}
        for quote in market_quotes:
            if quote.market_kind != MarketKind.SPOT or quote.asset.upper() not in {"BTC", "ETH"}:
                continue
            by_asset.setdefault(quote.asset.upper(), []).append(quote.mid)
        return {asset: median(values) for asset, values in by_asset.items() if values}

    async def collect_route_quotes(self) -> list[DexRouteQuote]:
        evidence = await self.core.collect_live_evidence()
        reference_prices = self._reference_prices(evidence.market_quotes)
        return await self.velora.quotes_for_market(
            reference_prices,
            notional_usd=self.settings.dex_route_evidence_notional_usd,
        )

    async def probe_dex_route_size_frontiers(
        self,
        *,
        notionals_usd: tuple[float, ...] | None = None,
    ) -> list[DexRouteSizeFrontier]:
        tiers = tuple(sorted(set(notionals_usd or self.settings.dex_route_frontier_notionals_usd)))
        if not tiers or any(value <= 0 for value in tiers):
            raise ValueError("DEX route frontier notionals must be positive")
        evidence = await self.core.collect_live_evidence()
        reference_prices = self._reference_prices(evidence.market_quotes)
        frontiers: list[DexRouteSizeFrontier] = []
        for asset in ("BTC", "ETH"):
            reference = reference_prices.get(asset)
            if reference is None or reference <= 0:
                continue
            for direction in ("buy_asset", "sell_asset"):
                results: list[tuple[float, DexRouteQuote | None, str | None]] = []
                for target in tiers:
                    try:
                        quote = await self.velora.quote(
                            asset,
                            direction,
                            notional_usd=target,
                            reference_price=reference,
                        )
                        results.append((target, quote, None))
                    except Exception as exc:
                        results.append((target, None, type(exc).__name__))
                frontiers.append(build_size_frontier(
                    asset=asset,
                    direction=direction,
                    reference_price=reference,
                    quote_results=results,
                    deterioration_limit_bps=self.settings.dex_route_frontier_max_deterioration_bps,
                ))
        if self.core.evidence_store is not None:
            self.core.evidence_store.record_dex_route_size_frontiers(frontiers)
        return frontiers

    async def collect_surface(self) -> UniversalSurfaceSnapshot:
        stablecoins = CoinbaseStablecoinAdapter()
        dex = DexScreenerAdapter()
        deribit = DeribitOptionsAdapter()

        async def capture(provider: str, awaitable, empty):
            try:
                value = await awaitable
                count = len(value)
                return value, ProviderStatus(
                    provider=provider,
                    ok=count > 0,
                    item_count=count,
                    error_type=None if count > 0 else "EmptyResult",
                )
            except Exception as exc:
                return empty, ProviderStatus(provider=provider, ok=False, item_count=0, error_type=type(exc).__name__)

        evidence_task = asyncio.create_task(self.core.collect_live_evidence())
        stable_task = asyncio.create_task(capture("coinbase-exchange:stablecoins", stablecoins.observations(), []))
        dex_task = asyncio.create_task(capture("dexscreener:token-pairs", dex.pools(), []))
        option_task = asyncio.create_task(capture("deribit:option-surface", deribit.option_quotes(), []))

        evidence = await evidence_task
        reference_prices = self._reference_prices(evidence.market_quotes)
        route_task = asyncio.create_task(capture(
            "velora-market:prices:v6.2",
            self.velora.quotes_for_market(
                reference_prices,
                notional_usd=self.settings.dex_route_evidence_notional_usd,
            ),
            [],
        ))

        stable_rows, stable_status = await stable_task
        dex_pools, dex_status = await dex_task
        option_quotes, option_status = await option_task
        dex_route_quotes, route_status = await route_task

        market_quotes = list(evidence.market_quotes)
        funding_quotes = list(evidence.funding_quotes)
        core_graph = self.core.market_graph(funding_quotes, market_quotes)
        core_opportunities = self.core.analyze(funding_quotes, market_quotes)
        conversion_edges = build_conversion_edges(
            stable_rows,
            depeg_multiplier=self.settings.stablecoin_depeg_risk_multiplier,
            risk_floor_bps=self.settings.stablecoin_conversion_risk_floor_bps,
        )
        conversion_model = StablecoinConversionModel(conversion_edges)
        candidates = [
            *detect_stablecoin_dislocations(
                stable_rows,
                minimum_edge_bps=self.settings.stablecoin_dislocation_min_edge_bps,
            ),
            *detect_dex_routes(
                market_quotes,
                dex_pools,
                minimum_edge_bps=self.settings.dex_dislocation_min_edge_bps,
                liquidity_risk_floor_bps=self.settings.dex_liquidity_risk_floor_bps,
            ),
            *detect_route_quoted_cex_dex(
                market_quotes,
                dex_route_quotes,
                conversion_model=conversion_model,
                minimum_edge_bps=self.settings.dex_dislocation_min_edge_bps,
                conversion_max_age_seconds=self.settings.max_quote_age_seconds,
            ),
            *detect_option_relative_value(
                option_quotes,
                minimum_iv_deviation=self.settings.option_relative_value_min_iv_points,
            ),
        ]
        graph = build_universal_graph(
            core_graph,
            conversion_edges=conversion_edges,
            dex_pools=dex_pools,
            option_quotes=option_quotes,
            candidates=candidates,
        )
        return UniversalSurfaceSnapshot(
            observed_at=graph.observed_at,
            graph=graph,
            providers=[*evidence.providers, stable_status, dex_status, option_status, route_status],
            conversion_observations=stable_rows,
            conversion_edges=conversion_edges,
            dex_pools=dex_pools,
            dex_route_quotes=dex_route_quotes,
            option_quotes=option_quotes,
            candidates=candidates,
            core_opportunity_count=len(core_opportunities),
        )

    async def run_dex_route_shadow_cycle(
        self,
        *,
        horizons_seconds: tuple[float, ...] | None = None,
        sleep: SleepFn = asyncio.sleep,
    ) -> DexRouteShadowCycle:
        horizons = tuple(sorted(set(
            max(0.0, value)
            for value in (horizons_seconds or self.settings.shadow_horizons_seconds)
        )))
        if not horizons:
            horizons = (max(0.0, self.settings.shadow_delay_seconds),)

        cycle_id = uuid.uuid4().hex
        started_at = datetime.now(timezone.utc)
        initial_quotes = await self.collect_route_quotes()
        records: list[DexRouteQuoteRecord] = []
        initial_records: dict[str, DexRouteQuoteRecord] = {}
        for quote in initial_quotes:
            signature = route_signature(quote)
            record = DexRouteQuoteRecord(
                cycle_id=cycle_id,
                phase="initial",
                horizon_seconds=0.0,
                route_signature=signature,
                observed_at=quote.observed_at,
                quote=quote,
            )
            records.append(record)
            initial_records[signature] = record

        observations = []
        elapsed = 0.0
        for horizon in horizons:
            wait = max(0.0, horizon - elapsed)
            if wait > 0:
                await sleep(wait)
            elapsed = horizon

            async def capture(initial_record: DexRouteQuoteRecord):
                try:
                    quote = await self.velora.requote(initial_record.quote)
                    return quote, None
                except Exception as exc:
                    return None, type(exc).__name__

            results = await asyncio.gather(*(capture(record) for record in initial_records.values()))
            verified_at = datetime.now(timezone.utc)
            for initial_record, (verification_quote, failure_type) in zip(initial_records.values(), results):
                verification_record = None
                if verification_quote is not None:
                    verification_record = DexRouteQuoteRecord(
                        cycle_id=cycle_id,
                        phase="verification",
                        horizon_seconds=horizon,
                        route_signature=initial_record.route_signature,
                        observed_at=verification_quote.observed_at,
                        quote=verification_quote,
                    )
                    records.append(verification_record)
                observations.append(build_shadow_observation(
                    cycle_id=cycle_id,
                    initial_record=initial_record,
                    verification_record=verification_record,
                    delay_seconds=horizon,
                    verified_at=verified_at,
                    failure_type=failure_type,
                ))

        cycle = DexRouteShadowCycle(
            cycle_id=cycle_id,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            horizons_seconds=list(horizons),
            initial_quote_count=len(initial_records),
            observations=observations,
            paper_only=True,
        )
        if self.core.evidence_store is not None:
            self.core.evidence_store.record_dex_route_shadow_cycle(cycle, records)
        return cycle

    async def paper_allocation(
        self,
        *,
        capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> AllocationPlan:
        snapshot = await self.core.collect_live_executability()
        constraints = AllocationConstraintSet(
            total_capital_usd=capital_usd,
            max_venue_fraction=max_venue_fraction or self.settings.allocator_max_venue_fraction,
            max_asset_fraction=max_asset_fraction or self.settings.allocator_max_asset_fraction,
            max_allocations=max_allocations or self.settings.allocator_max_allocations,
        )
        return allocate_qualified_opportunities(snapshot.opportunities, snapshot.executability, constraints)

    def conversion_model(self, observations: list[StablecoinConversionObservation]) -> StablecoinConversionModel:
        return StablecoinConversionModel(build_conversion_edges(
            observations,
            depeg_multiplier=self.settings.stablecoin_depeg_risk_multiplier,
            risk_floor_bps=self.settings.stablecoin_conversion_risk_floor_bps,
        ))

    def interface_manifest(self) -> dict[str, object]:
        return {
            "dex_route_quote": {
                "live_public_surface": True,
                "amount_specific": True,
                "transaction_building": False,
                "executable_eligible": False,
                "probe_notional_usd": self.settings.dex_route_evidence_notional_usd,
                "multi_horizon_shadow": True,
                "multi_notional_frontier": True,
                "quote_currency_conversion_required": True,
                "conversion_path_fail_closed": True,
                "capacity_claimed": False,
                "requirements": [
                    "statistically sufficient quote-survival evidence",
                    "conversion execution depth/capacity",
                    "cross-venue inventory/settlement model",
                    "atomic hedge/recovery model",
                ],
            },
            "liquidation_backstop": {
                "typed_interface": True, "live_source": False, "executable_eligible": False,
                "requirements": ["authoritative capacity", "expiry", "cost", "recovery path"],
            },
            "solver": {
                "typed_interface": True, "live_source": False, "executable_eligible": False,
                "requirements": ["authoritative quote", "capacity", "settlement guarantee", "expiry"],
            },
            "cross_chain_bridge": {
                "typed_quote_and_risk_model": True, "live_source": False, "executable_eligible": False,
                "requirements": ["fresh authoritative bridge quote", "fees", "fill time", "settlement risk"],
            },
            "options_relative_value": {
                "live_public_surface": True, "executable_eligible": False,
                "requirements": ["option L2", "fee model", "delta hedge", "vega/gamma risk", "paired capacity"],
            },
            "paper_allocator": {
                "enabled": True, "authorizes_execution": False,
                "input": "already qualified CEX opportunities only",
            },
        }

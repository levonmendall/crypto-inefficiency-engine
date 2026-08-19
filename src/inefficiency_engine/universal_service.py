from __future__ import annotations

import asyncio
from datetime import datetime

from pydantic import BaseModel, Field

from inefficiency_engine.adapters.okx import OKXPublicAdapter
from inefficiency_engine.adapters.universal_public import CoinbaseStablecoinAdapter, DeribitOptionsAdapter, DexScreenerAdapter
from inefficiency_engine.allocation import AllocationConstraintSet, AllocationPlan, allocate_qualified_opportunities
from inefficiency_engine.evidence import ProviderStatus
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.universal import (
    StablecoinConversionModel, build_conversion_edges, build_universal_graph,
    detect_dex_routes, detect_option_relative_value, detect_stablecoin_dislocations,
)
from inefficiency_engine.universal_models import (
    DexPoolSnapshot, OptionQuote, StablecoinConversionEdge, StablecoinConversionObservation,
    UniversalCandidate, UniversalGraphSnapshot,
)


class UniversalSurfaceSnapshot(BaseModel):
    observed_at: datetime
    graph: UniversalGraphSnapshot
    providers: list[ProviderStatus] = Field(default_factory=list)
    conversion_observations: list[StablecoinConversionObservation] = Field(default_factory=list)
    conversion_edges: list[StablecoinConversionEdge] = Field(default_factory=list)
    dex_pools: list[DexPoolSnapshot] = Field(default_factory=list)
    option_quotes: list[OptionQuote] = Field(default_factory=list)
    candidates: list[UniversalCandidate] = Field(default_factory=list)
    core_opportunity_count: int = 0
    paper_only: bool = True


class UniversalOpportunityService:
    def __init__(self, core_service: OpportunityService):
        self.core = core_service
        self.settings = core_service.settings

    async def collect_surface(self) -> UniversalSurfaceSnapshot:
        stablecoins, dex, deribit, okx = CoinbaseStablecoinAdapter(), DexScreenerAdapter(), DeribitOptionsAdapter(), OKXPublicAdapter()
        async def capture(provider: str, awaitable, empty):
            try:
                value = await awaitable
                return value, ProviderStatus(provider=provider, ok=True, item_count=len(value))
            except Exception as exc:
                return empty, ProviderStatus(provider=provider, ok=False, error_type=type(exc).__name__)
        evidence_task = asyncio.create_task(self.core.collect_live_evidence())
        stable_task = asyncio.create_task(capture("coinbase-exchange:stablecoins", stablecoins.observations(), []))
        dex_task = asyncio.create_task(capture("dexscreener:token-pairs", dex.pools(), []))
        option_task = asyncio.create_task(capture("deribit:option-surface", deribit.option_quotes(), []))
        okx_market_task = asyncio.create_task(capture("okx-v5:market", okx.market_quotes(), []))
        okx_funding_task = asyncio.create_task(capture("okx-v5:funding", okx.funding_quotes(), []))
        evidence = await evidence_task
        stable_rows, stable_status = await stable_task
        dex_pools, dex_status = await dex_task
        option_quotes, option_status = await option_task
        okx_market, okx_market_status = await okx_market_task
        okx_funding, okx_funding_status = await okx_funding_task
        market_quotes = [*evidence.market_quotes, *okx_market]
        funding_quotes = [*evidence.funding_quotes, *okx_funding]
        core_graph = self.core.market_graph(funding_quotes, market_quotes)
        core_opportunities = self.core.analyze(funding_quotes, market_quotes)
        conversion_edges = build_conversion_edges(stable_rows,
            depeg_multiplier=self.settings.stablecoin_depeg_risk_multiplier,
            risk_floor_bps=self.settings.stablecoin_conversion_risk_floor_bps)
        candidates = [
            *detect_stablecoin_dislocations(stable_rows, minimum_edge_bps=self.settings.stablecoin_dislocation_min_edge_bps),
            *detect_dex_routes(market_quotes, dex_pools, minimum_edge_bps=self.settings.dex_dislocation_min_edge_bps,
                               liquidity_risk_floor_bps=self.settings.dex_liquidity_risk_floor_bps),
            *detect_option_relative_value(option_quotes, minimum_iv_deviation=self.settings.option_relative_value_min_iv_points),
        ]
        graph = build_universal_graph(core_graph, conversion_edges=conversion_edges, dex_pools=dex_pools,
                                      option_quotes=option_quotes, candidates=candidates)
        return UniversalSurfaceSnapshot(
            observed_at=graph.observed_at, graph=graph,
            providers=[*evidence.providers, stable_status, dex_status, option_status, okx_market_status, okx_funding_status],
            conversion_observations=stable_rows, conversion_edges=conversion_edges, dex_pools=dex_pools,
            option_quotes=option_quotes, candidates=candidates, core_opportunity_count=len(core_opportunities))

    async def paper_allocation(self, *, capital_usd: float, max_venue_fraction: float | None = None,
                               max_asset_fraction: float | None = None, max_allocations: int | None = None) -> AllocationPlan:
        snapshot = await self.core.collect_live_executability()
        constraints = AllocationConstraintSet(
            total_capital_usd=capital_usd,
            max_venue_fraction=max_venue_fraction or self.settings.allocator_max_venue_fraction,
            max_asset_fraction=max_asset_fraction or self.settings.allocator_max_asset_fraction,
            max_allocations=max_allocations or self.settings.allocator_max_allocations)
        return allocate_qualified_opportunities(snapshot.opportunities, snapshot.executability, constraints)

    def conversion_model(self, observations: list[StablecoinConversionObservation]) -> StablecoinConversionModel:
        return StablecoinConversionModel(build_conversion_edges(observations,
            depeg_multiplier=self.settings.stablecoin_depeg_risk_multiplier,
            risk_floor_bps=self.settings.stablecoin_conversion_risk_floor_bps))

    def interface_manifest(self) -> dict[str, object]:
        return {
            "liquidation_backstop": {"typed_interface": True, "live_source": False, "executable_eligible": False,
                "requirements": ["authoritative capacity", "expiry", "cost", "recovery path"]},
            "solver": {"typed_interface": True, "live_source": False, "executable_eligible": False,
                "requirements": ["authoritative quote", "capacity", "settlement guarantee", "expiry"]},
            "cross_chain_bridge": {"typed_quote_and_risk_model": True, "live_source": False, "executable_eligible": False,
                "requirements": ["fresh authoritative bridge quote", "fees", "fill time", "settlement risk"]},
            "options_relative_value": {"live_public_surface": True, "executable_eligible": False,
                "requirements": ["option L2", "fee model", "delta hedge", "vega/gamma risk", "paired capacity"]},
            "paper_allocator": {"enabled": True, "authorizes_execution": False,
                "input": "already qualified CEX opportunities only"},
        }

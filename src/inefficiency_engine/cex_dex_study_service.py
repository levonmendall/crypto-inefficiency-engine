from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from inefficiency_engine.adapters.universal_public import CoinbaseStablecoinAdapter
from inefficiency_engine.cex_dex_evidence import build_cex_dex_composite_evidence
from inefficiency_engine.cex_dex_statistics import CexDexStudyCycle, CexDexStudyObservation, group_key
from inefficiency_engine.models import MarketKind
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.universal import StablecoinConversionModel, build_conversion_edges
from inefficiency_engine.universal_models import StablecoinConversionObservation
from inefficiency_engine.universal_service import UniversalOpportunityService


class StablecoinObservationAdapter(Protocol):
    async def observations(self) -> list[StablecoinConversionObservation]: ...


class CexDexStatisticalStudyService:
    def __init__(
        self,
        core: OpportunityService,
        *,
        universal: UniversalOpportunityService | None = None,
        conversion_depth: StablecoinConversionDepthService | None = None,
        stablecoin_adapter: StablecoinObservationAdapter | None = None,
    ):
        self.core = core
        self.settings = core.settings
        self.universal = universal or UniversalOpportunityService(core)
        self.conversion_depth = conversion_depth or StablecoinConversionDepthService(self.settings)
        self.stablecoin_adapter = stablecoin_adapter or CoinbaseStablecoinAdapter()

    async def run_cycle(self) -> CexDexStudyCycle:
        started_at = datetime.now(timezone.utc)
        frontiers = await self.universal.probe_dex_route_size_frontiers()
        core_snapshot = await self.core.collect_live_evidence()
        stable_rows = await self.stablecoin_adapter.observations()
        conversion_edges = build_conversion_edges(
            stable_rows,
            depeg_multiplier=self.settings.stablecoin_depeg_risk_multiplier,
            risk_floor_bps=self.settings.stablecoin_conversion_risk_floor_bps,
        )
        conversion_model = StablecoinConversionModel(conversion_edges)
        conversion_books = await self.conversion_depth.collect_books()
        attempted_at = datetime.now(timezone.utc)
        cycle = CexDexStudyCycle(started_at=started_at, completed_at=attempted_at, observations=[])

        spots = [quote for quote in core_snapshot.market_quotes if quote.market_kind == MarketKind.SPOT]
        observations: list[CexDexStudyObservation] = []
        for frontier in frontiers:
            asset_spots = [quote for quote in spots if quote.asset.upper() == frontier.asset.upper()]
            for point in frontier.points:
                if not point.quoted or point.quote is None:
                    continue
                for cex_quote in asset_spots:
                    key = group_key(
                        frontier.asset,
                        point.quote.direction,
                        cex_quote.venue,
                        cex_quote.symbol,
                        point.target_notional_usd,
                    )
                    base = {
                        "cycle_id": cycle.cycle_id,
                        "group_key": key,
                        "asset": frontier.asset,
                        "route_direction": point.quote.direction,
                        "cex_venue": cex_quote.venue,
                        "cex_symbol": cex_quote.symbol,
                        "target_notional_usd": point.target_notional_usd,
                        "attempted_at": attempted_at,
                    }
                    try:
                        evidence = build_cex_dex_composite_evidence(
                            frontier,
                            point,
                            cex_quote,
                            conversion_books,
                            conversion_model,
                            self.settings,
                            now=attempted_at,
                        )
                    except Exception as exc:
                        observations.append(CexDexStudyObservation(
                            **base,
                            evidence_complete=False,
                            failure_type=type(exc).__name__,
                        ))
                        continue
                    if evidence is None:
                        observations.append(CexDexStudyObservation(
                            **base,
                            evidence_complete=False,
                            failure_type="EvidenceIncomplete",
                        ))
                    else:
                        observations.append(CexDexStudyObservation(
                            **base,
                            evidence_complete=True,
                            net_research_edge_bps=evidence.net_research_edge_bps,
                            evidence_id=evidence.evidence_id,
                            evidence=evidence,
                        ))

        cycle = cycle.model_copy(update={
            "completed_at": datetime.now(timezone.utc),
            "observations": observations,
        })
        if self.core.evidence_store is not None:
            self.core.evidence_store.record_cex_dex_study_cycle(cycle)
        return cycle

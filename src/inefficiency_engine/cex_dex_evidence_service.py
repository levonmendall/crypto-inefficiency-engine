from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.adapters.universal_public import CoinbaseStablecoinAdapter
from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence, build_cex_dex_composite_evidence
from inefficiency_engine.models import MarketKind
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.universal import StablecoinConversionModel, build_conversion_edges
from inefficiency_engine.universal_service import UniversalOpportunityService


class CexDexCompositeProbe(BaseModel):
    observed_at: datetime
    frontier_count: int
    quoted_route_point_count: int
    comparison_attempt_count: int
    evidence_count: int
    rejection_reasons: dict[str, int] = Field(default_factory=dict)
    evidence: list[CexDexCompositeEvidence] = Field(default_factory=list)
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class CexDexCompositeEvidenceService:
    def __init__(
        self,
        core: OpportunityService,
        *,
        universal: UniversalOpportunityService | None = None,
        conversion_depth: StablecoinConversionDepthService | None = None,
    ):
        self.core = core
        self.settings = core.settings
        self.universal = universal or UniversalOpportunityService(core)
        self.conversion_depth = conversion_depth or StablecoinConversionDepthService(self.settings)

    async def probe(self) -> CexDexCompositeProbe:
        frontiers = await self.universal.probe_dex_route_size_frontiers()
        core_snapshot = await self.core.collect_live_evidence()
        stable_rows = await CoinbaseStablecoinAdapter().observations()
        conversion_edges = build_conversion_edges(
            stable_rows,
            depeg_multiplier=self.settings.stablecoin_depeg_risk_multiplier,
            risk_floor_bps=self.settings.stablecoin_conversion_risk_floor_bps,
        )
        conversion_model = StablecoinConversionModel(conversion_edges)
        conversion_books = await self.conversion_depth.collect_books()
        now = datetime.now(timezone.utc)

        spots = [quote for quote in core_snapshot.market_quotes if quote.market_kind == MarketKind.SPOT]
        evidence: list[CexDexCompositeEvidence] = []
        rejected: Counter[str] = Counter()
        quoted_points = 0
        attempts = 0
        for frontier in frontiers:
            asset_spots = [quote for quote in spots if quote.asset.upper() == frontier.asset.upper()]
            for point in frontier.points:
                if not point.quoted or point.quote is None:
                    continue
                quoted_points += 1
                for cex_quote in asset_spots:
                    attempts += 1
                    try:
                        row = build_cex_dex_composite_evidence(
                            frontier,
                            point,
                            cex_quote,
                            conversion_books,
                            conversion_model,
                            self.settings,
                            now=now,
                        )
                    except Exception as exc:
                        rejected[type(exc).__name__] += 1
                        continue
                    if row is None:
                        rejected["EvidenceIncomplete"] += 1
                    else:
                        evidence.append(row)

        evidence.sort(key=lambda item: item.net_research_edge_bps, reverse=True)
        return CexDexCompositeProbe(
            observed_at=now,
            frontier_count=len(frontiers),
            quoted_route_point_count=quoted_points,
            comparison_attempt_count=attempts,
            evidence_count=len(evidence),
            rejection_reasons=dict(sorted(rejected.items())),
            evidence=evidence,
            capacity_claimed=False,
            executable_eligible=False,
            paper_only=True,
        )

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel, Field

from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.basis import SpotPerpBasisDetector
from inefficiency_engine.detectors.funding import FundingDispersionDetector
from inefficiency_engine.detectors.futures_basis import FuturesBasisDetector
from inefficiency_engine.detectors.spot_dislocation import CexSpotDislocationDetector
from inefficiency_engine.market_graph import MarketGraphSnapshot, canonical_asset_id
from inefficiency_engine.models import FundingQuote, MarketQuote, Opportunity, Strategy


@dataclass(frozen=True)
class DetectorContext:
    funding_quotes: list[FundingQuote]
    market_quotes: list[MarketQuote]
    graph: MarketGraphSnapshot


class DetectorManifest(BaseModel):
    name: str
    strategies: list[Strategy]
    required_inputs: list[str] = Field(default_factory=list)
    graph_native: bool = False


class DetectorModule(Protocol):
    manifest: DetectorManifest

    def detect(self, context: DetectorContext) -> list[Opportunity]: ...


class FundingDispersionModule:
    def __init__(self, settings: Settings):
        self.detector = FundingDispersionDetector(settings)
        self.manifest = DetectorManifest(
            name="funding_dispersion",
            strategies=[Strategy.FUNDING_DISPERSION],
            required_inputs=["funding_quotes"],
            graph_native=False,
        )

    def detect(self, context: DetectorContext) -> list[Opportunity]:
        return self.detector.detect(context.funding_quotes)


class SpotPerpBasisModule:
    def __init__(self, settings: Settings):
        self.detector = SpotPerpBasisDetector(settings)
        self.manifest = DetectorManifest(
            name="spot_perp_basis",
            strategies=[Strategy.SPOT_PERP_BASIS],
            required_inputs=["market_quotes"],
            graph_native=False,
        )

    def detect(self, context: DetectorContext) -> list[Opportunity]:
        return self.detector.detect(context.market_quotes)


class FuturesBasisModule:
    def __init__(self, settings: Settings):
        self.detector = FuturesBasisDetector(settings)
        self.manifest = DetectorManifest(
            name="futures_basis",
            strategies=[Strategy.FUTURES_BASIS],
            required_inputs=["market_quotes", "dated_future_expiry"],
            graph_native=True,
        )

    def detect(self, context: DetectorContext) -> list[Opportunity]:
        return self.detector.detect(context.market_quotes)


class CexSpotDislocationModule:
    def __init__(self, settings: Settings):
        self.detector = CexSpotDislocationDetector(settings)
        self.manifest = DetectorManifest(
            name="cex_spot_dislocation",
            strategies=[Strategy.CEX_SPOT_DISLOCATION],
            required_inputs=["market_quotes", "same_quote_currency"],
            graph_native=True,
        )

    def detect(self, context: DetectorContext) -> list[Opportunity]:
        return self.detector.detect(context.market_quotes)


class OpportunityDetectorRegistry:
    """Strategy-neutral detector registry.

    All detector modules expose the same Opportunity contract. New modules can
    consume the graph directly while downstream risk/execution remains shared.
    """

    def __init__(self, modules: list[DetectorModule]):
        self.modules = modules

    @classmethod
    def default(cls, settings: Settings) -> "OpportunityDetectorRegistry":
        return cls([
            FundingDispersionModule(settings),
            SpotPerpBasisModule(settings),
            FuturesBasisModule(settings),
            CexSpotDislocationModule(settings),
        ])

    def manifests(self) -> list[DetectorManifest]:
        return [module.manifest.model_copy(deep=True) for module in self.modules]

    def discover(self, context: DetectorContext) -> list[Opportunity]:
        opportunities: list[Opportunity] = []
        for module in self.modules:
            for raw in module.detect(context):
                opportunity = raw.model_copy(deep=True)
                instrument_ids: list[str] = []
                for leg in opportunity.legs:
                    instrument_id = context.graph.instrument_id_for(
                        leg.venue,
                        leg.asset,
                        leg.market_kind,
                        leg.contract_key,
                    )
                    if instrument_id is not None:
                        instrument_ids.append(instrument_id)
                opportunity.evidence = {
                    **opportunity.evidence,
                    "detector_module": module.manifest.name,
                    "graph_version": context.graph.graph_version,
                    "canonical_asset_id": canonical_asset_id(opportunity.asset),
                    "canonical_instrument_ids": instrument_ids,
                }
                opportunities.append(opportunity)
        return sorted(opportunities, key=lambda item: item.net_annualized_return, reverse=True)

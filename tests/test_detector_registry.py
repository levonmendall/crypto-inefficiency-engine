from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.registry import DetectorContext, OpportunityDetectorRegistry
from inefficiency_engine.market_graph import build_market_graph
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, Strategy


NOW = datetime(2026, 8, 19, 0, 20, tzinfo=timezone.utc)


def test_default_registry_routes_existing_strategies_and_adds_graph_lineage():
    settings = Settings(
        min_net_annualized_return=0.0,
        pair_roundtrip_cost_bps=0.0,
        safety_buffer_bps_per_hour=0.0,
    )
    funding_quotes = [
        FundingQuote(venue="PerpA", asset="BTC", rate=-0.001, interval_hours=8, observed_at=NOW, source="a"),
        FundingQuote(venue="PerpB", asset="BTC", rate=0.001, interval_hours=8, observed_at=NOW, source="b"),
    ]
    market_quotes = [
        MarketQuote(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT,
            symbol="ETH-USD", mid=4000, observed_at=NOW, source="spot",
        ),
        MarketQuote(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL,
            symbol="ETH", mid=4040, observed_at=NOW, source="perp",
        ),
    ]
    graph = build_market_graph(funding_quotes, market_quotes)
    registry = OpportunityDetectorRegistry.default(settings)

    opportunities = registry.discover(
        DetectorContext(
            funding_quotes=funding_quotes,
            market_quotes=market_quotes,
            graph=graph,
        )
    )

    assert {item.strategy for item in opportunities} == {
        Strategy.FUNDING_DISPERSION,
        Strategy.SPOT_PERP_BASIS,
    }
    assert {manifest.name for manifest in registry.manifests()} == {
        "funding_dispersion",
        "spot_perp_basis",
    }
    for opportunity in opportunities:
        assert opportunity.evidence["graph_version"] == "v0.9.0"
        assert str(opportunity.evidence["canonical_asset_id"]).startswith("crypto:asset:")
        assert opportunity.evidence["canonical_instrument_ids"]
        assert opportunity.evidence["detector_module"] in {"funding_dispersion", "spot_perp_basis"}

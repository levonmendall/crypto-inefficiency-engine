from datetime import datetime, timezone

from inefficiency_engine.universal import StablecoinConversionModel, build_conversion_edges, detect_stablecoin_dislocations
from inefficiency_engine.universal_models import StablecoinConversionObservation

NOW = datetime(2026,8,19,tzinfo=timezone.utc)

def test_stablecoin_conversion_has_explicit_depeg_and_reverse_cost():
    rows = [StablecoinConversionObservation(venue="Coinbase",base_currency="USDT",quote_currency="USD",
        symbol="USDT-USD",bid=0.994,ask=0.996,mid=0.995,observed_at=NOW,source="test")]
    edges = build_conversion_edges(rows,depeg_multiplier=1.5,risk_floor_bps=2)
    assert len(edges) == 2
    assert all(edge.depeg_bps > 0 for edge in edges)
    model = StablecoinConversionModel(edges)
    path = model.best_path("USDT","USD")
    assert path is not None
    assert path[1] > 0

def test_stablecoin_dislocation_is_searchable_but_not_execution_authority():
    rows = [StablecoinConversionObservation(venue="Coinbase",base_currency="USDC",quote_currency="USD",
        symbol="USDC-USD",bid=1.009,ask=1.011,mid=1.010,observed_at=NOW,source="test")]
    candidates = detect_stablecoin_dislocations(rows,minimum_edge_bps=8)
    assert candidates
    assert candidates[0].executable_eligible is False
    assert candidates[0].blocked_reason

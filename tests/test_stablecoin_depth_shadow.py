from datetime import datetime, timezone

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, OrderBookLevel, OrderBookSnapshot
from inefficiency_engine.stablecoin_depth_shadow import (
    StablecoinDepthLedger,
    StablecoinDepthProbeSpec,
    StablecoinDepthShadowService,
    build_stablecoin_depth_statistical_qualification,
)


NOW = datetime.now(timezone.utc)


def book(asset: str, bid: float, ask: float) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue="Coinbase",
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot",
        bids=[OrderBookLevel(price=bid, size=100000.0)],
        asks=[OrderBookLevel(price=ask, size=100000.0)],
        observed_at=NOW,
        source="test",
        request_latency_ms=5.0,
    )


class FakeDepthService:
    def __init__(self):
        self.settings = Settings(
            max_order_book_age_seconds=60.0,
            max_order_book_skew_seconds=5.0,
        )
        self.calls = 0

    async def collect_books(self):
        self.calls += 1
        usdc_bid = 1.0 if self.calls == 1 else 0.9995
        return [book("USDC", usdc_bid, 1.0005), book("USDT", 0.999, 1.0005)]


async def no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_shadow_reprices_conversion_depth_and_persists(tmp_path):
    store = EvidenceStore(tmp_path / "stablecoin.sqlite3")
    spec = StablecoinDepthProbeSpec(source_currency="USDC", target_currency="USD", input_amount=1000.0)
    service = StablecoinDepthShadowService(
        FakeDepthService(),  # type: ignore[arg-type]
        evidence_store=store,
        sleep=no_sleep,
        specs=(spec,),
    )

    cycle = await service.run_cycle(horizons_seconds=(0.0,))

    assert cycle.initial_quote_count == 1
    assert len(cycle.observations) == 1
    observation = cycle.observations[0]
    assert observation.survived is True
    assert observation.output_change_bps == pytest.approx(-5.0)
    assert observation.adverse_deterioration_bps == pytest.approx(5.0)
    assert service.ledger is not None
    loaded = service.ledger.load_cycle(cycle.cycle_id)
    assert loaded.cycle_id == cycle.cycle_id
    summary = service.ledger.summary()
    assert summary["cycle_count"] == 1
    assert summary["record_count"] == 2
    assert summary["survived_count"] == 1
    assert summary["capacity_claimed"] is False


@pytest.mark.asyncio
async def test_statistical_gate_uses_independent_cycles_and_tail_evidence(tmp_path):
    store = EvidenceStore(tmp_path / "stablecoin-stats.sqlite3")
    spec = StablecoinDepthProbeSpec(source_currency="USDC", target_currency="USD", input_amount=1000.0)
    ledger = StablecoinDepthLedger(store)

    for index in range(2):
        service = StablecoinDepthShadowService(
            FakeDepthService(),  # type: ignore[arg-type]
            evidence_store=store,
            sleep=no_sleep,
            specs=(spec,),
        )
        cycle = await service.run_cycle(horizons_seconds=(5.0,))
        assert cycle.observations[0].survived is True

    settings = Settings(
        dex_statistical_reference_horizon_seconds=5.0,
        dex_statistical_min_effective_samples=2,
        dex_statistical_min_tail_samples=2,
        dex_statistical_min_survival_lower_bound=0.20,
        dex_statistical_max_ci_width=1.0,
    )
    model = build_stablecoin_depth_statistical_qualification(ledger.cycles_all(), spec, settings)

    assert model.effective_sample_count == 2
    assert model.adverse_tail_sample_count == 2
    assert model.survival.successes == 2
    assert model.p95_adverse_deterioration_bps == pytest.approx(5.0)
    assert model.statistically_qualified is True
    assert model.executable_eligible is False

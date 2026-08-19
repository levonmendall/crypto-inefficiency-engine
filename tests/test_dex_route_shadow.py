from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

from inefficiency_engine.adapters.velora import VeloraPriceRouteAdapter
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_routes import DexRouteQuote
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote, ShadowCycle
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import run_shadow_worker


NOW = datetime(2026, 8, 19, 2, 0, tzinfo=timezone.utc)


def route_quote(*, price: float = 4000.0, observed_at: datetime = NOW, exchanges=None) -> DexRouteQuote:
    source_amount = 0.25
    destination_amount = source_amount * price
    return DexRouteQuote(
        provider="Velora",
        network_id=1,
        chain_id="ethereum",
        asset="ETH",
        quote_currency="USDC",
        direction="sell_asset",
        source_token="0xeth",
        destination_token="0xusdc",
        source_decimals=18,
        destination_decimals=6,
        source_amount_raw="250000000000000000",
        destination_amount_raw=str(int(destination_amount * 1_000_000)),
        source_amount=source_amount,
        destination_amount=destination_amount,
        effective_asset_price=price,
        block_number=24_000_000,
        route_exchanges=exchanges or ["UniswapV3"],
        gas_cost_usd=5.0,
        request_latency_ms=20.0,
        observed_at=observed_at,
        source="test",
        amount_specific=True,
        transaction_built=False,
        executable_eligible=False,
    )


class FakeCore:
    def __init__(self, store: EvidenceStore):
        self.settings = Settings(
            shadow_horizons_seconds=(0.0, 5.0),
            shadow_delay_seconds=0.0,
        )
        self.evidence_store = store

    async def collect_live_evidence(self):
        quote = MarketQuote(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT,
            symbol="ETH-USD", quote_currency="USD", bid=3999, ask=4001, mid=4000,
            observed_at=NOW, source="test",
        )
        return SimpleNamespace(market_quotes=[quote])


class FakeVelora:
    def __init__(self):
        self.initial = route_quote()
        self.requote_source_amounts: list[str] = []
        self.calls = 0

    async def quotes_for_market(self, reference_prices, *, notional_usd):
        assert reference_prices["ETH"] == 4000
        assert notional_usd == 1000.0
        return [self.initial]

    async def requote(self, initial):
        self.calls += 1
        self.requote_source_amounts.append(initial.source_amount_raw)
        if self.calls == 2:
            raise TimeoutError("route unavailable")
        return route_quote(
            price=3980.0,
            observed_at=NOW + timedelta(seconds=1),
            exchanges=["CurveV1"],
        )


async def no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_route_shadow_persists_exact_amount_survival_and_failure(tmp_path):
    store = EvidenceStore(tmp_path / "route-shadow.sqlite3")
    velora = FakeVelora()
    universal = UniversalOpportunityService(FakeCore(store), velora_adapter=velora)  # type: ignore[arg-type]

    cycle = await universal.run_dex_route_shadow_cycle(
        horizons_seconds=(0.0, 5.0),
        sleep=no_sleep,
    )

    assert velora.requote_source_amounts == [
        "250000000000000000",
        "250000000000000000",
    ]
    assert len(cycle.observations) == 2
    first, second = cycle.observations
    assert first.survived is True
    assert first.price_deterioration_bps == pytest.approx(50.0)
    assert first.route_changed is True
    assert first.capacity_claimed is False
    assert first.transaction_built is False
    assert second.survived is False
    assert second.failure_type == "TimeoutError"

    counts = store.counts()
    assert counts.dex_route_shadow_cycles == 1
    assert counts.dex_route_quotes == 2  # one initial + one successful verification
    loaded = store.load_dex_route_shadow_cycle(cycle.cycle_id)
    assert loaded.cycle_id == cycle.cycle_id
    records = store.load_dex_route_quote_records(cycle.cycle_id)
    assert {record.phase for record in records} == {"initial", "verification"}
    summary = store.dex_route_shadow_summary()
    assert summary["cycle_count"] == 1
    assert summary["by_horizon"]["0.0"]["survival_rate"] == 1.0
    assert summary["by_horizon"]["5.0"]["survival_rate"] == 0.0
    assert summary["capacity_claimed"] is False


@pytest.mark.asyncio
async def test_velora_requote_uses_exact_original_raw_source_amount():
    seen_amounts = []

    async def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen_amounts.append(params["amount"])
        return httpx.Response(
            200,
            json={
                "priceRoute": {
                    "network": 1,
                    "srcToken": params["srcToken"],
                    "destToken": params["destToken"],
                    "srcAmount": params["amount"],
                    "destAmount": "995000000",
                    "srcDecimals": int(params["srcDecimals"]),
                    "destDecimals": int(params["destDecimals"]),
                    "bestRoute": [],
                }
            },
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = VeloraPriceRouteAdapter(client=client)
        initial = route_quote()
        verification = await adapter.requote(initial)

    assert seen_amounts == [initial.source_amount_raw]
    assert verification.source_amount_raw == initial.source_amount_raw
    assert verification.transaction_built is False


class AlwaysSuccessCoreWorker:
    def __init__(self):
        self.settings = Settings(worker_error_backoff_seconds=0.0, shadow_cycle_interval_seconds=0.0)

    async def run_shadow_cycle(self):
        now = datetime.now(timezone.utc)
        return ShadowCycle(
            cycle_id="core-ok",
            started_at=now,
            completed_at=now,
            delay_seconds=0.0,
            initial_scan_id="initial",
            verification_scan_id="verification",
            observations=[],
        )


@pytest.mark.asyncio
async def test_dex_route_failure_does_not_poison_successful_core_worker_cycle(tmp_path):
    async def failing_route_shadow():
        raise RuntimeError("Velora unavailable")

    store = EvidenceStore(tmp_path / "worker-isolation.sqlite3")
    stats = await run_shadow_worker(
        AlwaysSuccessCoreWorker(),  # type: ignore[arg-type]
        store,
        worker_id="worker-route-isolation",
        sleep=no_sleep,
        max_cycles=1,
        route_shadow_runner=failing_route_shadow,
    )
    assert stats.cycles_attempted == 1
    assert stats.cycles_succeeded == 1
    assert stats.cycles_failed == 0

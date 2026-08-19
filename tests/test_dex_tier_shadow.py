from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.dex_routes import DexRouteQuote
from inefficiency_engine.dex_tier_shadow import DexTierShadowService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.worker import run_shadow_worker


NOW = datetime(2026, 8, 19, 3, 30, tzinfo=timezone.utc)


class FakeCore:
    def __init__(self, *, store=None):
        self.settings = Settings(
            shadow_horizons_seconds=(5.0,),
            dex_route_tier_shadow_notionals_usd=(5000.0, 10000.0),
            dex_route_tier_shadow_max_concurrency=2,
            shadow_cycle_interval_seconds=0.0,
            worker_error_backoff_seconds=0.0,
        )
        self.evidence_store = store

    async def collect_live_evidence(self):
        return SimpleNamespace(market_quotes=[
            MarketQuote(venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTC-USD", quote_currency="USD", mid=100000.0, observed_at=NOW, source="test"),
            MarketQuote(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD", quote_currency="USD", mid=4000.0, observed_at=NOW, source="test"),
        ])

    async def run_shadow_cycle(self):
        return SimpleNamespace(cycle_id="core", verification_scan_id="scan", observations=[])


class FakeVelora:
    def __init__(self):
        self.quote_calls: list[tuple[str, str, float]] = []
        self.requote_calls: list[str] = []

    async def quote(self, asset, direction, *, notional_usd, reference_price):
        self.quote_calls.append((asset, direction, notional_usd))
        if direction == "buy_asset":
            source_amount = notional_usd
            destination_amount = notional_usd / reference_price
            source_decimals, destination_decimals = 6, 18
        else:
            source_amount = notional_usd / reference_price
            destination_amount = notional_usd
            source_decimals, destination_decimals = 18, 6
        return DexRouteQuote(
            provider="Velora",
            network_id=1,
            chain_id="ethereum",
            asset=asset,
            quote_currency="USDC",
            direction=direction,
            source_token="source",
            destination_token="dest",
            source_decimals=source_decimals,
            destination_decimals=destination_decimals,
            source_amount_raw=str(max(1, int(source_amount * 10**6))),
            destination_amount_raw=str(max(1, int(destination_amount * 10**6))),
            source_amount=source_amount,
            destination_amount=destination_amount,
            effective_asset_price=reference_price,
            observed_at=NOW,
            source="test",
        )

    async def requote(self, initial):
        self.requote_calls.append(initial.source_amount_raw)
        return initial.model_copy(update={"observed_at": datetime.now(timezone.utc)})


@pytest.mark.asyncio
async def test_tier_shadow_collects_each_configured_notional_and_requotes_exact_source_amounts():
    sleeps: list[float] = []

    async def fake_sleep(seconds: float):
        sleeps.append(seconds)

    adapter = FakeVelora()
    service = DexTierShadowService(FakeCore(), adapter=adapter, sleep=fake_sleep)  # type: ignore[arg-type]
    cycle = await service.run_cycle()

    assert len(adapter.quote_calls) == 8  # 2 notionals x 2 assets x 2 directions
    assert {call[2] for call in adapter.quote_calls} == {5000.0, 10000.0}
    assert cycle.initial_quote_count == 8
    assert len(cycle.observations) == 8
    assert all(item.delay_seconds == 5.0 for item in cycle.observations)
    assert all(item.survived for item in cycle.observations)
    assert len(adapter.requote_calls) == 8
    assert sleeps == [5.0]
    assert cycle.paper_only is True


@pytest.mark.asyncio
async def test_periodic_worker_runs_tier_shadow_only_on_configured_cadence(tmp_path):
    store = EvidenceStore(tmp_path / "tier-worker.sqlite3")
    core = FakeCore(store=store)
    tier_calls = 0

    async def no_sleep(_: float):
        return None

    async def tier_runner():
        nonlocal tier_calls
        tier_calls += 1
        return SimpleNamespace(cycle_id=f"tier-{tier_calls}", initial_quote_count=1, observations=[])

    stats = await run_shadow_worker(
        core,  # type: ignore[arg-type]
        store,
        worker_id="tier-worker",
        sleep=no_sleep,
        max_cycles=3,
        tier_shadow_runner=tier_runner,
        tier_shadow_every_cycles=2,
    )

    assert stats.cycles_succeeded == 3
    assert tier_calls == 1
    latest = store.latest_worker_heartbeat("tier-worker")
    assert latest is not None and latest.state == "completed"

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_runtime import (
    CYCLE_HISTORY_WORKER_ID,
    maintain_cycle_history_once,
    read_cycle_history_status,
)
from inefficiency_engine.evidence import EvidenceStore


class FakeHistoricalResearch:
    backfill_days = 365

    def __init__(self, now: datetime):
        self.now = now
        self.calls = 0
        self.complete = False

    async def ensure_backfilled(self, assets, *, now=None):
        self.calls += 1
        if self.complete:
            return SimpleNamespace(
                errors=(),
                fetched_assets=("BTC",),
                stored_quote_count=120,
            )
        return SimpleNamespace(
            errors=("BTC:TimeoutError",),
            fetched_assets=(),
            stored_quote_count=0,
        )

    def _coverage(self, asset):
        end = self.now.replace(hour=0, minute=0, second=0, microsecond=0)
        if self.complete:
            return 1460, end - timedelta(days=365), end - timedelta(hours=6)
        return 200, end - timedelta(days=50), end - timedelta(days=10)

    def replay_summaries(self, strategy, settings, *, total_capital_usd, now=None):
        if not self.complete:
            return {}
        return {
            ("BTC", "long"): SimpleNamespace(
                sample_count=40,
                hit_rate=0.60,
                mean_realized_net_return=0.01,
                regime_count=2,
                qualified_for_probationary_support=True,
            )
        }


def test_cycle_history_status_persists_and_incomplete_assets_can_retry(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    now = datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc)
    research = FakeHistoricalResearch(now)

    first = asyncio.run(
        maintain_cycle_history_once(
            store,
            settings=Settings(),
            assets=("BTC",),
            now=now,
            research=research,
        )
    )
    assert research.calls == 1
    assert first["all_complete"] is False
    assert first["assets"][0]["status"] == "retrying"
    assert first["assets"][0]["last_error_type"] == "TimeoutError"
    assert first["assets"][0]["historical_counts_as_forward"] is False

    research.complete = True
    second = asyncio.run(
        maintain_cycle_history_once(
            store,
            settings=Settings(),
            assets=("BTC",),
            now=now + timedelta(minutes=30),
            research=research,
        )
    )
    assert research.calls == 2
    assert second["all_complete"] is True
    assert second["assets"][0]["quote_count"] == 1460
    assert second["assets"][0]["walk_forward_ready"] is True
    assert second["assets"][0]["historical_replay_long_qualified"] is True
    assert second["assets"][0]["last_error_type"] is None
    assert second["historical_counts_as_forward"] is False
    assert second["full_forward_promotion_gate_unchanged"] is True

    reread = read_cycle_history_status(store)
    assert reread["all_complete"] is True
    assert reread["complete_asset_count"] == 1
    assert reread["total_quote_count"] == 1460

    heartbeat = store.latest_worker_heartbeat(CYCLE_HISTORY_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "success"
    assert heartbeat.detail["historical_counts_as_forward"] is False

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.active_volume_runtime import (
    maintain_active_cycle_history_once,
    read_active_cycle_history_status,
    read_active_volume_universe_status,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_runtime import CycleHistoryStatusLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.render_combined import API_APP, child_commands, heavy_commands
from inefficiency_engine.volume_universe import (
    STRICT_VOLUME_METHOD,
    STRICT_VOLUME_SOURCE,
    TOP_VOLUME_ASSET_COUNT,
    VOLUME_UNIVERSE_REFRESH_SECONDS,
    VolumeUniverseLedger,
)


ACTIVE_ASSETS = tuple(f"TKN{index}" for index in range(TOP_VOLUME_ASSET_COUNT))


def _volume_snapshot(now: datetime) -> dict[str, object]:
    return {
        "observed_at": now.isoformat(),
        "method": STRICT_VOLUME_METHOD,
        "ranking_metric": "reported_24h_trading_volume_usd",
        "ranking_source": STRICT_VOLUME_SOURCE,
        "ranking_scope": "marketwide",
        "volume_is_defining_metric": True,
        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
        "asset_count": TOP_VOLUME_ASSET_COUNT,
        "stable_value_assets_excluded": True,
        "eligibility_note": "test",
        "source_health": {},
        "assets": [
            {
                "rank": rank,
                "asset": asset,
                "reported_24h_volume_usd": float(2_000_000_000 - rank * 1_000_000),
                "aggregate_24h_notional_usd": float(2_000_000_000 - rank * 1_000_000),
                "sources": [STRICT_VOLUME_SOURCE],
                "source_asset_id": asset.lower(),
            }
            for rank, asset in enumerate(ACTIVE_ASSETS, start=1)
        ],
        "paper_only": True,
        "allocation_authority": False,
    }


def _history_status(asset: str, now: datetime, *, complete: bool = True) -> dict[str, object]:
    return {
        "asset": asset,
        "observed_at": now.isoformat(),
        "status": "complete" if complete else "retrying",
        "complete": complete,
        "quote_count": 1460 if complete else 200,
        "expected_quote_count": 1460,
        "coverage_fraction": 1.0 if complete else 200 / 1460,
        "walk_forward_ready": complete,
        "historical_replay_long_qualified": False,
        "historical_counts_as_forward": False,
        "live_execution_authority": False,
    }


def test_active_cycle_history_filters_archived_assets_and_preserves_volume_rank(tmp_path):
    store = EvidenceStore(tmp_path / "active-volume.db")
    now = datetime(2026, 8, 21, tzinfo=timezone.utc)
    VolumeUniverseLedger(store).record(_volume_snapshot(now))

    ledger = CycleHistoryStatusLedger(store)
    ledger.upsert(_history_status("ALGO", now))
    ledger.upsert(_history_status(ACTIVE_ASSETS[0], now))
    ledger.upsert(_history_status(ACTIVE_ASSETS[5], now, complete=False))

    volume = read_active_volume_universe_status(store, now=now + timedelta(minutes=10))
    assert volume["available"] is True
    assert volume["current_membership_available"] is True
    assert volume["asset_count"] == TOP_VOLUME_ASSET_COUNT == 25
    assert volume["universe_target_count"] == 25
    assert volume["volume_is_defining_metric"] is True
    assert [row["asset"] for row in volume["assets"]] == list(ACTIVE_ASSETS)
    assert [row["rank"] for row in volume["assets"]] == list(
        range(1, TOP_VOLUME_ASSET_COUNT + 1)
    )

    history = read_active_cycle_history_status(store, now=now + timedelta(minutes=10))
    assert history["asset_count"] == TOP_VOLUME_ASSET_COUNT
    assert history["universe_target_count"] == TOP_VOLUME_ASSET_COUNT
    assert [row["asset"] for row in history["assets"]] == list(ACTIVE_ASSETS)
    assert [row["active_rank"] for row in history["assets"]] == list(
        range(1, TOP_VOLUME_ASSET_COUNT + 1)
    )
    assert "ALGO" not in {row["asset"] for row in history["assets"]}
    assert history["archived_status_asset_count"] == 1
    assert history["assets"][0]["status"] == "complete"
    assert history["assets"][0]["active_top25"] is True
    assert history["assets"][1]["status"] == "pending"
    assert history["assets"][5]["status"] == "retrying"


def test_stale_top25_remains_visible_but_is_not_reported_current(tmp_path):
    store = EvidenceStore(tmp_path / "freshness.db")
    now = datetime(2026, 8, 21, tzinfo=timezone.utc)
    VolumeUniverseLedger(store).record(_volume_snapshot(now))

    fresh = read_active_volume_universe_status(
        store,
        now=now + timedelta(seconds=VOLUME_UNIVERSE_REFRESH_SECONDS / 2),
    )
    stale = read_active_volume_universe_status(
        store,
        now=now + timedelta(seconds=VOLUME_UNIVERSE_REFRESH_SECONDS + 1),
    )

    assert fresh["available"] is True
    assert fresh["current_membership_available"] is True
    assert fresh["stale"] is False

    # A short refresh delay should never blank a complete validated cohort.
    assert stale["available"] is True
    assert stale["current_membership_available"] is False
    assert stale["stale"] is True
    assert stale["unavailable_reason"] == "stale_snapshot"
    assert stale["asset_count"] == TOP_VOLUME_ASSET_COUNT
    assert [row["asset"] for row in stale["assets"]] == list(ACTIVE_ASSETS)
    assert stale["observed_at"] == now.isoformat()
    assert stale["last_known_good_retained"] is True
    assert stale["last_known_good_observed_at"] == now.isoformat()
    assert stale["last_known_good_asset_count"] == TOP_VOLUME_ASSET_COUNT
    assert stale["fresh_snapshot_required_for_current_membership"] is True


def test_stale_top25_history_remains_visible_and_explicitly_stale(tmp_path):
    store = EvidenceStore(tmp_path / "stale-history.db")
    now = datetime(2026, 8, 21, tzinfo=timezone.utc)
    VolumeUniverseLedger(store).record(_volume_snapshot(now))

    ledger = CycleHistoryStatusLedger(store)
    ledger.upsert(_history_status(ACTIVE_ASSETS[0], now))
    ledger.upsert(_history_status("ACE", now))
    ledger.upsert(_history_status("BOME", now))
    ledger.upsert(_history_status("JOHN", now))

    status = read_active_cycle_history_status(
        store,
        now=now + timedelta(seconds=VOLUME_UNIVERSE_REFRESH_SECONDS + 1),
    )

    assert status["available"] is True
    assert status["active_universe_available"] is True
    assert status["active_universe_stale"] is True
    assert status["active_universe_current"] is False
    assert status["active_universe_unavailable_reason"] == "stale_snapshot"
    assert status["active_universe_last_known_good_observed_at"] == now.isoformat()
    assert status["active_universe_last_known_good_asset_count"] == TOP_VOLUME_ASSET_COUNT
    assert status["asset_count"] == TOP_VOLUME_ASSET_COUNT
    assert [row["asset"] for row in status["assets"]] == list(ACTIVE_ASSETS)
    assert all(row["current_top_volume"] is False for row in status["assets"])
    assert status["archived_status_asset_count"] == 3
    assert {"ACE", "BOME", "JOHN"}.isdisjoint(
        {row["asset"] for row in status["assets"]}
    )


class _CompleteHistoricalResearch:
    backfill_days = 365

    def __init__(self, now: datetime):
        self.now = now
        self.requested: tuple[str, ...] = ()

    async def ensure_backfilled(self, assets, *, now=None):
        self.requested = tuple(assets)
        return SimpleNamespace(
            errors=(),
            fetched_assets=tuple(assets),
            stored_quote_count=100,
        )

    def _coverage(self, _asset):
        end = self.now.replace(hour=0, minute=0, second=0, microsecond=0)
        return 1460, end - timedelta(days=365), end - timedelta(hours=6)

    def replay_summaries(self, strategy, settings, *, total_capital_usd, now=None):
        return {}


def test_active_history_maintenance_uses_resolved_top25(monkeypatch, tmp_path):
    store = EvidenceStore(tmp_path / "maintenance.db")
    now = datetime(2026, 8, 21, tzinfo=timezone.utc)
    VolumeUniverseLedger(store).record(_volume_snapshot(now))
    research = _CompleteHistoricalResearch(now)
    calls: list[tuple[str, ...]] = []

    async def fake_resolve(_store, *, now=None):
        calls.append(ACTIVE_ASSETS)
        return ACTIVE_ASSETS

    monkeypatch.setattr(
        "inefficiency_engine.active_volume_runtime.resolve_top_volume_assets",
        fake_resolve,
    )

    result = asyncio.run(
        maintain_active_cycle_history_once(
            store,
            settings=Settings(),
            now=now,
            research=research,
        )
    )

    assert calls == [ACTIVE_ASSETS]
    assert set(research.requested) == set(ACTIVE_ASSETS)
    assert len(research.requested) == TOP_VOLUME_ASSET_COUNT == 25
    assert result["asset_count"] == TOP_VOLUME_ASSET_COUNT
    assert [row["asset"] for row in result["assets"]] == list(ACTIVE_ASSETS)
    assert "ALGO" not in {row["asset"] for row in result["assets"]}


def test_render_combined_keeps_active_history_disposable():
    permanent = child_commands("10000")
    disposable = heavy_commands()
    assert API_APP == "inefficiency_engine.read_api_active_volume_deploy:app"
    assert "history" not in permanent
    assert disposable["history"][-2] == "inefficiency_engine.disposable_heavy_job"
    assert disposable["history"][-1] == "history"

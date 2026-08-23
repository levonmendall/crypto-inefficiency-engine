from __future__ import annotations

import asyncio

import pytest

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.permanent_source_worker import (
    VOLUME_UNIVERSE_WORKER_ID,
    _volume_universe_refresh_loop,
)
from inefficiency_engine.volume_universe import TOP_VOLUME_ASSET_COUNT


@pytest.mark.asyncio
async def test_isolated_source_worker_refreshes_top25_without_waiting_for_heavy_job(monkeypatch, tmp_path):
    store = EvidenceStore(tmp_path / "membership.sqlite3")
    stop = asyncio.Event()
    calls: list[bool] = []
    assets = tuple(f"A{index:02d}" for index in range(TOP_VOLUME_ASSET_COUNT))

    async def fake_resolve(_store, *, force_refresh=False, **_kwargs):
        calls.append(force_refresh)
        stop.set()
        return assets

    monkeypatch.setattr(
        "inefficiency_engine.permanent_source_worker.resolve_top_volume_assets",
        fake_resolve,
    )

    await _volume_universe_refresh_loop(store, stop_event=stop)

    assert TOP_VOLUME_ASSET_COUNT == 25
    assert calls == [True]
    heartbeat = store.latest_worker_heartbeat(VOLUME_UNIVERSE_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "success"
    assert heartbeat.detail["asset_count"] == 25
    assert heartbeat.detail["universe_target_count"] == 25
    assert heartbeat.detail["isolated_source_process"] is True
    assert heartbeat.detail["portfolio_authority"] is False


@pytest.mark.asyncio
async def test_isolated_membership_refresh_failure_is_contained(monkeypatch, tmp_path):
    store = EvidenceStore(tmp_path / "membership-failure.sqlite3")
    stop = asyncio.Event()

    async def failed_resolve(_store, *, force_refresh=False, **_kwargs):
        assert force_refresh is True
        stop.set()
        raise TimeoutError("test provider delay")

    monkeypatch.setattr(
        "inefficiency_engine.permanent_source_worker.resolve_top_volume_assets",
        failed_resolve,
    )

    # Market-data routing failures remain inside the source process and cannot escape
    # into canonical portfolio accounting.
    await _volume_universe_refresh_loop(store, stop_event=stop)

    heartbeat = store.latest_worker_heartbeat(VOLUME_UNIVERSE_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "degraded"
    assert heartbeat.error_type == "TimeoutError"
    assert heartbeat.detail["retrying"] is True
    assert heartbeat.detail["portfolio_authority"] is False

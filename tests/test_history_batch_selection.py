from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.history_batch_job import maintain_history_batch_once, select_history_batch


NOW = datetime(2026, 8, 20, 19, 30, tzinfo=timezone.utc)


def test_top40_history_selection_never_expands_beyond_requested_batch():
    assets = tuple(f"ASSET{index:02d}" for index in range(40))
    status = {
        "assets": [
            {
                "asset": asset,
                "complete": index % 3 == 0,
                "observed_at": (NOW - timedelta(minutes=index)).isoformat(),
            }
            for index, asset in enumerate(assets)
        ]
    }

    selected = select_history_batch(assets, status, batch_size=4)

    assert len(selected) == 4
    assert set(selected).issubset(set(assets))
    assert all(
        not next(row for row in status["assets"] if row["asset"] == asset)["complete"]
        for asset in selected
    )


@pytest.mark.asyncio
async def test_history_batch_force_refreshes_membership_and_prioritizes_new_entrants(
    tmp_path,
    monkeypatch,
):
    store = EvidenceStore(tmp_path / "history-membership.db")
    active = tuple(f"ASSET{index:02d}" for index in range(40))
    seen: dict[str, object] = {}

    async def fake_resolve(_store, *, now=None, force_refresh=False):
        seen["force_refresh"] = force_refresh
        return active

    def fake_status(_store):
        # Existing rows for ASSET04+ make the four brand-new entrants ASSET00..03
        # least recently maintained and therefore first in the bounded batch.
        return {
            "assets": [
                {
                    "asset": asset,
                    "complete": False,
                    "observed_at": (NOW - timedelta(minutes=index)).isoformat(),
                }
                for index, asset in enumerate(active[4:], start=4)
            ]
        }

    class FakeResearch:
        def __init__(self, *_args, **_kwargs):
            pass

    async def fake_maintain(_store, *, assets, **_kwargs):
        seen["batch"] = tuple(assets)
        return {
            "all_complete": False,
            "asset_count": len(tuple(assets)),
            "complete_asset_count": 0,
            "overall_coverage_fraction": 0.0,
            "historical_counts_as_forward": False,
        }

    monkeypatch.setattr(
        "inefficiency_engine.history_batch_job.resolve_top_volume_assets",
        fake_resolve,
    )
    monkeypatch.setattr(
        "inefficiency_engine.history_batch_job.read_cycle_history_status",
        fake_status,
    )
    monkeypatch.setattr(
        "inefficiency_engine.history_batch_job.BatchedCycleHistoricalResearch",
        FakeResearch,
    )
    monkeypatch.setattr(
        "inefficiency_engine.history_batch_job.maintain_cycle_history_once",
        fake_maintain,
    )

    result = await maintain_history_batch_once(
        store,
        batch_size=4,
        now=NOW,
    )

    assert seen["force_refresh"] is True
    assert seen["batch"] == active[:4]
    assert result["top40_force_refreshed"] is True
    assert result["batch_assets"] == list(active[:4])

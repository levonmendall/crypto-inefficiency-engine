from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.history_batch_job import select_history_batch


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

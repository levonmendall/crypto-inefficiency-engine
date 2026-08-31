from __future__ import annotations

from types import SimpleNamespace

import pytest

from inefficiency_engine.instance_memory import InstanceMemorySnapshot
from inefficiency_engine import stage_one_market_memory_guard as guard


def _snapshot(*, usage_mb: float) -> InstanceMemorySnapshot:
    return InstanceMemorySnapshot(
        usage_mb=usage_mb,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )


def _progress_payload() -> dict[str, object]:
    return {
        "state": "running",
        "current_table": "market_quotes",
        "tables": {
            "market_quotes": {
                "verified": False,
                "migration_mode": "captured_primary_key_high_water",
                "last_primary_key": [2992160],
                "high_water_primary_key": [3094848],
            }
        },
    }


def test_market_batch_rows_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CIE_STAGE_ONE_MARKET_BATCH_ROWS", "999999")
    assert guard.market_batch_rows() == guard.MAX_MARKET_BATCH_ROWS
    monkeypatch.setenv("CIE_STAGE_ONE_MARKET_BATCH_ROWS", "1")
    assert guard.market_batch_rows() == 64
    monkeypatch.setenv("CIE_STAGE_ONE_MARKET_BATCH_ROWS", "not-an-int")
    assert guard.market_batch_rows() == guard.DEFAULT_MARKET_BATCH_ROWS


def test_guard_uses_smaller_copy_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def migrate(_source_url: str, *, batch_size: int) -> dict[str, object]:
        calls.append(batch_size)
        return {"batch_size": batch_size}

    module = SimpleNamespace(
        BATCH_SIZE=2000,
        migrate=migrate,
        _publish=lambda _payload, _path=None: None,
    )
    monkeypatch.setattr(guard, "market_batch_rows", lambda: 512)
    guard.install_market_copy_guard(module)

    assert module.migrate("postgresql://example") == {"batch_size": 512}
    assert calls == [512]


def test_memory_pressure_exits_only_after_durable_market_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[dict[str, object]] = []
    module = SimpleNamespace(
        BATCH_SIZE=2000,
        migrate=lambda _source_url, *, batch_size=2000: {"batch_size": batch_size},
        _publish=lambda payload, _path=None: published.append(payload),
    )
    monkeypatch.setattr(guard, "release_unused_memory", lambda: None)
    monkeypatch.setattr(guard, "instance_memory_snapshot", lambda: _snapshot(usage_mb=1800.0))
    monkeypatch.setattr(guard, "_write_memory_status", lambda *args, **kwargs: None)
    guard.install_market_copy_guard(module)

    payload = _progress_payload()
    with pytest.raises(SystemExit) as excinfo:
        module._publish(payload)

    assert excinfo.value.code == guard.MEMORY_PRESSURE_EXIT_CODE
    assert published == [payload]


def test_memory_guard_continues_below_termination_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = SimpleNamespace(
        BATCH_SIZE=2000,
        migrate=lambda _source_url, *, batch_size=2000: {"batch_size": batch_size},
        _publish=lambda _payload, _path=None: None,
    )
    monkeypatch.setattr(guard, "release_unused_memory", lambda: None)
    monkeypatch.setattr(guard, "instance_memory_snapshot", lambda: _snapshot(usage_mb=1500.0))
    monkeypatch.setattr(guard, "_write_memory_status", lambda *args, **kwargs: None)
    guard.install_market_copy_guard(module)

    module._publish(_progress_payload())

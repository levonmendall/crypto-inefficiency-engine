from __future__ import annotations

from inefficiency_engine import stage_one_local_persistence_storage_repair as repair


def _market_progress(*, checkpoint=True, high_water=True):
    market = {
        "verified": False,
        "migration_mode": "captured_primary_key_high_water",
    }
    if checkpoint:
        market["last_primary_key"] = [1_748_641]
    if high_water:
        market["high_water_primary_key"] = [2_812_933]
    return {
        "state": "running",
        "current_table": "market_quotes",
        "tables": {"market_quotes": market},
    }


def test_market_quotes_resume_checkpoint_requires_both_durable_bounds():
    assert repair._market_quotes_resume_checkpoint(_market_progress())
    assert not repair._market_quotes_resume_checkpoint(_market_progress(checkpoint=False))
    assert not repair._market_quotes_resume_checkpoint(_market_progress(high_water=False))


def test_storage_repaired_migrate_uses_larger_batch_only_for_market_resume(monkeypatch):
    observed = []
    monkeypatch.setattr(repair.migration, "_progress_path", lambda: None)
    monkeypatch.setattr(repair.migration, "_load_progress", lambda path: _market_progress())
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_MIGRATE",
        lambda source_url, *, batch_size: observed.append((source_url, batch_size)) or {"ok": True},
    )

    result = repair._storage_repaired_migrate("postgresql://source")

    assert result == {"ok": True}
    assert observed == [("postgresql://source", repair.MARKET_QUOTES_RESUME_BATCH_SIZE)]


def test_storage_repaired_migrate_keeps_base_batch_outside_market_resume(monkeypatch):
    observed = []
    monkeypatch.setattr(repair.migration, "_progress_path", lambda: None)
    monkeypatch.setattr(
        repair.migration,
        "_load_progress",
        lambda path: {"state": "running", "current_table": "funding_quotes", "tables": {}},
    )
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_MIGRATE",
        lambda source_url, *, batch_size: observed.append((source_url, batch_size)) or {"ok": True},
    )

    repair._storage_repaired_migrate("postgresql://source")

    assert observed == [("postgresql://source", repair.migration.BATCH_SIZE)]

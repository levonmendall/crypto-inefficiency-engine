from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import inefficiency_engine.local_persistence_migration_status as status


def test_status_projects_durable_funding_checkpoint_without_changing_authority(monkeypatch) -> None:
    base = {
        "state": "running",
        "current_table": "funding_quotes",
        "tables_total": 55,
        "tables_verified": 54,
        "postgresql_authoritative": True,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "cutover_ready": False,
    }
    progress = {
        "state": "running",
        "tables": {
            "funding_quotes": {
                "verified": False,
                "migration_mode": "captured_monotonic_integer_high_water",
                "verification_scope": "captured_monotonic_integer_high_water",
                "snapshot_phase": "copying",
                "snapshot_high_water_primary_key": [5712261],
                "snapshot_high_water_captured": True,
                "snapshot_rows_copied": 606720,
                "snapshot_rows_verified": 0,
                "last_primary_key": [606720],
                "last_progress_at": "2026-08-28T22:45:00+00:00",
                "source_transport_retries": 0,
            }
        },
    }

    monkeypatch.setattr(status, "_base_migration_status_payload", lambda: dict(base))
    monkeypatch.setattr(status, "_read_json", lambda _path: progress)
    monkeypatch.setattr(
        status,
        "local_storage_paths",
        lambda: SimpleNamespace(migration_progress=Path("progress.json")),
    )

    payload = status.migration_status_payload()

    assert payload["funding_quotes"] == {
        "verified": False,
        "migration_mode": "captured_monotonic_integer_high_water",
        "verification_scope": "captured_monotonic_integer_high_water",
        "snapshot_phase": "copying",
        "snapshot_high_water_primary_key": [5712261],
        "snapshot_high_water_captured": True,
        "snapshot_rows_copied": 606720,
        "snapshot_rows_verified": 0,
        "last_primary_key": [606720],
        "last_progress_at": "2026-08-28T22:45:00+00:00",
        "source_transport_retries": 0,
    }
    assert payload["postgresql_authoritative"] is True
    assert payload["paper_only"] is True
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["cutover_ready"] is False


def test_status_fails_closed_when_funding_checkpoint_is_absent(monkeypatch) -> None:
    monkeypatch.setattr(
        status,
        "_base_migration_status_payload",
        lambda: {
            "state": "pending",
            "postgresql_authoritative": True,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "cutover_ready": False,
        },
    )
    monkeypatch.setattr(status, "_read_json", lambda _path: {})
    monkeypatch.setattr(
        status,
        "local_storage_paths",
        lambda: SimpleNamespace(migration_progress=Path("missing.json")),
    )

    payload = status.migration_status_payload()

    assert set(payload["funding_quotes"]) == set(status._FUNDING_CHECKPOINT_FIELDS)
    assert all(value is None for value in payload["funding_quotes"].values())
    assert payload["postgresql_authoritative"] is True
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["cutover_ready"] is False

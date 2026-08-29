from __future__ import annotations

import json
from types import SimpleNamespace

import inefficiency_engine.local_persistence_migration_status as status


def test_status_projects_durable_funding_checkpoint_without_changing_authority(
    monkeypatch,
    tmp_path,
) -> None:
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
    guard = {
        "state": "running",
        "reason": "supervisor_entry",
        "started_at": "2026-08-29T02:10:00+00:00",
        "observed_at": "2026-08-29T02:10:01+00:00",
        "attempt": 1,
        "error_type": None,
        "error": None,
        "release_commit": "deadbeef",
    }

    monkeypatch.setenv("CIE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(status, "_base_migration_status_payload", lambda: dict(base))
    monkeypatch.setattr(
        status,
        "_GUARD_FALLBACK_STATUS_PATH",
        tmp_path / "guard-fallback.json",
    )
    progress_path = tmp_path / "migration" / status._PROGRESS_FILENAME
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps(progress))
    guard_path = tmp_path / "migration" / status._GUARD_STATUS_FILENAME
    guard_path.write_text(json.dumps(guard))

    payload = status.migration_status_payload()

    assert status._progress_path() == progress_path
    assert status._guard_status_path() == guard_path
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
    assert payload["migration_guard_state"] == "running"
    assert payload["migration_guard_reason"] == "supervisor_entry"
    assert payload["migration_guard_started_at"] == "2026-08-29T02:10:00+00:00"
    assert payload["migration_guard_attempt"] == 1
    assert payload["migration_guard_release_commit"] == "deadbeef"
    assert payload["migration_guard_durable_status_present"] is True
    assert payload["migration_guard_fallback_status_present"] is False
    assert payload["migration_storage_root_exists"] is True
    assert payload["migration_storage_root_is_dir"] is True
    assert payload["postgresql_authoritative"] is True
    assert payload["paper_only"] is True
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["cutover_ready"] is False


def test_status_fails_closed_when_funding_and_guard_evidence_are_absent(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("CIE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(
        status,
        "_GUARD_FALLBACK_STATUS_PATH",
        tmp_path / "guard-fallback.json",
    )
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

    payload = status.migration_status_payload()

    assert status._progress_path() == (
        tmp_path / "migration" / status._PROGRESS_FILENAME
    )
    assert status._guard_status_path() == (
        tmp_path / "migration" / status._GUARD_STATUS_FILENAME
    )
    assert set(payload["funding_quotes"]) == set(status._FUNDING_CHECKPOINT_FIELDS)
    assert all(value is None for value in payload["funding_quotes"].values())
    for field in status._GUARD_STATUS_FIELDS:
        assert payload[f"migration_guard_{field}"] is None
        assert payload[f"migration_guard_fallback_{field}"] is None
    assert payload["migration_guard_durable_status_present"] is False
    assert payload["migration_guard_fallback_status_present"] is False
    assert payload["postgresql_authoritative"] is True
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["cutover_ready"] is False


def test_status_exposes_fallback_guard_failure_and_disk_capacity(
    monkeypatch,
    tmp_path,
) -> None:
    fallback_path = tmp_path / "guard-fallback.json"
    fallback_path.write_text(
        json.dumps(
            {
                "state": "durable_status_write_failed",
                "reason": "main_startup_durable_status_write_failed",
                "started_at": "2026-08-29T03:00:00+00:00",
                "observed_at": "2026-08-29T03:00:01+00:00",
                "attempt": 0,
                "error_type": "OSError",
                "error": "No space left on device",
                "release_commit": "cafebabe",
            }
        )
    )
    monkeypatch.setenv("CIE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(status, "_GUARD_FALLBACK_STATUS_PATH", fallback_path)
    monkeypatch.setattr(
        status,
        "_base_migration_status_payload",
        lambda: {
            "state": "running",
            "postgresql_authoritative": True,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "cutover_ready": False,
        },
    )
    monkeypatch.setattr(
        status.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(total=10_000, used=9_500, free=500),
    )

    payload = status.migration_status_payload()

    assert payload["migration_guard_state"] is None
    assert payload["migration_guard_fallback_state"] == "durable_status_write_failed"
    assert (
        payload["migration_guard_fallback_reason"]
        == "main_startup_durable_status_write_failed"
    )
    assert payload["migration_guard_fallback_error_type"] == "OSError"
    assert payload["migration_guard_fallback_error"] == "No space left on device"
    assert payload["migration_guard_fallback_release_commit"] == "cafebabe"
    assert payload["migration_guard_durable_status_present"] is False
    assert payload["migration_guard_fallback_status_present"] is True
    assert payload["migration_storage_total_bytes"] == 10_000
    assert payload["migration_storage_used_bytes"] == 9_500
    assert payload["migration_storage_free_bytes"] == 500
    assert payload["migration_storage_free_ratio"] == 0.05
    assert payload["postgresql_authoritative"] is True
    assert payload["cutover_ready"] is False

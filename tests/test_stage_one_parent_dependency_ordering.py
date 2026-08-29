from __future__ import annotations

import json

from sqlalchemy import insert, select, text

import inefficiency_engine as package
import inefficiency_engine.postgres_local_migration as migration
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.stage_one_monotonic_append_only import (
    migrate_monotonic_integer_append_only_table,
)


def _scan_row(scan_id: str) -> dict[str, object]:
    return {
        "scan_id": scan_id,
        "started_at": "2026-08-29T00:00:00+00:00",
        "completed_at": "2026-08-29T00:00:01+00:00",
        "created_at": "2026-08-29T00:00:01+00:00",
        "analysis_config_json": "{}",
    }


def _funding_row(row_id: int, scan_id: str) -> dict[str, object]:
    return {
        "id": row_id,
        "scan_id": scan_id,
        "venue": "binance",
        "asset": "BTC",
        "observed_at": f"2026-08-29T00:00:0{row_id}+00:00",
        "payload_json": "{}",
        "lineage_hash": f"lineage-{row_id}",
    }


def test_priority_funding_resume_refreshes_scans_before_child_copy(tmp_path) -> None:
    source = EvidenceStore(tmp_path / "source.sqlite")
    target = EvidenceStore(tmp_path / "target.sqlite")

    with source.engine.begin() as db:
        db.execute(insert(source.scans), [_scan_row("scan-old"), _scan_row("scan-new")])
        db.execute(
            insert(source.funding_quotes),
            [_funding_row(1, "scan-old"), _funding_row(2, "scan-new")],
        )

    with target.engine.begin() as db:
        db.execute(insert(target.scans), _scan_row("scan-old"))
        db.execute(insert(target.funding_quotes), _funding_row(1, "scan-old"))

    progress_path = tmp_path / "progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "state": "running",
                "current_table": "funding_quotes",
                "tables": {
                    "scans": {
                        "verified": True,
                        "migration_mode": "legacy_relational_snapshot",
                        "source_transport_retries": 0,
                    },
                    "funding_quotes": {
                        "verified": False,
                        "migration_mode": "captured_monotonic_integer_high_water",
                        "verification_scope": "captured_monotonic_integer_high_water",
                        "snapshot_high_water_captured": True,
                        "snapshot_high_water_primary_key": [2],
                        "snapshot_phase": "copying_snapshot",
                        "last_primary_key": [1],
                        "snapshot_rows_copied": 1,
                        "snapshot_rows_verified": 0,
                        "source_transport_retries": 0,
                        "snapshot_capture_retries": 0,
                    },
                },
                "postgresql_authoritative": True,
                "cutover_ready": False,
                "paper_only": True,
                "live_execution_authority": False,
            }
        )
    )

    package._resume_durable_monotonic_checkpoints_first(
        migration,
        migrate_monotonic_integer_append_only_table,
        source.engine,
        target,
        progress_path=progress_path,
        batch_size=256,
        interrupt_after_batches=None,
    )

    with target.engine.connect() as db:
        assert db.execute(text("PRAGMA foreign_keys")).scalar_one() == 1
        assert list(db.execute(select(target.scans.c.scan_id).order_by(target.scans.c.scan_id)).scalars()) == [
            "scan-new",
            "scan-old",
        ]
        assert list(db.execute(select(target.funding_quotes.c.id).order_by(target.funding_quotes.c.id)).scalars()) == [
            1,
            2,
        ]

    persisted = json.loads(progress_path.read_text())
    scans = persisted["tables"]["scans"]
    funding = persisted["tables"]["funding_quotes"]

    assert scans["migration_mode"] == "captured_primary_key_membership_manifest"
    assert scans["verified"] is True
    assert funding["parent_snapshot_refresh_high_water_primary_key"] == [2]
    assert funding["parent_snapshot_verified_high_water_primary_key"] == [2]
    assert funding["parent_snapshot_table"] == "scans"
    assert funding["snapshot_phase"] == "verified"
    assert funding["last_primary_key"] == [2]
    assert funding["verified"] is True
    assert persisted["postgresql_authoritative"] is True
    assert persisted["cutover_ready"] is False


def test_scans_are_routed_through_captured_append_only_stage_one_path() -> None:
    assert "scans" in package._STAGE_ONE_CAPTURED_APPEND_ONLY_TABLES

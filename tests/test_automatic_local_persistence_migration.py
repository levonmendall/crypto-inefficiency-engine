from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

import inefficiency_engine.local_persistence_migration_supervisor as migration_supervisor
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.local_persistence_migration_supervisor import (
    migration_preflight,
    migration_status_payload,
    run_local_persistence_migration_supervisor,
)
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory
from inefficiency_engine.postgres_local_migration import migrate_engines


def _quote(index: int) -> MarketQuote:
    return MarketQuote(
        venue="coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        contract_key="spot-reference",
        mid=50_000 + index,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
        source=f"automatic-migration-test:{index}",
    )


def _source_store(path) -> EvidenceStore:
    source = EvidenceStore(path)
    for index in range(2):
        observed = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index)
        source.record_scan(
            scan_id=f"scan-{index}",
            started_at=observed,
            completed_at=observed,
            providers=[ProviderStatus(provider="test", ok=True, observed_at=observed)],
            funding_quotes=[],
            market_quotes=[_quote(index)],
            opportunities=[],
        )
    return source


def _temp_migration_paths(tmp_path: Path):
    migration = tmp_path / "migration"
    migration.mkdir(parents=True, exist_ok=True)
    return (
        migration / "postgres-import-supervisor.json",
        migration / "postgres-import-progress.json",
        migration / "postgres-import.lock",
        migration / "postgres-import.stdout.log",
        migration / "postgres-import.stderr.log",
    )


def test_render_stage_one_enables_guard_without_cutover():
    blueprint = yaml.safe_load(open("render.yaml").read())
    runtime = blueprint["services"][0]
    env = {item["key"]: item for item in runtime["envVars"]}
    assert runtime["disk"]["mountPath"] == "/var/data/cie"
    assert runtime["disk"]["sizeGB"] == 10
    assert env["CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION"]["value"] == "true"
    assert env["DATABASE_URL"]["fromDatabase"]["name"] == "cie-evidence"
    assert env["CIE_MIGRATION_POSTGRES_URL"]["fromDatabase"]["name"] == "cie-evidence"
    assert "CIE_MARKET_HISTORY_BACKEND" not in env
    assert "CIE_EVIDENCE_DB_PATH" not in env


def test_migration_preflight_requires_same_authoritative_postgres(monkeypatch):
    monkeypatch.setenv("CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION", "true")
    monkeypatch.setenv("CIE_STORAGE_ROOT", "/var/data/cie")
    monkeypatch.setenv("DATABASE_URL", "postgresql://authoritative")
    monkeypatch.setenv("CIE_MIGRATION_POSTGRES_URL", "postgresql://other")
    monkeypatch.setattr(migration_supervisor, "_storage_root_state", lambda: (True, "ready"))
    assert migration_preflight() == (False, "migration_source_not_authoritative_database")
    monkeypatch.setenv("CIE_MIGRATION_POSTGRES_URL", "postgresql://authoritative")
    assert migration_preflight() == (True, "ready")
    monkeypatch.setenv("CIE_MARKET_HISTORY_BACKEND", "parquet")
    assert migration_preflight() == (False, "local_history_authority_already_enabled")


def test_render_release_defaults_to_authoritative_database_without_blueprint_env(monkeypatch):
    monkeypatch.delenv("CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION", raising=False)
    monkeypatch.delenv("CIE_MIGRATION_POSTGRES_URL", raising=False)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "abc123")
    monkeypatch.setenv("DATABASE_URL", "postgresql://authoritative")
    monkeypatch.setattr(migration_supervisor, "_storage_root_state", lambda: (True, "ready"))
    assert migration_preflight() == (True, "ready")
    assert migration_supervisor._migration_source_url() == "postgresql://authoritative"


def test_missing_disk_is_truthfully_blocked_instead_of_status_503(monkeypatch):
    monkeypatch.setenv("CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION", "true")
    monkeypatch.setenv("CIE_STORAGE_ROOT", "/var/data/cie")
    monkeypatch.setenv("DATABASE_URL", "postgresql://authoritative")
    monkeypatch.setattr(
        migration_supervisor,
        "_storage_root_state",
        lambda: (False, "storage_root_missing"),
    )
    assert migration_preflight() == (False, "storage_root_missing")
    payload = migration_status_payload()
    assert payload["state"] == "blocked"
    assert payload["supervisor_reason"] == "storage_root_missing"
    assert payload["storage_root_ready"] is False
    assert payload["postgresql_authoritative"] is True
    assert payload["cutover_ready"] is False


def test_verified_progress_is_restart_safe_and_never_grants_cutover(tmp_path, monkeypatch):
    monkeypatch.setenv("CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION", "true")
    monkeypatch.setenv("CIE_STORAGE_ROOT", "/var/data/cie")
    monkeypatch.setenv("DATABASE_URL", "postgresql://same")
    monkeypatch.setenv("CIE_MIGRATION_POSTGRES_URL", "postgresql://same")
    paths = _temp_migration_paths(tmp_path)
    monkeypatch.setattr(migration_supervisor, "_storage_root_state", lambda: (True, "ready"))
    monkeypatch.setattr(migration_supervisor, "_paths", lambda: paths)
    progress_path = paths[1]
    progress_path.write_text(json.dumps({
        "state": "verified",
        "completed_at": "2026-08-28T01:00:00+00:00",
        "verification_scope": "captured_primary_key_high_water",
        "tables": {
            "market_quotes": {
                "verified": True,
                "source_rows": 2,
                "source_lineage_count": 2,
                "high_water_primary_key": [2],
                "last_primary_key": [2],
                "destination_inventory": {"lineage_count": 2, "valid": True},
            }
        },
    }))

    run_local_persistence_migration_supervisor(threading.Event())
    payload = migration_status_payload()
    assert payload["state"] == "verified"
    assert payload["progress_state"] == "verified"
    assert payload["storage_root_ready"] is True
    assert payload["postgresql_authoritative"] is True
    assert payload["cutover_ready"] is False
    assert payload["live_execution_authority"] is False


def test_market_import_uses_captured_high_water_while_source_keeps_writing(tmp_path):
    source = _source_store(tmp_path / "source.sqlite3")
    target = EvidenceStore(tmp_path / "target.sqlite3")

    class InjectingHistory(PartitionedMarketHistory):
        injected = False

        def append_records(self, records):
            if not self.injected:
                self.injected = True
                quote = _quote(3)
                payload = quote.model_dump_json()
                with source.engine.begin() as db:
                    db.execute(
                        source.market_quotes.insert(),
                        {
                            "scan_id": "scan-1",
                            "venue": quote.venue,
                            "asset": quote.asset,
                            "observed_at": quote.observed_at.isoformat(),
                            "payload_json": payload,
                            "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(),
                        },
                    )
            return super().append_records(records)

    history = InjectingHistory(tmp_path / "history")
    progress = tmp_path / "progress.json"
    first = migrate_engines(
        source.engine,
        target,
        history,
        progress_path=progress,
        batch_size=1,
    )
    assert first["state"] == "verified"
    assert first["verification_scope"] == "captured_primary_key_high_water"
    assert first["cutover_ready"] is False
    assert first["tables"]["market_quotes"]["source_rows"] == 2
    assert first["tables"]["market_quotes"]["destination_inventory"]["lineage_count"] == 2

    second = migrate_engines(
        source.engine,
        target,
        PartitionedMarketHistory(tmp_path / "history"),
        progress_path=progress,
        batch_size=1,
    )
    assert second["state"] == "verified"
    assert second["tables"]["market_quotes"]["source_rows"] == 3
    assert second["tables"]["market_quotes"]["destination_inventory"]["lineage_count"] == 3

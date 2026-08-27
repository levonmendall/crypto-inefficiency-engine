from __future__ import annotations

import inspect
import json

from inefficiency_engine import cycle_history_index_supervisor_probe as probe


def test_progress_probe_uses_real_postgres_index_relid_column() -> None:
    source = inspect.getsource(probe)

    assert "p.index_relid" in source
    assert "p.indexrelid" not in source


def test_terminal_probe_does_not_import_full_evidence_store_module() -> None:
    source = inspect.getsource(probe)

    assert "from inefficiency_engine.evidence import WorkerHeartbeat" not in source
    assert "WorkerHeartbeat.model_validate_json" not in source
    assert "build_cycle_history_index_runtime_store" in source


def test_raw_heartbeat_decoder_preserves_terminal_sql_payload() -> None:
    payload = json.dumps(
        {
            "worker_id": "cycle-history-index-maintenance",
            "observed_at": "2026-08-27T22:00:00+00:00",
            "state": "degraded",
            "error_type": "CycleHistoryExactIndexUnavailable",
            "detail": {
                "attempt_number": 285,
                "stage": "cycle_history_index_retry_pending",
                "maintenance_result": {
                    "failures": [
                        {
                            "error_type": "OperationalError",
                            "message": "canceling statement due to lock timeout",
                        }
                    ]
                },
            },
        }
    )

    decoded = probe._heartbeat_from_raw_payload(payload)

    assert decoded is not None
    heartbeat, detail = decoded
    assert heartbeat.state == "degraded"
    assert heartbeat.error_type == "CycleHistoryExactIndexUnavailable"
    assert detail["attempt_number"] == 285
    assert detail["stage"] == "cycle_history_index_retry_pending"
    assert probe._sql_error_fields(detail["maintenance_result"]) == (
        "OperationalError",
        "canceling statement due to lock timeout",
    )

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from inefficiency_engine.evidence import (
    WorkerHeartbeat,
    _database_url,
    evidence_location_from_env,
)


EXACT_INDEX_CONNECT_TIMEOUT_SECONDS = 8
EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS = 8_000
EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS = 3_000


def _payload(heartbeat: WorkerHeartbeat) -> str:
    return json.dumps(
        heartbeat.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )


def _configure_bounded_heartbeat_session(db: Any) -> None:
    dialect = str(getattr(db.dialect, "name", ""))
    if dialect != "postgresql":
        return
    db.execute(
        text(
            f"SET LOCAL statement_timeout TO '{EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS}ms'"
        )
    )
    db.execute(
        text(
            f"SET LOCAL lock_timeout TO '{EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS}ms'"
        )
    )


class CycleHistoryIndexRuntimeStore:
    """Schema-free store for the dedicated exact-index process and its probes.

    Production schema creation is owned by the serialized startup bootstrap. Exact-index
    maintenance must never run SQLAlchemy ``metadata.create_all`` while PostgreSQL is
    already under DDL pressure. This store therefore opens only a bounded engine and
    implements the two durable heartbeat operations the exact-index path needs.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def latest_worker_heartbeat(self, worker_id: str | None = None) -> WorkerHeartbeat | None:
        where = "WHERE worker_id = :worker_id" if worker_id else ""
        params = {"worker_id": worker_id} if worker_id else {}
        with self.engine.connect() as db:
            _configure_bounded_heartbeat_session(db)
            payload = db.execute(
                text(
                    "SELECT payload_json FROM worker_heartbeats "
                    f"{where} ORDER BY id DESC LIMIT 1"
                ),
                params,
            ).scalar_one_or_none()
        return WorkerHeartbeat.model_validate_json(payload) if payload else None

    def record_worker_heartbeat(
        self,
        *,
        worker_id: str,
        state: str,
        cycle_id: str | None = None,
        scan_id: str | None = None,
        error_type: str | None = None,
        detail: dict[str, object] | None = None,
        observed_at: datetime | None = None,
    ) -> WorkerHeartbeat:
        heartbeat = WorkerHeartbeat(
            worker_id=worker_id,
            observed_at=observed_at or datetime.now(timezone.utc),
            state=state,
            cycle_id=cycle_id,
            scan_id=scan_id,
            error_type=error_type,
            detail=detail or {},
        )
        payload = _payload(heartbeat)
        with self.engine.begin() as db:
            _configure_bounded_heartbeat_session(db)
            db.execute(
                text(
                    "INSERT INTO worker_heartbeats "
                    "(worker_id, observed_at, state, cycle_id, scan_id, error_type, payload_json, lineage_hash) "
                    "VALUES (:worker_id, :observed_at, :state, :cycle_id, :scan_id, :error_type, :payload_json, :lineage_hash)"
                ),
                {
                    "worker_id": worker_id,
                    "observed_at": heartbeat.observed_at.isoformat(),
                    "state": state,
                    "cycle_id": cycle_id,
                    "scan_id": scan_id,
                    "error_type": error_type,
                    "payload_json": payload,
                    "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(),
                },
            )
        return heartbeat

    def dispose(self) -> None:
        try:
            self.engine.dispose()
        except Exception:
            pass


def build_cycle_history_index_runtime_store(
    fallback_path: str | Path | None = None,
) -> CycleHistoryIndexRuntimeStore | None:
    location = evidence_location_from_env(fallback_path)
    if location is None:
        return None
    url = _database_url(location)
    kwargs: dict[str, Any] = {
        "pool_pre_ping": True,
        "poolclass": NullPool,
    }
    if url.startswith("postgresql"):
        kwargs["connect_args"] = {"connect_timeout": EXACT_INDEX_CONNECT_TIMEOUT_SECONDS}
    elif url.startswith("sqlite:"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return CycleHistoryIndexRuntimeStore(create_engine(url, **kwargs))


__all__ = [
    "CycleHistoryIndexRuntimeStore",
    "EXACT_INDEX_CONNECT_TIMEOUT_SECONDS",
    "EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS",
    "EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS",
    "build_cycle_history_index_runtime_store",
]

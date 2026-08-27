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
# The exact CREATE INDEX CONCURRENTLY call can remain quiet on one SSL/TCP connection
# for many minutes. Production proved that connection can otherwise disappear mid-DDL
# with ``SSL error: unexpected eof while reading``. Keep these libpq TCP keepalives
# isolated to the dedicated exact-index runtime store; no general evidence-store or
# provider connection settings are changed.
EXACT_INDEX_TCP_KEEPALIVES_ENABLED = 1
EXACT_INDEX_TCP_KEEPALIVES_IDLE_SECONDS = 30
EXACT_INDEX_TCP_KEEPALIVES_INTERVAL_SECONDS = 10
EXACT_INDEX_TCP_KEEPALIVES_COUNT = 3


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


def _postgres_connection_resilience_detail() -> dict[str, object]:
    return {
        "exact_index_tcp_keepalives_enabled": True,
        "exact_index_tcp_keepalives_idle_seconds": EXACT_INDEX_TCP_KEEPALIVES_IDLE_SECONDS,
        "exact_index_tcp_keepalives_interval_seconds": EXACT_INDEX_TCP_KEEPALIVES_INTERVAL_SECONDS,
        "exact_index_tcp_keepalives_count": EXACT_INDEX_TCP_KEEPALIVES_COUNT,
        "exact_index_connection_resilience_scope": "dedicated_exact_index_only",
    }


class CycleHistoryIndexRuntimeStore:
    """Schema-free store for the dedicated exact-index process and its probes."""

    schema_free_exact_index_runtime_store = True

    def __init__(
        self,
        engine: Engine,
        *,
        connection_resilience: dict[str, object] | None = None,
    ):
        self.engine = engine
        self._connection_resilience = dict(connection_resilience or {})

    def connection_resilience_detail(self) -> dict[str, object]:
        """Return non-secret connection settings suitable for runtime telemetry."""

        return dict(self._connection_resilience)

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
    connection_resilience: dict[str, object] = {
        "exact_index_tcp_keepalives_enabled": False,
        "exact_index_connection_resilience_scope": "dedicated_exact_index_only",
    }
    if url.startswith("postgresql"):
        # Connect acquisition and every session start with a short catalog/query bound.
        # The direct exact-index maintainer explicitly raises statement_timeout to the
        # dedicated one-hour value immediately before CREATE INDEX CONCURRENTLY. libpq
        # keepalives protect that otherwise-quiet long-lived SSL/TCP DDL connection.
        kwargs["connect_args"] = {
            "connect_timeout": EXACT_INDEX_CONNECT_TIMEOUT_SECONDS,
            "keepalives": EXACT_INDEX_TCP_KEEPALIVES_ENABLED,
            "keepalives_idle": EXACT_INDEX_TCP_KEEPALIVES_IDLE_SECONDS,
            "keepalives_interval": EXACT_INDEX_TCP_KEEPALIVES_INTERVAL_SECONDS,
            "keepalives_count": EXACT_INDEX_TCP_KEEPALIVES_COUNT,
            "options": (
                f"-c statement_timeout={EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS} "
                f"-c lock_timeout={EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS}"
            ),
        }
        connection_resilience = _postgres_connection_resilience_detail()
    elif url.startswith("sqlite:"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return CycleHistoryIndexRuntimeStore(
        create_engine(url, **kwargs),
        connection_resilience=connection_resilience,
    )


__all__ = [
    "CycleHistoryIndexRuntimeStore",
    "EXACT_INDEX_CONNECT_TIMEOUT_SECONDS",
    "EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS",
    "EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS",
    "EXACT_INDEX_TCP_KEEPALIVES_COUNT",
    "EXACT_INDEX_TCP_KEEPALIVES_ENABLED",
    "EXACT_INDEX_TCP_KEEPALIVES_IDLE_SECONDS",
    "EXACT_INDEX_TCP_KEEPALIVES_INTERVAL_SECONDS",
    "build_cycle_history_index_runtime_store",
]

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine

from inefficiency_engine.evidence import (
    EvidenceStore,
    _database_url,
    evidence_location_from_env,
)


READ_ONLY_POSTGRES_STATEMENT_TIMEOUT_MS = 2_500
READ_ONLY_POSTGRES_LOCK_TIMEOUT_MS = 1_000


class ReadOnlyEvidenceStore(EvidenceStore):
    """EvidenceStore variant for read-plane processes.

    The production web service is not a schema owner. This constructor builds the
    SQLAlchemy table metadata needed by the inherited read methods, but deliberately
    does not call ``metadata.create_all`` or run SQLite write-oriented PRAGMAs.
    Database connectivity is therefore lazy until a read endpoint actually needs it.

    PostgreSQL read sessions also carry finite server-side statement and lock deadlines.
    This complements request-level HTTP deadlines: if a browser request times out, the
    database query itself is still cancelled instead of leaving an unbounded worker
    thread behind.
    """

    def __init__(
        self,
        location: str | Path,
        *,
        connect_timeout_seconds: int = 3,
        statement_timeout_ms: int = READ_ONLY_POSTGRES_STATEMENT_TIMEOUT_MS,
        lock_timeout_ms: int = READ_ONLY_POSTGRES_LOCK_TIMEOUT_MS,
    ) -> None:
        url = _database_url(location)
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if url.startswith("sqlite:"):
            kwargs["connect_args"] = {"check_same_thread": False}
        elif url.startswith("postgresql+psycopg://"):
            timeout = max(1, int(connect_timeout_seconds))
            statement_ms = max(250, int(statement_timeout_ms))
            lock_ms = max(100, min(int(lock_timeout_ms), statement_ms))
            kwargs["connect_args"] = {
                "connect_timeout": timeout,
                "options": (
                    f"-c statement_timeout={statement_ms} "
                    f"-c lock_timeout={lock_ms}"
                ),
            }
            kwargs["pool_timeout"] = timeout

        self.engine: Engine = create_engine(url, **kwargs)
        self.backend = self.engine.url.get_backend_name()
        self.safe_database_url = self.engine.url.render_as_string(hide_password=True)
        self.metadata = MetaData()
        self._schema()
        self.schema_mutation_enabled = False


def build_read_only_evidence_store(
    fallback_path: str | Path | None = None,
    *,
    connect_timeout_seconds: int = 3,
    statement_timeout_ms: int = READ_ONLY_POSTGRES_STATEMENT_TIMEOUT_MS,
    lock_timeout_ms: int = READ_ONLY_POSTGRES_LOCK_TIMEOUT_MS,
) -> ReadOnlyEvidenceStore | None:
    location = evidence_location_from_env(fallback_path)
    if location is None:
        return None
    return ReadOnlyEvidenceStore(
        location,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_ms=statement_timeout_ms,
        lock_timeout_ms=lock_timeout_ms,
    )


__all__ = [
    "READ_ONLY_POSTGRES_LOCK_TIMEOUT_MS",
    "READ_ONLY_POSTGRES_STATEMENT_TIMEOUT_MS",
    "ReadOnlyEvidenceStore",
    "build_read_only_evidence_store",
]

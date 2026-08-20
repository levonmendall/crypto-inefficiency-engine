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


class ReadOnlyEvidenceStore(EvidenceStore):
    """EvidenceStore variant for read-plane processes.

    The production web service is not a schema owner. This constructor builds the
    SQLAlchemy table metadata needed by the inherited read methods, but deliberately
    does not call ``metadata.create_all`` or run SQLite write-oriented PRAGMAs.
    Database connectivity is therefore lazy until a read endpoint actually needs it.
    """

    def __init__(
        self,
        location: str | Path,
        *,
        connect_timeout_seconds: int = 3,
    ) -> None:
        url = _database_url(location)
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if url.startswith("sqlite:"):
            kwargs["connect_args"] = {"check_same_thread": False}
        elif url.startswith("postgresql+psycopg://"):
            timeout = max(1, int(connect_timeout_seconds))
            kwargs["connect_args"] = {"connect_timeout": timeout}
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
) -> ReadOnlyEvidenceStore | None:
    location = evidence_location_from_env(fallback_path)
    if location is None:
        return None
    return ReadOnlyEvidenceStore(
        location,
        connect_timeout_seconds=connect_timeout_seconds,
    )

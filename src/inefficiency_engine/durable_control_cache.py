from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Boolean, Column, Integer, MetaData, String, Table, Text, insert, select, update


_METADATA = MetaData()
CONTROL_CACHE_CHECKPOINTS = Table(
    "control_evidence_cache_checkpoints",
    _METADATA,
    Column("cache_key", String(191), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("complete", Boolean, nullable=False),
    Column("updated_at", Text, nullable=False),
    Column("payload_json", Text, nullable=False),
)
_SCHEMA_VERSION = 1


def durable_control_cache_namespace() -> str | None:
    value = os.getenv("CIE_CONTROL_CACHE_NAMESPACE")
    return value.strip() if value and value.strip() else None


def ensure_durable_control_cache_schema(store: Any) -> None:
    """Create the small checkpoint table during serial runtime bootstrap."""

    _METADATA.create_all(store.engine, tables=[CONTROL_CACHE_CHECKPOINTS])


def _qualified_key(cache_key: str) -> str | None:
    namespace = durable_control_cache_namespace()
    return f"{namespace}:{cache_key}" if namespace is not None else None


def load_control_cache_checkpoint(
    store: Any,
    *,
    cache_key: str,
) -> dict[str, Any] | None:
    qualified = _qualified_key(cache_key)
    if qualified is None:
        return None
    ensure_durable_control_cache_schema(store)
    with store.engine.connect() as db:
        row = db.execute(
            select(
                CONTROL_CACHE_CHECKPOINTS.c.schema_version,
                CONTROL_CACHE_CHECKPOINTS.c.payload_json,
            ).where(CONTROL_CACHE_CHECKPOINTS.c.cache_key == qualified)
        ).one_or_none()
    if row is None or int(row.schema_version) != _SCHEMA_VERSION:
        return None
    try:
        payload = json.loads(str(row.payload_json))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def save_control_cache_checkpoint(
    store: Any,
    *,
    cache_key: str,
    payload: dict[str, Any],
    complete: bool,
) -> bool:
    """Atomically replace one reproducible cache checkpoint after a bounded batch."""

    qualified = _qualified_key(cache_key)
    if qualified is None:
        return False
    ensure_durable_control_cache_schema(store)
    values = {
        "schema_version": _SCHEMA_VERSION,
        "complete": bool(complete),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }
    with store.engine.begin() as db:
        exists = db.execute(
            select(CONTROL_CACHE_CHECKPOINTS.c.cache_key).where(
                CONTROL_CACHE_CHECKPOINTS.c.cache_key == qualified
            )
        ).scalar_one_or_none()
        if exists is None:
            db.execute(
                insert(CONTROL_CACHE_CHECKPOINTS),
                {"cache_key": qualified, **values},
            )
        else:
            db.execute(
                update(CONTROL_CACHE_CHECKPOINTS)
                .where(CONTROL_CACHE_CHECKPOINTS.c.cache_key == qualified)
                .values(**values)
            )
    return True

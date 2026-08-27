from __future__ import annotations

import hashlib
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
_CHECKPOINT_IDENTITY_FIELD = "_checkpoint_structure_identity"


def durable_control_cache_namespace() -> str | None:
    value = os.getenv("CIE_CONTROL_CACHE_NAMESPACE")
    return value.strip() if value and value.strip() else None


def control_cache_structure_identity(*parts: object) -> str:
    """Return a stable, credential-free identity for one exact cache structure."""

    material = "\x1f".join(str(part) for part in parts)
    return "sha256:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def control_cache_checkpoint_identity(cache_key: str) -> str:
    """Identify the serialized contract behind one durable control checkpoint.

    The physical checkpoint key remains unchanged so already accumulated production
    progress is not discarded. Legacy payloads without an identity are accepted once
    and stamped on their next save. A payload carrying a conflicting identity is not
    trusted, which forces the existing fail-closed bounded rebuild path.
    """

    if cache_key == "strategy-evidence":
        contract = (
            "strategy-evidence-v1|"
            "alpha_forward_events:id,strategy_id,family,event_type,payload_json|"
            "allocation_forward_trials:id,strategy,settlement_supported|"
            "allocation_forward_outcomes:id,strategy,payload_json|"
            "signals,raw_outcomes,observed_identity,allocator_by_strategy,supported_trials"
        )
    elif cache_key.startswith("outcome-history:"):
        contract = f"outcome-history-json-model-v1|{cache_key}"
    elif cache_key == "cycle-history-live-compact-v3":
        contract = "cycle-history-live-compact-v3|daily-bucket-checkpoint-v3"
    else:
        contract = f"generic-control-checkpoint-v1|{cache_key}"
    return control_cache_structure_identity("durable-control-checkpoint", contract)


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
    if not isinstance(payload, dict):
        return None
    observed_identity = payload.get(_CHECKPOINT_IDENTITY_FIELD)
    expected_identity = control_cache_checkpoint_identity(cache_key)
    if observed_identity is not None and str(observed_identity) != expected_identity:
        return None
    return payload


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
    stamped_payload = dict(payload)
    stamped_payload[_CHECKPOINT_IDENTITY_FIELD] = control_cache_checkpoint_identity(cache_key)
    values = {
        "schema_version": _SCHEMA_VERSION,
        "complete": bool(complete),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "payload_json": json.dumps(stamped_payload, sort_keys=True, separators=(",", ":")),
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

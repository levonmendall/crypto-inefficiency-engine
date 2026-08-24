from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

from sqlalchemy import select

from inefficiency_engine.durable_control_cache import (
    durable_control_cache_namespace,
    load_control_cache_checkpoint,
    save_control_cache_checkpoint,
)


_PATCH_MARKER = "_cie_bounded_control_outcome_ledgers"
_CACHE_LOCK = threading.RLock()
_CACHE_CHECK_SECONDS = 5.0
_DEFAULT_BOOTSTRAP_BATCH_ROWS = 500
_CACHE: dict[tuple[int, str], dict[str, Any]] = {}


def _bootstrap_batch_rows() -> int:
    raw = os.getenv(
        "CIE_CONTROL_OUTCOME_BOOTSTRAP_BATCH_ROWS",
        str(_DEFAULT_BOOTSTRAP_BATCH_ROWS),
    )
    try:
        value = int(raw)
    except ValueError:
        value = _DEFAULT_BOOTSTRAP_BATCH_ROWS
    return max(100, min(20000, value))


def _new_state() -> dict[str, Any]:
    return {
        "tail": 0,
        "target_tail": 0,
        "rows": [],
        "checked_at": 0.0,
        "bootstrap_complete": False,
        "last_batch_rows": 0,
        "durable_checkpoint_active": durable_control_cache_namespace() is not None,
        "durable_checkpoint_loaded": False,
        "durable_checkpoint_persisted": False,
    }


def _state(ledger: Any, table_name: str, model: Any) -> dict[str, Any]:
    key = (id(ledger.store.engine), table_name)
    state = _CACHE.get(key)
    if state is not None:
        return state
    checkpoint = load_control_cache_checkpoint(
        ledger.store,
        cache_key=f"outcome-history:{table_name}",
    )
    state = _new_state()
    if checkpoint is not None:
        state["tail"] = int(checkpoint.get("tail") or 0)
        state["target_tail"] = int(checkpoint.get("target_tail") or 0)
        state["rows"] = [
            model.model_validate_json(payload)
            for payload in checkpoint.get("rows", [])
            if isinstance(payload, str)
        ]
        state["bootstrap_complete"] = bool(checkpoint.get("bootstrap_complete"))
        state["durable_checkpoint_loaded"] = True
    _CACHE[key] = state
    return state


def _serialize_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "tail": int(state["tail"]),
        "target_tail": int(state["target_tail"]),
        "rows": [
            row.model_dump_json()
            if callable(getattr(row, "model_dump_json", None))
            else json.dumps(row)
            for row in state["rows"]
        ],
        "bootstrap_complete": bool(state["bootstrap_complete"]),
    }


def _refresh_rows(ledger: Any, table: Any, model: Any) -> list[Any]:
    """Return exact append-only history without an unbounded cold-start read.

    Each refresh consumes at most one primary-key batch. Until the cache has caught
    up to the table tail observed at the start of the refresh, callers receive an
    empty history. That deliberately fails qualification closed rather than allowing
    a partial historical sample to certify or promote anything. Once caught up, the
    full exact history is exposed and later appends are consumed incrementally.
    """

    engine = ledger.store.engine
    now = time.monotonic()
    batch_rows = _bootstrap_batch_rows()
    with _CACHE_LOCK:
        state = _state(ledger, table.name, model)
        if now - float(state["checked_at"]) < _CACHE_CHECK_SECONDS:
            return list(state["rows"]) if bool(state["bootstrap_complete"]) else []

        with engine.connect() as db:
            tail = db.execute(
                select(table.c.id).order_by(table.c.id.desc()).limit(1)
            ).scalar_one_or_none()
            target_tail = int(tail or 0)
            prior_tail = int(state["tail"])
            if target_tail < prior_tail:
                state.update(
                    {
                        "tail": 0,
                        "target_tail": target_tail,
                        "rows": [],
                        "bootstrap_complete": False,
                        "last_batch_rows": 0,
                    }
                )
                prior_tail = 0

            additions: list[Any] = []
            if target_tail > prior_tail:
                query = (
                    select(table.c.id, table.c.payload_json)
                    .where(table.c.id > prior_tail)
                    .where(table.c.id <= target_tail)
                    .order_by(table.c.id)
                    .limit(batch_rows)
                )
                additions = list(db.execute(query))
                if additions:
                    state["rows"].extend(
                        model.model_validate_json(payload)
                        for _row_id, payload in additions
                    )
                    processed_tail = int(additions[-1][0])
                    if len(additions) < batch_rows:
                        processed_tail = target_tail
                    state["tail"] = processed_tail
                else:
                    # Append-only ids may contain gaps. If no rows remain in the
                    # observed range, the cache is exact through the observed tail.
                    state["tail"] = target_tail

            state["target_tail"] = target_tail
            state["last_batch_rows"] = len(additions)
            state["bootstrap_complete"] = int(state["tail"]) >= target_tail

        state["durable_checkpoint_persisted"] = save_control_cache_checkpoint(
            ledger.store,
            cache_key=f"outcome-history:{table.name}",
            payload=_serialize_state(state),
            complete=bool(state["bootstrap_complete"]),
        )
        state["checked_at"] = now
        return list(state["rows"]) if bool(state["bootstrap_complete"]) else []


def bounded_mechanism_outcomes(
    ledger,
    *,
    cohort_key: str | None = None,
    mechanism_id: str | None = None,
):
    from inefficiency_engine.mechanism_execution import MechanismForwardOutcome

    rows = _refresh_rows(ledger, ledger.outcomes_table, MechanismForwardOutcome)
    if cohort_key is not None:
        rows = [row for row in rows if row.cohort_key == cohort_key]
    if mechanism_id is not None:
        rows = [row for row in rows if row.mechanism_id == mechanism_id]
    return rows


def bounded_allocation_outcomes(ledger):
    from inefficiency_engine.allocation_certification import PaperAllocationOutcome

    return _refresh_rows(ledger, ledger.outcomes_table, PaperAllocationOutcome)


def install_bounded_control_outcome_ledgers() -> None:
    """Prevent reconciliation from rescanning or cold-loading histories without bounds.

    Mechanism readiness and operating status ask for the same historical outcome
    ledgers repeatedly by mechanism/cohort. The cache accumulates the complete exact
    append-only evidence in bounded primary-key batches. Qualification remains
    fail-closed until each cache is caught up; no statistical or economic evidence is
    truncated and all existing qualification rules see the same rows once ready.
    """

    from inefficiency_engine.allocation_certification import AllocationCertificationLedger
    from inefficiency_engine.mechanism_execution import MechanismExecutionLedger

    if not bool(getattr(MechanismExecutionLedger, _PATCH_MARKER, False)):
        MechanismExecutionLedger.outcomes = bounded_mechanism_outcomes
        setattr(MechanismExecutionLedger, _PATCH_MARKER, True)
    if not bool(getattr(AllocationCertificationLedger, _PATCH_MARKER, False)):
        AllocationCertificationLedger.outcomes = bounded_allocation_outcomes
        setattr(AllocationCertificationLedger, _PATCH_MARKER, True)


def advance_bounded_control_outcome_caches(
    *,
    mechanism_execution: Any,
    allocation_certification: Any,
) -> dict[str, object]:
    """Advance both exact outcome caches once before operating reconciliation.

    The canonical control executor is intentionally short-lived. If its first access
    to historical mechanism/allocation evidence happens deep inside mechanism
    readiness, a cold bootstrap can consume the entire process deadline before the
    durable checkpoint is written. Prime each patched ledger exactly once at the
    explicit cache boundary instead. A partial batch remains invisible to all
    qualification callers, is checkpointed durably, and the caller can fail closed
    for this control cycle without entering the heavier reconciliation graph.
    """

    mechanism_ledger = getattr(
        getattr(mechanism_execution, "ledger", None),
        "_base",
        getattr(mechanism_execution, "ledger", None),
    )
    allocation_ledger = getattr(allocation_certification, "ledger", None)
    if mechanism_ledger is None or allocation_ledger is None:
        raise RuntimeError("control outcome cache priming requires both durable ledgers")

    # The return values are deliberately ignored. During bootstrap they are empty by
    # contract, which preserves fail-closed qualification. The purpose of these calls
    # is solely to advance and persist one bounded exact-history batch per ledger.
    mechanism_ledger.outcomes()
    allocation_ledger.outcomes()
    return bounded_control_outcome_cache_diagnostics()


def bounded_control_outcome_cache_diagnostics() -> dict[str, object]:
    with _CACHE_LOCK:
        tables = {
            table_name: {
                "processed_tail": int(state["tail"]),
                "target_tail": int(state["target_tail"]),
                "row_count": len(state["rows"]),
                "bootstrap_complete": bool(state["bootstrap_complete"]),
                "last_batch_rows": int(state["last_batch_rows"]),
                "durable_checkpoint_active": bool(state["durable_checkpoint_active"]),
                "durable_checkpoint_loaded": bool(state["durable_checkpoint_loaded"]),
                "durable_checkpoint_persisted": bool(
                    state["durable_checkpoint_persisted"]
                ),
            }
            for (_engine_id, table_name), state in _CACHE.items()
        }
        return {
            "mode": "bounded_exact_bootstrap_then_incremental_tail",
            "batch_rows": _bootstrap_batch_rows(),
            "durable_namespace": durable_control_cache_namespace(),
            "table_count": len(_CACHE),
            "all_caches_complete": bool(tables)
            and all(bool(row["bootstrap_complete"]) for row in tables.values()),
            "tables": tables,
        }

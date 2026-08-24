from __future__ import annotations

import os
import threading
import time
from typing import Any

from sqlalchemy import select


_PATCH_MARKER = "_cie_bounded_control_outcome_ledgers"
_CACHE_LOCK = threading.RLock()
_CACHE_CHECK_SECONDS = 5.0
_DEFAULT_BOOTSTRAP_BATCH_ROWS = 5000
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


def _state(engine: Any, table_name: str) -> dict[str, Any]:
    return _CACHE.setdefault(
        (id(engine), table_name),
        {
            "tail": 0,
            "target_tail": 0,
            "rows": [],
            "checked_at": 0.0,
            "bootstrap_complete": False,
            "last_batch_rows": 0,
        },
    )


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
        state = _state(engine, table.name)
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


def bounded_control_outcome_cache_diagnostics() -> dict[str, object]:
    with _CACHE_LOCK:
        tables = {
            table_name: {
                "processed_tail": int(state["tail"]),
                "target_tail": int(state["target_tail"]),
                "row_count": len(state["rows"]),
                "bootstrap_complete": bool(state["bootstrap_complete"]),
                "last_batch_rows": int(state["last_batch_rows"]),
            }
            for (_engine_id, table_name), state in _CACHE.items()
        }
        return {
            "mode": "bounded_exact_bootstrap_then_incremental_tail",
            "batch_rows": _bootstrap_batch_rows(),
            "table_count": len(_CACHE),
            "all_caches_complete": bool(tables)
            and all(bool(row["bootstrap_complete"]) for row in tables.values()),
            "tables": tables,
        }

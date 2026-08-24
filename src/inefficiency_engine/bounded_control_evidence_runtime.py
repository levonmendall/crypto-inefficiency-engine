from __future__ import annotations

import threading
import time
from typing import Any

from sqlalchemy import select


_PATCH_MARKER = "_cie_bounded_control_outcome_ledgers"
_CACHE_LOCK = threading.RLock()
_CACHE_CHECK_SECONDS = 0.5
_CACHE: dict[tuple[int, str], dict[str, Any]] = {}


def _state(engine: Any, table_name: str) -> dict[str, Any]:
    return _CACHE.setdefault(
        (id(engine), table_name),
        {"tail": 0, "rows": [], "checked_at": 0.0},
    )


def _refresh_rows(ledger: Any, table: Any, model: Any) -> list[Any]:
    """Return exact append-only history using one initial read plus incremental tails."""

    engine = ledger.store.engine
    now = time.monotonic()
    with _CACHE_LOCK:
        state = _state(engine, table.name)
        if now - float(state["checked_at"]) < _CACHE_CHECK_SECONDS:
            return list(state["rows"])

        with engine.connect() as db:
            tail = db.execute(
                select(table.c.id).order_by(table.c.id.desc()).limit(1)
            ).scalar_one_or_none()
            tail_id = int(tail or 0)
            prior_tail = int(state["tail"])
            if tail_id < prior_tail:
                state["tail"] = 0
                state["rows"] = []
                prior_tail = 0

            if tail_id > prior_tail:
                query = select(table.c.id, table.c.payload_json).where(
                    table.c.id > prior_tail
                ).order_by(table.c.id)
                additions = list(db.execute(query))
                state["rows"].extend(
                    model.model_validate_json(payload)
                    for _row_id, payload in additions
                )
                state["tail"] = tail_id

        state["checked_at"] = now
        return list(state["rows"])


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
    """Prevent reconciliation from rescanning the same append-only outcomes N times.

    Mechanism readiness and operating status ask for the same historical outcome
    ledgers repeatedly by mechanism/cohort. Production reconciliation needs exact
    history, so this keeps the full accumulated evidence in-process and reads only
    newly appended rows after the initial load. No statistical or economic evidence
    is truncated and all existing qualification rules continue to see the same rows.
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
        return {
            "mode": "initial_exact_history_plus_incremental_tail",
            "table_count": len(_CACHE),
            "tables": {
                table_name: {
                    "tail": int(state["tail"]),
                    "row_count": len(state["rows"]),
                }
                for (_engine_id, table_name), state in _CACHE.items()
            },
        }

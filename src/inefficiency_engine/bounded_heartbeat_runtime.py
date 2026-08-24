from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text

from inefficiency_engine.evidence import EvidenceStore, WorkerHeartbeat


DEFAULT_HEARTBEAT_READ_WINDOW_ROWS = 10_000
MIN_HEARTBEAT_READ_WINDOW_ROWS = 1_000
MAX_HEARTBEAT_READ_WINDOW_ROWS = 50_000
_PATCH_MARKER = "_cie_bounded_heartbeat_read"


def heartbeat_read_window_rows() -> int:
    """Return the bounded recent append-only tail used for liveness reads."""

    raw = os.getenv(
        "CIE_HEARTBEAT_READ_WINDOW_ROWS",
        str(DEFAULT_HEARTBEAT_READ_WINDOW_ROWS),
    )
    try:
        value = int(raw)
    except ValueError:
        value = DEFAULT_HEARTBEAT_READ_WINDOW_ROWS
    return max(MIN_HEARTBEAT_READ_WINDOW_ROWS, min(MAX_HEARTBEAT_READ_WINDOW_ROWS, value))


def bounded_heartbeat_payloads(
    db: Any,
    *,
    worker_id: str | None,
    limit: int = 1,
    window_rows: int | None = None,
) -> list[str]:
    """Read heartbeats only from a recent primary-key tail.

    Worker liveness is intentionally fail-closed. If a worker has not emitted a row
    inside the bounded recent tail, callers treat it as unobserved/stale rather than
    scanning the entire append-only heartbeat ledger. This avoids requiring a large
    production index build on the constrained shared PostgreSQL instance.
    """

    bounded_limit = max(1, min(200, int(limit)))
    window = max(
        1,
        int(window_rows if window_rows is not None else heartbeat_read_window_rows()),
    )
    tail_id = db.execute(
        text("SELECT id FROM worker_heartbeats ORDER BY id DESC LIMIT 1")
    ).scalar_one_or_none()
    if tail_id is None:
        return []
    floor_id = max(1, int(tail_id) - window + 1)
    if worker_id:
        query = text(
            "SELECT payload_json FROM worker_heartbeats "
            "WHERE id >= :floor_id AND worker_id = :worker_id "
            "ORDER BY id DESC LIMIT :limit"
        )
        params = {
            "floor_id": floor_id,
            "worker_id": worker_id,
            "limit": bounded_limit,
        }
    else:
        query = text(
            "SELECT payload_json FROM worker_heartbeats "
            "WHERE id >= :floor_id ORDER BY id DESC LIMIT :limit"
        )
        params = {"floor_id": floor_id, "limit": bounded_limit}
    return list(db.execute(query, params).scalars())


def _bounded_latest_worker_heartbeat(
    self: EvidenceStore,
    worker_id: str | None = None,
) -> WorkerHeartbeat | None:
    with self.engine.connect() as db:
        payloads = bounded_heartbeat_payloads(
            db,
            worker_id=worker_id,
            limit=1,
        )
    return WorkerHeartbeat.model_validate_json(payloads[0]) if payloads else None


def _bounded_dashboard_heartbeat_history(
    db: Any,
    worker_id: str,
    available: set[str],
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if "worker_heartbeats" not in available:
        return []
    from inefficiency_engine.dashboard_projection import _rows

    return _rows(
        bounded_heartbeat_payloads(
            db,
            worker_id=worker_id,
            limit=limit,
        )
    )


def install_bounded_evidence_heartbeat_read() -> None:
    """Patch durable worker-liveness reads before API composition."""

    if bool(getattr(EvidenceStore, _PATCH_MARKER, False)):
        return
    EvidenceStore.latest_worker_heartbeat = _bounded_latest_worker_heartbeat  # type: ignore[method-assign]
    setattr(EvidenceStore, _PATCH_MARKER, True)


def install_bounded_dashboard_heartbeat_read() -> None:
    """Patch compact dashboard heartbeat history without changing research truth."""

    from inefficiency_engine import dashboard_projection

    dashboard_projection._heartbeat_history = _bounded_dashboard_heartbeat_history


def install_bounded_heartbeat_runtime() -> None:
    install_bounded_evidence_heartbeat_read()
    install_bounded_dashboard_heartbeat_read()

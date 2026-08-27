from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from urllib.request import urlopen

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store


WORKER_ID = "combined-runtime-parent-liveness"
HEARTBEAT_INTERVAL_SECONDS = 30.0
API_BIND_POLL_SECONDS = 1.0
API_BIND_READ_TIMEOUT_SECONDS = 1.0


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def _identity() -> tuple[int, str | None, str]:
    parent_pid = os.getppid()
    release_commit = _release_commit()
    generation = f"{release_commit or 'unknown'}:{parent_pid}"
    return parent_pid, release_commit, generation


def _api_is_bound(port: str | int) -> bool:
    """Check process-only API liveness without touching the durable store."""

    try:
        with urlopen(
            f"http://127.0.0.1:{port}/health",
            timeout=API_BIND_READ_TIMEOUT_SECONDS,
        ) as response:
            return int(getattr(response, "status", 200)) == 200
    except Exception:
        return False


def _record(*, state: str, stage: str, detail: dict[str, object]) -> None:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("combined runtime parent heartbeat requires durable persistence")
    parent_pid, release_commit, generation = _identity()
    store.record_worker_heartbeat(
        worker_id=WORKER_ID,
        state=state,
        detail={
            "stage": stage,
            "parent_pid": parent_pid,
            "parent_generation": generation,
            "release_commit": release_commit,
            "diagnostic_only": True,
            "qualification_thresholds_unchanged": True,
            "certification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
            **detail,
        },
    )


def _record_terminal(raw: str) -> int:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        return 2
    _record(
        state="degraded",
        stage="combined_runtime_parent_terminating",
        detail={
            "parent_exit_observed": True,
            "exit_reason": payload.get("exit_reason"),
            "return_code": payload.get("return_code"),
            "error_type": payload.get("error_type"),
            "message": payload.get("message"),
        },
    )
    return 0


def main() -> int:
    """Publish combined-runtime generation or one terminal parent observation.

    The long-lived heartbeat subprocess is spawned before the canonical combined runtime
    enters its serialized schema bootstrap. It therefore must not open the evidence
    store until the public zero-database `/health` endpoint proves that bootstrap has
    completed and the API has bound. This preserves the single-process bootstrap barrier
    while retaining durable generation diagnostics after startup.
    """

    if len(sys.argv) == 3 and sys.argv[1] == "--terminal":
        try:
            return _record_terminal(sys.argv[2])
        except Exception as exc:
            print(
                f"combined runtime terminal heartbeat unavailable: {type(exc).__name__}: {exc}",
                flush=True,
            )
            return 1

    stopping = False

    def request_stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    process_started_at = datetime.now(timezone.utc)
    port = os.getenv("PORT", "10000")
    while not stopping and not _api_is_bound(port):
        deadline = time.monotonic() + API_BIND_POLL_SECONDS
        while not stopping and time.monotonic() < deadline:
            time.sleep(0.1)
    if stopping:
        return 0

    sequence = 0
    while not stopping:
        sequence += 1
        try:
            _record(
                state="running",
                stage="combined_runtime_parent_alive",
                detail={
                    "parent_started_at": process_started_at.isoformat(),
                    "api_bound_before_durable_heartbeat": True,
                    "sequence": sequence,
                },
            )
        except Exception as exc:
            print(
                "combined runtime parent heartbeat unavailable: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

        deadline = time.monotonic() + HEARTBEAT_INTERVAL_SECONDS
        while not stopping and time.monotonic() < deadline:
            time.sleep(0.25)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "API_BIND_POLL_SECONDS",
    "API_BIND_READ_TIMEOUT_SECONDS",
    "HEARTBEAT_INTERVAL_SECONDS",
    "WORKER_ID",
    "_api_is_bound",
    "_identity",
    "_record_terminal",
    "main",
]

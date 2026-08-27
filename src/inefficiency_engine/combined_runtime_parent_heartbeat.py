from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store


WORKER_ID = "combined-runtime-parent-liveness"
HEARTBEAT_INTERVAL_SECONDS = 30.0


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def _identity() -> tuple[int, str | None, str]:
    parent_pid = os.getppid()
    release_commit = _release_commit()
    generation = f"{release_commit or 'unknown'}:{parent_pid}"
    return parent_pid, release_commit, generation


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
    """Publish combined-runtime generation or one terminal parent observation."""

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

    started_at = datetime.now(timezone.utc)
    sequence = 0
    while not stopping:
        sequence += 1
        try:
            _record(
                state="running",
                stage="combined_runtime_parent_alive",
                detail={
                    "parent_started_at": started_at.isoformat(),
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
    "HEARTBEAT_INTERVAL_SECONDS",
    "WORKER_ID",
    "_identity",
    "_record_terminal",
    "main",
]

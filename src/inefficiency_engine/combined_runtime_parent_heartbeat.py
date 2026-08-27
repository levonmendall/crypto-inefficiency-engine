from __future__ import annotations

import os
import signal
import time
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store


WORKER_ID = "combined-runtime-parent-liveness"
HEARTBEAT_INTERVAL_SECONDS = 30.0


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def main() -> int:
    """Publish the owning combined-runtime generation from an isolated process.

    The worker has no authority over certification, allocation or execution. Its only
    purpose is to make whole-service restarts directly observable: a new parent PID and
    generation replaces the previous one after each Render/runtime recycle.
    """

    stopping = False

    def request_stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    parent_pid = os.getppid()
    started_at = datetime.now(timezone.utc)
    release_commit = _release_commit()
    generation = f"{release_commit or 'unknown'}:{parent_pid}:{started_at.isoformat()}"
    sequence = 0

    while not stopping:
        sequence += 1
        try:
            settings = Settings.from_env()
            store = build_evidence_store(settings.evidence_db_path)
            if store is not None:
                store.record_worker_heartbeat(
                    worker_id=WORKER_ID,
                    state="running",
                    detail={
                        "stage": "combined_runtime_parent_alive",
                        "parent_pid": parent_pid,
                        "parent_generation": generation,
                        "parent_started_at": started_at.isoformat(),
                        "release_commit": release_commit,
                        "sequence": sequence,
                        "diagnostic_only": True,
                        "qualification_thresholds_unchanged": True,
                        "certification_authority": False,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
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


__all__ = ["HEARTBEAT_INTERVAL_SECONDS", "WORKER_ID", "main"]

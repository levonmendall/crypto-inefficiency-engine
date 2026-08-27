from __future__ import annotations

import json

from inefficiency_engine import read_api_certification_fast_readiness as readiness
from inefficiency_engine.combined_runtime_parent_heartbeat import WORKER_ID as PARENT_WORKER_ID


PARENT_LABEL = "combined_runtime_parent"


def main() -> int:
    """Emit one compact batched worker-heartbeat snapshot and exit.

    This process is intentionally disposable. The API parent can kill it on deadline,
    so a slow PostgreSQL connection or query cannot strand the web event loop or the
    long-lived runtime watchdog. Extend the existing mapping before the query so parent
    generation truth is included in the same single targeted latest-per-worker UNION
    statement rather than adding a second database round trip.
    """

    readiness._CERTIFICATION_EXTRA_HEARTBEATS.setdefault(  # noqa: SLF001
        PARENT_LABEL,
        PARENT_WORKER_ID,
    )
    payload = {
        "runtime_heartbeats": readiness._runtime_heartbeats(),  # noqa: SLF001
        "diagnostic_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["PARENT_LABEL", "main"]

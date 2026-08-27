from __future__ import annotations

import json

from inefficiency_engine.read_api_certification_fast_readiness import _runtime_heartbeats


def main() -> int:
    """Emit one compact batched worker-heartbeat snapshot and exit.

    This process is intentionally disposable. The API parent can kill it on deadline,
    so a slow PostgreSQL connection or query cannot strand the web event loop or the
    long-lived runtime watchdog. The underlying read remains the existing single
    targeted latest-per-worker UNION query.
    """

    payload = {
        "runtime_heartbeats": _runtime_heartbeats(),
        "diagnostic_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]

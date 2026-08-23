from __future__ import annotations

import signal
import time


_STOP = False


def _request_stop(_signum: int, _frame: object) -> None:
    global _STOP
    _STOP = True


def main() -> int:
    """Hold the inner supervisor's mechanism slot without owning mechanism work.

    The canonical Render wrapper owns the real mechanism-forward process in a
    heartbeat-aware guard. The legacy inner supervisor still expects a `mechanism`
    child key during startup, so this inert process preserves that structural slot
    until the inner supervisor is refactored to iterate only configured children.
    It performs no provider, research, qualification, allocation, or execution work.
    """

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)
    while not _STOP:
        time.sleep(1.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

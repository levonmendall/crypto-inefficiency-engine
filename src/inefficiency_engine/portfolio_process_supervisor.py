from __future__ import annotations

import signal
import subprocess
import sys
import time


PORTFOLIO_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.lightweight_portfolio_worker_bounded_heartbeat",
]
RESTART_BACKOFF_SECONDS = 5.0
TERMINATION_GRACE_SECONDS = 15.0


def _terminate(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    deadline = time.monotonic() + TERMINATION_GRACE_SECONDS
    while time.monotonic() < deadline:
        if child.poll() is not None:
            return
        time.sleep(0.2)
    if child.poll() is None:
        child.kill()


def main() -> int:
    """Keep portfolio recovery process-local instead of recycling the whole service.

    The combined runtime historically treats its portfolio subprocess as critical: an
    unexpected portfolio exit returns from the parent and causes Render to restart the
    API plus every background supervisor. This lightweight wrapper becomes the stable
    permanent child while the actual canonical portfolio worker remains replaceable.
    A database/runtime failure therefore recycles only portfolio and cannot terminate a
    healthy API or an in-flight one-hour exact-index build.
    """

    stopping = False
    child: subprocess.Popen[bytes] | None = None

    def request_stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True
        if child is not None:
            _terminate(child)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    while not stopping:
        print(
            "starting isolated canonical portfolio worker: " + " ".join(PORTFOLIO_COMMAND),
            flush=True,
        )
        child = subprocess.Popen(PORTFOLIO_COMMAND)
        while not stopping and child.poll() is None:
            time.sleep(0.5)
        if stopping:
            _terminate(child)
            return 0

        return_code = child.poll()
        print(
            "isolated canonical portfolio worker exited "
            f"code={return_code}; restarting portfolio only",
            flush=True,
        )
        deadline = time.monotonic() + RESTART_BACKOFF_SECONDS
        while not stopping and time.monotonic() < deadline:
            time.sleep(0.2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "PORTFOLIO_COMMAND",
    "RESTART_BACKOFF_SECONDS",
    "TERMINATION_GRACE_SECONDS",
    "main",
]

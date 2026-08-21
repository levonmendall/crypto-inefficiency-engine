from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence


API_APP = "inefficiency_engine.read_api_active_volume_deploy:app"


def child_commands(port: str | int) -> dict[str, list[str]]:
    """Return the critical child commands for the consolidated Render service."""
    port_text = str(port)
    return {
        "api": [
            sys.executable,
            "-m",
            "uvicorn",
            API_APP,
            "--host",
            "0.0.0.0",
            "--port",
            port_text,
        ],
        "worker": [
            sys.executable,
            "-m",
            "inefficiency_engine.cli",
            "worker",
        ],
        "history": [
            sys.executable,
            "-m",
            "inefficiency_engine.active_volume_runtime",
        ],
    }


def _terminate_children(children: Sequence[subprocess.Popen[bytes]], *, timeout_seconds: float = 20.0) -> None:
    for child in children:
        if child.poll() is None:
            child.terminate()

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if all(child.poll() is not None for child in children):
            return
        time.sleep(0.2)

    for child in children:
        if child.poll() is None:
            child.kill()


def main() -> int:
    """Run the read API, governed worker, and active-universe history maintenance.

    The history child resolves the same validated market-wide top-40 volume universe
    used by live research before each maintenance pass. Historical rows for assets
    that later leave the top 40 remain archived in PostgreSQL, but the active read
    plane filters them out so archived research can never masquerade as current
    universe membership. All three children remain supervised as critical processes.
    """
    port = os.getenv("PORT", "10000")
    commands = child_commands(port)
    children: dict[str, subprocess.Popen[bytes]] = {}
    stopping = False

    def _request_stop(signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        print(f"combined runtime received signal {signum}; shutting down", flush=True)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    try:
        for name in ("worker", "history", "api"):
            command = commands[name]
            print(f"starting critical child {name}: {' '.join(command)}", flush=True)
            children[name] = subprocess.Popen(command)

        while not stopping:
            for name, child in children.items():
                return_code = child.poll()
                if return_code is not None:
                    print(
                        f"critical child {name} exited with code {return_code}; restarting Render service",
                        flush=True,
                    )
                    return return_code if return_code != 0 else 1
            time.sleep(0.5)
        return 0
    finally:
        _terminate_children(list(children.values()))


if __name__ == "__main__":
    raise SystemExit(main())

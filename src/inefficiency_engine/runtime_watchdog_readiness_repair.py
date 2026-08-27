from __future__ import annotations

import json
from urllib.request import urlopen

from inefficiency_engine.read_api_liveness_deploy import INTERNAL_RUNTIME_HEARTBEAT_PATH


_PATCH_MARKER = "_cie_runtime_watchdog_readiness_repair_installed"
WATCHDOG_HEARTBEAT_READ_TIMEOUT_SECONDS = 10.0


def watchdog_diagnostic_path(path: str) -> str:
    """Keep Render liveness process-only while watchdogs read bounded durable truth."""

    return INTERNAL_RUNTIME_HEARTBEAT_PATH if path == "/health" else path


def _bounded_runtime_heartbeat_json(port: str | int) -> dict[str, object]:
    with urlopen(
        f"http://127.0.0.1:{port}{INTERNAL_RUNTIME_HEARTBEAT_PATH}",
        timeout=WATCHDOG_HEARTBEAT_READ_TIMEOUT_SECONDS,
    ) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("local runtime-heartbeat payload is not an object")
    return payload


def install_runtime_watchdog_readiness_repair() -> None:
    """Route only worker diagnostics to the bounded heartbeat-only endpoint.

    `/health` remains database-independent for Render. `/ready` remains the broader
    readiness contract. The combined runtime parent needs only durable worker heartbeat
    truth, so it now uses a dedicated endpoint backed by one batched latest-worker query
    in a disposable subprocess. A slow database read therefore cannot keep the watchdog
    blocked indefinitely, while unrelated local dashboard reads retain their original
    shorter timeout.
    """

    from inefficiency_engine import render_combined_runtime as runtime

    if bool(getattr(runtime, _PATCH_MARKER, False)):
        return

    original = runtime._local_json

    def readiness_aware_local_json(port: str | int, path: str):
        if path == "/health":
            return _bounded_runtime_heartbeat_json(port)
        return original(port, path)

    runtime._local_json = readiness_aware_local_json
    setattr(runtime, _PATCH_MARKER, True)


__all__ = [
    "WATCHDOG_HEARTBEAT_READ_TIMEOUT_SECONDS",
    "_bounded_runtime_heartbeat_json",
    "install_runtime_watchdog_readiness_repair",
    "watchdog_diagnostic_path",
]

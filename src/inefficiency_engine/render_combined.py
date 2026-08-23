from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

# The canonical Render entrypoint owns the API application selection. The large
# supervisor implementation lives in a private module so both the stable command
# and the former compatibility command execute exactly the same runtime.
from inefficiency_engine import render_combined_runtime as _runtime
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store


CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"
_runtime.API_APP = CANONICAL_API_APP

# Preserve the public helper surface used by tests and operational tooling while
# making the production API target impossible to depend on a Render command swap.
from inefficiency_engine.render_combined_runtime import *  # noqa: E402,F401,F403

API_APP = CANONICAL_API_APP
CONTROL_WORKER_ID = "canonical-control-operating-loop"
CONTROL_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.permanent_control_worker",
]

_CONTROL_GUARD_CHECK_SECONDS = 15.0
_CONTROL_GUARD_STARTUP_GRACE_SECONDS = 180.0
_CONTROL_GUARD_STALE_SECONDS = 180.0
_CONTROL_GUARD_DEGRADED_SECONDS = 180.0
_CONTROL_RESTART_GRACE_SECONDS = 15.0


def control_child_command() -> list[str]:
    """Return the dedicated canonical-control process command."""

    return list(CONTROL_COMMAND)


def _terminate_control_child(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    deadline = time.monotonic() + _CONTROL_RESTART_GRACE_SECONDS
    while time.monotonic() < deadline:
        if child.poll() is not None:
            return
        time.sleep(0.2)
    if child.poll() is None:
        child.kill()


def _start_control_child() -> subprocess.Popen[bytes]:
    command = control_child_command()
    print(f"starting isolated control child: {' '.join(command)}", flush=True)
    return subprocess.Popen(command)


def _control_plane_guard(stop_event: threading.Event) -> None:
    """Own and supervise canonical reconciliation/publication as its own process.

    Mechanism-forward work is intentionally not part of this lifecycle. A stuck
    mechanism cycle therefore cannot prevent operating reconciliation, qualified
    bridge publication, or dashboard projection. If the control child exits, stops
    publishing, or remains degraded, only this failure domain is restarted.
    """

    child = _start_control_child()
    child_started = time.monotonic()
    degraded_since: float | None = None

    try:
        settings = Settings.from_env()
        store = build_evidence_store(settings.evidence_db_path)
    except Exception as exc:
        print(f"control-plane heartbeat guard unavailable: {type(exc).__name__}", flush=True)
        store = None

    try:
        while not stop_event.wait(_CONTROL_GUARD_CHECK_SECONDS):
            now_mono = time.monotonic()

            return_code = child.poll()
            if return_code is not None:
                print(
                    f"isolated control child exited code={return_code}; restarting control only",
                    flush=True,
                )
                child = _start_control_child()
                child_started = now_mono
                degraded_since = None
                continue

            if store is None or now_mono - child_started < _CONTROL_GUARD_STARTUP_GRACE_SECONDS:
                continue

            try:
                heartbeat = store.latest_worker_heartbeat(CONTROL_WORKER_ID)
            except Exception:
                continue

            reason: str | None = None
            if heartbeat is None:
                reason = "canonical control heartbeat remains unobserved after startup grace"
            else:
                observed_at = heartbeat.observed_at
                if observed_at.tzinfo is None:
                    observed_at = observed_at.replace(tzinfo=timezone.utc)
                age_seconds = max(
                    0.0,
                    (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
                )
                state = str(heartbeat.state or "unknown")
                detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
                control_errors = detail.get("control_plane_errors")
                has_control_errors = isinstance(control_errors, dict) and bool(control_errors)

                if state in {"error", "stopped", "completed"}:
                    reason = f"canonical control heartbeat state={state}"
                elif age_seconds > _CONTROL_GUARD_STALE_SECONDS:
                    reason = (
                        f"canonical control heartbeat is {age_seconds:.1f}s old "
                        f"(limit {_CONTROL_GUARD_STALE_SECONDS:.1f}s)"
                    )
                elif state == "degraded" and has_control_errors:
                    if degraded_since is None:
                        degraded_since = now_mono
                    elif now_mono - degraded_since >= _CONTROL_GUARD_DEGRADED_SECONDS:
                        reason = (
                            "canonical control reconciliation/publication remained "
                            f"degraded for {now_mono - degraded_since:.1f}s"
                        )
                else:
                    degraded_since = None

            if reason is None:
                continue

            print(f"control-plane guard restarting control child: {reason}", flush=True)
            _terminate_control_child(child)
            child = _start_control_child()
            child_started = time.monotonic()
            degraded_since = None
    finally:
        _terminate_control_child(child)


_ORIGINAL_MAIN = _runtime.main


def main() -> int:
    stop_event = threading.Event()
    guard = threading.Thread(
        target=_control_plane_guard,
        args=(stop_event,),
        name="canonical-control-plane-guard",
        daemon=True,
    )
    guard.start()
    try:
        return _ORIGINAL_MAIN()
    finally:
        stop_event.set()
        guard.join(timeout=_CONTROL_RESTART_GRACE_SECONDS + 2.0)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import os
import signal
import threading
import time
from datetime import datetime, timezone

# The canonical Render entrypoint owns the API application selection. The large
# supervisor implementation lives in a private module so both the stable command
# and the former compatibility command execute exactly the same runtime.
from inefficiency_engine import render_combined_runtime as _runtime
from inefficiency_engine.config import Settings
from inefficiency_engine.critical_evidence_recovery import MECHANISM_FORWARD_WORKER_ID
from inefficiency_engine.evidence import build_evidence_store


CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"
_runtime.API_APP = CANONICAL_API_APP

# Preserve the public helper surface used by tests and operational tooling while
# making the production API target impossible to depend on a Render command swap.
from inefficiency_engine.render_combined_runtime import *  # noqa: E402,F401,F403

API_APP = CANONICAL_API_APP

_CONTROL_GUARD_CHECK_SECONDS = 15.0
_CONTROL_GUARD_STARTUP_GRACE_SECONDS = 180.0
_CONTROL_GUARD_STALE_SECONDS = 180.0
_CONTROL_GUARD_DEGRADED_SECONDS = 180.0


def _mechanism_child_pids() -> list[int]:
    """Locate only the permanent mechanism subprocess owned by this supervisor."""

    result: list[int] = []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return result
    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == os.getpid():
            continue
        try:
            raw = open(f"/proc/{pid}/cmdline", "rb").read()  # noqa: PTH123,S310
        except OSError:
            continue
        command = raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore")
        if "inefficiency_engine.permanent_mechanism_worker" in command:
            result.append(pid)
    return result


def _restart_mechanism_child(reason: str) -> None:
    """Terminate the isolated mechanism child; the canonical supervisor restarts it."""

    pids = _mechanism_child_pids()
    if not pids:
        # If process discovery itself cannot find the child, fail closed by asking the
        # Render service supervisor to recycle the whole process instead of silently
        # leaving a dead control plane behind.
        print(f"control-plane guard recycling service: {reason}", flush=True)
        os.kill(os.getpid(), signal.SIGTERM)
        return
    for pid in pids:
        print(f"control-plane guard restarting mechanism pid={pid}: {reason}", flush=True)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue


def _control_plane_guard(stop_event: threading.Event) -> None:
    """Supervise durable mechanism/control-plane progress outside its event loop.

    The main supervisor already restarts a mechanism subprocess when it exits. This
    independent thread closes the remaining gap: a process that is alive but hung, or
    one that repeatedly reports canonical reconciliation/publication failure, is
    terminated so the existing supervisor can restart only that failure domain.
    """

    try:
        settings = Settings.from_env()
        store = build_evidence_store(settings.evidence_db_path)
    except Exception as exc:
        print(f"control-plane guard unavailable: {type(exc).__name__}", flush=True)
        return
    if store is None:
        print("control-plane guard unavailable: evidence persistence not configured", flush=True)
        return

    started = time.monotonic()
    degraded_since: float | None = None
    while not stop_event.wait(_CONTROL_GUARD_CHECK_SECONDS):
        now_mono = time.monotonic()
        if now_mono - started < _CONTROL_GUARD_STARTUP_GRACE_SECONDS:
            continue
        try:
            heartbeat = store.latest_worker_heartbeat(MECHANISM_FORWARD_WORKER_ID)
        except Exception:
            continue

        reason: str | None = None
        detail: dict[str, object] = {}
        if heartbeat is None:
            reason = "mechanism/control heartbeat remains unobserved after startup grace"
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
                reason = f"mechanism/control heartbeat state={state}"
            elif age_seconds > _CONTROL_GUARD_STALE_SECONDS:
                reason = (
                    f"mechanism/control heartbeat is {age_seconds:.1f}s old "
                    f"(limit {_CONTROL_GUARD_STALE_SECONDS:.1f}s)"
                )
            elif state == "degraded" and has_control_errors:
                if degraded_since is None:
                    degraded_since = now_mono
                elif now_mono - degraded_since >= _CONTROL_GUARD_DEGRADED_SECONDS:
                    reason = (
                        "canonical control-plane reconciliation/publication remained "
                        f"degraded for {now_mono - degraded_since:.1f}s"
                    )
            else:
                degraded_since = None

        if reason is None:
            continue
        _restart_mechanism_child(reason)
        degraded_since = None
        # Give the canonical supervisor time to observe the exit and start a fresh
        # mechanism process before evaluating the previous durable heartbeat again.
        stop_event.wait(60.0)


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
        guard.join(timeout=2.0)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations


_PATCH_MARKER = "_cie_runtime_watchdog_readiness_repair_installed"


def watchdog_diagnostic_path(path: str) -> str:
    """Keep Render liveness process-only while internal watchdogs read durable truth."""

    return "/ready" if path == "/health" else path


def install_runtime_watchdog_readiness_repair() -> None:
    """Redirect only the combined runtime's internal `/health` reads to `/ready`.

    Production `/health` is intentionally database-independent so Render liveness can
    never be coupled to PostgreSQL. The long-lived parent watchdog historically reused
    that path to read durable worker heartbeats, but those diagnostics now live on
    `/ready`. Patch only the parent's local diagnostic helper; the public ASGI routes
    and their liveness/readiness semantics are unchanged.
    """

    from inefficiency_engine import render_combined_runtime as runtime

    if bool(getattr(runtime, _PATCH_MARKER, False)):
        return

    original = runtime._local_json

    def readiness_aware_local_json(port: str | int, path: str):
        return original(port, watchdog_diagnostic_path(path))

    runtime._local_json = readiness_aware_local_json
    setattr(runtime, _PATCH_MARKER, True)


__all__ = [
    "install_runtime_watchdog_readiness_repair",
    "watchdog_diagnostic_path",
]

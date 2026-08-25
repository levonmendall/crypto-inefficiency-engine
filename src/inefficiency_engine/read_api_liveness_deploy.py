from __future__ import annotations

import json
import os
from typing import Any

from inefficiency_engine import __version__
from inefficiency_engine.read_api_historical_observatory_ui_deploy import app as _inner_app


class DatabaseIndependentLivenessApp:
    """Answer Render liveness without touching PostgreSQL or worker diagnostics.

    The composed read API deliberately keeps `/ready` and diagnostic endpoints on the
    full application, where durable database/runtime truth remains fail-closed. Render's
    `/health` probe has a different contract: prove only that the web process is alive
    and able to serve HTTP. Intercepting that one route at the outer ASGI boundary makes
    it impossible for future diagnostic composition to put synchronous database reads
    back onto the liveness critical path.
    """

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        method = str(scope.get("method") or "").upper()
        if (
            scope.get("type") == "http"
            and scope.get("path") == "/health"
            and method in {"GET", "HEAD"}
        ):
            encoded = json.dumps(
                liveness_payload(),
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            headers = [
                (b"content-type", b"application/json"),
                (b"cache-control", b"no-store"),
                (b"content-length", str(len(encoded)).encode("ascii")),
            ]
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": headers,
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b"" if method == "HEAD" else encoded,
                    "more_body": False,
                }
            )
            return

        await self.inner(scope, receive, send)


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def liveness_payload() -> dict[str, object]:
    """Return process-only liveness metadata with zero durable-store reads."""

    return {
        "status": "ok",
        "version": __version__,
        "paper_only": True,
        "read_plane": True,
        "live_execution": False,
        "release_commit": _release_commit(),
        "database_check": "deferred_to_readiness",
        "runtime_diagnostics": "deferred_to_readiness",
        "readiness_endpoint": "/ready",
        "liveness_database_independent": True,
    }


app = DatabaseIndependentLivenessApp(_inner_app)


__all__ = ["DatabaseIndependentLivenessApp", "app", "liveness_payload"]

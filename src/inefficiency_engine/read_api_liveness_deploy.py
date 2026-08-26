from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from inefficiency_engine import __version__
from inefficiency_engine.read_api_durable_history_projection_deploy import app as _inner_app


async def _send_json(
    send: Any,
    *,
    status: int,
    value: object,
    head_only: bool,
    jsonable: bool = False,
) -> None:
    if jsonable:
        from fastapi.encoders import jsonable_encoder

        value = jsonable_encoder(value)
    encoded = json.dumps(
        value,
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
            "status": int(status),
            "headers": headers,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": b"" if head_only else encoded,
            "more_body": False,
        }
    )


class DatabaseIndependentLivenessApp:
    """Answer Render liveness without touching PostgreSQL or worker diagnostics.

    `/health` remains a process-only branch with no database imports or reads. The
    explicit E2E diagnostic is also intercepted at this outer boundary, but only after
    path selection and in a worker thread; that lets us correct cycle-history telemetry
    without changing the canonical production ASGI target or liveness contract.
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
            await _send_json(
                send,
                status=200,
                value=liveness_payload(),
                head_only=method == "HEAD",
            )
            return

        if (
            scope.get("type") == "http"
            and scope.get("path") == "/v3/operations/end-to-end-certification"
            and method in {"GET", "HEAD"}
        ):
            from fastapi import HTTPException
            from inefficiency_engine.read_api_cycle_history_truth_repair import (
                repaired_end_to_end_certification_payload,
            )

            status = 200
            try:
                body_value: object = await asyncio.to_thread(
                    repaired_end_to_end_certification_payload
                )
            except HTTPException as exc:
                status = int(exc.status_code)
                body_value = {"detail": exc.detail}
            except Exception as exc:
                status = 503
                body_value = {
                    "detail": {
                        "message": "end-to-end certification truth repair unavailable",
                        "error_type": type(exc).__name__,
                    }
                }
            await _send_json(
                send,
                status=status,
                value=body_value,
                head_only=method == "HEAD",
                jsonable=True,
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
        "end_to_end_certification_endpoint": "/v3/operations/end-to-end-certification",
        "durable_history_endpoint": "/v3/dashboard/durable-lane-history",
        "durable_history_read_model": "persisted_background_projection",
        "liveness_database_independent": True,
    }


app = DatabaseIndependentLivenessApp(_inner_app)


__all__ = ["DatabaseIndependentLivenessApp", "app", "liveness_payload"]

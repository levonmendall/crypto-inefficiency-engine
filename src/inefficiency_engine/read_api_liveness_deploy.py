from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from typing import Any

from inefficiency_engine import __version__
from inefficiency_engine.read_api_durable_history_projection_deploy import app as _inner_app


END_TO_END_CERTIFICATION_DEADLINE_SECONDS = 8.0
RUNTIME_HEARTBEAT_SNAPSHOT_DEADLINE_SECONDS = 8.0
INTERNAL_RUNTIME_HEARTBEAT_PATH = "/v3/internal/runtime-heartbeats"
LOCAL_PERSISTENCE_MIGRATION_PATH = "/v3/internal/local-persistence-migration"
RUNTIME_HEARTBEAT_SNAPSHOT_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.runtime_heartbeat_snapshot_child",
]


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


def _runtime_heartbeat_snapshot_subprocess() -> dict[str, object]:
    completed = subprocess.run(
        RUNTIME_HEARTBEAT_SNAPSHOT_COMMAND,
        capture_output=True,
        text=True,
        check=False,
        timeout=RUNTIME_HEARTBEAT_SNAPSHOT_DEADLINE_SECONDS,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "runtime heartbeat snapshot child exited "
            f"code={completed.returncode}: {completed.stderr[-500:]}"
        )
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise ValueError("runtime heartbeat snapshot payload is not an object")
    return payload


class DatabaseIndependentLivenessApp:
    """Answer Render liveness without touching PostgreSQL or worker diagnostics.

    `/health` remains a process-only branch with no database imports or reads. Explicit
    diagnostics are intercepted only after path selection and run in disposable bounded
    subprocesses/threads. A slow durable read therefore cannot strand the web event loop
    or the long-lived runtime parent, without changing the canonical production ASGI
    target or liveness contract.
    """

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        method = str(scope.get("method") or "").upper()
        path = scope.get("path")
        if (
            scope.get("type") == "http"
            and path == "/health"
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
            and path == INTERNAL_RUNTIME_HEARTBEAT_PATH
            and method in {"GET", "HEAD"}
        ):
            status = 200
            try:
                body_value: object = await asyncio.to_thread(
                    _runtime_heartbeat_snapshot_subprocess
                )
            except (subprocess.TimeoutExpired, asyncio.TimeoutError):
                status = 503
                body_value = {
                    "detail": {
                        "message": "runtime heartbeat snapshot exceeded its bounded deadline",
                        "error_type": "RuntimeHeartbeatSnapshotDeadlineExceeded",
                        "deadline_seconds": RUNTIME_HEARTBEAT_SNAPSHOT_DEADLINE_SECONDS,
                        "retryable": True,
                        "diagnostic_only": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    }
                }
            except Exception as exc:
                status = 503
                body_value = {
                    "detail": {
                        "message": "runtime heartbeat snapshot is temporarily unavailable",
                        "error_type": type(exc).__name__,
                        "retryable": True,
                        "diagnostic_only": True,
                    }
                }
            await _send_json(
                send,
                status=status,
                value=body_value,
                head_only=method == "HEAD",
            )
            return

        if (
            scope.get("type") == "http"
            and path == LOCAL_PERSISTENCE_MIGRATION_PATH
            and method in {"GET", "HEAD"}
        ):
            from inefficiency_engine.local_persistence_migration_status import (
                migration_status_payload,
            )

            status = 200
            try:
                body_value = await asyncio.to_thread(migration_status_payload)
            except Exception as exc:
                status = 503
                body_value = {
                    "detail": {
                        "message": "local persistence migration status is temporarily unavailable",
                        "error_type": type(exc).__name__,
                        "diagnostic_only": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    }
                }
            await _send_json(
                send,
                status=status,
                value=body_value,
                head_only=method == "HEAD",
            )
            return

        if (
            scope.get("type") == "http"
            and path == "/v3/operations/end-to-end-certification"
            and method in {"GET", "HEAD"}
        ):
            from fastapi import HTTPException
            from inefficiency_engine.read_api_cycle_history_truth_repair import (
                repaired_end_to_end_certification_payload,
            )

            status = 200
            try:
                body_value = await asyncio.wait_for(
                    asyncio.to_thread(repaired_end_to_end_certification_payload),
                    timeout=END_TO_END_CERTIFICATION_DEADLINE_SECONDS,
                )
            except asyncio.TimeoutError:
                status = 503
                body_value = {
                    "detail": {
                        "message": "end-to-end certification read exceeded its bounded deadline",
                        "error_type": "EndToEndCertificationDeadlineExceeded",
                        "deadline_seconds": END_TO_END_CERTIFICATION_DEADLINE_SECONDS,
                        "retryable": True,
                        "certification_authority": False,
                        "paper_only": True,
                    }
                }
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
        # Preserve the public contract: runtime diagnostics are not part of /health.
        # The internal watchdog now has a separate bounded heartbeat-only endpoint,
        # advertised below, without redefining the external liveness semantics.
        "runtime_diagnostics": "deferred_to_readiness",
        "readiness_endpoint": "/ready",
        "internal_runtime_heartbeat_endpoint": INTERNAL_RUNTIME_HEARTBEAT_PATH,
        "internal_runtime_heartbeat_deadline_seconds": (
            RUNTIME_HEARTBEAT_SNAPSHOT_DEADLINE_SECONDS
        ),
        "local_persistence_migration_endpoint": LOCAL_PERSISTENCE_MIGRATION_PATH,
        "end_to_end_certification_endpoint": "/v3/operations/end-to-end-certification",
        "end_to_end_certification_deadline_seconds": END_TO_END_CERTIFICATION_DEADLINE_SECONDS,
        "durable_history_endpoint": "/v3/dashboard/durable-lane-history",
        "durable_history_read_model": "persisted_background_projection",
        "liveness_database_independent": True,
    }


app = DatabaseIndependentLivenessApp(_inner_app)


__all__ = [
    "DatabaseIndependentLivenessApp",
    "END_TO_END_CERTIFICATION_DEADLINE_SECONDS",
    "INTERNAL_RUNTIME_HEARTBEAT_PATH",
    "LOCAL_PERSISTENCE_MIGRATION_PATH",
    "RUNTIME_HEARTBEAT_SNAPSHOT_COMMAND",
    "RUNTIME_HEARTBEAT_SNAPSHOT_DEADLINE_SECONDS",
    "app",
    "liveness_payload",
]

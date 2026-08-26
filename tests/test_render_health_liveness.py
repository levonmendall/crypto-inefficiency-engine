from __future__ import annotations

import asyncio
import json

from inefficiency_engine import read_api_liveness_deploy as liveness
from inefficiency_engine import render_combined_postbind_lane_repair as render_repair


class _InnerApp:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def __call__(self, scope, receive, send) -> None:
        self.calls.append(scope)
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"",
                "more_body": False,
            }
        )


def _request(app, path: str, *, method: str = "GET"):
    messages: list[dict[str, object]] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 10000),
    }
    asyncio.run(app(scope, receive, send))
    return messages


def test_render_health_bypasses_composed_api_and_database_diagnostics(monkeypatch):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "repair-commit")
    inner = _InnerApp()
    app = liveness.DatabaseIndependentLivenessApp(inner)

    messages = _request(app, "/health")

    assert inner.calls == []
    assert messages[0]["status"] == 200
    payload = json.loads(messages[1]["body"])
    assert payload["status"] == "ok"
    assert payload["release_commit"] == "repair-commit"
    assert payload["database_check"] == "deferred_to_readiness"
    assert payload["runtime_diagnostics"] == "deferred_to_readiness"
    assert payload["readiness_endpoint"] == "/ready"
    assert payload["liveness_database_independent"] is True
    assert payload["end_to_end_certification_deadline_seconds"] == 8.0


def test_ready_and_diagnostics_remain_on_full_composed_application():
    inner = _InnerApp()
    app = liveness.DatabaseIndependentLivenessApp(inner)

    messages = _request(app, "/ready")

    assert len(inner.calls) == 1
    assert inner.calls[0]["path"] == "/ready"
    assert messages[0]["status"] == 204


def test_head_health_is_also_database_independent():
    inner = _InnerApp()
    app = liveness.DatabaseIndependentLivenessApp(inner)

    messages = _request(app, "/health", method="HEAD")

    assert inner.calls == []
    assert messages[0]["status"] == 200
    assert messages[1]["body"] == b""


def test_e2e_certification_returns_bounded_503_instead_of_hanging(monkeypatch):
    inner = _InnerApp()
    app = liveness.DatabaseIndependentLivenessApp(inner)

    async def slow_to_thread(_func):
        await asyncio.sleep(0.05)
        return {"certified": False}

    monkeypatch.setattr(liveness, "END_TO_END_CERTIFICATION_DEADLINE_SECONDS", 0.001)
    monkeypatch.setattr(liveness.asyncio, "to_thread", slow_to_thread)

    messages = _request(app, "/v3/operations/end-to-end-certification")

    assert inner.calls == []
    assert messages[0]["status"] == 503
    payload = json.loads(messages[1]["body"])
    detail = payload["detail"]
    assert detail["error_type"] == "EndToEndCertificationDeadlineExceeded"
    assert detail["deadline_seconds"] == 0.001
    assert detail["retryable"] is True
    assert detail["certification_authority"] is False
    assert detail["paper_only"] is True


def test_e2e_certification_still_returns_payload_when_read_finishes(monkeypatch):
    inner = _InnerApp()
    app = liveness.DatabaseIndependentLivenessApp(inner)

    async def fast_to_thread(_func):
        return {"certified": False, "status": "blocked", "paper_only": True}

    monkeypatch.setattr(liveness.asyncio, "to_thread", fast_to_thread)

    messages = _request(app, "/v3/operations/end-to-end-certification")

    assert inner.calls == []
    assert messages[0]["status"] == 200
    payload = json.loads(messages[1]["body"])
    assert payload["status"] == "blocked"
    assert payload["paper_only"] is True


def test_render_child_uses_database_independent_liveness_app():
    assert (
        render_repair.BOUNDED_HEARTBEAT_API_APP
        == "inefficiency_engine.read_api_liveness_deploy:app"
    )

from __future__ import annotations

import sys

from inefficiency_engine.render_combined import API_APP, child_commands


def test_combined_runtime_starts_worker_history_and_api_on_render_port():
    commands = child_commands("12345")

    assert set(commands) == {"worker", "history", "api"}
    assert commands["worker"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.cli",
        "worker",
    ]
    assert commands["history"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.active_volume_runtime",
    ]
    assert commands["api"] == [
        sys.executable,
        "-m",
        "uvicorn",
        API_APP,
        "--host",
        "0.0.0.0",
        "--port",
        "12345",
    ]


def test_combined_runtime_uses_active_volume_deployment_read_plane():
    assert API_APP == "inefficiency_engine.read_api_active_volume_deploy:app"

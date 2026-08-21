from __future__ import annotations

import sys

from inefficiency_engine.render_combined import API_APP, child_commands, heavy_commands


def test_combined_runtime_keeps_only_portfolio_and_api_permanent():
    commands = child_commands("12345")

    assert set(commands) == {"portfolio", "api"}
    assert commands["portfolio"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.lightweight_portfolio_worker",
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


def test_combined_runtime_makes_research_and_history_disposable_and_mutually_scheduled():
    commands = heavy_commands()

    assert set(commands) == {"research", "history"}
    assert commands["research"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.disposable_heavy_job",
        "research",
    ]
    assert commands["history"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.disposable_heavy_job",
        "history",
    ]


def test_combined_runtime_uses_active_volume_deployment_read_plane():
    assert API_APP == "inefficiency_engine.read_api_active_volume_deploy:app"

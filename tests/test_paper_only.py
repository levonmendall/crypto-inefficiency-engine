import pytest

from inefficiency_engine.config import Settings


def test_live_execution_cannot_be_enabled(monkeypatch):
    monkeypatch.setenv("CIE_PAPER_ONLY", "false")
    with pytest.raises(RuntimeError):
        Settings.from_env()

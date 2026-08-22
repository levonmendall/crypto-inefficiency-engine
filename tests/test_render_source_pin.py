from pathlib import Path

from inefficiency_engine.dashboard_cards_v5 import DASHBOARD_UI_CONTRACT_VERSION
from inefficiency_engine.render_combined import CANONICAL_API_APP


def test_render_runtime_imports_current_checkout_source() -> None:
    render = Path("render.yaml").read_text()
    assert "--no-cache-dir ." in render
    assert "startCommand: PYTHONPATH=src python -m inefficiency_engine.render_combined" in render


def test_release_forces_new_local_project_build() -> None:
    project = Path("pyproject.toml").read_text()
    assert 'version = "3.8.3"' in project


def test_canonical_runtime_still_targets_v5_dashboard() -> None:
    assert CANONICAL_API_APP == "inefficiency_engine.read_api_card_history_deploy:app"
    assert DASHBOARD_UI_CONTRACT_VERSION == "v5_mechanism_truth"

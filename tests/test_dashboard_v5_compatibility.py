from pathlib import Path


def test_v5_snapshot_preserves_legacy_dashboard_payload_for_open_tabs():
    source = Path("src/inefficiency_engine/read_api_card_history_deploy.py").read_text()

    assert '@app.get("/v3/dashboard/snapshot")' in source
    assert "legacy = _legacy_snapshot()" in source
    assert "v5 = _v5_from_legacy(legacy)" in source
    assert "result = dict(legacy)" in source
    assert "result.update(v5)" in source
    assert '"dashboard_snapshot_backward_compatible": True' in source
    assert '"legacy_snapshot_fields_preserved": True' in source


def test_restored_command_center_uses_v5_cards_without_erasing_legacy_sections():
    command = Path("src/inefficiency_engine/dashboard_command_center_v6.py").read_text()
    deploy = Path("src/inefficiency_engine/read_api_card_history_deploy.py").read_text()

    assert "fetch('/v3/dashboard/v5-snapshot'" in command
    assert "build_dashboard_v5_snapshot(source)" in deploy
    assert '"command_center": _command_center_payload(source)' in deploy
    assert deploy.index("result = dict(legacy)") < deploy.index("result.update(v5)")

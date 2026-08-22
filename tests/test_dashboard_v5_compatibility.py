from pathlib import Path


def test_v5_snapshot_preserves_legacy_dashboard_payload_for_open_tabs():
    source = Path("src/inefficiency_engine/read_api_card_history_deploy.py").read_text()

    assert '@app.get("/v3/dashboard/snapshot")' in source
    assert "legacy = dict(_base.dashboard_snapshot())" in source
    assert "v5 = build_dashboard_v5_snapshot(legacy)" in source
    assert "result = dict(legacy)" in source
    assert "result.update(v5)" in source
    assert '"dashboard_snapshot_backward_compatible": True' in source
    assert '"legacy_snapshot_fields_preserved": True' in source


def test_v5_dashboard_keeps_single_endpoint_without_erasing_legacy_sections():
    html = Path("src/inefficiency_engine/dashboard_cards_v5.py").read_text()
    deploy = Path("src/inefficiency_engine/read_api_card_history_deploy.py").read_text()

    assert "fetch('/v3/dashboard/snapshot'" in html
    assert "build_dashboard_v5_snapshot(legacy)" in deploy
    # The compatibility merge must happen after the V5 read model is built so
    # portfolio/performance/runtime/mechanisms remain available to older loaded UI.
    assert deploy.index("result = dict(legacy)") < deploy.index("result.update(v5)")

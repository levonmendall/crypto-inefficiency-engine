from __future__ import annotations


def test_old_command_center_displays_historical_observatory_backfill():
    from inefficiency_engine.read_api_historical_observatory_ui_deploy import (
        historical_observatory_dashboard_html,
    )

    html = historical_observatory_dashboard_html()

    # Preserve the restored command-center layout while adding one diagnostic section.
    assert "Portfolio Command Center" in html
    assert "Evidence accumulation" in html
    assert "Profit mechanism certification" in html
    assert "Historical opportunity evidence" in html
    assert "Recovered Aug. 21 → live observatory evidence" in html

    # The UI reads the bounded historical API directly and refreshes it independently.
    assert "/v3/research/candidate-observatory/history?limit=50" in html
    assert "function refreshHistoricalReplay()" in html
    assert "setInterval(()=>{if(document.visibilityState==='visible')refreshHistoricalReplay()},30000)" in html

    # Replay truth is visibly separated from investment authority.
    assert "diagnostic only" in html
    assert "never counted as forward qualification" in html
    assert "Historical replay never changes forward samples, qualification, allocation, or execution." in html
    assert "Legacy rejected-candidate identities were not persisted" in html


def test_historical_observatory_ui_renders_recovered_candidates_and_funnels():
    from inefficiency_engine.read_api_historical_observatory_ui_deploy import (
        historical_observatory_dashboard_html,
    )

    html = historical_observatory_dashboard_html()

    assert "Recovered selected candidates" in html
    assert "Recovered rejection funnels" in html
    assert "selected.slice(0,10).map(renderHistoricalCandidate)" in html
    assert "funnels.slice(0,14).map(renderHistoricalFunnel)" in html
    assert "raw ${raw}" in html
    assert "emitted ${emitted}" in html
    assert "best net ${historicalPct(f.best_net_economics)}" in html
    assert "hurdle ${historicalPct(f.required_net_economics)}" in html


def test_production_liveness_wraps_historical_observatory_ui():
    from inefficiency_engine import read_api_historical_observatory_ui_deploy as historical
    from inefficiency_engine import read_api_liveness_deploy as liveness

    assert liveness.app.inner is historical.app

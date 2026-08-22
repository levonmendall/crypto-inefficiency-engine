from inefficiency_engine.dashboard_research_closure import RESEARCH_CLOSURE_DASHBOARD_HTML


def test_provider_card_uses_normalized_card_truth_status():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "function providerCardValue(r)" in html
    assert "r?.card_truth?.provider_status" in html
    assert "status==='connected'" in html
    assert "status==='stale'" in html
    assert "status==='missing'" in html
    assert "providerCardValue(r)" in html


def test_observation_card_labels_current_authoritative_items_not_legacy_tail_count():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "current authoritative" in html
    assert "`${phase} · ${obs} authoritative`" not in html

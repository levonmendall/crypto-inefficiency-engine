from inefficiency_engine.dashboard_card_currentness import preserve_meaningful_card_conclusions


def _snapshot(*, status="RESEARCH STALE", research_status="stale", state="statistical_failure"):
    return {
        "system": {"projection_current_for_execution": False},
        "summary": {"lane_count": 1},
        "cards": [
            {
                "mechanism_id": "trend_momentum",
                "status": status,
                "provider_status": "connected",
                "evidence_status": "complete",
                "research_status": research_status,
                "persisted_state": state,
                "last_conclusion": state.replace("_", " "),
                "paper_capable": False,
                "qualified_count": 0,
                "primary_blocker": "generic runtime blocker",
                "next_action": "generic runtime action",
                "live_execution_authority": False,
            }
        ],
        "paper_only": True,
        "live_execution_authority": False,
    }


def test_statistical_failure_remains_visible_when_runtime_is_stale():
    result = preserve_meaningful_card_conclusions(_snapshot())
    card = result["cards"][0]

    assert card["status"] == "STATISTICAL FAILURE · STALE"
    assert card["research_conclusion_current"] is False
    assert card["paper_decision_current"] is False
    assert card["paper_capable"] is False
    assert "Last persisted conclusion: statistical failure" in card["primary_blocker"]
    assert "preserve all existing qualification and execution thresholds" in card["next_action"]
    assert result["summary"]["historical_substantive_conclusion_lanes"] == 1
    assert result["live_execution_authority"] is False


def test_poor_economics_remains_visible_when_runtime_is_degraded():
    result = preserve_meaningful_card_conclusions(
        _snapshot(status="RESEARCH DEGRADED", research_status="degraded", state="poor_economics")
    )
    card = result["cards"][0]

    assert card["status"] == "POOR ECONOMICS · RUNTIME DEGRADED"
    assert card["research_conclusion_current"] is False
    assert card["paper_capable"] is False


def test_source_blocker_keeps_precedence_over_historical_conclusion():
    payload = _snapshot(status="SOURCE STALE")
    payload["cards"][0]["provider_status"] = "stale"
    payload["cards"][0]["evidence_status"] = "stale"
    result = preserve_meaningful_card_conclusions(payload)
    card = result["cards"][0]

    assert card["status"] == "SOURCE STALE"
    assert card["research_conclusion_current"] is False
    assert card["runtime_warning"] is not None
    assert result["summary"]["historical_substantive_conclusion_lanes"] == 0


def test_current_research_does_not_rewrite_current_status():
    payload = _snapshot(status="STATISTICAL FAILURE", research_status="current")
    payload["system"]["projection_current_for_execution"] = True
    result = preserve_meaningful_card_conclusions(payload)
    card = result["cards"][0]

    assert card["status"] == "STATISTICAL FAILURE"
    assert card["research_conclusion_current"] is True
    assert card["paper_decision_current"] is True
    assert card["runtime_warning"] is None

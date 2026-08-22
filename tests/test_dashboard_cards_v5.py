from datetime import datetime, timedelta, timezone

from inefficiency_engine.dashboard_cards_v5 import (
    DASHBOARD_UI_CONTRACT_VERSION,
    DASHBOARD_V5_HTML,
    build_dashboard_v5_snapshot,
)


def _payload(now: datetime, *, research_stale: bool = False):
    research_at = now - (timedelta(hours=2) if research_stale else timedelta(seconds=30))
    return {
        "release_commit": "abcdef123456",
        "research_projection_stale": False,
        "operating_projection_stale": False,
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "state": "running",
                    "observed_at": research_at.isoformat(),
                    "age_seconds": (now - research_at).total_seconds(),
                    "stale": research_stale,
                    "error_type": None,
                },
                "portfolio": {
                    "available": True,
                    "state": "running",
                    "observed_at": (now - timedelta(seconds=20)).isoformat(),
                    "age_seconds": 20,
                    "stale": False,
                    "error_type": None,
                },
            }
        },
        "current_source_truth": {
            "carry": {
                "provider_status": "connected",
                "connected": True,
                "source_state": "sufficient",
                "evidence_complete": True,
                "current_authoritative_item_count": 7,
                "latest_authoritative_observation_at": (now - timedelta(minutes=2)).isoformat(),
                "covered_evidence_classes": ["price", "funding", "executable_depth"],
                "missing_evidence_classes": [],
                "independent_authoritative_source_count": 2,
                "admitted_source_ids": ["source-a", "source-b"],
            },
            "yield": {
                "provider_status": "stale",
                "connected": False,
                "source_state": "stale",
                "evidence_complete": False,
                "current_authoritative_item_count": 0,
                "latest_seen_source_observation_at": (now - timedelta(days=2)).isoformat(),
                "covered_evidence_classes": [],
                "missing_evidence_classes": ["yield_quote"],
                "stale_source_ids": ["source-y"],
            },
        },
        "lane_executability": {
            "available": True,
            "projection_current_for_execution": True,
            "paper_execution_capable_lanes": ["carry"],
        },
        "mechanisms": {
            "observed_at": (now - timedelta(minutes=1)).isoformat(),
            "requirements": {
                "independent_forward_outcomes": 30,
                "settled_allocator_outcomes": 20,
            },
            "mechanisms": [
                {
                    "mechanism_id": "carry",
                    "name": "Carry / basis / funding",
                    "state": "collecting",
                    "stage": "forward_testable",
                    "authoritative_observation_count": 5_087_329,
                    "legacy_projected_observation_count": 5_087_329,
                    "raw_candidate_count": 41,
                    "emitted_candidate_count": 3,
                    "forward_signal_count": 11,
                    "independent_forward_outcome_count": 8,
                    "current_statistically_qualified_count": 0,
                    "settled_allocator_outcome_count": 0,
                    "mean_forward_net_return": 0.001,
                    "mean_forward_net_return_ci_lower": -0.0002,
                    "forward_hit_rate": 0.55,
                    "forward_hit_rate_ci_lower": 0.41,
                    "forward_evidence_last_outcome_at": (now - timedelta(minutes=10)).isoformat(),
                    "forward_evidence_next_expected_at": (now + timedelta(minutes=5)).isoformat(),
                    "primary_reason": "collecting independent forward outcomes",
                    "next_action": "continue collection",
                },
                {
                    "mechanism_id": "yield",
                    "name": "Yield / staking / lending",
                    "state": "collecting",
                    "stage": "waiting_for_source:stale",
                    "authoritative_observation_count": 999_999,
                    "forward_signal_count": 0,
                    "independent_forward_outcome_count": 0,
                    "current_statistically_qualified_count": 0,
                    "settled_allocator_outcome_count": 0,
                    "forward_evidence_next_expected_at": (now - timedelta(hours=1)).isoformat(),
                },
            ],
        },
    }


def test_v5_uses_current_source_truth_and_never_legacy_high_water_counts():
    now = datetime(2026, 8, 22, 5, 30, tzinfo=timezone.utc)
    result = build_dashboard_v5_snapshot(_payload(now), now=now)
    carry = next(card for card in result["cards"] if card["mechanism_id"] == "carry")

    assert result["dashboard_ui_contract_version"] == "v5_mechanism_truth"
    assert carry["provider_status"] == "connected"
    assert carry["evidence_status"] == "complete"
    assert carry["source_item_count"] == 7
    assert carry["source_item_count"] != 5_087_329
    assert carry["raw_candidate_count"] == 41
    assert carry["emitted_candidate_count"] == 3
    assert carry["signal_count"] == 11
    assert carry["forward_outcome_count"] == 8
    assert carry["paper_capable"] is True


def test_v5_distinguishes_stale_source_from_missing_provider_and_marks_overdue_research():
    now = datetime(2026, 8, 22, 5, 30, tzinfo=timezone.utc)
    result = build_dashboard_v5_snapshot(_payload(now), now=now)
    card = next(card for card in result["cards"] if card["mechanism_id"] == "yield")

    assert card["provider_status"] == "stale"
    assert card["evidence_status"] == "stale"
    assert card["status"] == "SOURCE STALE"
    assert card["source_item_count"] == 0
    assert card["research_due_state"] == "overdue"
    assert "latest usable source evidence is stale" in card["primary_blocker"]


def test_stale_research_runtime_forces_every_card_paper_capability_fail_closed():
    now = datetime(2026, 8, 22, 5, 30, tzinfo=timezone.utc)
    result = build_dashboard_v5_snapshot(_payload(now, research_stale=True), now=now)
    carry = next(card for card in result["cards"] if card["mechanism_id"] == "carry")

    assert result["system"]["research_runtime"]["status"] == "stale"
    assert result["system"]["projection_current_for_execution"] is False
    assert carry["paper_capable"] is False
    assert carry["status"] == "RESEARCH STALE"


def test_v5_html_is_standalone_and_does_not_contain_legacy_card_contract():
    assert DASHBOARD_UI_CONTRACT_VERSION == "v5_mechanism_truth"
    assert "Current source" in DASHBOARD_V5_HTML
    assert "Raw / emitted" in DASHBOARD_V5_HTML
    assert "Research overdue since" in DASHBOARD_V5_HTML
    assert "function renderCard(c)" in DASHBOARD_V5_HTML
    assert "_replace_once" not in DASHBOARD_V5_HTML
    assert "Observations" not in DASHBOARD_V5_HTML
    assert "Last evidence" not in DASHBOARD_V5_HTML

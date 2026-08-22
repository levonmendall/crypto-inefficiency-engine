from inefficiency_engine.dashboard_card_history import (
    CARD_HISTORY_DASHBOARD_HTML,
    DASHBOARD_UI_CONTRACT_VERSION,
    restore_card_history_truth,
)


def _payload(row, *, research_state="success", research_stale=False):
    return {
        "mechanisms": {"mechanisms": [row]},
        "research_projection_stale": False,
        "operating_projection_stale": False,
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "state": research_state,
                    "stale": research_stale,
                }
            }
        },
        "lane_executability": {
            "available": True,
            "lane_count": 13,
            "projection_current_for_execution": True,
            "paper_execution_capable_count": 1,
            "paper_execution_capable_lanes": ["carry"],
            "all_lanes_paper_execution_capable": False,
        },
    }


def test_legacy_table_high_water_never_regains_card_display_authority():
    row = {
        "mechanism_id": "carry",
        "authoritative_observation_count": 26,
        "current_authoritative_item_count": 26,
        "legacy_projected_observation_count": 5_087_329,
        "card_truth": {
            "provider_status": "connected",
            "current_authoritative_item_count": 26,
        },
        "current_source_truth": {
            "current_authoritative_item_count": 26,
            "latest_authoritative_observation_at": "2026-08-22T00:35:00+00:00",
            "latest_seen_source_observation_at": "2026-08-22T00:35:00+00:00",
        },
        "independent_forward_outcome_count": 0,
        "current_statistically_qualified_count": 0,
        "current_promoted_count": 0,
        "settled_allocator_outcome_count": 0,
        "profitability_certified": False,
    }

    result = restore_card_history_truth(_payload(row))
    repaired = result["mechanisms"]["mechanisms"][0]

    assert repaired["authoritative_observation_count"] == 26
    assert repaired["authoritative_observation_count_semantics"] == "current_admitted_source_items"
    assert repaired["current_authoritative_item_count"] == 26
    assert repaired["current_source_observation_at"] == "2026-08-22T00:35:00+00:00"
    assert repaired["historical_input_record_count"] is None
    assert repaired["legacy_table_high_water_mark"] == 5_087_329
    assert repaired["legacy_table_high_water_mark_display_authority"] is False
    assert repaired["card_truth"]["legacy_high_water_display_authority"] is False

    # Presentation reconciliation cannot create investment authority.
    assert repaired["independent_forward_outcome_count"] == 0
    assert repaired["current_statistically_qualified_count"] == 0
    assert repaired["current_promoted_count"] == 0
    assert repaired["settled_allocator_outcome_count"] == 0
    assert repaired["profitability_certified"] is False


def test_current_source_snapshot_remains_visible_without_legacy_history():
    row = {
        "mechanism_id": "fundamental_onchain",
        "authoritative_observation_count": 1,
        "current_authoritative_item_count": 1,
        "card_truth": {"provider_status": "connected"},
        "current_source_truth": {
            "current_authoritative_item_count": 1,
            "latest_authoritative_observation_at": "2026-08-22T00:36:00+00:00",
        },
    }

    repaired = restore_card_history_truth(_payload(row))["mechanisms"]["mechanisms"][0]

    assert repaired["authoritative_observation_count"] == 1
    assert repaired["current_authoritative_item_count"] == 1
    assert repaired["historical_input_record_count_available"] is False


def test_stale_research_runtime_forces_paper_capability_fail_closed_even_if_projection_was_republished():
    row = {
        "mechanism_id": "carry",
        "authoritative_observation_count": 3,
        "current_authoritative_item_count": 3,
        "card_truth": {"provider_status": "connected", "research_status": "current"},
    }

    repaired = restore_card_history_truth(
        _payload(row, research_state="degraded", research_stale=False)
    )

    assert repaired["research_runtime_stale"] is True
    assert repaired["lane_executability"]["projection_current_for_execution"] is False
    assert repaired["lane_executability"]["paper_execution_capable_count"] == 0
    assert repaired["lane_executability"]["paper_execution_capable_lanes"] == []
    card = repaired["mechanisms"]["mechanisms"][0]
    assert card["card_truth"]["research_status"] == "stale"


def test_dashboard_uses_current_source_counts_and_separates_source_from_research_time():
    html = CARD_HISTORY_DASHBOARD_HTML

    assert DASHBOARD_UI_CONTRACT_VERSION == "v4_truthful_source_runtime"
    assert "Current source" in html
    assert "current source items" not in html
    assert "Current source data" in html
    assert "Provider connected" in html
    assert "Current source evidence" in html
    assert "Last source evidence" in html
    assert "Research evidence" in html
    assert "Research overdue since" in html
    assert "Next research expected" in html
    assert "Last evidence ${when(last)}" not in html
    assert "Next expected ${when(next)}" not in html
    assert "Projection refreshed" in html
    assert "source and research timestamps shown per card" in html
    assert "5_087_329" not in html

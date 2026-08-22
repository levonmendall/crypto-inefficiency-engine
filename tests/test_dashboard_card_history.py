from inefficiency_engine.dashboard_card_history import (
    CARD_HISTORY_DASHBOARD_HTML,
    restore_card_history_truth,
)


def _payload(row):
    return {
        "mechanisms": {"mechanisms": [row]},
        "research_projection_stale": True,
        "operating_projection_stale": True,
    }


def test_restores_cumulative_input_history_without_overwriting_current_source_items():
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

    assert repaired["authoritative_observation_count"] == 5_087_329
    assert repaired["authoritative_observation_count_semantics"] == "persisted_cumulative_source_records"
    assert repaired["current_authoritative_item_count"] == 26
    assert repaired["current_source_observation_at"] == "2026-08-22T00:35:00+00:00"
    assert repaired["card_truth"]["display_input_record_count"] == 5_087_329
    assert repaired["card_truth"]["current_authoritative_item_count"] == 26

    # Presentation reconciliation cannot create investment authority.
    assert repaired["independent_forward_outcome_count"] == 0
    assert repaired["current_statistically_qualified_count"] == 0
    assert repaired["current_promoted_count"] == 0
    assert repaired["settled_allocator_outcome_count"] == 0
    assert repaired["profitability_certified"] is False


def test_current_snapshot_remains_visible_when_no_prior_cumulative_history_exists():
    row = {
        "mechanism_id": "fundamental_onchain",
        "authoritative_observation_count": 902,
        "current_authoritative_item_count": 902,
        "legacy_projected_observation_count": 0,
        "card_truth": {"provider_status": "connected"},
        "current_source_truth": {
            "current_authoritative_item_count": 902,
            "latest_authoritative_observation_at": "2026-08-22T00:36:00+00:00",
        },
    }

    repaired = restore_card_history_truth(_payload(row))["mechanisms"]["mechanisms"][0]

    assert repaired["authoritative_observation_count"] == 902
    assert repaired["current_authoritative_item_count"] == 902


def test_dashboard_separates_current_source_time_from_research_time_and_overdue_deadline():
    html = CARD_HISTORY_DASHBOARD_HTML

    assert "current source items" in html
    assert "Input records" in html
    assert "Current source evidence" in html
    assert "Last source evidence" in html
    assert "Research evidence" in html
    assert "Research overdue since" in html
    assert "Next research expected" in html
    assert "Last evidence ${when(last)}" not in html
    assert "Next expected ${when(next)}" not in html
    assert "Research projection" in html
    assert "source timestamps shown per card" in html

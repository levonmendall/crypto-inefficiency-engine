from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine import dashboard_source_truth_stable as stable


def test_lane_truth_uses_admitted_stable_connectivity_even_when_refresh_is_degraded(monkeypatch):
    now = datetime(2026, 8, 25, 16, 15, tzinfo=timezone.utc)
    lane_id = "microstructure"
    required = [str(value) for value in list(stable.LANES[lane_id].get("required") or [])]

    rows = [
        {
            "source_id": "stable-a",
            "lane_ids": [lane_id],
            "classes": required,
            "group": "group-a",
            "state": "healthy",
            "admitted": True,
            "item_count": 123,
            "observed_at": now.isoformat(),
            "latest_attempt_observed_at": now.isoformat(),
            "refresh_degraded": True,
            "latest_attempt_error_type": "TimeoutError",
        },
        {
            "source_id": "stable-b",
            "lane_ids": [lane_id],
            "classes": required,
            "group": "group-b",
            "state": "healthy",
            "admitted": True,
            "item_count": 456,
            "observed_at": now.isoformat(),
            "latest_attempt_observed_at": now.isoformat(),
            "refresh_degraded": False,
        },
    ]

    monkeypatch.setattr(
        stable,
        "read_source_connectivity",
        lambda _store, now=None: {
            "available": True,
            "diagnostic_read_degraded": False,
            "served_last_successful_snapshot": False,
            "sources": rows,
        },
    )

    truth = stable.read_current_source_truth(object(), now=now)[lane_id]

    assert truth["connected"] is True
    assert truth["provider_status"] == "connected"
    assert truth["source_state"] == "sufficient"
    assert truth["independent_authoritative_source_count"] == 2
    assert truth["current_authoritative_source_count"] == 2
    assert truth["admitted_source_ids"] == ["stable-a", "stable-b"]
    assert truth["refresh_degraded_source_ids"] == ["stable-a"]
    assert truth["source_refresh_degraded"] is True
    assert truth["latest_refresh_error_types"] == {"stable-a": "TimeoutError"}
    assert truth["covered_evidence_class_count"] == len(set(required))
    assert truth["required_evidence_class_count"] == len(set(required))


def test_lane_truth_still_fails_closed_when_no_source_is_admitted(monkeypatch):
    now = datetime(2026, 8, 25, 16, 15, tzinfo=timezone.utc)
    lane_id = "microstructure"
    required = [str(value) for value in list(stable.LANES[lane_id].get("required") or [])]

    monkeypatch.setattr(
        stable,
        "read_source_connectivity",
        lambda _store, now=None: {
            "available": True,
            "diagnostic_read_degraded": False,
            "served_last_successful_snapshot": False,
            "sources": [
                {
                    "source_id": "stale-a",
                    "lane_ids": [lane_id],
                    "classes": required,
                    "group": "group-a",
                    "state": "stale",
                    "admitted": False,
                    "item_count": 0,
                    "observed_at": now.isoformat(),
                    "latest_attempt_observed_at": now.isoformat(),
                    "refresh_degraded": False,
                }
            ],
        },
    )

    truth = stable.read_current_source_truth(object(), now=now)[lane_id]

    assert truth["connected"] is False
    assert truth["provider_status"] == "stale"
    assert truth["source_state"] == "stale"
    assert truth["current_authoritative_source_count"] == 0
    assert truth["current_authoritative_item_count"] == 0


def test_mobile_card_contract_uses_coverage_not_raw_item_count(monkeypatch):
    from inefficiency_engine import read_api_mobile_truth_deploy as mobile

    monkeypatch.setattr(
        mobile,
        "_original_card_builder",
        lambda row, payload, now: {"mechanism_id": "microstructure"},
    )
    payload = {
        "current_source_truth": {
            "microstructure": {
                "admitted_source_ids": ["a", "b"],
                "covered_evidence_classes": ["order_book", "trade_flow"],
                "missing_evidence_classes": [],
                "independent_authoritative_source_count": 2,
                "current_authoritative_source_count": 2,
                "covered_evidence_class_count": 2,
                "required_evidence_class_count": 2,
                "source_refresh_degraded": True,
                "refresh_degraded_source_ids": ["a"],
                "latest_refresh_error_types": {"a": "TimeoutError"},
                "source_truth_model": "stable_connectivity_history_v1",
            }
        }
    }

    result = mobile._stable_card_builder(
        {"mechanism_id": "microstructure"}, payload, datetime.now(timezone.utc)
    )

    assert result["current_source_count"] == 2
    assert result["covered_evidence_class_count"] == 2
    assert result["required_evidence_class_count"] == 2
    assert result["source_refresh_degraded"] is True
    assert result["refresh_degraded_source_ids"] == ["a"]
    assert result["source_item_count_display_authority"] is False


def test_mobile_mechanism_cards_are_keyed_and_do_not_render_raw_source_item_count():
    from inefficiency_engine import read_api_mobile_truth_deploy as mobile

    script = mobile._STABLE_MECHANISM_CARDS_JS
    html = mobile.repaired_dashboard_html()

    assert "mechanismCardNodes=new Map()" in script
    assert "mechanismCardSignatures=new Map()" in script
    assert "data.mechanismId" not in script
    assert "node.dataset.mechanismId=id" in script
    assert "Source coverage" in script
    assert "source_item_count" not in script
    assert "prior evidence remains valid" in script
    assert "renderStableMechanismCards(p.cards||[]);" in html
    assert "$('cards').innerHTML=(p.cards||[]).length?p.cards.map(renderCard).join('')" not in html

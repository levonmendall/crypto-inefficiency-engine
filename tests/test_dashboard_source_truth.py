from datetime import datetime, timedelta, timezone

from inefficiency_engine.dashboard_source_truth import overlay_dashboard_source_truth
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoverageLedger, SourceCoverageObservation


NOW = datetime(2026, 8, 21, 23, 30, tzinfo=timezone.utc)


def _row(mechanism_id: str, *, state: str = "provider_gap", stage: str = "waiting_for_source:provider_gap") -> dict[str, object]:
    return {
        "mechanism_id": mechanism_id,
        "name": mechanism_id,
        "state": state,
        "stage": stage,
        "provider_ready": False,
        "authoritative_observation_count": 0,
        "forward_signal_count": 0,
        "independent_forward_outcome_count": 0,
        "current_statistically_qualified_count": 0,
        "current_promoted_count": 0,
        "settled_allocator_outcome_count": 0,
        "profitability_certified": False,
        "research_projection_stale": True,
        "operating_projection_stale": True,
        "paper_only": True,
        "live_execution_authority": False,
    }


def _payload(*rows: dict[str, object]) -> dict[str, object]:
    return {
        "research_projection_stale": True,
        "operating_projection_stale": True,
        "mechanisms": {
            "mechanisms": list(rows) if rows else [_row("fundamental_onchain")],
        },
    }


def _record(
    ledger: SourceCoverageLedger,
    *,
    lane_id: str,
    source_id: str,
    evidence_classes: list[str],
    item_count: int = 3,
    observed_at: datetime = NOW,
) -> None:
    ledger.record(
        SourceCoverageObservation(
            source_id=source_id,
            lane_id=lane_id,
            observed_at=observed_at,
            healthy=True,
            item_count=item_count,
            evidence_classes=evidence_classes,
            authoritative=True,
            commercial_use_permitted=True,
            point_in_time=True,
            economic_fields_complete=True,
            forward_testable_evidence=True,
        )
    )


def test_current_onchain_sources_repair_obsolete_provider_gap_without_qualifying_lane(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = SourceCoverageLedger(store)
    _record(
        ledger,
        lane_id="fundamental_onchain",
        source_id="ethereum-publicnode",
        evidence_classes=["chain_activity"],
    )
    _record(
        ledger,
        lane_id="fundamental_onchain",
        source_id="morpho-markets",
        evidence_classes=["protocol_fundamentals"],
    )

    result = overlay_dashboard_source_truth(store, _payload(), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["stage"] == "research_active_waiting_for_complete_forward_evidence"
    assert row["authoritative_observation_count"] == 6
    assert row["authoritative_observation_count_semantics"] == "current_admitted_source_items"
    assert row["current_source_truth"]["source_state"] == "sufficient"
    assert row["current_source_truth"]["independent_authoritative_source_count"] == 2
    assert row["source_state_authority"] == "canonical_current_source_truth"
    assert row["card_truth"]["provider_status"] == "connected"
    assert row["card_truth"]["research_status"] == "stale"

    # Presentation truth must never manufacture qualification or execution authority.
    assert row["independent_forward_outcome_count"] == 0
    assert row["current_statistically_qualified_count"] == 0
    assert row["current_promoted_count"] == 0
    assert row["profitability_certified"] is False
    assert row["live_execution_authority"] is False
    assert result["research_projection_stale"] is True
    assert result["operating_projection_stale"] is True


def test_one_current_source_reports_redundancy_pending_not_provider_gap(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = SourceCoverageLedger(store)
    _record(
        ledger,
        lane_id="fundamental_onchain",
        source_id="ethereum-publicnode",
        evidence_classes=["chain_activity", "protocol_fundamentals"],
    )

    result = overlay_dashboard_source_truth(store, _payload(), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["stage"] == "forward_learning_active_redundancy_pending"
    assert row["current_source_truth"]["source_state"] == "redundancy_gap"
    assert row["card_truth"]["source_status"] == "redundancy_pending"
    assert row["current_promoted_count"] == 0


def test_stale_source_is_distinct_from_missing_provider_and_stays_fail_closed(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = SourceCoverageLedger(store)
    _record(
        ledger,
        lane_id="fundamental_onchain",
        source_id="ethereum-publicnode",
        evidence_classes=["chain_activity", "protocol_fundamentals"],
        observed_at=NOW - timedelta(days=3),
    )

    result = overlay_dashboard_source_truth(store, _payload(), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is False
    assert row["state"] == "collecting"
    assert row["stage"] == "waiting_for_source:stale"
    assert row["authoritative_observation_count"] == 0
    assert row["current_source_truth"]["connected"] is False
    assert row["current_source_truth"]["source_state"] == "stale"
    assert row["card_truth"]["provider_status"] == "stale"
    assert row["current_promoted_count"] == 0


def test_missing_onchain_source_remains_real_provider_gap(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    SourceCoverageLedger(store)

    result = overlay_dashboard_source_truth(store, _payload(), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is False
    assert row["state"] == "provider_gap"
    assert row["stage"] == "waiting_for_source:provider_gap"
    assert row["authoritative_observation_count"] == 0
    assert row["card_truth"]["provider_status"] == "missing"


def test_carry_card_replaces_primary_key_style_million_count_with_current_source_items(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = SourceCoverageLedger(store)
    # Two independent groups cover market, funding, and executable depth.
    _record(
        ledger,
        lane_id="carry",
        source_id="bybit-market",
        evidence_classes=["market_quotes"],
        item_count=8,
    )
    _record(
        ledger,
        lane_id="carry",
        source_id="bybit-funding",
        evidence_classes=["funding_or_basis"],
        item_count=2,
    )
    _record(
        ledger,
        lane_id="carry",
        source_id="bybit-l2",
        evidence_classes=["executable_depth"],
        item_count=4,
    )
    _record(
        ledger,
        lane_id="carry",
        source_id="okx-market",
        evidence_classes=["market_quotes"],
        item_count=7,
    )
    _record(
        ledger,
        lane_id="carry",
        source_id="okx-funding",
        evidence_classes=["funding_or_basis"],
        item_count=2,
    )
    _record(
        ledger,
        lane_id="carry",
        source_id="okx-l2",
        evidence_classes=["executable_depth"],
        item_count=3,
    )

    carry = _row("carry", state="collecting", stage="profitability_certifiable")
    carry["authoritative_observation_count"] = 5_087_329
    carry["primary_reason"] = "required authoritative input evidence is not currently available"
    result = overlay_dashboard_source_truth(store, _payload(carry), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["authoritative_observation_count"] == 26
    assert row["legacy_projected_observation_count"] == 5_087_329
    assert row["legacy_projected_observation_count_display_authority"] is False
    assert row["authoritative_observation_count_semantics"] == "current_admitted_source_items"
    assert row["card_truth"]["provider_status"] == "connected"
    assert "not currently available" not in row["primary_reason"]
    assert row["current_promoted_count"] == 0


def test_liquidity_l2_without_trade_flow_is_evidence_incomplete_not_provider_gap(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = SourceCoverageLedger(store)
    _record(
        ledger,
        lane_id="liquidity_provision",
        source_id="coinbase-l2",
        evidence_classes=["order_book"],
        item_count=11,
    )

    liquidity = _row("liquidity_provision", state="collecting", stage="economics_modelled")
    liquidity["authoritative_observation_count"] = 24_228
    result = overlay_dashboard_source_truth(store, _payload(liquidity), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["stage"] == "waiting_for_source:evidence_class_gap"
    assert row["authoritative_observation_count"] == 11
    assert row["legacy_projected_observation_count"] == 24_228
    assert row["current_source_truth"]["missing_evidence_classes"] == ["trade_flow"]
    assert row["card_truth"]["source_status"] == "incomplete"
    assert row["current_promoted_count"] == 0

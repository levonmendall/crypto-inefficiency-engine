from datetime import datetime, timedelta, timezone

from inefficiency_engine.dashboard_source_truth import overlay_dashboard_source_truth
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoverageLedger, SourceCoverageObservation


NOW = datetime(2026, 8, 21, 23, 30, tzinfo=timezone.utc)


def _payload() -> dict[str, object]:
    return {
        "research_projection_stale": True,
        "operating_projection_stale": True,
        "mechanisms": {
            "mechanisms": [
                {
                    "mechanism_id": "fundamental_onchain",
                    "name": "On-chain / fundamental factor alpha",
                    "state": "provider_gap",
                    "stage": "waiting_for_source:provider_gap",
                    "provider_ready": False,
                    "authoritative_observation_count": 0,
                    "independent_forward_outcome_count": 0,
                    "current_statistically_qualified_count": 0,
                    "current_promoted_count": 0,
                    "profitability_certified": False,
                    "paper_only": True,
                    "live_execution_authority": False,
                }
            ]
        },
    }


def _record(
    ledger: SourceCoverageLedger,
    *,
    source_id: str,
    evidence_classes: list[str],
    observed_at: datetime = NOW,
) -> None:
    ledger.record(
        SourceCoverageObservation(
            source_id=source_id,
            lane_id="fundamental_onchain",
            observed_at=observed_at,
            healthy=True,
            item_count=3,
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
        source_id="ethereum-publicnode",
        evidence_classes=["chain_activity"],
    )
    _record(
        ledger,
        source_id="morpho-markets",
        evidence_classes=["protocol_fundamentals"],
    )

    result = overlay_dashboard_source_truth(store, _payload(), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["stage"] == "research_active_waiting_for_complete_forward_evidence"
    assert row["authoritative_observation_count"] == 6
    assert row["current_source_truth"]["source_state"] == "sufficient"
    assert row["current_source_truth"]["independent_authoritative_source_count"] == 2
    assert row["source_state_authority"] == "canonical_source_coverage_observations"

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
        source_id="ethereum-publicnode",
        evidence_classes=["chain_activity", "protocol_fundamentals"],
    )

    result = overlay_dashboard_source_truth(store, _payload(), now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["stage"] == "forward_learning_active_redundancy_pending"
    assert row["current_source_truth"]["source_state"] == "redundancy_gap"
    assert row["current_promoted_count"] == 0


def test_stale_source_observations_cannot_close_provider_gap(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = SourceCoverageLedger(store)
    _record(
        ledger,
        source_id="ethereum-publicnode",
        evidence_classes=["chain_activity", "protocol_fundamentals"],
        observed_at=NOW - timedelta(days=3),
    )

    source = _payload()
    result = overlay_dashboard_source_truth(store, source, now=NOW)
    row = result["mechanisms"]["mechanisms"][0]

    assert row["provider_ready"] is False
    assert row["state"] == "provider_gap"
    assert row["stage"] == "waiting_for_source:provider_gap"
    assert row["current_source_truth"]["connected"] is False
    assert row["current_promoted_count"] == 0

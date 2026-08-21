from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.provider_gap_collection import (
    ProviderAdmissionLedger,
    ProviderAdmissionObservation,
)
from inefficiency_engine.provider_readiness_read import reconcile_provider_readiness


NOW = datetime(2026, 8, 20, 22, 45, tzinfo=timezone.utc)


def _payload(mechanism_id: str = "yield") -> dict[str, object]:
    return {
        "paper_only": True,
        "mechanisms": [
            {
                "mechanism_id": mechanism_id,
                "name": "Test mechanism",
                "state": "provider_gap",
                "stage": "waiting_for_source:provider_gap",
                "provider_ready": False,
                "authoritative_observation_count": 0,
                "economic_candidate_count": 0,
                "independent_forward_outcome_count": 0,
                "current_promoted_count": 0,
                "primary_reason": "authoritative point-in-time evidence is missing",
                "next_action": "connect provider",
                "paper_only": True,
                "live_execution_authority": False,
            }
        ],
    }


def test_fresh_legacy_admission_is_diagnostic_only(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="yield",
            provider="lido:steth-apr-sma",
            observed_at=NOW,
            healthy=True,
            item_count=1,
            source_reference="https://example.test/lido",
        )
    )

    source = _payload()
    result = reconcile_provider_readiness(store, source, now=NOW)
    row = result["mechanisms"][0]

    assert row["state"] == source["mechanisms"][0]["state"]
    assert row["stage"] == source["mechanisms"][0]["stage"]
    assert row["provider_ready"] is False
    assert row["authoritative_observation_count"] == 0
    assert row["primary_reason"] == source["mechanisms"][0]["primary_reason"]
    assert row["next_action"] == source["mechanisms"][0]["next_action"]
    assert row["provider_admission_ready"] is True
    assert row["provider_admission"]["admitted_provider_count"] == 1
    assert row["provider_readiness_state_override_applied"] is False
    assert row["source_state_authority"] == "canonical_13_lane_source_coverage"
    assert row["live_execution_authority"] is False


def test_failed_legacy_probe_cannot_reopen_canonically_connected_lane(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="volatility",
            provider="deribit:public-option-order-book",
            observed_at=NOW,
            healthy=True,
            item_count=4,
            source_reference="https://example.test/deribit",
        )
    )
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="volatility",
            provider="deribit:public-option-order-book",
            observed_at=NOW + timedelta(minutes=1),
            healthy=False,
            item_count=0,
            source_reference="https://example.test/deribit",
            error_type="TimeoutError",
        )
    )

    source = _payload("volatility")
    canonical = source["mechanisms"][0]
    canonical["state"] = "collecting"
    canonical["stage"] = "research_active_waiting_for_complete_forward_evidence"
    canonical["provider_ready"] = True
    canonical["authoritative_observation_count"] = 7
    canonical["primary_reason"] = (
        "authoritative research evidence is connected, but forward-test evidence is incomplete"
    )
    canonical["next_action"] = "collect the missing forward evidence classes"

    result = reconcile_provider_readiness(
        store,
        source,
        now=NOW + timedelta(minutes=1),
    )
    row = result["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["stage"] == "research_active_waiting_for_complete_forward_evidence"
    assert row["authoritative_observation_count"] == 7
    assert row["primary_reason"] == canonical["primary_reason"]
    assert row["next_action"] == canonical["next_action"]
    assert row["provider_admission_ready"] is False
    assert row["provider_admission"]["admitted_provider_count"] == 0
    assert row["provider_admission"]["providers"][0]["error_type"] == "TimeoutError"
    assert row["provider_readiness_state_override_applied"] is False


def test_stale_legacy_admission_cannot_replace_canonical_freshness_state(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="fundamental_onchain",
            provider="ethereum-mainnet:publicnode-finalized",
            observed_at=NOW - timedelta(hours=25),
            healthy=True,
            item_count=1,
            source_reference="https://example.test/ethereum",
        )
    )

    source = _payload("fundamental_onchain")
    canonical = source["mechanisms"][0]
    canonical["state"] = "collecting"
    canonical["stage"] = "waiting_for_source:stale"
    canonical["provider_ready"] = True
    canonical["primary_reason"] = (
        "provider integration exists, but its authoritative evidence is stale"
    )
    canonical["next_action"] = "refresh admitted source evidence"

    result = reconcile_provider_readiness(store, source, now=NOW)
    row = result["mechanisms"][0]

    assert row["state"] == "collecting"
    assert row["stage"] == "waiting_for_source:stale"
    assert row["provider_ready"] is True
    assert row["primary_reason"] == canonical["primary_reason"]
    assert row["provider_admission_ready"] is False
    assert row["provider_admission"]["providers"][0]["fresh"] is False


def test_alternate_source_truth_survives_failed_primary_legacy_probe(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="event_driven",
            provider="bybit-v5:instrument-catalog",
            observed_at=NOW,
            healthy=False,
            item_count=0,
            source_reference="https://example.test/bybit",
            error_type="HTTPStatusError",
        )
    )

    source = _payload("event_driven")
    canonical = source["mechanisms"][0]
    canonical["state"] = "collecting"
    canonical["stage"] = "forward_learning_active_redundancy_pending"
    canonical["provider_ready"] = True
    canonical["authoritative_observation_count"] = 12
    canonical["primary_reason"] = (
        "complete authoritative forward evidence is connected through an alternate admitted source; "
        "independent-source redundancy remains pending"
    )
    canonical["next_action"] = "restore independent source redundancy"

    result = reconcile_provider_readiness(store, source, now=NOW)
    row = result["mechanisms"][0]

    assert row["state"] == "collecting"
    assert row["stage"] == "forward_learning_active_redundancy_pending"
    assert row["provider_ready"] is True
    assert row["authoritative_observation_count"] == 12
    assert row["provider_admission_ready"] is False
    assert row["source_state_authority"] == "canonical_13_lane_source_coverage"


def test_unprobed_mechanism_is_left_unchanged(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    source = _payload("yield")

    result = reconcile_provider_readiness(store, source, now=NOW)

    assert result["mechanisms"][0] == source["mechanisms"][0]
    assert result["provider_readiness_reconciled"] is False
    assert result["provider_readiness_state_override_applied"] is False
    assert result["source_state_authority"] == "canonical_13_lane_source_coverage"

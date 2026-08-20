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


def test_fresh_admitted_provider_closes_only_provider_gap(tmp_path):
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

    result = reconcile_provider_readiness(store, _payload(), now=NOW)
    row = result["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["authoritative_observation_count"] == 1
    assert row["economic_candidate_count"] == 0
    assert row["independent_forward_outcome_count"] == 0
    assert row["current_promoted_count"] == 0
    assert row["live_execution_authority"] is False
    assert row["provider_admission"]["admitted_provider_count"] == 1
    assert row["provider_readiness_presentation_only"] is True


def test_latest_failed_provider_probe_restores_provider_gap(tmp_path):
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
    source["mechanisms"][0]["state"] = "collecting"
    source["mechanisms"][0]["provider_ready"] = True
    result = reconcile_provider_readiness(
        store,
        source,
        now=NOW + timedelta(minutes=1),
    )
    row = result["mechanisms"][0]

    assert row["provider_ready"] is False
    assert row["state"] == "provider_gap"
    assert row["provider_admission"]["admitted_provider_count"] == 0
    assert row["provider_admission"]["providers"][0]["error_type"] == "TimeoutError"


def test_stale_provider_admission_does_not_claim_readiness(tmp_path):
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

    result = reconcile_provider_readiness(
        store,
        _payload("fundamental_onchain"),
        now=NOW,
    )
    row = result["mechanisms"][0]

    assert row["provider_ready"] is False
    assert row["state"] == "provider_gap"
    assert row["provider_admission"]["providers"][0]["fresh"] is False


def test_unprobed_mechanism_is_left_unchanged(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    source = _payload("yield")

    result = reconcile_provider_readiness(store, source, now=NOW)

    assert result["mechanisms"][0] == source["mechanisms"][0]
    assert result["provider_readiness_reconciled"] is False

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.provider_gap_collection import (
    AdmissionAwareDistressResearchService,
    AdmissionAwareYieldResearchService,
    ProviderAdmissionLedger,
    ProviderAdmissionObservation,
    ProviderCatalogLedger,
    ProviderGapCollectionService,
    _bounded_change,
    _safe_reference,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_provider_admission_requires_all_governance_flags(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    now = _now()

    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="yield",
            provider="example",
            observed_at=now,
            healthy=True,
            item_count=1,
            authoritative=True,
            commercial_use_permitted=False,
            point_in_time=True,
            source_reference="https://example.test/api",
        )
    )
    assert ledger.admitted_count("yield", now=now) == 0

    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="yield",
            provider="example",
            observed_at=now + timedelta(seconds=1),
            healthy=True,
            item_count=1,
            authoritative=True,
            commercial_use_permitted=True,
            point_in_time=True,
            source_reference="https://example.test/api",
        )
    )
    assert ledger.admitted_count("yield", now=now + timedelta(seconds=1)) == 1


def test_latest_failed_probe_revokes_provider_admission(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    now = _now()

    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="liquidation_distress",
            provider="example",
            observed_at=now,
            healthy=True,
            item_count=2,
            source_reference="https://example.test/api",
        )
    )
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="liquidation_distress",
            provider="example",
            observed_at=now + timedelta(seconds=1),
            healthy=False,
            item_count=0,
            source_reference="https://example.test/api",
            error_type="TimeoutError",
        )
    )
    assert ledger.admitted_count(
        "liquidation_distress", now=now + timedelta(seconds=1)
    ) == 0


def test_admission_aware_research_summary_keeps_economic_count_separate(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    ledger = ProviderAdmissionLedger(store)
    now = _now()
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="yield",
            provider="lido",
            observed_at=now,
            healthy=True,
            item_count=1,
            source_reference="https://eth-api.lido.fi/v1/protocol/steth/apr/sma",
        )
    )
    ledger.record(
        ProviderAdmissionObservation(
            mechanism_id="liquidation_distress",
            provider="bybit",
            observed_at=now,
            healthy=True,
            item_count=4,
            source_reference="https://api.bybit.com/v5/market/insurance",
        )
    )

    yield_summary = AdmissionAwareYieldResearchService(store, ledger).summary()
    distress_summary = AdmissionAwareDistressResearchService(store, ledger).summary()

    assert yield_summary["authoritative_economic_observation_count"] == 0
    assert yield_summary["provider_surface_admitted_count"] == 1
    assert yield_summary["authoritative_count"] == 1
    assert yield_summary["paper_allocation_count"] == 0

    assert distress_summary["authoritative_economic_observation_count"] == 0
    assert distress_summary["provider_surface_admitted_count"] == 1
    assert distress_summary["authoritative_count"] == 1
    assert distress_summary["paper_allocation_count"] == 0


def test_catalog_baseline_does_not_report_items_as_new_on_second_pass(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite")
    catalog = ProviderCatalogLedger(store)
    now = _now()
    rows = [
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "asset": "BTC",
            "launch_time_ms": "1585526400000",
        },
        {
            "category": "spot",
            "symbol": "ETHUSDT",
            "asset": "ETH",
            "launch_time_ms": None,
        },
    ]

    baseline, new_rows = catalog.observe(
        provider="bybit-v5:instrument-catalog",
        items=rows,
        observed_at=now,
        source_reference="https://api.bybit.com/v5/market/instruments-info",
    )
    assert baseline is True
    assert len(new_rows) == 2

    baseline, new_rows = catalog.observe(
        provider="bybit-v5:instrument-catalog",
        items=rows,
        observed_at=now + timedelta(minutes=1),
        source_reference="https://api.bybit.com/v5/market/instruments-info",
    )
    assert baseline is False
    assert new_rows == []


def test_option_name_parser_and_normalization_helpers():
    parsed = ProviderGapCollectionService._parse_option_name("BTC-25DEC26-100000-C")
    assert parsed is not None
    asset, expiry, strike, option_type = parsed
    assert asset == "BTC"
    assert expiry.tzinfo is not None
    assert strike == 100000.0
    assert option_type == "call"

    assert -1.0 <= _bounded_change(120.0, 100.0, scale=2.0) <= 1.0
    assert _safe_reference("https://rpc.example.test/path?api_key=secret") == (
        "https://rpc.example.test/path"
    )


def test_provider_admission_is_paper_only_and_never_authorizes_execution():
    row = ProviderAdmissionObservation(
        mechanism_id="volatility",
        provider="deribit",
        healthy=True,
        item_count=4,
        source_reference="https://www.deribit.com/api/v2/public/get_order_book",
    )
    assert row.paper_only is True
    assert row.live_execution_authority is False

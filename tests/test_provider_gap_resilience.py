from __future__ import annotations

import asyncio

from inefficiency_engine.provider_gap_collection import (
    ProviderGapCollectionService,
    ProviderProbeResult,
)
from inefficiency_engine.provider_gap_resilience import (
    COINBASE_PRODUCTS_URL,
    HYPERLIQUID_INFO_URL,
    ResilientProviderGapCollectionService,
    _coinbase_catalog_items,
    _hyperliquid_context_rows,
)


def test_coinbase_catalog_parser_keeps_only_active_tradable_products():
    rows = _coinbase_catalog_items(
        [
            {"id": "BTC-USD", "base_currency": "BTC", "status": "online"},
            {"id": "ETH-USD", "base_currency": "ETH", "status": "online", "trading_disabled": True},
            {"id": "SOL-USD", "base_currency": "SOL", "status": "offline"},
            {"id": "DOGE-USD", "base_currency": "DOGE"},
        ]
    )

    assert [row["symbol"] for row in rows] == ["BTC-USD", "DOGE-USD"]
    assert all(row["category"] == "spot" for row in rows)


def test_hyperliquid_context_parser_requires_mark_and_open_interest():
    payload = [
        {"universe": [{"name": "BTC"}, {"name": "ETH"}]},
        [
            {"markPx": "60000", "openInterest": "1000", "funding": "0.0001"},
            {"markPx": "3000", "openInterest": "0"},
            {"markPx": "0", "openInterest": "12"},
            {"markPx": "1.0"},
        ],
    ]

    rows = _hyperliquid_context_rows(payload)

    assert len(rows) == 2
    assert rows[0]["markPx"] == "60000"


def test_event_catalog_falls_back_to_coinbase_when_bybit_is_unavailable(monkeypatch):
    service = object.__new__(ResilientProviderGapCollectionService)

    async def fail_bybit(self):
        raise RuntimeError("regional provider block")

    async def coinbase(self):
        return ProviderProbeResult(
            mechanism_id="event_driven",
            provider=self.COINBASE_CATALOG_PROVIDER,
            item_count=250,
            source_reference=COINBASE_PRODUCTS_URL,
            detail={"baseline": False},
        )

    monkeypatch.setattr(ProviderGapCollectionService, "_collect_bybit_catalog", fail_bybit)
    monkeypatch.setattr(ResilientProviderGapCollectionService, "_collect_coinbase_catalog", coinbase)

    probe = asyncio.run(service._collect_bybit_catalog())

    assert probe.provider == service.COINBASE_CATALOG_PROVIDER
    failures = probe.detail.get("fallback_failures")
    assert isinstance(failures, list) and len(failures) == 1
    assert failures[0]["provider"] == service.BYBIT_CATALOG_PROVIDER


def test_distress_falls_back_to_hyperliquid_after_both_bybit_hosts_fail(monkeypatch):
    service = object.__new__(ResilientProviderGapCollectionService)

    async def fail_adl(self, base_url):
        raise RuntimeError(f"ADL unavailable at {base_url}")

    async def fail_insurance(self, base_url):
        raise RuntimeError(f"insurance unavailable at {base_url}")

    async def hyperliquid(self):
        return ProviderProbeResult(
            mechanism_id="liquidation_distress",
            provider=self.HYPERLIQUID_DISTRESS_PROVIDER,
            item_count=180,
            source_reference=HYPERLIQUID_INFO_URL,
            detail={"economic_opportunity_complete": False},
        )

    monkeypatch.setattr(ResilientProviderGapCollectionService, "_collect_bybit_adl_surface", fail_adl)
    monkeypatch.setattr(ResilientProviderGapCollectionService, "_collect_bybit_insurance_surface", fail_insurance)
    monkeypatch.setattr(ResilientProviderGapCollectionService, "_collect_hyperliquid_distress_surface", hyperliquid)

    probe = asyncio.run(service._collect_bybit_distress_surface())

    assert probe.provider == service.HYPERLIQUID_DISTRESS_PROVIDER
    failures = probe.detail.get("fallback_failures")
    assert isinstance(failures, list) and len(failures) == 4
    assert probe.detail["economic_opportunity_complete"] is False


def test_run_cycle_records_failed_primary_and_actual_fallback_provider(monkeypatch):
    service = object.__new__(ResilientProviderGapCollectionService)

    class Admissions:
        def __init__(self):
            self.rows = []

        def record(self, row):
            self.rows.append(row)
            return row.admission_id

    admissions = Admissions()
    service.admissions = admissions

    def fake_probe(mechanism_id: str, provider: str, source: str, *, failure=None):
        async def collect(self):
            detail = {}
            if failure is not None:
                detail["fallback_failures"] = [failure]
            return ProviderProbeResult(
                mechanism_id=mechanism_id,
                provider=provider,
                item_count=1,
                source_reference=source,
                detail=detail,
            )
        return collect

    bybit_failure = {
        "provider": service.BYBIT_CATALOG_PROVIDER,
        "source_reference": "https://api.bybit.com/v5/market/instruments-info",
        "error_type": "HTTPStatusError",
        "message": "blocked",
    }
    monkeypatch.setattr(
        ResilientProviderGapCollectionService,
        "_collect_ethereum_fundamentals",
        fake_probe("fundamental_onchain", service.ETHEREUM_PROVIDER, "https://ethereum-rpc.publicnode.com"),
    )
    monkeypatch.setattr(
        ResilientProviderGapCollectionService,
        "_collect_bybit_catalog",
        fake_probe("event_driven", service.COINBASE_CATALOG_PROVIDER, COINBASE_PRODUCTS_URL, failure=bybit_failure),
    )
    monkeypatch.setattr(
        ResilientProviderGapCollectionService,
        "_collect_lido_yield_surface",
        fake_probe("yield", service.LIDO_PROVIDER, "https://eth-api.lido.fi/v1/protocol/steth/apr/sma"),
    )
    monkeypatch.setattr(
        ResilientProviderGapCollectionService,
        "_collect_deribit_options",
        fake_probe("volatility", service.DERIBIT_PROVIDER, "https://www.deribit.com/api/v2/public/get_order_book"),
    )
    monkeypatch.setattr(
        ResilientProviderGapCollectionService,
        "_collect_bybit_distress_surface",
        fake_probe("liquidation_distress", service.HYPERLIQUID_DISTRESS_PROVIDER, HYPERLIQUID_INFO_URL),
    )

    result = asyncio.run(service.run_cycle())

    assert result["mechanisms"]["event_driven"]["provider"] == service.COINBASE_CATALOG_PROVIDER
    assert result["mechanisms"]["event_driven"]["fallback_used"] is True
    event_rows = [row for row in admissions.rows if row.mechanism_id == "event_driven"]
    assert len(event_rows) == 2
    assert event_rows[0].provider == service.BYBIT_CATALOG_PROVIDER
    assert event_rows[0].healthy is False
    assert event_rows[1].provider == service.COINBASE_CATALOG_PROVIDER
    assert event_rows[1].healthy is True

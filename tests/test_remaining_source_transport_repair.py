from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

import httpx

from inefficiency_engine import option_capacity
from inefficiency_engine import permanent_source_worker_lane_repair
from inefficiency_engine import priority_source_collection as priority_sources
from inefficiency_engine import production_source_recovery_runtime as recovery_v1
from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2
from inefficiency_engine import remaining_source_transport_repair as repair
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService


class _Response:
    def __init__(self, payload, *, url: str, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request("GET", url)

    def raise_for_status(self):
        if self.status_code >= 400:
            response = httpx.Response(self.status_code, request=self.request)
            raise httpx.HTTPStatusError(
                "provider rejected request",
                request=self.request,
                response=response,
            )

    def json(self):
        return self._payload


def test_aave_repair_adds_documented_public_rpc_fallbacks_without_replacing_existing(monkeypatch):
    monkeypatch.setattr(
        recovery_v1,
        "AAVE_RPC_FALLBACK_URLS",
        ("https://existing.example",),
    )

    repair._append_aave_public_rpc_fallbacks()

    assert recovery_v1.AAVE_RPC_FALLBACK_URLS == (
        "https://existing.example",
        "https://eth.drpc.org",
        "https://public.1rpc.io/eth",
    )


def test_transport_preflight_budgets_stay_inside_unchanged_source_freshness_windows():
    assert repair._transport_preflight_timeout("aave-liquidations", 15.0) == 30.0
    assert repair._transport_preflight_timeout("public-trade-flow", 15.0) == 20.0
    assert repair._transport_preflight_timeout("hyperliquid-distress", 15.0) == 15.0
    assert repair.AAVE_SOURCE_PREFLIGHT_TIMEOUT_SECONDS < 300.0
    assert repair.TRADE_FLOW_SOURCE_PREFLIGHT_TIMEOUT_SECONDS < 120.0


def test_defillama_retries_connect_timeout_then_keeps_same_endpoint_and_evidence(monkeypatch):
    attempts = 0

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            nonlocal attempts
            attempts += 1
            assert url == repair.event_yield.DEFILLAMA_PROTOCOLS_URL
            if attempts == 1:
                raise httpx.ConnectTimeout(
                    "temporary",
                    request=httpx.Request("GET", url),
                )
            return _Response(
                [{"name": "Aave", "tvl": 1.0}, {"name": "Lido", "tvl": 2.0}],
                url=url,
            )

    monkeypatch.setattr(repair.httpx, "AsyncClient", Client)

    probe = asyncio.run(repair.collect_defillama_protocols_resilient())

    assert attempts == 2
    assert probe.source_id == "defillama-protocols"
    assert probe.item_count == 2
    assert probe.source_reference == repair.event_yield.DEFILLAMA_PROTOCOLS_URL
    assert probe.evidence_by_lane == {"fundamental_onchain": ["protocol_fundamentals"]}
    assert probe.authoritative is False
    assert probe.detail["same_defillama_endpoint"] is True
    assert probe.detail["qualification_thresholds_unchanged"] is True
    assert probe.detail["paper_only"] is True
    assert probe.detail["allocation_authority"] is False
    assert probe.detail["live_execution_authority"] is False


def test_defillama_permanent_client_error_fails_closed_without_retry(monkeypatch):
    attempts = 0

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            nonlocal attempts
            attempts += 1
            return _Response([], url=url, status_code=404)

    monkeypatch.setattr(repair.httpx, "AsyncClient", Client)

    try:
        asyncio.run(repair.collect_defillama_protocols_resilient())
    except httpx.HTTPStatusError as exc:
        assert exc.response.status_code == 404
    else:
        raise AssertionError("permanent DefiLlama client errors must fail closed")

    assert attempts == 1


def _deribit_capacity_probe(*, quote_count: int = 16) -> SourceProbeResult:
    return SourceProbeResult(
        source_id="deribit-option-capacity",
        item_count=16,
        source_reference="https://www.deribit.com/api/v2/public/get_order_book",
        evidence_by_lane={"volatility": ["option_capacity"]},
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
        economic_fields_complete=True,
        forward_testable_evidence=True,
        detail={
            "option_quote_greek_observation_count": quote_count,
            "visible_capacity_observation_count": 16,
            "paper_only": True,
            "allocation_authority": False,
        },
    )


def test_deribit_retries_connect_timeout_with_fresh_full_collector_attempt(monkeypatch):
    attempts = 0

    async def collector(store):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectTimeout(
                "temporary",
                request=httpx.Request(
                    "GET",
                    "https://www.deribit.com/api/v2/public/get_order_book",
                ),
            )
        return _deribit_capacity_probe()

    monkeypatch.setattr(repair, "_ORIGINAL_DERIBIT_CAPACITY_COLLECTOR", collector)

    probe = asyncio.run(repair.collect_deribit_option_capacity_resilient(object()))

    assert attempts == 2
    assert probe.source_id == "deribit-option-capacity"
    assert probe.item_count == 16
    assert probe.detail["transport_attempt"] == 2
    assert probe.detail["transport_retry_failures"] == [
        {"attempt": 1, "error_type": "ConnectTimeout"}
    ]
    assert probe.detail["fresh_client_per_retry"] is True
    assert probe.detail["same_deribit_summary_and_order_book_endpoints"] is True
    assert probe.detail["qualification_thresholds_unchanged"] is True
    assert probe.detail["paper_only"] is True
    assert probe.detail["allocation_authority"] is False
    assert probe.detail["live_execution_authority"] is False


def test_deribit_permanent_client_error_remains_fail_closed_without_retry(monkeypatch):
    attempts = 0

    async def collector(store):
        nonlocal attempts
        attempts += 1
        request = httpx.Request(
            "GET",
            "https://www.deribit.com/api/v2/public/get_order_book",
        )
        response = httpx.Response(403, request=request)
        raise httpx.HTTPStatusError(
            "forbidden",
            request=request,
            response=response,
        )

    monkeypatch.setattr(repair, "_ORIGINAL_DERIBIT_CAPACITY_COLLECTOR", collector)

    try:
        asyncio.run(repair.collect_deribit_option_capacity_resilient(object()))
    except httpx.HTTPStatusError as exc:
        assert exc.response.status_code == 403
    else:
        raise AssertionError("permanent Deribit client errors must remain fail closed")

    assert attempts == 1


def test_provider_gap_deribit_reuses_capacity_books_and_requires_quote_greeks(monkeypatch):
    async def collector(store):
        return _deribit_capacity_probe(quote_count=12)

    monkeypatch.setattr(repair, "collect_deribit_option_capacity_resilient", collector)
    service = SimpleNamespace(
        store=object(),
        DERIBIT_PROVIDER="deribit:public-option-order-book",
    )

    probe = asyncio.run(repair._collect_deribit_options_via_capacity(service))

    assert probe.mechanism_id == "volatility"
    assert probe.provider == "deribit:public-option-order-book"
    assert probe.item_count == 12
    assert probe.detail["provider_gap_reuses_capacity_collector"] is True
    assert probe.detail["same_deribit_order_books"] is True
    assert probe.detail["provider_policy_unchanged"] is True
    assert probe.detail["qualification_thresholds_unchanged"] is True
    assert probe.detail["paper_only"] is True
    assert probe.detail["allocation_authority"] is False
    assert probe.detail["live_execution_authority"] is False



def test_provider_gap_deribit_does_not_admit_capacity_without_quotes_and_greeks(monkeypatch):
    async def collector(store):
        return _deribit_capacity_probe(quote_count=0)

    monkeypatch.setattr(repair, "collect_deribit_option_capacity_resilient", collector)
    service = SimpleNamespace(
        store=object(),
        DERIBIT_PROVIDER="deribit:public-option-order-book",
    )

    try:
        asyncio.run(repair._collect_deribit_options_via_capacity(service))
    except ValueError as exc:
        assert "no bounded executable option quotes with Greeks" in str(exc)
    else:
        raise AssertionError("Deribit provider admission must remain fail closed without Greeks")


def test_deribit_install_unifies_priority_critical_and_provider_gap_owners(monkeypatch):
    original_option_capacity = option_capacity.collect_deribit_option_capacity
    original_priority = priority_sources.collect_deribit_option_capacity
    original_critical = recovery_v2.collect_deribit_option_capacity
    original_provider_gap = ResilientProviderGapCollectionService._collect_deribit_options
    try:
        repair._install_deribit_transport_unification()
        assert (
            option_capacity.collect_deribit_option_capacity
            is repair.collect_deribit_option_capacity_resilient
        )
        assert (
            priority_sources.collect_deribit_option_capacity
            is repair.collect_deribit_option_capacity_resilient
        )
        assert (
            recovery_v2.collect_deribit_option_capacity
            is repair.collect_deribit_option_capacity_resilient
        )
        assert (
            ResilientProviderGapCollectionService._collect_deribit_options
            is repair._collect_deribit_options_via_capacity
        )
    finally:
        option_capacity.collect_deribit_option_capacity = original_option_capacity
        priority_sources.collect_deribit_option_capacity = original_priority
        recovery_v2.collect_deribit_option_capacity = original_critical
        ResilientProviderGapCollectionService._collect_deribit_options = original_provider_gap


def test_production_source_child_installs_remaining_transport_repair():
    source = inspect.getsource(
        permanent_source_worker_lane_repair.install_remaining_source_lane_repairs
    )
    assert "install_remaining_source_transport_repairs()" in source
    assert "qualification" in source

from __future__ import annotations

import asyncio
import inspect

import httpx

from inefficiency_engine import permanent_source_worker_lane_repair
from inefficiency_engine import production_source_recovery_runtime as recovery_v1
from inefficiency_engine import remaining_source_transport_repair as repair


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


def test_production_source_child_installs_remaining_transport_repair():
    source = inspect.getsource(
        permanent_source_worker_lane_repair.install_remaining_source_lane_repairs
    )
    assert "install_remaining_source_transport_repairs()" in source
    assert "qualification" in source

from __future__ import annotations

import asyncio

import httpx

from inefficiency_engine import production_source_recovery_runtime as recovery
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService


class _Response:
    def __init__(self, payload, *, url: str, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request("POST", url)

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


def test_aave_transport_fallback_uses_same_contract_query(monkeypatch):
    primary = "https://primary-rpc.example"
    calls: list[tuple[str, str]] = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            calls.append((url, json["method"]))
            if url == primary:
                raise httpx.ConnectTimeout(
                    "primary unavailable",
                    request=httpx.Request("POST", url),
                )
            if json["method"] == "eth_blockNumber":
                return _Response({"result": "0x1000"}, url=url)
            request = json["params"][0]
            assert request["address"] == recovery.AAVE_V3_ETHEREUM_POOL
            assert request["topics"] == [recovery.AAVE_LIQUIDATION_TOPIC]
            assert request["toBlock"] == "0x1000"
            return _Response({"result": []}, url=url)

    class Coverage:
        def record_event(self, row):
            raise AssertionError("empty log set should not create an event")

    monkeypatch.setenv("CIE_ETHEREUM_RPC_URL", primary)
    monkeypatch.setattr(recovery.httpx, "AsyncClient", Client)

    probe = asyncio.run(
        recovery.collect_aave_liquidations_production_resilient(Coverage())
    )

    assert probe.source_id == "aave-liquidations"
    assert probe.item_count == 0
    assert probe.source_reference == "https://eth.llamarpc.com"
    assert probe.detail["rpc_transport_fallback_used"] is True
    assert probe.detail["same_aave_contract_and_topic"] is True
    assert calls[0] == (primary, "eth_blockNumber")
    assert any(url == "https://eth.llamarpc.com" for url, _ in calls)


def test_lido_retries_sma_then_falls_back_to_first_party_latest(monkeypatch):
    calls: list[str] = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            calls.append(url)
            if url == recovery.LIDO_APR_URL:
                raise httpx.ConnectTimeout(
                    "temporary",
                    request=httpx.Request("GET", url),
                )
            return _Response({"data": {"apr": 3.25}}, url=url)

    monkeypatch.setattr(recovery.httpx, "AsyncClient", Client)

    probe = asyncio.run(recovery.collect_lido_yield_resilient())

    assert calls.count(recovery.LIDO_APR_URL) == recovery.LIDO_ATTEMPTS_PER_ENDPOINT
    assert calls[-1] == recovery.LIDO_LAST_APR_URL
    assert probe.source_id == "lido-yield"
    assert probe.source_reference == recovery.LIDO_LAST_APR_URL
    assert probe.evidence_by_lane == {"yield": ["yield_rate"]}
    assert probe.detail["endpoint_fallback_used"] is True
    assert probe.detail["first_party_lido_api"] is True
    assert probe.detail["observed_apr"] == 3.25


def test_lido_provider_recovery_keeps_same_authoritative_provider(monkeypatch):
    async def fake_collect():
        return recovery.SourceProbeResult(
            source_id="lido-yield",
            item_count=1,
            source_reference=recovery.LIDO_APR_URL,
            evidence_by_lane={"yield": ["yield_rate"]},
            detail={"observed_apr": 3.0},
        )

    monkeypatch.setattr(recovery, "collect_lido_yield_resilient", fake_collect)

    class Service:
        LIDO_PROVIDER = "lido:steth-apr-sma"

    result = asyncio.run(recovery.collect_lido_provider_resilient(Service()))

    assert result.mechanism_id == "yield"
    assert result.provider == "lido:steth-apr-sma"
    assert result.source_reference == recovery.LIDO_APR_URL
    assert result.detail["provider_policy_unchanged"] is True


def test_install_lido_provider_recovery_changes_transport_only():
    original = ResilientProviderGapCollectionService._collect_lido_yield_surface
    try:
        recovery.install_lido_provider_recovery()
        assert (
            ResilientProviderGapCollectionService._collect_lido_yield_surface
            is recovery.collect_lido_provider_resilient
        )
    finally:
        ResilientProviderGapCollectionService._collect_lido_yield_surface = original

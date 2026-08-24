from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from inefficiency_engine import production_source_recovery_v2_runtime as recovery


class _Response:
    def __init__(self, payload, *, url: str, method: str = "POST", status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request(method, url)

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


def test_aave_adapts_down_to_exact_latest_block(monkeypatch):
    url = "https://rpc.example"
    log_queries: list[dict[str, object]] = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, request_url, json):
            assert request_url == url
            method = json["method"]
            if method == "eth_blockNumber":
                return _Response({"result": "0x100"}, url=request_url)
            if method == "eth_getLogs":
                request = json["params"][0]
                log_queries.append(request)
                assert request["address"] == recovery.AAVE_V3_ETHEREUM_POOL
                assert request["topics"] == [recovery.AAVE_LIQUIDATION_TOPIC]
                if request.get("fromBlock") != request.get("toBlock"):
                    return _Response({"error": {"code": -32005}}, url=request_url)
                return _Response({"result": []}, url=request_url)
            raise AssertionError(method)

    class Coverage:
        def record_event(self, row):
            raise AssertionError("empty latest block should not create a liquidation event")

    monkeypatch.setattr(recovery.httpx, "AsyncClient", Client)
    monkeypatch.setattr(recovery, "aave_rpc_candidates", lambda: (url,))

    probe = asyncio.run(recovery.collect_aave_liquidations_resilient_v2(Coverage()))

    assert probe.source_id == "aave-liquidations"
    assert probe.item_count == 0
    assert probe.detail["query_mode"] == "range"
    assert probe.detail["lookback_blocks"] == 0
    assert log_queries[-1]["fromBlock"] == log_queries[-1]["toBlock"] == "0x100"
    assert probe.detail["same_aave_contract_and_topic"] is True


def test_aave_preserves_transport_failure_types(monkeypatch):
    async def fail(coverage, *, url):
        raise httpx.ConnectTimeout(
            f"connect failed {url}",
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(recovery, "_collect_aave_from_rpc_adaptive", fail)
    monkeypatch.setattr(
        recovery,
        "aave_rpc_candidates",
        lambda: ("https://one.example", "https://two.example"),
    )

    try:
        asyncio.run(recovery.collect_aave_liquidations_resilient_v2(object()))
    except recovery.AaveAllTransportsFailed as exc:
        message = str(exc)
    else:
        raise AssertionError("all failed transports must fail closed")

    assert "https://one.example" in message
    assert "https://two.example" in message
    assert "ConnectTimeout" in message


def test_coinbase_trade_flow_retries_transient_product_connect_timeout(monkeypatch):
    attempts: dict[str, int] = {}

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params):
            product_id = url.split("/products/", 1)[1].split("/", 1)[0]
            attempts[product_id] = attempts.get(product_id, 0) + 1
            if product_id == "ETH-USD" and attempts[product_id] == 1:
                raise httpx.ConnectTimeout(
                    "temporary",
                    request=httpx.Request("GET", url),
                )
            payload = [
                {
                    "trade_id": f"{product_id}-1",
                    "side": "sell",
                    "price": "100.0",
                    "size": "1.0",
                    "time": "2026-08-24T22:00:00Z",
                }
            ]
            return _Response(payload, url=url, method="GET")

    monkeypatch.setattr(recovery.httpx, "AsyncClient", Client)
    monkeypatch.setattr(
        recovery,
        "_persist_trade_events_bulk",
        lambda coverage, rows: len(rows),
    )

    probe = asyncio.run(recovery.collect_coinbase_trade_flow_resilient(object()))

    assert probe.source_id == "public-trade-flow"
    assert probe.item_count == len(recovery.DEFAULT_TRADE_PRODUCTS)
    assert attempts["ETH-USD"] == 2
    assert probe.detail["all_configured_products_required"] is True
    assert any(
        row["product_id"] == "ETH-USD" and row["error_type"] == "ConnectTimeout"
        for row in probe.detail["transport_retry_failures"]
    )


def test_critical_cadence_keeps_option_sources_inside_unchanged_900_second_ttl():
    calls: list[dict[str, object]] = []

    class Ledger:
        def latest(self):
            return {}

    class Coverage:
        ledger = Ledger()

    class Transfer:
        def status(self):
            return {"state": "awaiting_endogenous"}

    class Service:
        source_coverage = Coverage()
        capital_transfer_evidence = Transfer()
        volatility_service = object()
        store = object()

        async def _preflight(self, **kwargs):
            calls.append(kwargs)
            return {"source_id": kwargs["source_id"], "state": "fresh_cached"}

    result = asyncio.run(recovery.run_critical_source_refresh_once(Service()))
    by_source = {str(row["source_id"]): row for row in calls}

    assert calls[0]["source_id"] == "public-trade-flow"
    assert by_source["okx-options"]["refresh_seconds"] == recovery.OKX_OPTIONS_PREFLIGHT_REFRESH_SECONDS
    assert by_source["deribit-option-capacity"]["refresh_seconds"] == recovery.DERIBIT_CAPACITY_PREFLIGHT_REFRESH_SECONDS
    assert recovery.OKX_OPTIONS_PREFLIGHT_REFRESH_SECONDS < 900.0
    assert recovery.DERIBIT_CAPACITY_PREFLIGHT_REFRESH_SECONDS < 900.0
    assert result["okx_options"]["source_id"] == "okx-options"
    assert result["deribit_option_capacity"]["source_id"] == "deribit-option-capacity"

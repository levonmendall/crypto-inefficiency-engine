from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from inefficiency_engine.capital_transfer_evidence import (
    CapitalTransferEvidenceLedger,
    VerifiedCapitalTransferObservation,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine import source_lane_repair_runtime as runtime


class _Response:
    def __init__(self, payload, *, status_code: int = 200, url: str = "https://example.test"):
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request("POST", url)

    def raise_for_status(self):
        if self.status_code >= 400:
            response = httpx.Response(self.status_code, request=self.request)
            raise httpx.HTTPStatusError("provider rejected request", request=self.request, response=response)

    def json(self):
        return self._payload


def test_aave_liquidation_collector_shrinks_rejected_log_window(monkeypatch):
    calls: list[int] = []

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            if json["method"] == "eth_blockNumber":
                return _Response({"result": "0x1000"}, url=url)
            request = json["params"][0]
            lookback = 0x1000 - int(request["fromBlock"], 16)
            calls.append(lookback)
            if lookback == 512:
                return _Response({"error": "range rejected"}, status_code=413, url=url)
            return _Response({"result": []}, url=url)

    monkeypatch.setattr(runtime.httpx, "AsyncClient", lambda **kwargs: Client())

    class Coverage:
        def __init__(self):
            self.events = []

        def record_event(self, row):
            self.events.append(row)

    probe = asyncio.run(runtime.collect_aave_liquidations_resilient(Coverage()))

    assert calls[:2] == [512, 128]
    assert probe.source_id == "aave-liquidations"
    assert probe.detail["range_fallback_used"] is True
    assert probe.detail["lookback_blocks"] == 128
    assert probe.evidence_by_lane == {"liquidation_distress": ["liquidation_events"]}


def test_hyperliquid_distress_retries_transient_connect_timeout(monkeypatch):
    attempts = 0

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectTimeout("temporary", request=httpx.Request("POST", url))
            return _Response(
                [
                    {"universe": [{"name": "BTC"}]},
                    [{"markPx": "60000", "openInterest": "1000", "funding": "0.0001"}],
                ],
                url=url,
            )

    monkeypatch.setattr(runtime.httpx, "AsyncClient", lambda **kwargs: Client())

    probe = asyncio.run(runtime.collect_hyperliquid_distress_resilient())

    assert attempts == 2
    assert probe.source_id == "hyperliquid-distress"
    assert probe.item_count == 1
    assert probe.detail["attempt"] == 2
    assert probe.evidence_by_lane == {"liquidation_distress": ["distress_state"]}


def test_verified_capital_transfer_sink_rejects_synthetic_evidence(tmp_path):
    store = EvidenceStore(tmp_path / "capital.sqlite")
    ledger = CapitalTransferEvidenceLedger(store)
    started = datetime.now(timezone.utc)
    settled = started + timedelta(seconds=30)

    with pytest.raises(ValueError, match="verified external transfers"):
        ledger.record(
            VerifiedCapitalTransferObservation(
                transfer_id="paper-estimate",
                initiated_at=started,
                settled_at=settled,
                from_venue="Coinbase",
                to_venue="Kraken",
                asset="USDC",
                transfer_cost_usd=1.0,
                latency_seconds=30.0,
                source_reference="paper:configured-fee",
                verified_external_transfer=False,
            )
        )

    status = ledger.status()
    assert status["producer_implemented"] is True
    assert status["verified_observation_available"] is False
    assert status["synthetic_transfer_evidence_allowed"] is False


def test_verified_capital_transfer_sink_persists_real_observation(tmp_path):
    store = EvidenceStore(tmp_path / "capital.sqlite")
    ledger = CapitalTransferEvidenceLedger(store)
    started = datetime.now(timezone.utc)
    settled = started + timedelta(seconds=42)

    ledger.record(
        VerifiedCapitalTransferObservation(
            transfer_id="external-withdrawal-1",
            initiated_at=started,
            settled_at=settled,
            from_venue="Coinbase",
            to_venue="Kraken",
            asset="USDC",
            network="ethereum",
            transfer_cost_usd=1.25,
            latency_seconds=42.0,
            source_reference="verified:test-fixture",
        )
    )

    status = ledger.status()
    assert status["verified_observation_available"] is True
    assert status["qualification_thresholds_unchanged"] is True
    assert status["paper_only"] is True


def test_collection_headroom_does_not_change_evidence_freshness_thresholds():
    from inefficiency_engine.evidence_velocity import EVIDENCE_CLASS_FRESHNESS_SECONDS

    assert runtime.TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS < EVIDENCE_CLASS_FRESHNESS_SECONDS["trade_flow"]
    assert runtime.AAVE_PREFLIGHT_REFRESH_SECONDS < EVIDENCE_CLASS_FRESHNESS_SECONDS["liquidation_events"]
    assert (
        runtime.HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS
        < EVIDENCE_CLASS_FRESHNESS_SECONDS["distress_state"]
    )

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from inefficiency_engine import critical_source_cadence as cadence
from inefficiency_engine.priority_source_models import SourceProbeResult


def test_critical_refresh_gives_trade_flow_first_access_to_cadence():
    calls: list[str] = []

    class Ledger:
        def latest(self):
            return {}

    class Transfer:
        def status(self):
            return {"verified_observation_available": False}

    class Service:
        source_coverage = SimpleNamespace(ledger=Ledger())
        capital_transfer_evidence = Transfer()

        async def _preflight(self, **kwargs):
            calls.append(str(kwargs["source_id"]))
            return {"source_id": kwargs["source_id"], "state": "fresh_cached"}

    result = asyncio.run(cadence.run_critical_source_refresh_once(Service()))  # type: ignore[arg-type]

    assert calls[0] == "public-trade-flow"
    assert set(calls) == {
        "public-trade-flow",
        "aave-liquidations",
        "hyperliquid-distress",
    }
    assert result["qualification_thresholds_unchanged"] is True
    assert result["paper_only"] is True
    assert result["allocation_authority"] is False
    assert result["live_execution_authority"] is False


def test_critical_cadence_stays_inside_trade_flow_validity_window(monkeypatch):
    from inefficiency_engine.evidence_velocity import EVIDENCE_CLASS_FRESHNESS_SECONDS

    monkeypatch.setenv("CIE_CRITICAL_SOURCE_INTERVAL_SECONDS", "30")
    interval = cadence.critical_source_interval_seconds()

    assert interval == 30.0
    assert interval < EVIDENCE_CLASS_FRESHNESS_SECONDS["trade_flow"]
    assert cadence.TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS < EVIDENCE_CLASS_FRESHNESS_SECONDS["trade_flow"]


def test_aave_transport_fallback_keeps_same_evidence_contract(monkeypatch):
    calls: list[str] = []

    async def fail_primary(coverage):
        calls.append("primary")
        raise RuntimeError("primary rpc unavailable")

    async def succeed_fallback(coverage, *, url):
        calls.append(url)
        return SourceProbeResult(
            source_id="aave-liquidations",
            item_count=0,
            source_reference=url,
            evidence_by_lane={"liquidation_distress": ["liquidation_events"]},
            detail={"qualification_thresholds_unchanged": True},
        )

    monkeypatch.setattr(cadence, "collect_aave_liquidations_resilient", fail_primary)
    monkeypatch.setattr(cadence, "_collect_aave_from_rpc", succeed_fallback)
    monkeypatch.setattr(
        cadence,
        "_aave_rpc_candidates",
        lambda: ("https://primary.test", "https://fallback.test"),
    )

    probe = asyncio.run(cadence.collect_aave_liquidations_with_transport_fallback(object()))  # type: ignore[arg-type]

    assert calls == ["primary", "https://fallback.test"]
    assert probe.source_id == "aave-liquidations"
    assert probe.evidence_by_lane == {"liquidation_distress": ["liquidation_events"]}
    assert probe.detail["rpc_transport_fallback_used"] is True
    assert len(probe.detail["transport_failures"]) == 1


def test_source_worker_wrapper_runs_critical_cadence_separately():
    source = Path("src/inefficiency_engine/permanent_source_worker_lane_repair.py").read_text()

    assert "critical_source_refresh_loop" in source
    assert "_run_permanent_source_worker_with_critical_cadence" in source
    assert "base.run_permanent_source_worker = _run_permanent_source_worker_with_critical_cadence" in source
    assert 'name="critical-source-freshness"' in source

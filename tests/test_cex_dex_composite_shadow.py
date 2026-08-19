from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeProbe
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore


NOW = datetime(2026, 8, 19, 13, 45, tzinfo=timezone.utc)


def evidence(evidence_id: str, net_edge_bps: float, *, observed_at: datetime = NOW) -> CexDexCompositeEvidence:
    return CexDexCompositeEvidence(
        evidence_id=evidence_id,
        frontier_id=f"frontier-{evidence_id}",
        asset="ETH",
        route_direction="buy_asset",
        target_notional_usd=5000.0,
        route_contiguous_acceptable=True,
        cex_venue="Coinbase",
        cex_symbol="ETH-USD",
        cex_quote_currency="USD",
        cex_reference_price=4000.0,
        route_quote_currency="USDC",
        route_effective_asset_price=3980.0,
        route_quote_notional_usd_proxy=5000.0,
        conversion_depth_quote=None,
        conversion_risk_haircut_bps=2.0,
        cex_taker_fee_bps=6.0,
        gas_cost_bps=4.0,
        gross_edge_after_conversion_depth_bps=net_edge_bps + 12.0,
        net_research_edge_bps=net_edge_bps,
        observed_at=observed_at,
        evidence_complete=True,
        blocked_reason="research only",
    )


def probe(rows: list[CexDexCompositeEvidence], *, observed_at: datetime) -> CexDexCompositeProbe:
    return CexDexCompositeProbe(
        observed_at=observed_at,
        frontier_count=1,
        quoted_route_point_count=1,
        comparison_attempt_count=max(1, len(rows)),
        evidence_count=len(rows),
        evidence=rows,
    )


class FakeCompositeService:
    def __init__(self, probes, *, store=None):
        self.settings = Settings(
            shadow_horizons_seconds=(5.0,),
            dex_statistical_min_net_edge_bps=12.0,
        )
        self.core = SimpleNamespace(evidence_store=store)
        self._probes = list(probes)

    async def probe(self):
        return self._probes.pop(0)


async def no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_composite_shadow_tracks_full_net_edge_survival_and_deterioration():
    initial = evidence("initial", 30.0)
    verification = evidence("verification", 18.0, observed_at=NOW + timedelta(seconds=5))
    composite = FakeCompositeService(
        [probe([initial], observed_at=NOW), probe([verification], observed_at=NOW + timedelta(seconds=5))]
    )
    service = CexDexCompositeEdgeShadowService(composite, sleep=no_sleep)  # type: ignore[arg-type]

    cycle = await service.run_cycle(horizons_seconds=(5.0,))

    assert cycle.initial_evidence_count == 1
    assert len(cycle.records) == 2
    assert len(cycle.observations) == 1
    observation = cycle.observations[0]
    assert observation.survived is True
    assert observation.initial_net_edge_bps == pytest.approx(30.0)
    assert observation.verification_net_edge_bps == pytest.approx(18.0)
    assert observation.net_edge_change_bps == pytest.approx(-12.0)
    assert observation.adverse_deterioration_bps == pytest.approx(12.0)
    assert observation.retained_edge_fraction == pytest.approx(0.6)
    assert observation.initial_above_hurdle is True
    assert observation.verification_above_hurdle is True
    assert observation.hurdle_survived is True
    assert observation.allocation_eligible is False
    assert observation.executable_eligible is False


@pytest.mark.asyncio
async def test_composite_shadow_distinguishes_missing_edge_from_below_hurdle():
    first = evidence("initial-a", 30.0)
    below = evidence("verification-a", 5.0, observed_at=NOW + timedelta(seconds=5))
    composite = FakeCompositeService(
        [probe([first], observed_at=NOW), probe([below], observed_at=NOW + timedelta(seconds=5))]
    )
    service = CexDexCompositeEdgeShadowService(composite, sleep=no_sleep)  # type: ignore[arg-type]
    cycle = await service.run_cycle(horizons_seconds=(5.0,))
    observation = cycle.observations[0]
    assert observation.survived is True
    assert observation.verification_above_hurdle is False
    assert observation.hurdle_survived is False
    assert observation.failure_type is None

    missing_composite = FakeCompositeService(
        [probe([first], observed_at=NOW), probe([], observed_at=NOW + timedelta(seconds=5))]
    )
    missing_service = CexDexCompositeEdgeShadowService(missing_composite, sleep=no_sleep)  # type: ignore[arg-type]
    missing_cycle = await missing_service.run_cycle(horizons_seconds=(5.0,))
    missing = missing_cycle.observations[0]
    assert missing.survived is False
    assert missing.verification_net_edge_bps is None
    assert missing.hurdle_survived is False
    assert missing.failure_type == "CompositeMissing"


@pytest.mark.asyncio
async def test_composite_shadow_persists_append_only_cycle_and_summary(tmp_path):
    store = EvidenceStore(tmp_path / "composite.sqlite3")
    initial = evidence("initial-persist", 25.0)
    verification = evidence("verification-persist", 20.0, observed_at=NOW + timedelta(seconds=5))
    composite = FakeCompositeService(
        [probe([initial], observed_at=NOW), probe([verification], observed_at=NOW + timedelta(seconds=5))],
        store=store,
    )
    service = CexDexCompositeEdgeShadowService(composite, evidence_store=store, sleep=no_sleep)  # type: ignore[arg-type]

    cycle = await service.run_cycle(horizons_seconds=(5.0,))

    assert service.ledger is not None
    loaded = service.ledger.load_cycle(cycle.cycle_id)
    assert loaded.cycle_id == cycle.cycle_id
    assert len(service.ledger.load_records(cycle.cycle_id)) == 2
    summary = service.ledger.summary()
    assert summary["cycle_count"] == 1
    assert summary["record_count"] == 2
    assert summary["observation_count"] == 1
    assert summary["matched_count"] == 1
    assert summary["hurdle_survived_count"] == 1
    assert summary["capacity_claimed"] is False
    assert summary["allocation_eligible"] is False
    assert summary["executable_eligible"] is False
    assert summary["paper_only"] is True

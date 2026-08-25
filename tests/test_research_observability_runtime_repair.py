from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.alpha_funnel_projection import DASHBOARD_ALPHA_FUNNEL_LANES
from inefficiency_engine.candidate_observatory_runtime import (
    CandidateObservedAllLaneEvidenceFactoryService,
)
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.research_closure_worker import (
    ResearchClosureCycleSummary,
    ResearchClosureSummaryLedger,
)
from inefficiency_engine.research_observability_runtime_repair import (
    ObservableDisposableExpandedAlphaFactoryService,
    _without_mixed_microstructure_funnel,
    closure_recovery_required,
)
from inefficiency_engine.render_combined_postbind_lane_repair import (
    RESEARCH_OBSERVABILITY_COMMAND,
)


NOW = datetime(2026, 8, 25, 16, 0, tzinfo=timezone.utc)


def _summary(*, observed_at: datetime, summary_id: str = "summary-1") -> ResearchClosureCycleSummary:
    return ResearchClosureCycleSummary(
        summary_id=summary_id,
        observed_at=observed_at,
        source_scan_id="scan-1",
        source_order_book_count=0,
        usable_order_book_count=0,
        rejection_funnels={
            "carry": {"raw_candidate_count": 1, "emitted_candidate_count": 0},
            "microstructure": {"raw_candidate_count": 0, "emitted_candidate_count": 5},
        },
        capital_location_forward={},
        maker_shadow={},
        canonical_capabilities={},
        provider_admission={},
        diagnostic_errors={},
    )


def test_production_disposable_factory_super_chain_includes_candidate_observatory():
    mro = ObservableDisposableExpandedAlphaFactoryService.mro()

    assert issubclass(
        ObservableDisposableExpandedAlphaFactoryService,
        DisposableExpandedAlphaFactoryService,
    )
    assert issubclass(
        ObservableDisposableExpandedAlphaFactoryService,
        CandidateObservedAllLaneEvidenceFactoryService,
    )
    assert mro.index(DisposableExpandedAlphaFactoryService) < mro.index(
        CandidateObservedAllLaneEvidenceFactoryService
    )


def test_structural_closure_never_publishes_cross_cycle_microstructure_join():
    original = _summary(observed_at=NOW)

    corrected = _without_mixed_microstructure_funnel(original)

    assert "microstructure" in original.rejection_funnels
    assert "microstructure" not in corrected.rejection_funnels
    assert corrected.rejection_funnels["carry"] == original.rejection_funnels["carry"]
    assert corrected.summary_id != original.summary_id


def test_microstructure_dashboard_funnel_comes_from_same_alpha_cycle_projection():
    assert "microstructure" in DASHBOARD_ALPHA_FUNNEL_LANES


def test_closure_recovery_is_required_only_after_presentation_sla(tmp_path):
    store = EvidenceStore(tmp_path / "closure-recovery.sqlite3")
    ledger = ResearchClosureSummaryLedger(store)
    ledger.record(_summary(observed_at=NOW - timedelta(minutes=10)))

    assert closure_recovery_required(store, now=NOW, stale_seconds=1_800.0) is False

    ledger.record(
        _summary(
            observed_at=NOW - timedelta(hours=1),
            summary_id="summary-2",
        )
    )
    assert closure_recovery_required(store, now=NOW, stale_seconds=1_800.0) is True


def test_render_research_child_uses_observability_repair_launcher():
    assert RESEARCH_OBSERVABILITY_COMMAND[-1] == (
        "inefficiency_engine.disposable_heavy_job_research_observability"
    )

from pathlib import Path

from inefficiency_engine.disposable_research_worker import _due


def test_alpha_evidence_cadence_is_unchanged():
    due = [sequence for sequence in range(1, 31) if _due(sequence, 10, 0.75)]
    assert due == [7, 17, 27]


def test_dashboard_critical_evidence_runs_before_heavy_shadow_tail():
    source = Path("src/inefficiency_engine/disposable_research_worker.py").read_text()

    refresh = source.index('_record_progress("pre_alpha_source_refresh")')
    alpha = source.index('_record_progress("alpha_forward_evidence")')
    publish = source.index('_reconcile_and_publish("critical_evidence")')
    core = source.index('_record_progress("core_shadow")')
    certification = source.index('_record_progress("allocation_certification")')

    assert refresh < alpha < publish < core < certification
    assert source.count('_record_progress("pre_alpha_source_refresh")') == 1
    assert source.count('_record_progress("alpha_forward_evidence")') == 1
    assert '"critical_evidence_before_heavy_tail": True' in source


def test_source_bootstrap_is_published_before_core_shadow():
    source = Path("src/inefficiency_engine/disposable_research_worker.py").read_text()
    bootstrap = source.index('_record_progress("provider_gap_bootstrap")')
    publish = source.index('_reconcile_and_publish("source_bootstrap")')
    core = source.index('_record_progress("core_shadow")')

    assert bootstrap < publish < core
    assert "sequence % alpha_every == 1" in source

from __future__ import annotations

import inspect

from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService


def test_disposable_alpha_factory_never_network_backfills_history_inline():
    source = inspect.getsource(DisposableExpandedAlphaFactoryService._ensure_historical_research)

    assert "ensure_backfilled" not in source
    assert "_historical_backfill_attempted = True" in source
    assert "_historical_backfill_report = None" in source


def test_disposable_alpha_factory_delegates_mechanism_mutation_to_permanent_worker():
    source = inspect.getsource(DisposableExpandedAlphaFactoryService.run_evidence_cycle)

    assert "_mechanism_evidence_enabled = False" in source

from __future__ import annotations

from inefficiency_engine.batched_cycle_history import BatchedCycleHistoricalResearch
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService


class DisposableExpandedAlphaFactoryService(MemoryBoundedExpandedAlphaFactoryService):
    """Research alpha factory that consumes persisted history but never backfills it.

    Historical backfill/replay maintenance is a separate disposable heavy job. Keeping
    that authority out of the research process prevents two distinct heavyweight
    domains from materializing during one subprocess lifetime.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._historical_research = BatchedCycleHistoricalResearch(self.store)

    async def _ensure_historical_research(self) -> None:
        # A separate history subprocess owns all network backfill. Research may read
        # whatever durable history already exists, but it never expands the archive.
        self._historical_backfill_attempted = True
        self._historical_backfill_report = None

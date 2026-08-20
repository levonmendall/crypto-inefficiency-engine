from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import Opportunity
from inefficiency_engine.service import OpportunityService, _books_for_opportunity, _opportunity_signature


DEFAULT_SHADOW_MAX_OPPORTUNITIES = 16
HARD_SHADOW_MAX_OPPORTUNITIES = 48


def runtime_shadow_opportunity_limit() -> int:
    """Return a fail-safe production L2 working-set bound.

    Full public-market discovery remains unbounded. This limit only controls how
    many discovered opportunities may own heavyweight order-book/executability
    state inside one multi-horizon shadow cycle.
    """

    raw = os.getenv("CIE_SHADOW_MAX_OPPORTUNITIES")
    if raw is None or not raw.strip():
        return DEFAULT_SHADOW_MAX_OPPORTUNITIES
    try:
        requested = int(raw)
    except ValueError:
        return DEFAULT_SHADOW_MAX_OPPORTUNITIES
    return min(HARD_SHADOW_MAX_OPPORTUNITIES, max(1, requested))


def _rank_key(opportunity: Opportunity) -> tuple[float, float, float, str]:
    return (
        float(getattr(opportunity, "net_annualized_return", 0.0) or 0.0),
        float(getattr(opportunity, "net_edge_bps_per_hour", 0.0) or 0.0),
        float(getattr(opportunity, "gross_edge_bps_per_hour", 0.0) or 0.0),
        _opportunity_signature(opportunity),
    )


def select_rotating_shadow_opportunities(
    opportunities: list[Opportunity],
    *,
    limit: int,
    cursor: int,
) -> tuple[list[Opportunity], int]:
    """Bound expensive L2 work while retaining top-priority and rotating coverage.

    Half of the working set is reserved for the strongest cheap pre-L2 candidates
    on every cycle. The other half walks the remaining discovered universe so no
    tail opportunity is permanently excluded by the memory guard.
    """

    ranked = sorted(opportunities, key=_rank_key, reverse=True)
    unique: list[Opportunity] = []
    seen: set[str] = set()
    for opportunity in ranked:
        signature = _opportunity_signature(opportunity)
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(opportunity)

    bounded_limit = max(1, int(limit))
    if len(unique) <= bounded_limit:
        return unique, 0

    priority_count = max(1, bounded_limit // 2)
    priority = unique[:priority_count]
    remainder = unique[priority_count:]
    exploration_slots = bounded_limit - len(priority)
    if exploration_slots <= 0 or not remainder:
        return priority, 0

    start = max(0, int(cursor)) % len(remainder)
    exploration = [
        remainder[(start + index) % len(remainder)]
        for index in range(min(exploration_slots, len(remainder)))
    ]
    next_cursor = (start + len(exploration)) % len(remainder)
    return priority + exploration, next_cursor


class MemoryBoundedShadowService(OpportunityService):
    """Production shadow service with a bounded heavyweight L2 working set.

    The service still discovers and persists the complete public opportunity set on
    every scan. Only order-book collection, tier qualification and in-memory shadow
    retention are bounded. Verification horizons stay locked to the initial cycle's
    selected signatures, preserving point-in-time comparability while preventing one
    shadow surface from exceeding a small Render worker's memory envelope.
    """

    def __init__(
        self,
        core: OpportunityService,
        store: EvidenceStore | None = None,
        *,
        max_opportunities: int | None = None,
    ) -> None:
        # Reuse the canonical research graph instead of constructing another set of
        # provider/detector objects inside the same 512 MB process.
        self.settings = core.settings
        self.evidence_store = store if store is not None else core.evidence_store
        self.adapter_registry = core.adapter_registry
        self.detector_registry = core.detector_registry
        self.risk_gate = core.risk_gate
        requested = runtime_shadow_opportunity_limit() if max_opportunities is None else int(max_opportunities)
        self.max_opportunities = min(HARD_SHADOW_MAX_OPPORTUNITIES, max(1, requested))
        self._rotation_cursor = 0
        self._cycle_scope_signatures: set[str] | None = None

    def _initial_scope(self, opportunities: list[Opportunity]) -> list[Opportunity]:
        selected, next_cursor = select_rotating_shadow_opportunities(
            opportunities,
            limit=self.max_opportunities,
            cursor=self._rotation_cursor,
        )
        self._rotation_cursor = next_cursor
        self._cycle_scope_signatures = {_opportunity_signature(opportunity) for opportunity in selected}
        return selected

    def _verification_scope(self, opportunities: list[Opportunity]) -> list[Opportunity]:
        signatures = self._cycle_scope_signatures or set()
        return [
            opportunity
            for opportunity in opportunities
            if _opportunity_signature(opportunity) in signatures
        ]

    async def collect_live_executability(self) -> ScanSnapshot:
        started_at, funding_quotes, market_quotes, opportunities, providers = await self._collect_live_inputs()
        if self._cycle_scope_signatures is None:
            execution_opportunities = self._initial_scope(opportunities)
        else:
            execution_opportunities = self._verification_scope(opportunities)

        if execution_opportunities:
            order_books, book_statuses = await self.adapter_registry.collect_books_for_opportunities(
                execution_opportunities
            )
        else:
            order_books, book_statuses = [], []
        providers.extend(book_statuses)

        qualification_time = datetime.now(timezone.utc)
        latency_resolver = self.empirical_latency_resolver()
        executability = [
            qualify_opportunity(
                opportunity,
                _books_for_opportunity(opportunity, order_books),
                self.settings,
                notionals_usd=self.settings.capital_tiers_usd,
                now=qualification_time,
                latency_model_resolver=latency_resolver.resolve,
            )
            for opportunity in execution_opportunities
        ]
        completed_at = datetime.now(timezone.utc)

        scan_id = "unpersisted"
        if self.evidence_store is not None:
            # Persist complete market discovery even though the returned working set
            # is intentionally compact. This preserves broad research/replay evidence.
            scan_id = self.evidence_store.record_scan(
                funding_quotes=funding_quotes,
                market_quotes=market_quotes,
                opportunities=opportunities,
                providers=providers,
                started_at=started_at,
                completed_at=completed_at,
                analysis_config=asdict(self.settings),
                order_books=order_books,
                executability=executability,
            )

        # The full raw market and full discovery set have already been durably
        # recorded. Do not keep a second copy alive through every shadow horizon.
        return ScanSnapshot(
            scan_id=scan_id,
            started_at=started_at,
            completed_at=completed_at,
            providers=providers,
            funding_quotes=[],
            market_quotes=[],
            opportunities=execution_opportunities,
            order_books=order_books,
            executability=executability,
            analysis_config=asdict(self.settings),
        )

    async def run_shadow_cycle(self, *, delay_seconds: float | None = None):
        self._cycle_scope_signatures = None
        try:
            return await super().run_shadow_cycle(delay_seconds=delay_seconds)
        finally:
            # Never allow a prior cycle's scope to leak into a fresh market scan.
            self._cycle_scope_signatures = None

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService, UnifiedPaperCandidate


class CanonicalPortfolioAllocatorService(UnifiedPaperAllocatorService):
    """Allocator view restricted to families the canonical ledger can settle.

    The canonical ledger currently supports spot directional-long alpha positions.
    Core CEX and CEX↔DEX candidates remain fully researched and certified by the
    general allocator, but evaluating those unsupported settlement families again
    inside the liveness-critical portfolio cycle only adds provider I/O that can
    never produce a position. This allocator consumes the latest executable scan
    already persisted by the portfolio's point-in-time scan and evaluates alpha
    only, preserving broad research without putting it on the accounting path.
    """

    def _latest_executable_snapshot(self) -> tuple[ScanSnapshot | None, str | None]:
        if self.alpha_factory is None:
            return None, "AlphaFactoryUnavailable"
        store = self.alpha_factory.store
        executable_scan_ids = select(store.order_books.c.scan_id)
        query = (
            select(store.scans.c.scan_id)
            .where(store.scans.c.scan_id.in_(executable_scan_ids))
            .order_by(store.scans.c.completed_at.desc())
            .limit(1)
        )
        with store.engine.connect() as db:
            scan_id = db.execute(query).scalar_one_or_none()
        if scan_id is None:
            return None, "PersistedExecutabilitySnapshotUnavailable"

        snapshot = store.load_scan(scan_id)
        age = max(
            0.0,
            (datetime.now(timezone.utc) - snapshot.completed_at).total_seconds(),
        )
        freshness_limit = max(
            30.0,
            float(self.settings.max_quote_age_seconds),
            float(self.settings.max_order_book_age_seconds),
        )
        if age > freshness_limit:
            return None, "PersistedExecutabilitySnapshotStale"
        return snapshot, None

    async def _candidates_with_failures(
        self,
        *,
        total_capital_usd: float,
    ) -> tuple[list[UnifiedPaperCandidate], list[dict[str, object]]]:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")

        snapshot, snapshot_error = self._latest_executable_snapshot()
        if snapshot is None:
            return [], [{
                "family": "alpha",
                "error_type": snapshot_error or "PersistedExecutabilitySnapshotUnavailable",
                "reason": "canonical portfolio requires a fresh persisted executable market snapshot",
            }]

        try:
            rows = await self._alpha_family_candidates(
                snapshot=snapshot,
                total_capital_usd=total_capital_usd,
            )
        except Exception as exc:
            return [], [{
                "family": "alpha",
                "error_type": type(exc).__name__,
                "reason": "settlement-compatible alpha candidate family failed closed",
            }]

        rows.sort(
            key=lambda item: (
                item.expected_return_on_reserved_capital,
                item.expected_profit_usd_per_deployment,
                -item.capital_required_usd,
            ),
            reverse=True,
        )
        return rows, []

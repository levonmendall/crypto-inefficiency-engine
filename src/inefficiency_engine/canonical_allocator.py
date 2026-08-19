from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService, UnifiedPaperCandidate


class CanonicalPortfolioAllocatorService(UnifiedPaperAllocatorService):
    """Allocator view restricted to families the canonical ledger can settle.

    The canonical ledger currently supports spot directional-long alpha positions.
    Broad CEX/CEX↔DEX executability remains a research concern. Canonical allocation
    consumes the latest fresh persisted market snapshot and only asks for current
    L2 after an alpha candidate has already cleared statistical qualification.
    """

    def _latest_market_snapshot(self) -> tuple[ScanSnapshot | None, str | None]:
        if self.alpha_factory is None:
            return None, "AlphaFactoryUnavailable"
        store = self.alpha_factory.store
        query = select(store.scans.c.scan_id).order_by(store.scans.c.completed_at.desc()).limit(1)
        with store.engine.connect() as db:
            scan_id = db.execute(query).scalar_one_or_none()
        if scan_id is None:
            return None, "PersistedMarketSnapshotUnavailable"

        snapshot = store.load_scan(scan_id)
        age = max(
            0.0,
            (datetime.now(timezone.utc) - snapshot.completed_at).total_seconds(),
        )
        freshness_limit = max(30.0, float(self.settings.max_quote_age_seconds))
        if age > freshness_limit:
            return None, "PersistedMarketSnapshotStale"
        if not snapshot.market_quotes:
            return None, "PersistedMarketSnapshotEmpty"
        return snapshot, None

    async def _candidates_with_failures(
        self,
        *,
        total_capital_usd: float,
    ) -> tuple[list[UnifiedPaperCandidate], list[dict[str, object]]]:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")

        snapshot, snapshot_error = self._latest_market_snapshot()
        if snapshot is None:
            return [], [{
                "family": "alpha",
                "error_type": snapshot_error or "PersistedMarketSnapshotUnavailable",
                "reason": "canonical portfolio requires a fresh persisted public market snapshot",
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

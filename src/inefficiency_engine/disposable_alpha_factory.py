from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import func, select

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.batched_cycle_history import BatchedCycleHistoricalResearch
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessMechanismExecutionService,
)
from inefficiency_engine.models import MarketKind, OpportunityLeg, Side


ALPHA_L2_WORKER_ID = "alpha-l2-research-sampling"
DEFAULT_ALPHA_L2_ASSET_BATCH_SIZE = 4
MAX_ALPHA_L2_ASSET_BATCH_SIZE = 8


class DisposableExpandedAlphaFactoryService(AllLaneEvidenceFactoryService):
    """Production disposable all-lane research factory.

    Research consumes persisted history but never performs network backfill in the
    disposable research process. It does, however, run the executable alpha
    refinements and all five native mechanism forward loops. Mechanism outcomes are
    also fed into the Release D subtractive lane-success calibration plane.

    L2-dependent strategies must not depend on an unrelated structural arbitrage or
    carry signal existing first. The disposable alpha cycle therefore samples a
    rotating, bounded slice of the active top-volume universe and asks the existing
    adapter registry for books. The registry's normal batching, timeouts, depth trim
    and memory fail-closed behavior remain the only L2 acquisition implementation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._historical_research = BatchedCycleHistoricalResearch(self.store)
        self.mechanism_execution = EvidenceVelocityLaneSuccessMechanismExecutionService(
            self.core,
            self.store,
        )

    async def _ensure_historical_research(self) -> None:
        # A separate history subprocess owns all network backfill. Research may read
        # whatever durable history already exists, but it never expands the archive.
        self._historical_backfill_attempted = True
        self._historical_backfill_report = None

    def _l2_batch_size(self) -> int:
        try:
            requested = int(
                os.getenv(
                    "CIE_ALPHA_L2_ASSET_BATCH_SIZE",
                    str(DEFAULT_ALPHA_L2_ASSET_BATCH_SIZE),
                )
            )
        except ValueError:
            requested = DEFAULT_ALPHA_L2_ASSET_BATCH_SIZE
        return max(1, min(MAX_ALPHA_L2_ASSET_BATCH_SIZE, requested))

    def _l2_rotation_index(self) -> int:
        try:
            with self.store.engine.connect() as db:
                value = db.execute(
                    select(func.count())
                    .select_from(self.store.worker_heartbeats)
                    .where(self.store.worker_heartbeats.c.worker_id == ALPHA_L2_WORKER_ID)
                ).scalar_one()
            return max(0, int(value or 0))
        except Exception:
            return 0

    def _active_l2_assets(self, snapshot) -> tuple[str, ...]:
        registry = getattr(self.core, "adapter_registry", None)
        managed = getattr(getattr(registry, "coinbase", None), "assets", ())
        ordered: list[str] = []
        seen: set[str] = set()
        for raw in managed:
            asset = str(raw).upper().strip()
            if asset and asset not in seen:
                ordered.append(asset)
                seen.add(asset)
        if not ordered:
            for quote in snapshot.market_quotes:
                asset = quote.asset.upper()
                if asset and asset not in seen:
                    ordered.append(asset)
                    seen.add(asset)
        return tuple(ordered)

    def _select_l2_assets(self, snapshot) -> tuple[str, ...]:
        assets = self._active_l2_assets(snapshot)
        if not assets:
            return ()
        batch_size = min(len(assets), self._l2_batch_size())
        start = (self._l2_rotation_index() * batch_size) % len(assets)
        return tuple(assets[(start + offset) % len(assets)] for offset in range(batch_size))

    async def _collect_alpha_l2_snapshot(self, quote_collector):
        snapshot = await quote_collector()
        selected_assets = self._select_l2_assets(snapshot)
        selected = set(selected_assets)
        pseudo_opportunities = []
        seen: set[tuple[str, str, MarketKind, str | None]] = set()
        for quote in snapshot.market_quotes:
            if quote.asset.upper() not in selected:
                continue
            if quote.market_kind not in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                continue
            key = (
                quote.venue,
                quote.asset.upper(),
                quote.market_kind,
                quote.contract_key or quote.symbol,
            )
            if key in seen:
                continue
            seen.add(key)
            pseudo_opportunities.append(
                SimpleNamespace(
                    legs=[
                        OpportunityLeg(
                            venue=quote.venue,
                            asset=quote.asset.upper(),
                            market_kind=quote.market_kind,
                            side=Side.LONG,
                            symbol=quote.symbol,
                            quote_currency=quote.quote_currency,
                            contract_key=quote.contract_key,
                            expires_at=quote.expires_at,
                            reference_price=quote.mid,
                        )
                    ]
                )
            )

        books = []
        book_statuses = []
        if pseudo_opportunities:
            books, book_statuses = (
                await self.core.adapter_registry.collect_books_for_opportunities(
                    pseudo_opportunities
                )
            )

        observed_at = datetime.now(timezone.utc)
        if books or book_statuses:
            # Persist only the L2/status payload. Re-persisting the quote snapshot
            # would duplicate market-history observations and bias time-series alpha.
            self.store.record_scan(
                funding_quotes=[],
                market_quotes=[],
                opportunities=[],
                providers=book_statuses,
                started_at=observed_at,
                completed_at=observed_at,
                analysis_config={
                    "alpha_l2_sampling": True,
                    "selected_assets": list(selected_assets),
                    "bounded_asset_batch_size": len(selected_assets),
                    "quote_history_duplicated": False,
                    "allocation_authority": False,
                    "paper_only": True,
                },
                order_books=books,
            )

        state = "success" if books else "degraded"
        error_type = None if books else "AlphaL2SampleEmpty"
        try:
            self.store.record_worker_heartbeat(
                worker_id=ALPHA_L2_WORKER_ID,
                state=state,
                error_type=error_type,
                detail={
                    "selected_assets": list(selected_assets),
                    "requested_instrument_count": len(pseudo_opportunities),
                    "retained_book_count": len(books),
                    "provider_status_count": len(book_statuses),
                    "rotating_top_volume_sample": True,
                    "structural_opportunity_required": False,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                    "live_execution_authority": False,
                },
            )
        except Exception:
            pass

        return snapshot.model_copy(
            update={
                "providers": [*snapshot.providers, *book_statuses],
                "order_books": books,
            }
        )

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        """Run alpha + mechanism evidence with independent bounded L2 coverage."""

        original = self.core.collect_live_evidence

        async def collect_with_l2():
            return await self._collect_alpha_l2_snapshot(original)

        self.core.collect_live_evidence = collect_with_l2
        try:
            return await super().run_evidence_cycle(
                total_capital_usd=total_capital_usd
            )
        finally:
            self.core.collect_live_evidence = original

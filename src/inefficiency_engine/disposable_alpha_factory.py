from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import func, select

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.batched_cycle_history import BatchedCycleHistoricalResearch
from inefficiency_engine.models import MarketKind, OpportunityLeg, Side
from inefficiency_engine.yield_shadow_runtime import (
    YieldResearchShadowMechanismExecutionService,
)


ALPHA_L2_WORKER_ID = "alpha-l2-research-sampling"
DEFAULT_ALPHA_L2_ASSET_BATCH_SIZE = 4
MAX_ALPHA_L2_ASSET_BATCH_SIZE = 8


class DisposableExpandedAlphaFactoryService(AllLaneEvidenceFactoryService):
    """Production disposable all-lane research factory.

    Research consumes persisted history but never performs network backfill in the
    disposable research process. It runs executable alpha refinements and the
    bounded L2 sampler, while native mechanism-forward mutation is owned by the
    permanent mechanism worker. Durable mechanism outcomes remain available to the
    Release D subtractive lane-success calibration plane.

    L2-dependent strategies must not depend on an unrelated structural arbitrage or
    carry signal existing first. When the permanent source owner is current, research
    now reuses that exact durable quote/funding/L2 snapshot instead of launching a
    duplicate market/L2 request and then persisting an L2-only row that can race the
    canonical bridge. If the permanent owner is unavailable, the existing bounded
    network sampler remains the fail-safe fallback.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._historical_research = BatchedCycleHistoricalResearch(self.store)
        self.mechanism_execution = YieldResearchShadowMechanismExecutionService(
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

    def _latest_permanent_source_snapshot(self):
        """Load the newest durable full snapshot owned by the source process."""

        with self.store.engine.connect() as db:
            rows = list(
                db.execute(
                    select(
                        self.store.scans.c.scan_id,
                        self.store.scans.c.analysis_config_json,
                    )
                    .order_by(self.store.scans.c.completed_at.desc())
                    .limit(200)
                ).mappings()
            )
        for row in rows:
            raw = row["analysis_config_json"] or "{}"
            try:
                config = json.loads(raw) if isinstance(raw, str) else {}
            except (TypeError, ValueError):
                config = {}
            if isinstance(config, dict) and bool(config.get("permanent_source_plane")):
                return self.store.load_scan(str(row["scan_id"]))
        return None

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
                    "shared_with_native_mechanism_research": True,
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

    async def refresh_l2_source_snapshot(self, quote_collector=None):
        """Return one current bounded L2 snapshot without duplicating source work.

        The permanent source process is the canonical owner of market/funding/L2
        acquisition. While its durable heartbeat is current, research and the
        permanent mechanism worker reuse its newest full snapshot. This prevents a
        duplicate provider cycle from delaying mechanism evidence and prevents an
        L2-only maintenance scan from becoming the bridge's accidental latest input.
        If the source owner is unavailable, the original bounded sampler is retained
        as a fail-safe source-recovery path.
        """

        try:
            from inefficiency_engine.permanent_source_plane import (
                permanent_source_plane_current,
            )

            stale_seconds = max(
                120.0,
                float(
                    getattr(
                        self.core.settings,
                        "worker_heartbeat_stale_seconds",
                        180.0,
                    )
                ),
            )
            if permanent_source_plane_current(
                self.store,
                max_age_seconds=stale_seconds,
            ):
                snapshot = await asyncio.to_thread(
                    self._latest_permanent_source_snapshot
                )
                if snapshot is not None:
                    return snapshot.model_copy(
                        update={
                            "analysis_config": {
                                **snapshot.analysis_config,
                                "reused_by_alpha_factory": True,
                                "duplicate_provider_requests": 0,
                                "allocation_authority": False,
                                "live_execution_authority": False,
                                "paper_only": True,
                            }
                        }
                    )
        except Exception:
            # Fail-safe fallback below intentionally retains the pre-existing source
            # recovery path if durable source ownership cannot be established.
            pass

        collector = quote_collector or self.core.collect_live_evidence
        return await self._collect_alpha_l2_snapshot(collector)

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        """Run disposable alpha research against one independent bounded L2 snapshot.

        The earlier production repair routed only ``collect_live_evidence`` through
        the independent L2 sampler. Native maker/liquidity-provision research calls
        ``collect_live_executability`` internally, so it could still receive zero books
        whenever structural arbitrage/carry discovery produced no opportunity first.
        One cached bounded snapshot now serves both research entry points for the same
        evidence cycle. This removes that accidental dependency without increasing the
        L2 batch size or weakening any qualification gate.

        Small unit-test/fallback cores that expose only ``collect_live_evidence`` remain
        supported; production cores expose both entry points and therefore receive the
        shared native-mechanism wiring.
        """

        original_evidence = self.core.collect_live_evidence
        original_executability = getattr(self.core, "collect_live_executability", None)
        cached_snapshot = None

        async def collect_with_l2():
            nonlocal cached_snapshot
            if cached_snapshot is None:
                cached_snapshot = await self.refresh_l2_source_snapshot(original_evidence)
            return cached_snapshot

        self.core.collect_live_evidence = collect_with_l2
        if original_executability is not None:
            self.core.collect_live_executability = collect_with_l2
        mechanism_evidence_enabled = getattr(self, "_mechanism_evidence_enabled", True)
        self._mechanism_evidence_enabled = False
        try:
            return await super().run_evidence_cycle(
                total_capital_usd=total_capital_usd
            )
        finally:
            self._mechanism_evidence_enabled = mechanism_evidence_enabled
            self.core.collect_live_evidence = original_evidence
            if original_executability is not None:
                self.core.collect_live_executability = original_executability

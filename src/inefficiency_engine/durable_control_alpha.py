from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import and_, func, or_, select

from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.models import MarketKind, MarketQuote


class DurableControlAlphaFactoryService(DisposableExpandedAlphaFactoryService):
    """Alpha promotion surface for the canonical control process.

    The permanent control plane is a durable-state projection process. It must never
    perform provider acquisition while publishing the qualified-opportunity bridge.
    The inherited bounded alpha promotion already prefers order books carried by the
    persisted source snapshot; its only network escape hatch is
    ``_bounded_current_l2_cost`` when that snapshot lacks the exact book required by a
    candidate. Disable that escape hatch here so missing/stale executable depth fails
    the candidate closed instead of turning the control process into an exchange
    client.

    The control bridge also only needs short-history rows for venue/assets present in
    its current persisted source snapshot. Filter that exact cohort in SQL and cache it
    for the lifetime of the one-shot control executor. This preserves the same
    point-in-time history and strategy thresholds while preventing repeated promotion
    passes from transferring/parsing unrelated append-only market history.

    Disposable research keeps its existing bounded provider fallback in its own
    process. This subclass is used only by ``permanent_control_worker`` and therefore
    changes plumbing, not any economic/statistical/source/risk qualification hurdle.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._durable_missing_depth_count = 0
        self._durable_history_cache_key = None
        self._durable_history_cache: dict[
            tuple[str, str, MarketKind], list[MarketQuote]
        ] = {}
        self._durable_history_cache_hits = 0
        self._durable_history_query_count = 0

    def _history_for_snapshot(self, snapshot):
        """Read the unchanged short history only for the current source cohort."""

        current_keys = self._current_keys(snapshot)
        if not current_keys:
            self._durable_history_cache_key = None
            self._durable_history_cache = {}
            return {}

        cache_key = (
            str(snapshot.scan_id),
            snapshot.completed_at.isoformat(),
            tuple(
                sorted(
                    (venue, asset, market_kind.value)
                    for venue, asset, market_kind in current_keys
                )
            ),
        )
        if self._durable_history_cache_key == cache_key:
            self._durable_history_cache_hits = int(
                getattr(self, "_durable_history_cache_hits", 0)
            ) + 1
            return {
                key: list(values)
                for key, values in self._durable_history_cache.items()
            }

        cutoff = snapshot.completed_at - timedelta(hours=self._effective_history_hours())
        table = self.store.market_quotes
        pair_filters = [
            and_(table.c.venue == venue, func.upper(table.c.asset) == asset)
            for venue, asset in sorted({(venue, asset) for venue, asset, _ in current_keys})
        ]
        query = (
            select(table.c.payload_json)
            .where(table.c.observed_at >= cutoff.isoformat())
            .where(table.c.observed_at <= snapshot.completed_at.isoformat())
            .where(or_(*pair_filters))
            .order_by(table.c.observed_at)
        )

        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
        with self.store.engine.connect() as db:
            self._durable_history_query_count = int(
                getattr(self, "_durable_history_query_count", 0)
            ) + 1
            payloads = db.execution_options(stream_results=True).execute(query).scalars()
            for payload in payloads:
                quote = MarketQuote.model_validate_json(payload)
                key = (quote.venue, quote.asset.upper(), quote.market_kind)
                if key in current_keys:
                    grouped[key].append(quote)

        self._durable_history_cache_key = cache_key
        self._durable_history_cache = {
            key: list(values)
            for key, values in grouped.items()
        }
        return {
            key: list(values)
            for key, values in self._durable_history_cache.items()
        }

    async def _bounded_current_l2_cost(self, candidate):
        # Deliberately do not call the adapter registry. The caller already attempted
        # to locate a fresh matching book inside the current persisted ScanSnapshot.
        # No such book means allocation-grade current execution cost is unavailable.
        self._durable_missing_depth_count += 1
        return None

    async def _current_l2_cost(self, candidate):
        # Defense in depth for future refactors: any direct use of the old live-L2
        # promotion hook from the canonical control process must fail visibly rather
        # than silently reintroduce network I/O.
        raise RuntimeError("ProviderAccessForbiddenInCanonicalControl")

    async def refresh_l2_source_snapshot(self, quote_collector=None):
        raise RuntimeError("ProviderAcquisitionForbiddenInCanonicalControl")

    async def promoted_candidates(self, snapshot, *, total_capital_usd: float):
        self._durable_missing_depth_count = 0
        self._durable_history_cache_key = None
        self._durable_history_cache = {}
        self._durable_history_cache_hits = 0
        self._durable_history_query_count = 0
        rows = await super().promoted_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
        )
        for candidate in rows:
            candidate.features.update(
                {
                    "canonical_control_durable_promotion": True,
                    "current_cost_source": "persisted_order_book",
                    "provider_requests_used_for_promotion": False,
                }
            )
        return rows

    def durable_promotion_diagnostics(self) -> dict[str, object]:
        return {
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "missing_current_executable_depth_count": self._durable_missing_depth_count,
            "short_history_query_count": int(
                getattr(self, "_durable_history_query_count", 0)
            ),
            "short_history_cache_hits": int(
                getattr(self, "_durable_history_cache_hits", 0)
            ),
            "short_history_current_cohort_only": True,
            "missing_depth_policy": "fail_closed",
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

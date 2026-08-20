from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import and_, func, or_, select

from inefficiency_engine.alpha_factory import AlphaStrategyRegistry
from inefficiency_engine.bounded_alpha_factory import BoundedExpandedAlphaFactoryService
from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote


class MemoryBoundedExpandedAlphaFactoryService(BoundedExpandedAlphaFactoryService):
    """Exact fast-alpha history plus isolated compact long-horizon trend evidence."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_registry = self.registry
        self._cycle_strategy = CycleAwareMultiHorizonTrendStrategy()
        strategies = list(getattr(self._base_registry, "_strategies", []))
        self.registry = AlphaStrategyRegistry([*strategies, self._cycle_strategy])
        self._cycle_history_cache: dict[
            tuple[str, str, MarketKind], list[MarketQuote]
        ] = {}
        self._cycle_history_pairs: frozenset[tuple[str, str]] = frozenset()
        self._cycle_history_refreshed_at = None

    def _short_history_hours(self) -> float:
        settings = self._expanded_settings
        active_lookbacks = (
            float(settings.alpha_momentum_lookback_hours),
            float(settings.alpha_reversion_lookback_hours),
            float(settings.alpha_cross_sectional_lookback_hours),
            float(settings.alpha_microstructure_lookback_hours),
            float(settings.alpha_event_max_age_hours),
        )
        return max(1.0, *active_lookbacks)

    def _effective_history_hours(self) -> float:
        # Preserve the v3.7.1 contract exactly for every existing fast strategy.
        return min(
            max(1.0, float(self.settings.alpha_history_hours)),
            self._short_history_hours(),
        )

    def _current_keys(
        self,
        snapshot: ScanSnapshot,
    ) -> set[tuple[str, str, MarketKind]]:
        return {
            (quote.venue, quote.asset.upper(), quote.market_kind)
            for quote in snapshot.market_quotes
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}
        }

    def _compact_cycle_history(
        self,
        snapshot: ScanSnapshot,
        current_keys: set[tuple[str, str, MarketKind]],
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        pairs = frozenset((venue, asset) for venue, asset, _ in current_keys)
        if not pairs:
            return {}

        refresh_hours = CycleAwareMultiHorizonTrendStrategy.history_refresh(
            self._expanded_settings
        )
        if (
            self._cycle_history_refreshed_at is not None
            and self._cycle_history_pairs == pairs
        ):
            age = max(
                0.0,
                (
                    snapshot.completed_at - self._cycle_history_refreshed_at
                ).total_seconds()
                / 3600.0,
            )
            if age < refresh_hours:
                return self._cycle_history_cache

        long_cutoff = snapshot.completed_at - timedelta(
            hours=CycleAwareMultiHorizonTrendStrategy.required_history_hours(
                self._expanded_settings
            )
        )
        recent_cutoff = snapshot.completed_at - timedelta(
            hours=self._effective_history_hours()
        )

        table = self.store.market_quotes
        pair_filters = [
            and_(table.c.venue == venue, table.c.asset == asset)
            for venue, asset in sorted(pairs)
        ]
        day_bucket = func.substr(table.c.observed_at, 1, 10)
        ranked = (
            select(
                table.c.payload_json.label("payload_json"),
                func.row_number()
                .over(
                    partition_by=(table.c.venue, table.c.asset, day_bucket),
                    order_by=table.c.id.desc(),
                )
                .label("daily_rank"),
            )
            .where(table.c.observed_at >= long_cutoff.isoformat())
            .where(table.c.observed_at < recent_cutoff.isoformat())
            .where(or_(*pair_filters))
            .subquery()
        )
        query = select(ranked.c.payload_json).where(
            ranked.c.daily_rank
            <= CycleAwareMultiHorizonTrendStrategy.rows_per_day(self._expanded_settings)
        )

        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
        with self.store.engine.connect() as db:
            for payload in db.execution_options(stream_results=True).execute(query).scalars():
                quote = MarketQuote.model_validate_json(payload)
                key = (quote.venue, quote.asset.upper(), quote.market_kind)
                if key in current_keys:
                    grouped[key].append(quote)

        compact = {
            key: sorted(values, key=lambda item: item.observed_at)
            for key, values in grouped.items()
        }
        self._cycle_history_cache = compact
        self._cycle_history_pairs = pairs
        self._cycle_history_refreshed_at = snapshot.completed_at
        return compact

    def _history_for_snapshot(
        self,
        snapshot: ScanSnapshot,
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        """Return the original exact short-history projection, unchanged."""
        current_keys = self._current_keys(snapshot)
        if not current_keys:
            return {}

        cutoff = snapshot.completed_at - timedelta(hours=self._effective_history_hours())
        query = (
            select(self.store.market_quotes.c.payload_json)
            .where(self.store.market_quotes.c.observed_at >= cutoff.isoformat())
            .where(
                self.store.market_quotes.c.observed_at
                <= snapshot.completed_at.isoformat()
            )
            .order_by(self.store.market_quotes.c.observed_at)
        )
        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
        with self.store.engine.connect() as db:
            payloads = db.execution_options(stream_results=True).execute(query).scalars()
            for payload in payloads:
                quote = MarketQuote.model_validate_json(payload)
                key = (quote.venue, quote.asset.upper(), quote.market_kind)
                if key in current_keys:
                    grouped[key].append(quote)
        return grouped

    def _cycle_history_for_snapshot(
        self,
        snapshot: ScanSnapshot,
        exact: dict[tuple[str, str, MarketKind], list[MarketQuote]],
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        current_keys = self._current_keys(snapshot)
        compact = self._compact_cycle_history(snapshot, current_keys)
        combined: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
        for key in current_keys:
            rows = [*compact.get(key, []), *exact.get(key, [])]
            seen: set[tuple[str, str, float]] = set()
            deduped: list[MarketQuote] = []
            for quote in sorted(rows, key=lambda item: item.observed_at):
                identity = (quote.observed_at.isoformat(), quote.symbol, quote.mid)
                if identity in seen:
                    continue
                seen.add(identity)
                deduped.append(quote)
            if deduped:
                combined[key] = deduped
        return combined

    def discover(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ):
        exact = self._history_for_snapshot(snapshot)
        rows = self._base_registry.discover(
            snapshot,
            exact,
            self._expanded_settings,  # type: ignore[arg-type]
            total_capital_usd=total_capital_usd,
        )
        try:
            cycle_history = self._cycle_history_for_snapshot(snapshot, exact)
            rows.extend(
                self._cycle_strategy.discover(
                    snapshot,
                    cycle_history,
                    self._expanded_settings,  # type: ignore[arg-type]
                    total_capital_usd=total_capital_usd,
                )
            )
        except Exception:
            # New slow-history research fails closed without suppressing legacy alpha.
            pass
        rows.sort(
            key=lambda item: (item.expected_net_return, item.confidence_score),
            reverse=True,
        )
        return rows

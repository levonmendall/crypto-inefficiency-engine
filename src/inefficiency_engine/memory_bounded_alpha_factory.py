from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import and_, func, or_, select

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaStrategyRegistry
from inefficiency_engine.bounded_alpha_factory import BoundedExpandedAlphaFactoryService
from inefficiency_engine.cycle_probation import (
    PROBATIONARY_FORWARD_MEAN_HAIRCUT,
    PROBATIONARY_HISTORICAL_MEAN_HAIRCUT,
    CycleHistoricalResearch,
)
from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.incremental_forward_sizing import (
    FORWARD_EVIDENCE_STEP_SAMPLES,
    incremental_forward_policy,
)
from inefficiency_engine.models import MarketKind, MarketQuote


class MemoryBoundedExpandedAlphaFactoryService(BoundedExpandedAlphaFactoryService):
    """Exact fast-alpha history plus isolated compact long-horizon trend evidence.

    The cycle-aware lane owns separated historical backfill/replay plus an
    incrementally sized pre-certification paper tier. Historical replay can warm
    signal construction and support bounded paper learning, but it never increments
    genuine forward qualification counts and can never grant live execution authority.
    """

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
        self._historical_research = CycleHistoricalResearch(self.store)
        self._historical_backfill_attempted = False
        self._historical_backfill_report = None

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

    def _historical_assets(self) -> tuple[str, ...]:
        registry = getattr(self.core, "adapter_registry", None)
        coinbase = getattr(registry, "coinbase", None)
        assets = getattr(coinbase, "assets", ("BTC", "ETH", "SOL"))
        return tuple(sorted({str(asset).upper() for asset in assets if str(asset).strip()}))

    async def _ensure_historical_research(self) -> None:
        if self._historical_backfill_attempted:
            return
        self._historical_backfill_attempted = True
        try:
            self._historical_backfill_report = await self._historical_research.ensure_backfilled(
                self._historical_assets()
            )
            # A successful backfill changes the long-history projection. Force one
            # refresh so the current process sees the new history immediately.
            self._cycle_history_refreshed_at = None
            self._cycle_history_cache = {}
        except Exception:
            # Historical acceleration is additive. Failure must not suppress legacy
            # alpha, live evidence collection, or the canonical portfolio worker.
            self._historical_backfill_report = None

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

        # Historical backfill is stored separately from live evidence and merged
        # only into the slow strategy's history projection. It never contaminates
        # live ScanSnapshot records or AlphaForwardOutcome counts.
        try:
            historical = self._historical_research.history(
                start=long_cutoff,
                end=recent_cutoff,
                assets={asset for _, asset, _ in current_keys},
            )
            for key, values in historical.items():
                if key in current_keys:
                    grouped[key].extend(values)
        except Exception:
            pass

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

    async def _probationary_cycle_candidates(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
        fully_promoted: list[AlphaCandidate],
    ) -> list[AlphaCandidate]:
        full_keys = {
            (item.strategy_id, item.asset.upper(), item.direction)
            for item in fully_promoted
        }
        try:
            replay = self._historical_research.replay_summaries(
                self._cycle_strategy,
                self._expanded_settings,
                total_capital_usd=total_capital_usd,
                now=snapshot.completed_at,
            )
        except Exception:
            return []

        eligible: list[AlphaCandidate] = []

        for candidate in self.discover(snapshot, total_capital_usd=total_capital_usd):
            key = (candidate.strategy_id, candidate.asset.upper(), candidate.direction)
            if candidate.strategy_id != self._cycle_strategy.strategy_id or key in full_keys:
                continue

            qualification = self.qualification(candidate)
            health = self.strategy_health(candidate)
            historical = replay.get((candidate.asset.upper(), candidate.direction))
            policy = incremental_forward_policy(
                qualification,
                health,
                historical,
                self._expanded_settings,
            )
            if not policy.eligible or historical is None:
                continue

            book = self._snapshot_book(candidate, snapshot)
            current_cost = (
                self._cost_from_book(candidate, book)
                if book is not None
                else await self._bounded_current_l2_cost(candidate)
            )
            if current_cost is None:
                continue
            current_cost += self._holding_carry_cost(candidate)
            current_net = candidate.expected_gross_return - current_cost
            forward_mean = qualification.mean_realized_net_return or 0.0
            historical_mean = historical.mean_realized_net_return or 0.0
            conservative = min(
                current_net,
                forward_mean * PROBATIONARY_FORWARD_MEAN_HAIRCUT,
                historical_mean * PROBATIONARY_HISTORICAL_MEAN_HAIRCUT,
            )
            if conservative <= self.settings.alpha_min_current_net_return:
                continue

            scale = policy.allocation_fraction
            if scale <= 0:
                continue

            probationary = candidate.model_copy(deep=True)
            probationary.candidate_id = f"probationary:{candidate.candidate_id}"
            probationary.notional_usd *= scale
            probationary.capital_required_usd *= scale
            if probationary.notional_usd < self.settings.alpha_min_notional_usd:
                continue
            probationary.estimated_cost_return = current_cost
            probationary.expected_net_return = conservative
            probationary.expected_profit_usd = probationary.notional_usd * conservative
            # Keep stage='research' deliberately: this candidate has paper authority
            # only through the explicit incremental tier, not full qualification.
            probationary.paper_allocation_eligible = True
            probationary.features.update(
                {
                    "allocation_tier": "incremental_forward_paper",
                    "probationary_paper": True,
                    "forward_evidence_allocation_fraction": policy.allocation_fraction,
                    "forward_evidence_allocation_percent": int(
                        round(policy.allocation_fraction * 100.0)
                    ),
                    "forward_evidence_step_samples": FORWARD_EVIDENCE_STEP_SAMPLES,
                    "forward_independent_samples": qualification.sample_count,
                    "full_forward_sample_target": self.settings.alpha_min_forward_samples,
                    "historical_walk_forward_support_only": True,
                    "historical_replay_samples": historical.sample_count,
                    "historical_replay_hit_rate": historical.hit_rate or 0.0,
                    "historical_replay_mean_net_return": historical.mean_realized_net_return or 0.0,
                    "historical_replay_regime_count": historical.regime_count,
                    "health_recent_sample_count": health.recent_sample_count,
                    "health_recent_mean_net_return": health.recent_mean_net_return or 0.0,
                    "health_recent_hit_rate": health.recent_hit_rate or 0.0,
                    "health_capture_ratio_median": health.forecast_capture_ratio_median or 0.0,
                    "health_recent_to_long_run_ratio": health.recent_to_long_run_ratio or 0.0,
                    "health_max_compounded_drawdown": health.max_compounded_drawdown or 0.0,
                    "health_trailing_loss_streak": health.trailing_loss_streak,
                    "live_execution_authority": False,
                }
            )
            eligible.append(probationary)

        eligible.sort(
            key=lambda item: (item.expected_net_return, item.expected_profit_usd),
            reverse=True,
        )
        return eligible

    async def promoted_candidates(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        await self._ensure_historical_research()
        fully_promoted = await super().promoted_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
        )
        probationary = await self._probationary_cycle_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
            fully_promoted=fully_promoted,
        )
        rows = [*fully_promoted, *probationary]
        rows.sort(
            key=lambda item: (item.expected_net_return, item.expected_profit_usd),
            reverse=True,
        )
        return rows

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        await self._ensure_historical_research()
        return await super().run_evidence_cycle(total_capital_usd=total_capital_usd)

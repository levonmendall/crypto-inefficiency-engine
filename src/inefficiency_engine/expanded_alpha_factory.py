from __future__ import annotations

import math
import statistics
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Protocol

from sqlalchemy import select

from inefficiency_engine.alpha_coverage_strategies import (
    CrossSectionalRelativeValueStrategy,
    EventDrivenStrategy,
    EventLedger,
    EventObservation,
    MicrostructureImbalanceStrategy,
)
from inefficiency_engine.alpha_extensions import (
    FundamentalFactorLedger,
    FundamentalFactorObservation,
    MeanReversionStrategy,
    OnChainFundamentalStrategy,
)
from inefficiency_engine.alpha_factory import (
    AlphaCandidate,
    AlphaEvidenceCycle,
    AlphaFactoryService,
    AlphaForwardOutcome,
    AlphaForwardSignal,
    AlphaQualification,
    AlphaStrategyRegistry,
    TimeSeriesMomentumStrategy,
    _mean_lower,
    _quantile,
    _wilson_lower,
)
from inefficiency_engine.alpha_risk import AlphaRiskController, AlphaStrategyHealth
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot


class FundamentalObservationProvider(Protocol):
    async def collect(self) -> list[FundamentalFactorObservation]: ...


class EventObservationProvider(Protocol):
    async def collect(self) -> list[EventObservation]: ...


class _ExpandedSettingsView:
    """Forward-compatible defaults for post-V2.0 alpha families and controls."""

    _defaults = {
        "alpha_reversion_horizon_hours": 6.0,
        "alpha_reversion_lookback_hours": 24.0,
        "alpha_reversion_min_robust_z": 2.0,
        "alpha_reversion_forecast_shrinkage": 0.20,
        "alpha_reversion_max_expected_return": 0.03,
        "alpha_factor_max_age_hours": 48.0,
        "alpha_factor_min_count": 2,
        "alpha_factor_min_abs_score": 0.35,
        "alpha_factor_max_expected_return": 0.03,
        "alpha_factor_return_scale": 0.02,
        "alpha_factor_forecast_shrinkage": 0.25,
        "alpha_factor_horizon_hours": 24.0,
        "alpha_factor_lookback_hours": 24.0 * 14.0,
        # V3 active coverage-closure alpha families.
        "alpha_cross_sectional_lookback_hours": 48.0,
        "alpha_cross_sectional_horizon_hours": 12.0,
        "alpha_cross_sectional_min_assets": 3,
        "alpha_cross_sectional_min_abs_z": 0.75,
        "alpha_cross_sectional_forecast_shrinkage": 0.20,
        "alpha_cross_sectional_max_expected_return": 0.03,
        "alpha_cross_sectional_max_candidates": 6,
        "alpha_microstructure_horizon_hours": 0.25,
        "alpha_microstructure_lookback_hours": 6.0,
        "alpha_microstructure_depth_levels": 5,
        "alpha_microstructure_min_abs_imbalance": 0.20,
        "alpha_microstructure_return_scale": 0.012,
        "alpha_microstructure_max_expected_return": 0.006,
        "alpha_microstructure_max_candidates": 6,
        "alpha_event_horizon_hours": 12.0,
        "alpha_event_max_age_hours": 24.0,
        "alpha_event_min_abs_surprise": 0.30,
        "alpha_event_return_scale": 0.04,
        "alpha_event_forecast_shrinkage": 0.25,
        "alpha_event_max_expected_return": 0.04,
        # Adaptive health controls. These remain strictly subtractive.
        "alpha_health_recent_window": 12,
        "alpha_health_min_recent_samples": 8,
        "alpha_health_min_recent_mean_return": 0.00025,
        "alpha_health_min_recent_hit_rate": 0.50,
        "alpha_health_min_capture_ratio": 0.35,
        "alpha_health_full_capture_ratio": 0.80,
        "alpha_health_min_recent_to_long_ratio": 0.35,
        "alpha_health_max_drawdown": 0.06,
        "alpha_health_max_trailing_losses": 4,
        "alpha_health_capital_multiplier_floor": 0.25,
    }

    def __init__(self, base):
        self._base = base

    def __getattr__(self, name: str):
        if name in self._defaults:
            return self._defaults[name]
        return getattr(self._base, name)


class ExpandedAlphaFactoryService(AlphaFactoryService):
    """Multi-family alpha factory with independent evidence and adaptive sizing.

    Discovery is intentionally broad. Paper promotion remains forward-only and
    requires the base statistical and fresh-L2 gates. Recent health can only
    reduce or revoke paper capital; no research source can manufacture authority.
    """

    def __init__(
        self,
        core,
        store: EvidenceStore,
        *,
        fundamental_provider: FundamentalObservationProvider | None = None,
        event_provider: EventObservationProvider | None = None,
    ):
        self.fundamental_ledger = FundamentalFactorLedger(store)
        self.event_ledger = EventLedger(store)
        registry = AlphaStrategyRegistry([
            TimeSeriesMomentumStrategy(),
            MeanReversionStrategy(),
            OnChainFundamentalStrategy(self.fundamental_ledger),
            CrossSectionalRelativeValueStrategy(),
            MicrostructureImbalanceStrategy(),
            EventDrivenStrategy(self.event_ledger),
        ])
        super().__init__(core, store, registry=registry)
        self.fundamental_provider = fundamental_provider
        self.event_provider = event_provider
        self._expanded_settings = _ExpandedSettingsView(self.settings)
        self.risk_controller = AlphaRiskController(self._expanded_settings)

    def manifests(self):
        return self.registry.manifests()

    def discover(self, snapshot: ScanSnapshot, *, total_capital_usd: float) -> list[AlphaCandidate]:
        return self.registry.discover(
            snapshot,
            self._history(now=snapshot.completed_at),
            self._expanded_settings,  # type: ignore[arg-type]
            total_capital_usd=total_capital_usd,
        )

    def record_fundamental_observation(self, observation: FundamentalFactorObservation) -> str:
        return self.fundamental_ledger.record(observation)

    def fundamental_summary(self) -> dict[str, object]:
        return self.fundamental_ledger.summary()

    def record_event_observation(self, observation: EventObservation) -> str:
        return self.event_ledger.record(observation)

    def event_summary(self) -> dict[str, object]:
        return self.event_ledger.summary()

    @staticmethod
    def _independent_outcomes(outcomes: list[AlphaForwardOutcome]) -> list[AlphaForwardOutcome]:
        selected: list[AlphaForwardOutcome] = []
        next_independent_at = None
        for outcome in sorted(outcomes, key=lambda item: (item.observed_at, item.due_at, item.signal_id)):
            if next_independent_at is not None and outcome.observed_at < next_independent_at:
                continue
            selected.append(outcome)
            next_independent_at = outcome.due_at
        return selected

    def _outcomes_for(self, candidate: AlphaCandidate) -> list[AlphaForwardOutcome]:
        return self._independent_outcomes(self.ledger.outcomes(
            strategy_id=candidate.strategy_id,
            asset=candidate.asset,
            direction=candidate.direction,
        ))

    def strategy_health(self, candidate: AlphaCandidate) -> AlphaStrategyHealth:
        return self.risk_controller.evaluate(candidate, self._outcomes_for(candidate))

    def _has_open_signal(self, candidate: AlphaCandidate, *, now) -> bool:
        events = self.ledger.events
        with self.store.engine.connect() as db:
            signals = list(db.execute(
                select(events.c.signal_id, events.c.due_at)
                .where(events.c.event_type == "signal")
                .where(events.c.strategy_id == candidate.strategy_id)
                .where(events.c.asset == candidate.asset)
                .where(events.c.direction == candidate.direction)
            ))
            completed = set(db.execute(
                select(events.c.signal_id).where(events.c.event_type == "outcome")
            ).scalars())
        # An unresolved matured signal also blocks a new overlapping cohort. Missing
        # settlement data must not silently increase effective sample size.
        return any(signal_id not in completed for signal_id, _ in signals)

    def qualification(self, candidate: AlphaCandidate) -> AlphaQualification:
        outcomes = self._outcomes_for(candidate)
        values = [item.realized_net_return for item in outcomes]
        positives = sum(value > 0 for value in values)
        hit_lower = _wilson_lower(positives, len(values))
        mean_lower = _mean_lower(values)
        regime_values: dict[str, list[float]] = defaultdict(list)
        for item in outcomes:
            regime_values[item.regime].append(item.realized_net_return)
        regime_means = {key: statistics.fmean(rows) for key, rows in regime_values.items() if rows}
        strategy_count = max(1, len(self.registry.manifests()))
        penalty = self.settings.alpha_multiple_testing_penalty_return * math.sqrt(math.log(strategy_count + 1.0))
        required = self.settings.alpha_min_forward_mean_return + penalty
        blockers: list[str] = []
        if len(values) < self.settings.alpha_min_forward_samples:
            blockers.append("insufficient independent forward samples")
        if mean_lower is None or mean_lower <= required:
            blockers.append("forward net-return confidence lower bound is below hurdle")
        if hit_lower is None or hit_lower < self.settings.alpha_min_hit_rate_lower_bound:
            blockers.append("forward hit-rate confidence lower bound is below hurdle")
        if len(regime_means) < self.settings.alpha_min_regimes:
            blockers.append("insufficient regime coverage")
        elif any(value <= self.settings.alpha_min_regime_mean_return for value in regime_means.values()):
            blockers.append("one or more observed regimes have non-qualifying mean return")
        qualified = not blockers
        return AlphaQualification(
            strategy_id=candidate.strategy_id,
            family=candidate.family,
            asset=candidate.asset,
            direction=candidate.direction,
            sample_count=len(values),
            positive_count=positives,
            hit_rate=positives / len(values) if values else None,
            hit_rate_ci_lower=hit_lower,
            mean_realized_net_return=statistics.fmean(values) if values else None,
            mean_realized_net_return_ci_lower=mean_lower,
            p10_realized_net_return=_quantile(values, 0.10),
            worst_realized_net_return=min(values) if values else None,
            regime_count=len(regime_means),
            regime_means=regime_means,
            multiple_testing_penalty_return=penalty,
            required_mean_lower_bound=required,
            statistically_qualified=qualified,
            blockers=blockers,
            paper_allocation_authority=qualified,
            live_execution_authority=False,
            paper_only=True,
        )

    async def promoted_candidates(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        statistically_promoted = await super().promoted_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
        )
        healthy: list[AlphaCandidate] = []
        for candidate in statistically_promoted:
            health = self.strategy_health(candidate)
            if not health.healthy_for_paper_allocation or health.capital_multiplier <= 0:
                continue
            scaled_notional = candidate.notional_usd * health.capital_multiplier
            if scaled_notional < self.settings.alpha_min_notional_usd:
                continue
            candidate.notional_usd = scaled_notional
            candidate.capital_required_usd *= health.capital_multiplier
            candidate.expected_profit_usd = candidate.expected_net_return * scaled_notional
            candidate.features.update({
                "health_score": health.health_score,
                "health_capital_multiplier": health.capital_multiplier,
                "health_recent_mean_net_return": health.recent_mean_net_return or 0.0,
                "health_recent_hit_rate": health.recent_hit_rate or 0.0,
                "health_capture_ratio_median": health.forecast_capture_ratio_median or 0.0,
                "health_recent_to_long_run_ratio": health.recent_to_long_run_ratio or 0.0,
                "health_max_compounded_drawdown": health.max_compounded_drawdown or 0.0,
                "health_trailing_loss_streak": health.trailing_loss_streak,
            })
            healthy.append(candidate)
        healthy.sort(key=lambda item: (item.expected_net_return, item.expected_profit_usd), reverse=True)
        return healthy

    async def health_snapshot(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for candidate in self.discover(snapshot, total_capital_usd=total_capital_usd):
            rows.append({
                "candidate": candidate.model_dump(mode="json"),
                "qualification": self.qualification(candidate).model_dump(mode="json"),
                "health": self.strategy_health(candidate).model_dump(mode="json"),
            })
        return rows

    async def _refresh_fundamentals(self) -> None:
        if self.fundamental_provider is None:
            return
        try:
            observations = await self.fundamental_provider.collect()
        except Exception:
            return
        for observation in observations:
            self.fundamental_ledger.record(observation)

    async def _refresh_events(self) -> None:
        if self.event_provider is None:
            return
        try:
            observations = await self.event_provider.collect()
        except Exception:
            return
        for observation in observations:
            self.event_ledger.record(observation)

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None) -> AlphaEvidenceCycle:
        await self._refresh_fundamentals()
        await self._refresh_events()
        total_capital_usd = total_capital_usd or self.settings.alpha_research_capital_usd
        snapshot = await self.core.collect_live_evidence()
        matured = 0
        current_index = {
            (quote.venue, quote.asset.upper(), quote.market_kind, quote.symbol): quote
            for quote in snapshot.market_quotes
        }
        for signal in self.ledger.pending_signals(now=snapshot.completed_at):
            candidate = signal.candidate
            quote = current_index.get((candidate.venue, candidate.asset, candidate.market_kind, candidate.symbol))
            if quote is None or quote.mid <= 0:
                continue
            raw = quote.mid / candidate.entry_reference_price - 1.0
            directional = raw if candidate.direction == "long" else -raw
            outcome = AlphaForwardOutcome(
                signal_id=signal.signal_id,
                strategy_id=candidate.strategy_id,
                family=candidate.family,
                asset=candidate.asset,
                direction=candidate.direction,
                venue=candidate.venue,
                market_kind=candidate.market_kind,
                symbol=candidate.symbol,
                observed_at=candidate.observed_at,
                due_at=signal.due_at,
                matured_at=snapshot.completed_at,
                horizon_hours=candidate.horizon_hours,
                regime=candidate.regime,
                predicted_net_return=candidate.expected_net_return,
                entry_price=candidate.entry_reference_price,
                exit_price=quote.mid,
                realized_gross_return=directional,
                realized_net_return=directional - candidate.estimated_cost_return,
                correct_direction=directional > 0,
            )
            self.ledger.record_outcome(outcome)
            matured += 1

        candidates = self.discover(snapshot, total_capital_usd=total_capital_usd)
        recorded = 0
        for candidate in candidates:
            if self._has_open_signal(candidate, now=snapshot.completed_at):
                continue
            signal = AlphaForwardSignal(
                signal_id=candidate.candidate_id,
                candidate=candidate,
                due_at=candidate.observed_at + timedelta(hours=candidate.horizon_hours),
            )
            self.ledger.record_signal(signal)
            recorded += 1
        return AlphaEvidenceCycle(
            cycle_id=uuid.uuid4().hex,
            observed_at=snapshot.completed_at,
            candidate_count=len(candidates),
            signals_recorded=recorded,
            outcomes_matured=matured,
        )

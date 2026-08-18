from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from inefficiency_engine.config import Settings
from inefficiency_engine.models import EmpiricalLatencyModel, Opportunity, ShadowCycle, ShadowObservation, Strategy

if TYPE_CHECKING:
    from inefficiency_engine.evidence import EvidenceStore


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    q = min(1.0, max(0.0, q))
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def _pair_adverse_selection_bps(observation: ShadowObservation) -> float | None:
    values = [
        max(0.0, leg.adverse_selection_bps)
        for leg in observation.leg_attribution
        if leg.adverse_selection_bps is not None
    ]
    return sum(values) if values else None


def _venue_pair(opportunity: Opportunity) -> str:
    return "|".join(leg.venue for leg in opportunity.legs)


def _same_capital(observed: float, requested: float) -> bool:
    return abs(observed - requested) <= max(1e-6, abs(requested) * 1e-9)


class EmpiricalLatencyResolver:
    """Resolve the narrowest statistically valid shadow cohort for a trade.

    Observation-path latency is measured globally because it is a property of the
    collection runtime. Fillability and adverse-selection distributions are then
    selected hierarchically by strategy -> venue pair -> asset -> capital size.
    Sparse cohorts fall back one level at a time rather than silently borrowing a
    global distribution while claiming opportunity-specific evidence.
    """

    def __init__(self, store: EvidenceStore | None, settings: Settings):
        self.settings = settings
        self.quantile = min(1.0, max(0.0, settings.empirical_latency_quantile))
        self.observations: list[ShadowObservation] = []
        if store is not None:
            with store.engine.connect() as db:
                payloads = list(
                    db.execute(
                        select(store.shadow_cycles.c.payload_json).order_by(store.shadow_cycles.c.completed_at)
                    ).scalars()
                )
            cycles = [ShadowCycle.model_validate_json(payload) for payload in payloads]
            self.observations = [observation for cycle in cycles for observation in cycle.observations]

        latency_by_scan: dict[str, float] = {}
        for observation in self.observations:
            if observation.verification_scan_latency_ms is not None:
                latency_by_scan[observation.verification_scan_id] = observation.verification_scan_latency_ms
        self.latency_samples = list(latency_by_scan.values())
        self.reference_latency_ms = _quantile(self.latency_samples, self.quantile)
        self.available_horizons = sorted({
            observation.delay_seconds
            for observation in self.observations
            if observation.delay_seconds > 0 and observation.pair_fillable is not None
        })
        self.reference_horizon_seconds: float | None = None
        if self.reference_latency_ms is not None:
            latency_seconds = self.reference_latency_ms / 1000.0
            self.reference_horizon_seconds = next(
                (horizon for horizon in self.available_horizons if horizon >= latency_seconds),
                None,
            )

    def resolve(
        self,
        opportunity: Opportunity | None = None,
        notional_usd_per_leg: float | None = None,
        *,
        strategy: Strategy | str | None = None,
        venue_pair: str | None = None,
        asset: str | None = None,
    ) -> EmpiricalLatencyModel:
        if opportunity is not None:
            strategy = opportunity.strategy
            venue_pair = _venue_pair(opportunity)
            asset = opportunity.asset
        strategy_value = strategy.value if isinstance(strategy, Strategy) else strategy

        p50 = _quantile(self.latency_samples, 0.50)
        p90 = _quantile(self.latency_samples, 0.90)
        p95 = _quantile(self.latency_samples, 0.95)
        reference_horizon = self.reference_horizon_seconds

        base_rows = [
            observation
            for observation in self.observations
            if reference_horizon is not None
            and abs(observation.delay_seconds - reference_horizon) < 1e-9
            and observation.pair_fillable is not None
        ]

        scopes: list[tuple[str, tuple[str, ...]]] = []
        if strategy_value and venue_pair and asset and notional_usd_per_leg is not None:
            scopes.append(("strategy+venue_pair+asset+capital", ("strategy", "venue_pair", "asset", "capital")))
        if strategy_value and venue_pair and asset:
            scopes.append(("strategy+venue_pair+asset", ("strategy", "venue_pair", "asset")))
        if strategy_value and venue_pair:
            scopes.append(("strategy+venue_pair", ("strategy", "venue_pair")))
        if strategy_value:
            scopes.append(("strategy", ("strategy",)))
        scopes.append(("global", ()))

        def matches(row: ShadowObservation, dimensions: tuple[str, ...]) -> bool:
            if "strategy" in dimensions and row.strategy.value != strategy_value:
                return False
            if "venue_pair" in dimensions and row.venue_pair != venue_pair:
                return False
            if "asset" in dimensions and row.asset != asset:
                return False
            if "capital" in dimensions:
                if notional_usd_per_leg is None or not _same_capital(row.notional_usd_per_leg, notional_usd_per_leg):
                    return False
            return True

        minimum = max(1, self.settings.empirical_latency_min_samples)
        scope_rows: dict[str, list[ShadowObservation]] = {}
        scope_candidate_counts: dict[str, int] = {}
        skipped: list[str] = []
        selected_scope = scopes[-1][0]
        selected_dimensions = scopes[-1][1]
        selected_rows: list[ShadowObservation] = []

        for name, dimensions in scopes:
            rows = [row for row in base_rows if matches(row, dimensions)]
            scope_rows[name] = rows
            scope_candidate_counts[name] = len(rows)
            adverse_count = sum(_pair_adverse_selection_bps(row) is not None for row in rows)
            if len(rows) >= minimum and adverse_count >= minimum:
                selected_scope = name
                selected_dimensions = dimensions
                selected_rows = rows
                break
            if name != "global":
                skipped.append(f"{name}:{len(rows)}")
        else:
            selected_rows = scope_rows.get("global", [])

        fill_probability = None
        reserve_probability = None
        capture_probability = None
        hedge_recovery_probability = None
        adverse_samples: list[float] = []
        if selected_rows:
            fill_probability = sum(bool(row.pair_fillable) for row in selected_rows) / len(selected_rows)
            reserve_probability = sum(bool(row.pair_fillable_with_reserve) for row in selected_rows) / len(selected_rows)
            capture_probability = sum(
                bool(row.pair_fillable_with_reserve) and row.survived for row in selected_rows
            ) / len(selected_rows)
            hedge_recovery_probability = sum(bool(row.hedge_recovery_required) for row in selected_rows) / len(selected_rows)
            adverse_samples = [
                adverse
                for row in selected_rows
                if (adverse := _pair_adverse_selection_bps(row)) is not None
            ]

        adverse_p50 = _quantile(adverse_samples, 0.50)
        adverse_p90 = _quantile(adverse_samples, 0.90)
        adverse_p95 = _quantile(adverse_samples, 0.95)

        reasons: list[str] = []
        if not self.settings.empirical_latency_enabled:
            reasons.append("empirical latency model disabled by configuration")
        if len(self.latency_samples) < max(1, self.settings.empirical_latency_min_scan_samples):
            reasons.append(
                f"need {self.settings.empirical_latency_min_scan_samples} unique verification-scan latency samples; have {len(self.latency_samples)}"
            )
        if reference_horizon is None:
            reasons.append("measured latency exceeds available shadow horizons or fill reconstruction is unavailable")
        if len(selected_rows) < minimum:
            reasons.append(
                f"need {minimum} fill-reconstruction cohorts at selected scope; have {len(selected_rows)}"
            )
        if len(adverse_samples) < minimum:
            reasons.append(
                f"need {minimum} adverse-selection samples at selected scope; have {len(adverse_samples)}"
            )

        usable = not reasons
        return EmpiricalLatencyModel(
            model_scope=selected_scope,
            scope_strategy=Strategy(strategy_value) if strategy_value in {item.value for item in Strategy} and "strategy" in selected_dimensions else None,
            scope_venue_pair=venue_pair if "venue_pair" in selected_dimensions else None,
            scope_asset=asset if "asset" in selected_dimensions else None,
            scope_notional_usd_per_leg=notional_usd_per_leg if "capital" in selected_dimensions else None,
            scope_candidate_counts=scope_candidate_counts,
            scope_fallbacks=skipped,
            latency_quantile=self.quantile,
            scan_latency_sample_count=len(self.latency_samples),
            cohort_sample_count=len(selected_rows),
            reference_latency_ms=self.reference_latency_ms,
            reference_horizon_seconds=reference_horizon,
            scan_latency_p50_ms=p50,
            scan_latency_p90_ms=p90,
            scan_latency_p95_ms=p95,
            pair_fill_probability=fill_probability,
            reserve_fill_probability=reserve_probability,
            capture_probability=capture_probability,
            hedge_recovery_probability=hedge_recovery_probability,
            adverse_selection_p50_bps=adverse_p50,
            adverse_selection_p90_bps=adverse_p90,
            adverse_selection_p95_bps=adverse_p95,
            empirical_latency_risk_bps=adverse_p95,
            usable_for_qualification=usable,
            reason=None if usable else "; ".join(reasons),
        )


def build_empirical_latency_model(
    store: EvidenceStore | None,
    settings: Settings,
    opportunity: Opportunity | None = None,
    notional_usd_per_leg: float | None = None,
) -> EmpiricalLatencyModel:
    """Backward-compatible one-shot model builder."""
    return EmpiricalLatencyResolver(store, settings).resolve(
        opportunity,
        notional_usd_per_leg,
    )

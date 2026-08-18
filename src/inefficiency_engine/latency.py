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


def _linear(lower: float | None, upper: float | None, weight: float) -> float | None:
    if lower is None or upper is None:
        return None
    return lower + ((upper - lower) * weight)


def _conservative_probability(lower: float | None, upper: float | None, weight: float) -> float | None:
    value = _linear(lower, upper, weight)
    if value is None or lower is None:
        return None
    return min(lower, value)


def _conservative_risk(lower: float | None, upper: float | None, weight: float) -> float | None:
    value = _linear(lower, upper, weight)
    if value is None or lower is None:
        return None
    return max(lower, value)


def _horizon_key(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def _metrics(rows: list[ShadowObservation]) -> dict[str, float | int | None]:
    if not rows:
        return {
            "count": 0,
            "adverse_count": 0,
            "pair_fill": None,
            "reserve_fill": None,
            "capture": None,
            "hedge_recovery": None,
            "adverse_p50": None,
            "adverse_p90": None,
            "adverse_p95": None,
        }
    adverse = [
        value
        for row in rows
        if (value := _pair_adverse_selection_bps(row)) is not None
    ]
    return {
        "count": len(rows),
        "adverse_count": len(adverse),
        "pair_fill": sum(bool(row.pair_fillable) for row in rows) / len(rows),
        "reserve_fill": sum(bool(row.pair_fillable_with_reserve) for row in rows) / len(rows),
        "capture": sum(bool(row.pair_fillable_with_reserve) and row.survived for row in rows) / len(rows),
        "hedge_recovery": sum(bool(row.hedge_recovery_required) for row in rows) / len(rows),
        "adverse_p50": _quantile(adverse, 0.50),
        "adverse_p90": _quantile(adverse, 0.90),
        "adverse_p95": _quantile(adverse, 0.95),
    }


class EmpiricalLatencyResolver:
    """Resolve a hierarchical empirical fill/latency model for a trade.

    Scan latency is measured globally from unique verification scans. When that
    measured latency falls between shadow checkpoints, v0.8.2 uses both adjacent
    horizons. Probability metrics are interpolated so they can never improve as
    time passes; adverse-selection and hedge-recovery risk can never decrease.
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

        self.reference_lower_horizon_seconds: float | None = None
        self.reference_upper_horizon_seconds: float | None = None
        self.interpolation_weight = 0.0
        if self.reference_latency_ms is not None and self.available_horizons:
            latency_seconds = self.reference_latency_ms / 1000.0
            first = self.available_horizons[0]
            last = self.available_horizons[-1]
            if latency_seconds <= first:
                self.reference_lower_horizon_seconds = first
                self.reference_upper_horizon_seconds = first
            elif latency_seconds <= last:
                exact = next(
                    (h for h in self.available_horizons if abs(h - latency_seconds) < 1e-12),
                    None,
                )
                if exact is not None:
                    self.reference_lower_horizon_seconds = exact
                    self.reference_upper_horizon_seconds = exact
                else:
                    lower = max(h for h in self.available_horizons if h < latency_seconds)
                    upper = min(h for h in self.available_horizons if h > latency_seconds)
                    self.reference_lower_horizon_seconds = lower
                    self.reference_upper_horizon_seconds = upper
                    self.interpolation_weight = (latency_seconds - lower) / (upper - lower)

        self.reference_horizon_seconds = self.reference_upper_horizon_seconds

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

        lower_horizon = self.reference_lower_horizon_seconds
        upper_horizon = self.reference_upper_horizon_seconds
        required_horizons = [] if lower_horizon is None else [lower_horizon]
        if upper_horizon is not None and upper_horizon != lower_horizon:
            required_horizons.append(upper_horizon)

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

        def rows_at(horizon: float, dimensions: tuple[str, ...]) -> list[ShadowObservation]:
            return [
                row for row in self.observations
                if abs(row.delay_seconds - horizon) < 1e-9
                and row.pair_fillable is not None
                and matches(row, dimensions)
            ]

        minimum = max(1, self.settings.empirical_latency_min_samples)
        scope_candidate_counts: dict[str, int] = {}
        scope_horizon_counts: dict[str, dict[str, int]] = {}
        skipped: list[str] = []
        selected_scope = scopes[-1][0]
        selected_dimensions = scopes[-1][1]
        selected_rows_by_horizon: dict[float, list[ShadowObservation]] = {}

        for name, dimensions in scopes:
            horizon_rows = {h: rows_at(h, dimensions) for h in required_horizons}
            horizon_counts = {_horizon_key(h): len(rows) for h, rows in horizon_rows.items()}
            scope_horizon_counts[name] = horizon_counts
            effective_count = min(horizon_counts.values(), default=0)
            scope_candidate_counts[name] = effective_count
            adverse_counts = [
                sum(_pair_adverse_selection_bps(row) is not None for row in rows)
                for rows in horizon_rows.values()
            ]
            effective_adverse = min(adverse_counts, default=0)
            if required_horizons and effective_count >= minimum and effective_adverse >= minimum:
                selected_scope = name
                selected_dimensions = dimensions
                selected_rows_by_horizon = horizon_rows
                break
            if name != "global":
                skipped.append(f"{name}:{effective_count}")
        else:
            selected_rows_by_horizon = {
                h: rows_at(h, scopes[-1][1]) for h in required_horizons
            }

        lower_rows = selected_rows_by_horizon.get(lower_horizon, []) if lower_horizon is not None else []
        upper_rows = selected_rows_by_horizon.get(upper_horizon, []) if upper_horizon is not None else []
        if lower_horizon == upper_horizon:
            upper_rows = lower_rows

        lower_metrics = _metrics(lower_rows)
        upper_metrics = _metrics(upper_rows)
        weight = self.interpolation_weight if lower_horizon != upper_horizon else 0.0

        if lower_horizon == upper_horizon:
            pair_fill = lower_metrics["pair_fill"]
            reserve_fill = lower_metrics["reserve_fill"]
            capture = lower_metrics["capture"]
            hedge_recovery = lower_metrics["hedge_recovery"]
            adverse_p50 = lower_metrics["adverse_p50"]
            adverse_p90 = lower_metrics["adverse_p90"]
            adverse_p95 = lower_metrics["adverse_p95"]
            interpolation_mode = "single_horizon"
        else:
            pair_fill = _conservative_probability(lower_metrics["pair_fill"], upper_metrics["pair_fill"], weight)
            reserve_fill = _conservative_probability(lower_metrics["reserve_fill"], upper_metrics["reserve_fill"], weight)
            capture = _conservative_probability(lower_metrics["capture"], upper_metrics["capture"], weight)
            hedge_recovery = _conservative_risk(lower_metrics["hedge_recovery"], upper_metrics["hedge_recovery"], weight)
            adverse_p50 = _conservative_risk(lower_metrics["adverse_p50"], upper_metrics["adverse_p50"], weight)
            adverse_p90 = _conservative_risk(lower_metrics["adverse_p90"], upper_metrics["adverse_p90"], weight)
            adverse_p95 = _conservative_risk(lower_metrics["adverse_p95"], upper_metrics["adverse_p95"], weight)
            interpolation_mode = "linear_interval"

        effective_count = min(
            int(lower_metrics["count"] or 0),
            int(upper_metrics["count"] or 0),
        ) if required_horizons else 0
        effective_adverse_count = min(
            int(lower_metrics["adverse_count"] or 0),
            int(upper_metrics["adverse_count"] or 0),
        ) if required_horizons else 0

        reasons: list[str] = []
        if not self.settings.empirical_latency_enabled:
            reasons.append("empirical latency model disabled by configuration")
        if len(self.latency_samples) < max(1, self.settings.empirical_latency_min_scan_samples):
            reasons.append(
                f"need {self.settings.empirical_latency_min_scan_samples} unique verification-scan latency samples; have {len(self.latency_samples)}"
            )
        if lower_horizon is None or upper_horizon is None:
            reasons.append("measured latency exceeds available shadow horizons or fill reconstruction is unavailable")
        if effective_count < minimum:
            reasons.append(f"need {minimum} fill-reconstruction cohorts at each reference horizon; have {effective_count}")
        if effective_adverse_count < minimum:
            reasons.append(f"need {minimum} adverse-selection samples at each reference horizon; have {effective_adverse_count}")

        usable = not reasons
        return EmpiricalLatencyModel(
            model_scope=selected_scope,
            scope_strategy=Strategy(strategy_value) if strategy_value in {item.value for item in Strategy} and "strategy" in selected_dimensions else None,
            scope_venue_pair=venue_pair if "venue_pair" in selected_dimensions else None,
            scope_asset=asset if "asset" in selected_dimensions else None,
            scope_notional_usd_per_leg=notional_usd_per_leg if "capital" in selected_dimensions else None,
            scope_candidate_counts=scope_candidate_counts,
            scope_horizon_counts=scope_horizon_counts,
            scope_fallbacks=skipped,
            latency_quantile=self.quantile,
            scan_latency_sample_count=len(self.latency_samples),
            cohort_sample_count=effective_count,
            lower_horizon_sample_count=int(lower_metrics["count"] or 0),
            upper_horizon_sample_count=int(upper_metrics["count"] or 0),
            reference_latency_ms=self.reference_latency_ms,
            reference_horizon_seconds=self.reference_horizon_seconds,
            reference_lower_horizon_seconds=lower_horizon,
            reference_upper_horizon_seconds=upper_horizon,
            interpolation_weight=weight,
            interpolation_mode=interpolation_mode,
            scan_latency_p50_ms=_quantile(self.latency_samples, 0.50),
            scan_latency_p90_ms=_quantile(self.latency_samples, 0.90),
            scan_latency_p95_ms=_quantile(self.latency_samples, 0.95),
            pair_fill_probability=pair_fill,
            reserve_fill_probability=reserve_fill,
            capture_probability=capture,
            hedge_recovery_probability=hedge_recovery,
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
    return EmpiricalLatencyResolver(store, settings).resolve(opportunity, notional_usd_per_leg)

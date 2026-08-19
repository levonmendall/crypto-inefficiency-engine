from __future__ import annotations

from statistics import NormalDist
from typing import TYPE_CHECKING, Callable

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


def _conservative_quality(lower: float | None, upper: float | None, weight: float) -> float | None:
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


def _cluster_key(row: ShadowObservation) -> tuple[str, str]:
    # Capital tiers from the same detected market event are correlated and do not
    # count as independent evidence when a broader scope pools notionals.
    return (row.initial_scan_id, row.opportunity_signature)


def _clusters(rows: list[ShadowObservation]) -> dict[tuple[str, str], list[ShadowObservation]]:
    grouped: dict[tuple[str, str], list[ShadowObservation]] = {}
    for row in rows:
        grouped.setdefault(_cluster_key(row), []).append(row)
    return grouped


def _wilson(successes: int, n: int, confidence: float) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    confidence = min(0.999999, max(0.500001, confidence))
    z = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    phat = successes / n
    z2 = z * z
    denominator = 1.0 + (z2 / n)
    center = (phat + (z2 / (2.0 * n))) / denominator
    margin = z * ((phat * (1.0 - phat) / n + z2 / (4.0 * n * n)) ** 0.5) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def _cluster_probability(
    rows: list[ShadowObservation],
    predicate: Callable[[ShadowObservation], bool],
    *,
    risk: bool,
    confidence: float,
) -> tuple[float | None, float | None, float | None, int]:
    grouped = _clusters(rows)
    if not grouped:
        return None, None, None, 0
    outcomes: list[bool] = []
    for cluster_rows in grouped.values():
        values = [bool(predicate(row)) for row in cluster_rows]
        outcomes.append(any(values) if risk else all(values))
    successes = sum(outcomes)
    probability = successes / len(outcomes)
    lower, upper = _wilson(successes, len(outcomes), confidence)
    return probability, lower, upper, len(outcomes)


def _cluster_numeric(
    rows: list[ShadowObservation],
    extractor: Callable[[ShadowObservation], float | None],
    *,
    risk: bool,
) -> list[float]:
    values: list[float] = []
    for cluster_rows in _clusters(rows).values():
        cluster_values = [value for row in cluster_rows if (value := extractor(row)) is not None]
        if cluster_values:
            values.append(max(cluster_values) if risk else min(cluster_values))
    return values


def _metrics(rows: list[ShadowObservation], confidence: float) -> dict[str, object]:
    pair_fill, pair_low, pair_high, effective = _cluster_probability(
        rows, lambda row: bool(row.pair_fillable), risk=False, confidence=confidence
    )
    reserve_fill, reserve_low, reserve_high, _ = _cluster_probability(
        rows, lambda row: bool(row.pair_fillable_with_reserve), risk=False, confidence=confidence
    )
    capture, capture_low, capture_high, _ = _cluster_probability(
        rows,
        lambda row: bool(row.pair_fillable_with_reserve) and row.survived,
        risk=False,
        confidence=confidence,
    )
    recovery, recovery_low, recovery_high, _ = _cluster_probability(
        rows, lambda row: bool(row.hedge_recovery_required), risk=True, confidence=confidence
    )
    partial_fill, _, _, _ = _cluster_probability(
        rows,
        lambda row: bool(row.partial_fill_state) if row.partial_fill_state is not None else not bool(row.pair_fillable),
        risk=True,
        confidence=confidence,
    )

    adverse = _cluster_numeric(rows, _pair_adverse_selection_bps, risk=True)
    pair_fraction = _cluster_numeric(
        rows,
        lambda row: row.pair_fill_fraction if row.pair_fill_fraction is not None else (1.0 if row.pair_fillable else 0.0),
        risk=False,
    )
    unhedged = _cluster_numeric(rows, lambda row: row.unhedged_fraction, risk=True)
    recovery_loss = _cluster_numeric(rows, lambda row: row.hedge_recovery_loss_proxy_bps, risk=True)

    widths = [
        high - low
        for low, high in ((pair_low, pair_high), (reserve_low, reserve_high), (capture_low, capture_high))
        if low is not None and high is not None
    ]
    return {
        "count": len(rows),
        "effective_count": effective,
        "adverse_count": len(adverse),
        "recovery_loss_count": len(recovery_loss),
        "max_ci_width": max(widths) if widths else None,
        "pair_fill": pair_fill,
        "pair_fill_low": pair_low,
        "pair_fill_high": pair_high,
        "reserve_fill": reserve_fill,
        "reserve_fill_low": reserve_low,
        "reserve_fill_high": reserve_high,
        "capture": capture,
        "capture_low": capture_low,
        "capture_high": capture_high,
        "hedge_recovery": recovery,
        "hedge_recovery_low": recovery_low,
        "hedge_recovery_high": recovery_high,
        "partial_fill": partial_fill,
        "pair_fraction_p10": _quantile(pair_fraction, 0.10),
        "pair_fraction_p50": _quantile(pair_fraction, 0.50),
        "unhedged_p50": _quantile(unhedged, 0.50),
        "unhedged_p90": _quantile(unhedged, 0.90),
        "unhedged_p95": _quantile(unhedged, 0.95),
        "recovery_loss_p50": _quantile(recovery_loss, 0.50),
        "recovery_loss_p90": _quantile(recovery_loss, 0.90),
        "recovery_loss_p95": _quantile(recovery_loss, 0.95),
        "adverse_p50": _quantile(adverse, 0.50),
        "adverse_p90": _quantile(adverse, 0.90),
        "adverse_p95": _quantile(adverse, 0.95),
    }


class EmpiricalLatencyResolver:
    """Resolve the completed v0.8 empirical execution-risk model.

    New evidence measures public L2 request round-trip latency directly. Historical
    v0.8 evidence can fall back to whole-scan duration, but the source is explicit.
    Because the system does not submit orders, order acknowledgement and second-leg
    timing remain assumptions and are added to measured data latency before shadow
    horizons are interpolated. Public L2 supports taker depth reconstruction, not
    maker queue position; queue probability therefore remains intentionally absent.
    """

    def __init__(self, store: EvidenceStore | None, settings: Settings):
        self.settings = settings
        self.quantile = min(1.0, max(0.0, settings.empirical_latency_quantile))
        self.confidence = min(0.999999, max(0.500001, settings.empirical_probability_confidence_level))
        self.observations: list[ShadowObservation] = []
        if store is not None:
            with store.engine.connect() as db:
                payloads = list(
                    db.execute(select(store.shadow_cycles.c.payload_json).order_by(store.shadow_cycles.c.completed_at)).scalars()
                )
            cycles = [ShadowCycle.model_validate_json(payload) for payload in payloads]
            self.observations = [observation for cycle in cycles for observation in cycle.observations]

        scan_by_id: dict[str, float] = {}
        l2_by_id: dict[str, float] = {}
        for observation in self.observations:
            if observation.verification_scan_latency_ms is not None:
                scan_by_id[observation.verification_scan_id] = observation.verification_scan_latency_ms
            if observation.verification_data_path_latency_ms is not None:
                l2_by_id[observation.verification_scan_id] = observation.verification_data_path_latency_ms
        self.scan_latency_samples = list(scan_by_id.values())
        minimum_scan_samples = max(1, settings.empirical_latency_min_scan_samples)
        if len(l2_by_id) >= minimum_scan_samples:
            self.data_latency_samples = list(l2_by_id.values())
            self.data_latency_source = "l2_request_roundtrip"
        elif self.scan_latency_samples:
            self.data_latency_samples = self.scan_latency_samples
            self.data_latency_source = "scan_duration_fallback"
        else:
            self.data_latency_samples = []
            self.data_latency_source = "unavailable"

        self.collector_latency_reference_ms = _quantile(self.data_latency_samples, self.quantile)
        self.effective_reference_ms: float | None = None
        if self.collector_latency_reference_ms is not None:
            self.effective_reference_ms = (
                self.collector_latency_reference_ms
                + max(0.0, settings.expected_order_ack_latency_ms)
                + max(0.0, settings.expected_hedge_latency_ms)
            )

        self.available_horizons = sorted({
            observation.delay_seconds
            for observation in self.observations
            if observation.delay_seconds > 0 and observation.pair_fillable is not None
        })
        self.reference_lower_horizon_seconds: float | None = None
        self.reference_upper_horizon_seconds: float | None = None
        self.interpolation_weight = 0.0
        if self.effective_reference_ms is not None and self.available_horizons:
            latency_seconds = self.effective_reference_ms / 1000.0
            first = self.available_horizons[0]
            last = self.available_horizons[-1]
            if latency_seconds <= first:
                self.reference_lower_horizon_seconds = first
                self.reference_upper_horizon_seconds = first
            elif latency_seconds <= last:
                exact = next((h for h in self.available_horizons if abs(h - latency_seconds) < 1e-12), None)
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
        required_horizons: list[float] = [] if lower_horizon is None else [lower_horizon]
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

        minimum_raw = max(1, self.settings.empirical_latency_min_samples)
        minimum_effective = max(1, self.settings.empirical_latency_min_effective_samples)
        max_ci_width = max(0.0, self.settings.empirical_probability_max_ci_width)
        scope_candidate_counts: dict[str, int] = {}
        scope_horizon_counts: dict[str, dict[str, int]] = {}
        scope_effective_counts: dict[str, dict[str, int]] = {}
        skipped: list[str] = []
        selected_scope = scopes[-1][0]
        selected_dimensions = scopes[-1][1]
        selected_metrics: dict[float, dict[str, object]] = {}
        scope_confidence_valid = False

        for name, dimensions in scopes:
            horizon_metrics = {h: _metrics(rows_at(h, dimensions), self.confidence) for h in required_horizons}
            raw_counts = {_horizon_key(h): int(metric["count"] or 0) for h, metric in horizon_metrics.items()}
            effective_counts = {_horizon_key(h): int(metric["effective_count"] or 0) for h, metric in horizon_metrics.items()}
            scope_horizon_counts[name] = raw_counts
            scope_effective_counts[name] = effective_counts
            effective_raw = min(raw_counts.values(), default=0)
            effective_n = min(effective_counts.values(), default=0)
            adverse_n = min((int(metric["adverse_count"] or 0) for metric in horizon_metrics.values()), default=0)
            recovery_n = min((int(metric["recovery_loss_count"] or 0) for metric in horizon_metrics.values()), default=0)
            ci_width = max((float(metric["max_ci_width"] or 1.0) for metric in horizon_metrics.values()), default=1.0)
            scope_candidate_counts[name] = effective_raw
            valid = bool(required_horizons) and all((
                effective_raw >= minimum_raw,
                effective_n >= minimum_effective,
                adverse_n >= minimum_effective,
                recovery_n >= minimum_effective,
                ci_width <= max_ci_width,
            ))
            if valid:
                selected_scope = name
                selected_dimensions = dimensions
                selected_metrics = horizon_metrics
                scope_confidence_valid = True
                break
            if name != "global":
                skipped.append(
                    f"{name}:raw={effective_raw},effective={effective_n},adverse={adverse_n},recovery={recovery_n},ci_width={ci_width:.4f}"
                )
        else:
            selected_metrics = {h: _metrics(rows_at(h, scopes[-1][1]), self.confidence) for h in required_horizons}

        lower = selected_metrics.get(lower_horizon, {}) if lower_horizon is not None else {}
        upper = selected_metrics.get(upper_horizon, {}) if upper_horizon is not None else {}
        if lower_horizon == upper_horizon:
            upper = lower
        weight = self.interpolation_weight if lower_horizon != upper_horizon else 0.0

        def quality(name: str) -> float | None:
            if lower_horizon == upper_horizon:
                value = lower.get(name)
                return float(value) if isinstance(value, (int, float)) else None
            return _conservative_quality(
                float(lower[name]) if isinstance(lower.get(name), (int, float)) else None,
                float(upper[name]) if isinstance(upper.get(name), (int, float)) else None,
                weight,
            )

        def risk(name: str) -> float | None:
            if lower_horizon == upper_horizon:
                value = lower.get(name)
                return float(value) if isinstance(value, (int, float)) else None
            return _conservative_risk(
                float(lower[name]) if isinstance(lower.get(name), (int, float)) else None,
                float(upper[name]) if isinstance(upper.get(name), (int, float)) else None,
                weight,
            )

        pair_fill = quality("pair_fill")
        pair_low = quality("pair_fill_low")
        pair_high = quality("pair_fill_high")
        reserve_fill = quality("reserve_fill")
        reserve_low = quality("reserve_fill_low")
        reserve_high = quality("reserve_fill_high")
        capture = quality("capture")
        capture_low = quality("capture_low")
        capture_high = quality("capture_high")
        hedge_recovery = risk("hedge_recovery")
        hedge_recovery_low = risk("hedge_recovery_low")
        hedge_recovery_high = risk("hedge_recovery_high")
        partial_fill = risk("partial_fill")
        pair_fraction_p10 = quality("pair_fraction_p10")
        pair_fraction_p50 = quality("pair_fraction_p50")
        unhedged_p50 = risk("unhedged_p50")
        unhedged_p90 = risk("unhedged_p90")
        unhedged_p95 = risk("unhedged_p95")
        recovery_p50 = risk("recovery_loss_p50")
        recovery_p90 = risk("recovery_loss_p90")
        recovery_p95 = risk("recovery_loss_p95")
        adverse_p50 = risk("adverse_p50")
        adverse_p90 = risk("adverse_p90")
        adverse_p95 = risk("adverse_p95")

        effective_raw = min(int(lower.get("count") or 0), int(upper.get("count") or 0)) if required_horizons else 0
        effective_n = min(int(lower.get("effective_count") or 0), int(upper.get("effective_count") or 0)) if required_horizons else 0
        final_widths = [
            high - low
            for low, high in ((pair_low, pair_high), (reserve_low, reserve_high), (capture_low, capture_high))
            if low is not None and high is not None
        ]
        final_ci_width = max(final_widths) if final_widths else None

        reasons: list[str] = []
        if not self.settings.empirical_latency_enabled:
            reasons.append("empirical latency model disabled by configuration")
        if len(self.data_latency_samples) < max(1, self.settings.empirical_latency_min_scan_samples):
            reasons.append(
                f"need {self.settings.empirical_latency_min_scan_samples} data-latency samples; have {len(self.data_latency_samples)}"
            )
        if lower_horizon is None or upper_horizon is None:
            reasons.append("effective decision-to-hedge latency exceeds available shadow horizons or fill reconstruction is unavailable")
        if not scope_confidence_valid:
            reasons.append("no hierarchical cohort passes raw/effective sample, tail-sample, and confidence-width gates at every reference horizon")

        usable = not reasons
        interpolation_mode = "single_horizon" if lower_horizon == upper_horizon else "linear_interval"
        valid_strategy_values = {item.value for item in Strategy}
        return EmpiricalLatencyModel(
            model_scope=selected_scope,
            scope_strategy=Strategy(strategy_value) if strategy_value in valid_strategy_values and "strategy" in selected_dimensions else None,
            scope_venue_pair=venue_pair if "venue_pair" in selected_dimensions else None,
            scope_asset=asset if "asset" in selected_dimensions else None,
            scope_notional_usd_per_leg=notional_usd_per_leg if "capital" in selected_dimensions else None,
            scope_candidate_counts=scope_candidate_counts,
            scope_horizon_counts=scope_horizon_counts,
            scope_effective_counts=scope_effective_counts,
            scope_fallbacks=skipped,
            latency_quantile=self.quantile,
            scan_latency_sample_count=len(self.scan_latency_samples),
            data_latency_sample_count=len(self.data_latency_samples),
            data_latency_source=self.data_latency_source,
            cohort_sample_count=effective_raw,
            effective_sample_size=effective_n,
            lower_horizon_sample_count=int(lower.get("count") or 0),
            upper_horizon_sample_count=int(upper.get("count") or 0),
            reference_latency_ms=self.effective_reference_ms,
            collector_latency_reference_ms=self.collector_latency_reference_ms,
            assumed_order_ack_latency_ms=max(0.0, self.settings.expected_order_ack_latency_ms),
            assumed_second_leg_latency_ms=max(0.0, self.settings.expected_hedge_latency_ms),
            effective_decision_to_hedge_latency_ms=self.effective_reference_ms,
            execution_latency_empirical=False,
            reference_horizon_seconds=self.reference_horizon_seconds,
            reference_lower_horizon_seconds=lower_horizon,
            reference_upper_horizon_seconds=upper_horizon,
            interpolation_weight=weight,
            interpolation_mode=interpolation_mode,
            scan_latency_p50_ms=_quantile(self.scan_latency_samples, 0.50),
            scan_latency_p90_ms=_quantile(self.scan_latency_samples, 0.90),
            scan_latency_p95_ms=_quantile(self.scan_latency_samples, 0.95),
            data_latency_p50_ms=_quantile(self.data_latency_samples, 0.50),
            data_latency_p90_ms=_quantile(self.data_latency_samples, 0.90),
            data_latency_p95_ms=_quantile(self.data_latency_samples, 0.95),
            confidence_level=self.confidence,
            probability_max_ci_width=final_ci_width,
            confidence_gate_passed=scope_confidence_valid,
            pair_fill_probability=pair_fill,
            pair_fill_ci_lower=pair_low,
            pair_fill_ci_upper=pair_high,
            reserve_fill_probability=reserve_fill,
            reserve_fill_ci_lower=reserve_low,
            reserve_fill_ci_upper=reserve_high,
            capture_probability=capture,
            capture_ci_lower=capture_low,
            capture_ci_upper=capture_high,
            hedge_recovery_probability=hedge_recovery,
            hedge_recovery_ci_lower=hedge_recovery_low,
            hedge_recovery_ci_upper=hedge_recovery_high,
            partial_fill_probability=partial_fill,
            pair_fill_fraction_p10=pair_fraction_p10,
            pair_fill_fraction_p50=pair_fraction_p50,
            unhedged_fraction_p50=unhedged_p50,
            unhedged_fraction_p90=unhedged_p90,
            unhedged_fraction_p95=unhedged_p95,
            hedge_recovery_loss_p50_bps=recovery_p50,
            hedge_recovery_loss_p90_bps=recovery_p90,
            hedge_recovery_loss_p95_bps=recovery_p95,
            adverse_selection_p50_bps=adverse_p50,
            adverse_selection_p90_bps=adverse_p90,
            adverse_selection_p95_bps=adverse_p95,
            empirical_latency_risk_bps=adverse_p95,
            fill_model_kind="visible_l2_taker_reconstruction",
            queue_position_supported=False,
            maker_fill_probability=None,
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

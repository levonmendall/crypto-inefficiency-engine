from __future__ import annotations

from statistics import median
from typing import TYPE_CHECKING

from sqlalchemy import select

from inefficiency_engine.config import Settings
from inefficiency_engine.models import EmpiricalLatencyModel, ShadowCycle, ShadowObservation

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


def build_empirical_latency_model(store: EvidenceStore | None, settings: Settings) -> EmpiricalLatencyModel:
    quantile = min(1.0, max(0.0, settings.empirical_latency_quantile))
    if store is None:
        return EmpiricalLatencyModel(
            latency_quantile=quantile,
            usable_for_qualification=False,
            reason="evidence persistence is not configured",
        )

    with store.engine.connect() as db:
        payloads = list(
            db.execute(
                select(store.shadow_cycles.c.payload_json).order_by(store.shadow_cycles.c.completed_at)
            ).scalars()
        )
    cycles = [ShadowCycle.model_validate_json(payload) for payload in payloads]
    observations = [observation for cycle in cycles for observation in cycle.observations]

    latency_by_scan: dict[str, float] = {}
    for observation in observations:
        if observation.verification_scan_latency_ms is not None:
            latency_by_scan[observation.verification_scan_id] = observation.verification_scan_latency_ms
    latency_samples = list(latency_by_scan.values())
    p50 = _quantile(latency_samples, 0.50)
    p90 = _quantile(latency_samples, 0.90)
    p95 = _quantile(latency_samples, 0.95)
    reference_latency = _quantile(latency_samples, quantile)

    available_horizons = sorted({
        observation.delay_seconds
        for observation in observations
        if observation.delay_seconds > 0 and observation.pair_fillable is not None
    })
    reference_horizon: float | None = None
    if reference_latency is not None:
        latency_seconds = reference_latency / 1000.0
        reference_horizon = next((h for h in available_horizons if h >= latency_seconds), None)

    cohort_rows: list[ShadowObservation] = []
    if reference_horizon is not None:
        cohort_rows = [
            observation
            for observation in observations
            if abs(observation.delay_seconds - reference_horizon) < 1e-9
            and observation.pair_fillable is not None
        ]

    fill_probability = None
    reserve_probability = None
    capture_probability = None
    hedge_recovery_probability = None
    adverse_samples: list[float] = []
    if cohort_rows:
        fill_probability = sum(bool(row.pair_fillable) for row in cohort_rows) / len(cohort_rows)
        reserve_probability = sum(bool(row.pair_fillable_with_reserve) for row in cohort_rows) / len(cohort_rows)
        capture_probability = sum(bool(row.pair_fillable_with_reserve) and row.survived for row in cohort_rows) / len(cohort_rows)
        hedge_recovery_probability = sum(bool(row.hedge_recovery_required) for row in cohort_rows) / len(cohort_rows)
        for row in cohort_rows:
            adverse = _pair_adverse_selection_bps(row)
            if adverse is not None:
                adverse_samples.append(adverse)

    adverse_p50 = _quantile(adverse_samples, 0.50)
    adverse_p90 = _quantile(adverse_samples, 0.90)
    adverse_p95 = _quantile(adverse_samples, 0.95)

    reasons: list[str] = []
    if not settings.empirical_latency_enabled:
        reasons.append("empirical latency model disabled by configuration")
    if len(latency_samples) < max(1, settings.empirical_latency_min_scan_samples):
        reasons.append(
            f"need {settings.empirical_latency_min_scan_samples} unique verification-scan latency samples; have {len(latency_samples)}"
        )
    if len(cohort_rows) < max(1, settings.empirical_latency_min_samples):
        reasons.append(
            f"need {settings.empirical_latency_min_samples} fill-reconstruction cohorts at reference horizon; have {len(cohort_rows)}"
        )
    if reference_horizon is None:
        reasons.append("measured latency exceeds available shadow horizons or fill reconstruction is unavailable")
    if adverse_p95 is None:
        reasons.append("adverse-selection distribution is unavailable")

    usable = not reasons
    return EmpiricalLatencyModel(
        latency_quantile=quantile,
        scan_latency_sample_count=len(latency_samples),
        cohort_sample_count=len(cohort_rows),
        reference_latency_ms=reference_latency,
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

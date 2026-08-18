from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from statistics import mean, median

from inefficiency_engine.models import (
    CapitalTierQualification,
    Opportunity,
    OrderBookSnapshot,
    ShadowCycle,
    ShadowFailureCause,
    ShadowLegAttribution,
    ShadowObservation,
    Side,
)


def opportunity_signature(opportunity: Opportunity) -> str:
    """Stable economic signature that ignores observation timestamps and IDs."""
    legs = "|".join(
        f"{leg.venue}:{leg.asset}:{leg.market_kind.value}:{leg.side.value}"
        for leg in opportunity.legs
    )
    return f"{opportunity.strategy.value}:{opportunity.asset}:{legs}"


def venue_pair(opportunity: Opportunity) -> str:
    return "|".join(leg.venue for leg in opportunity.legs)


def time_of_day_bucket(observed_at: datetime) -> str:
    return f"{observed_at.hour:02d}:00Z"


def expected_return_bucket(value: float) -> str:
    if value < 0.10:
        return "<10%"
    if value < 0.20:
        return "10-20%"
    if value < 0.50:
        return "20-50%"
    if value < 1.00:
        return "50-100%"
    return ">=100%"


def _book_map(books: list[OrderBookSnapshot]) -> dict[tuple[str, str, str], OrderBookSnapshot]:
    return {(book.venue, book.asset, book.market_kind.value): book for book in books}


def _book_metrics(book: OrderBookSnapshot, side: Side, target_quantity: float | None) -> dict[str, float | None]:
    best_bid = max(level.price for level in book.bids)
    best_ask = min(level.price for level in book.asks)
    mid = (best_bid + best_ask) / 2.0
    spread_bps = ((best_ask - best_bid) / mid) * 10_000.0 if mid > 0 else None
    levels = sorted(
        book.asks if side == Side.LONG else book.bids,
        key=lambda level: level.price,
        reverse=side == Side.SHORT,
    )
    best_price = levels[0].price
    available_base_quantity = sum(level.size for level in levels)
    available_depth_usd = sum(level.price * level.size for level in levels)
    depth_multiple = (
        available_base_quantity / target_quantity
        if target_quantity is not None and target_quantity > 0
        else None
    )

    if not target_quantity or target_quantity <= 0:
        return {
            "best_price": best_price,
            "spread_bps": spread_bps,
            "available_depth_usd": available_depth_usd,
            "available_base_quantity": available_base_quantity,
            "depth_multiple": depth_multiple,
            "slippage_bps": None,
        }

    remaining = target_quantity
    filled_quantity = 0.0
    filled_notional = 0.0
    for level in levels:
        if remaining <= 1e-12:
            break
        take = min(remaining, level.size)
        filled_quantity += take
        filled_notional += take * level.price
        remaining -= take

    if filled_quantity <= 0:
        slippage_bps = None
    else:
        average_price = filled_notional / filled_quantity
        if side == Side.LONG:
            slippage_bps = max(0.0, ((average_price / best_price) - 1.0) * 10_000.0)
        else:
            slippage_bps = max(0.0, (1.0 - (average_price / best_price)) * 10_000.0)

    return {
        "best_price": best_price,
        "spread_bps": spread_bps,
        "available_depth_usd": available_depth_usd,
        "available_base_quantity": available_base_quantity,
        "depth_multiple": depth_multiple,
        "slippage_bps": slippage_bps,
    }


def build_leg_attribution(
    opportunity: Opportunity,
    initial_books: list[OrderBookSnapshot],
    verification_books: list[OrderBookSnapshot],
    *,
    target_quantity: float | None,
) -> tuple[list[ShadowLegAttribution], float | None]:
    initial_map = _book_map(initial_books)
    verification_map = _book_map(verification_books)
    rows: list[ShadowLegAttribution] = []
    adverse_moves: list[float] = []

    for leg in opportunity.legs:
        key = (leg.venue, leg.asset, leg.market_kind.value)
        initial_book = initial_map.get(key)
        verification_book = verification_map.get(key)
        initial = _book_metrics(initial_book, leg.side, target_quantity) if initial_book else {}
        verification = _book_metrics(verification_book, leg.side, target_quantity) if verification_book else {}
        initial_best = initial.get("best_price")
        verification_best = verification.get("best_price")
        adverse: float | None = None
        if isinstance(initial_best, (int, float)) and isinstance(verification_best, (int, float)):
            if leg.side == Side.LONG:
                adverse = ((verification_best / initial_best) - 1.0) * 10_000.0
            else:
                adverse = ((initial_best / verification_best) - 1.0) * 10_000.0
            adverse_moves.append(adverse)

        rows.append(
            ShadowLegAttribution(
                venue=leg.venue,
                asset=leg.asset,
                market_kind=leg.market_kind,
                side=leg.side,
                initial_best_price=initial_best,
                verification_best_price=verification_best,
                adverse_selection_bps=adverse,
                initial_spread_bps=initial.get("spread_bps"),
                verification_spread_bps=verification.get("spread_bps"),
                initial_available_depth_usd=initial.get("available_depth_usd"),
                verification_available_depth_usd=verification.get("available_depth_usd"),
                initial_available_base_quantity=initial.get("available_base_quantity"),
                verification_available_base_quantity=verification.get("available_base_quantity"),
                initial_depth_multiple=initial.get("depth_multiple"),
                verification_depth_multiple=verification.get("depth_multiple"),
                initial_slippage_bps=initial.get("slippage_bps"),
                verification_slippage_bps=verification.get("slippage_bps"),
            )
        )

    divergence = (max(adverse_moves) - min(adverse_moves)) if len(adverse_moves) >= 2 else None
    return rows, divergence


def reconstruct_pair_fill_state(
    leg_attribution: list[ShadowLegAttribution],
    *,
    reserve_ratio: float,
) -> tuple[bool, bool, bool]:
    """Reconstruct visible pair fillability without claiming real queue fills.

    `pair_fillable` means both legs retained at least 1.0x the original target
    base quantity. `pair_fillable_with_reserve` also preserves the configured
    hedge-liquidity reserve. `hedge_recovery_required` marks asymmetric visible
    depth where one leg remained fillable and the other did not.
    """
    depth_multiples = [leg.verification_depth_multiple for leg in leg_attribution]
    if not depth_multiples or any(value is None for value in depth_multiples):
        return False, False, False
    full = [float(value) >= 1.0 for value in depth_multiples if value is not None]
    reserve = [float(value) >= max(1.0, reserve_ratio) for value in depth_multiples if value is not None]
    pair_fillable = bool(full) and all(full)
    pair_fillable_with_reserve = bool(reserve) and all(reserve)
    hedge_recovery_required = any(full) and not all(full)
    return pair_fillable, pair_fillable_with_reserve, hedge_recovery_required


def classify_shadow_failure(
    *,
    current_present: bool,
    provider_failed: bool,
    verification_tier: CapitalTierQualification | None,
    initial_tier: CapitalTierQualification,
    hedge_leg_divergence_bps: float | None,
    slippage_expansion_threshold_bps: float,
    hedge_divergence_threshold_bps: float,
) -> ShadowFailureCause:
    reason = (verification_tier.rejection_reason or "").lower() if verification_tier else ""
    if provider_failed or "stale" in reason or "time skew" in reason or "missing order book" in reason:
        return ShadowFailureCause.STALE_DATA_PROVIDER_FAILURE
    if not current_present:
        return ShadowFailureCause.SIGNAL_DISAPPEARED
    if "depth" in reason or "fill" in reason or "liquidity reserve" in reason:
        return ShadowFailureCause.INSUFFICIENT_DEPTH

    if verification_tier is not None:
        slippage_expansion = verification_tier.observed_entry_slippage_bps - initial_tier.observed_entry_slippage_bps
        if hedge_leg_divergence_bps is not None and hedge_leg_divergence_bps >= hedge_divergence_threshold_bps:
            return ShadowFailureCause.HEDGE_LEG_DIVERGENCE
        if slippage_expansion >= slippage_expansion_threshold_bps:
            return ShadowFailureCause.SLIPPAGE_EXPANSION

    return ShadowFailureCause.FEE_COST_HURDLE_FAILURE


def _rate(rows: list[ShadowObservation]) -> dict[str, object]:
    survived = sum(1 for row in rows if row.survived)
    return {
        "observations": len(rows),
        "survived": survived,
        "survival_rate": survived / len(rows) if rows else None,
    }


def _horizon_key(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def _group_survival(observations: list[ShadowObservation], key_fn) -> dict[str, dict[str, object]]:
    grouped: dict[str, dict[float, list[ShadowObservation]]] = defaultdict(lambda: defaultdict(list))
    for observation in observations:
        grouped[str(key_fn(observation))][observation.delay_seconds].append(observation)
    return {
        group: {
            "horizons": {
                _horizon_key(horizon): _rate(rows)
                for horizon, rows in sorted(horizons.items())
            }
        }
        for group, horizons in sorted(grouped.items())
    }


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def summarize_shadow_cycles(cycles: list[ShadowCycle]) -> dict[str, object]:
    observations = [observation for cycle in cycles for observation in cycle.observations]
    survived_count = sum(1 for observation in observations if observation.survived)
    outcomes: dict[str, int] = {}
    failures: dict[str, int] = {}
    for observation in observations:
        outcomes[observation.outcome.value] = outcomes.get(observation.outcome.value, 0) + 1
        if not observation.survived:
            cause = observation.failure_cause
            if cause is None:
                cause = (
                    ShadowFailureCause.SIGNAL_DISAPPEARED
                    if observation.outcome.value == "signal_disappeared"
                    else ShadowFailureCause.FEE_COST_HURDLE_FAILURE
                )
            failures[cause.value] = failures.get(cause.value, 0) + 1

    horizon_groups: dict[float, list[ShadowObservation]] = defaultdict(list)
    for observation in observations:
        horizon_groups[observation.delay_seconds].append(observation)
    survival_by_horizon = {
        _horizon_key(horizon): _rate(rows)
        for horizon, rows in sorted(horizon_groups.items())
    }

    def exact_probability(seconds: float) -> float | None:
        rows = [observation for observation in observations if abs(observation.delay_seconds - seconds) < 1e-9]
        return _rate(rows)["survival_rate"] if rows else None

    positive_horizons = sorted(horizon for horizon in horizon_groups if horizon > 0)
    capture_horizon = positive_horizons[0] if positive_horizons else None
    capture_rows = horizon_groups.get(capture_horizon, []) if capture_horizon is not None else []
    capture_probability = _rate(capture_rows)["survival_rate"] if capture_rows else None

    cohorts: dict[tuple[str, str, float], list[ShadowObservation]] = defaultdict(list)
    for observation in observations:
        cohorts[(observation.initial_scan_id, observation.opportunity_signature, observation.notional_usd_per_leg)].append(observation)
    lifetimes: list[float] = []
    for rows in cohorts.values():
        survived_horizons = [row.delay_seconds for row in rows if row.survived]
        lifetimes.append(max(survived_horizons, default=0.0))

    edge_decays = [observation.edge_decay_annualized for observation in observations if observation.edge_decay_annualized is not None]
    deployable_capacities = [
        min(observation.initial_capacity_notional_usd, observation.verification_capacity_notional_usd)
        for observation in observations
        if observation.survived
        and observation.verification_capacity_notional_usd is not None
        and observation.verification_capacity_notional_usd > 0
    ]

    edge_decay_by_horizon: dict[str, dict[str, float | int | None]] = {}
    for horizon, rows in sorted(horizon_groups.items()):
        values = [row.edge_decay_annualized for row in rows if row.edge_decay_annualized is not None]
        edge_decay_by_horizon[_horizon_key(horizon)] = {
            "observations": len(values),
            "mean_edge_decay_annualized": mean(values) if values else None,
            "median_edge_decay_annualized": median(values) if values else None,
        }

    fill_rows = [row for row in observations if row.pair_fillable is not None]
    reconstructed_pair_fill_rate = (
        sum(bool(row.pair_fillable) for row in fill_rows) / len(fill_rows)
        if fill_rows else None
    )
    reconstructed_reserve_fill_rate = (
        sum(bool(row.pair_fillable_with_reserve) for row in fill_rows) / len(fill_rows)
        if fill_rows else None
    )
    hedge_recovery_rate = (
        sum(bool(row.hedge_recovery_required) for row in fill_rows) / len(fill_rows)
        if fill_rows else None
    )

    return {
        "cycle_count": len(cycles),
        "observation_count": len(observations),
        "survived_count": survived_count,
        "survival_rate": survived_count / len(observations) if observations else None,
        "outcomes": outcomes,
        "failure_causes": failures,
        "survival_by_horizon_seconds": survival_by_horizon,
        "probability_surviving_5_seconds": exact_probability(5.0),
        "probability_surviving_15_seconds": exact_probability(15.0),
        "probability_surviving_30_seconds": exact_probability(30.0),
        "estimated_capture_probability": capture_probability,
        "capture_probability_proxy_horizon_seconds": capture_horizon,
        "false_positive_rate": (1.0 - capture_probability) if capture_probability is not None else None,
        "median_opportunity_lifetime_seconds": median(lifetimes) if lifetimes else None,
        "lifetime_metric_note": "lower bound: last configured horizon survived; final-horizon survivors are right-censored",
        "mean_post_detection_edge_decay_annualized": mean(edge_decays) if edge_decays else None,
        "median_post_detection_edge_decay_annualized": median(edge_decays) if edge_decays else None,
        "edge_decay_by_horizon_seconds": edge_decay_by_horizon,
        "reconstructed_pair_fill_rate": reconstructed_pair_fill_rate,
        "reconstructed_reserve_fill_rate": reconstructed_reserve_fill_rate,
        "reconstructed_hedge_recovery_rate": hedge_recovery_rate,
        "fill_metric_note": "visible-L2 reconstruction only; this does not claim queue position or an exchange-confirmed fill",
        "max_realistically_deployable_capital_usd": _quantile(deployable_capacities, 0.10),
        "deployable_capital_definition": "conservative p10 of min(initial capacity, surviving verification capacity)",
        "capacity_quantiles_usd": {
            "p10": _quantile(deployable_capacities, 0.10),
            "p50": _quantile(deployable_capacities, 0.50),
            "p90": _quantile(deployable_capacities, 0.90),
            "max_observed": max(deployable_capacities) if deployable_capacities else None,
        },
        "survival_by": {
            "strategy": _group_survival(observations, lambda row: row.strategy.value),
            "asset": _group_survival(observations, lambda row: row.asset),
            "venue_pair": _group_survival(observations, lambda row: row.venue_pair or "unknown"),
            "capital_size_usd_per_leg": _group_survival(observations, lambda row: f"{row.notional_usd_per_leg:g}"),
            "time_of_day_utc": _group_survival(observations, lambda row: row.time_of_day_bucket or "unknown"),
            "initial_expected_return": _group_survival(observations, lambda row: row.initial_expected_return_bucket or "unknown"),
        },
    }

from __future__ import annotations

from dataclasses import dataclass

from inefficiency_engine.models import ShadowLegAttribution


@dataclass(frozen=True)
class PartialFillState:
    pair_fillable: bool
    pair_fillable_with_reserve: bool
    hedge_recovery_required: bool
    pair_fill_fraction: float
    max_leg_fill_fraction: float
    unhedged_fraction: float
    partial_fill_state: bool
    recovery_loss_proxy_bps: float


def _fill_fraction(leg: ShadowLegAttribution) -> float:
    multiple = leg.verification_depth_multiple
    if multiple is None:
        return 0.0
    return min(1.0, max(0.0, float(multiple)))


def reconstruct_partial_fill_state(
    leg_attribution: list[ShadowLegAttribution],
    *,
    reserve_ratio: float,
) -> PartialFillState:
    """Reconstruct visible-L2 taker fill fractions and hedge-recovery exposure.

    This is deliberately not a queue model. It asks only how much of the original
    target could be crossed against visible public depth at the verification
    snapshot. A one-sided depth shortfall is treated as a hypothetical unhedged
    fraction. The recovery-loss proxy applies observed adverse price movement and
    incremental slippage only to that unmatched fraction.
    """
    if not leg_attribution:
        return PartialFillState(False, False, False, 0.0, 0.0, 0.0, True, 0.0)

    fill_fractions = [_fill_fraction(leg) for leg in leg_attribution]
    minimum_fill = min(fill_fractions)
    maximum_fill = max(fill_fractions)
    unhedged = max(0.0, maximum_fill - minimum_fill)
    pair_fillable = minimum_fill >= 1.0 - 1e-12
    reserve_threshold = max(1.0, reserve_ratio)
    reserve_fillable = all(
        leg.verification_depth_multiple is not None
        and float(leg.verification_depth_multiple) >= reserve_threshold
        for leg in leg_attribution
    )
    hedge_recovery_required = unhedged > 1e-12
    partial_state = not pair_fillable or hedge_recovery_required

    adverse_bps = sum(
        max(0.0, float(leg.adverse_selection_bps or 0.0))
        for leg in leg_attribution
    )
    slippage_expansion_bps = sum(
        max(
            0.0,
            float(leg.verification_slippage_bps or 0.0)
            - float(leg.initial_slippage_bps or 0.0),
        )
        for leg in leg_attribution
    )
    recovery_loss = unhedged * (adverse_bps + slippage_expansion_bps)

    return PartialFillState(
        pair_fillable=pair_fillable,
        pair_fillable_with_reserve=reserve_fillable,
        hedge_recovery_required=hedge_recovery_required,
        pair_fill_fraction=minimum_fill,
        max_leg_fill_fraction=maximum_fill,
        unhedged_fraction=unhedged,
        partial_fill_state=partial_state,
        recovery_loss_proxy_bps=max(0.0, recovery_loss),
    )

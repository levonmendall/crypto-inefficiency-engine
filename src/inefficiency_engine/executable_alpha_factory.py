from __future__ import annotations

import math
from collections import defaultdict

from inefficiency_engine.alpha_factory import (
    AlphaQualification,
    AlphaStrategyRegistry,
    _quantile,
)
from inefficiency_engine.alpha_refinements import (
    BtcRelativeResidualMeanReversionStrategy,
    CrossVenueResidualMeanReversionStrategy,
    LiquidityConditionedMeanReversionStrategy,
    MultiHorizonMeanReversionStrategy,
    OnChainFactorBreadthStrategy,
    TradeFlowLeadLagStrategy,
    VolatilityConditionedMeanReversionStrategy,
)
from inefficiency_engine.evidence_velocity import alpha_lane_for_family, source_group_for_venue
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.source_coverage import (
    CandidateSourceSufficiency,
    SourceCoveragePlane,
    SourceCoverageSnapshot,
)
from inefficiency_engine.source_coverage_catalog import LANES
from inefficiency_engine.trade_flow import TradeFlowImbalanceStrategy, TradeFlowLedger


class _ResearchCostSettingsView:
    """Use the cheapest plausible venue floor only for broad discovery.

    Every emitted candidate is immediately repriced against its own venue/book below,
    and final promotion repeats the live L2 cost gate. This prevents a universal
    research-cost assumption from hiding low-cost venue opportunities without ever
    allowing the cheap floor to become allocation economics.
    """

    def __init__(self, base, *, discovery_floor_bps: float):
        self._base = base
        self._discovery_floor_bps = max(0.0, float(discovery_floor_bps))

    def __getattr__(self, name: str):
        if name == "alpha_research_cost_floor_bps":
            return self._discovery_floor_bps
        return getattr(self._base, name)


class ExecutableExpandedAlphaFactoryService(MemoryBoundedExpandedAlphaFactoryService):
    """Executable research alpha with high-throughput evidence and hard allocation gates.

    The service broadens *learning*, not authority:
    - refinement strategies are actually wired into the memory-bounded registry;
    - discovery uses a low global prefilter then candidate-specific venue/L2 costs;
    - complete single-source evidence may accumulate forward outcomes;
    - final paper promotion still requires decision-grade source redundancy;
    - cross-asset evidence is correlation-discounted and requires local samples.
    """

    CROSS_ASSET_EVIDENCE_WEIGHT_DEFAULT = 0.35
    CROSS_ASSET_LOCAL_SAMPLE_MINIMUM = 3
    RESEARCH_FALLBACK_SLIPPAGE_BPS = 5.0
    VENUE_ANCHORED_SOURCE_LANES = {
        "trend_momentum",
        "mean_reversion",
        "cross_sectional_relative_value",
        "microstructure",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trade_flow_ledger = TradeFlowLedger(self.store)
        self.source_plane = SourceCoveragePlane(self.store)

        # MemoryBoundedExpandedAlphaFactoryService deliberately keeps a separate
        # fast-strategy registry. Extend that registry (not only self.registry) so
        # the refinements are actually executed by its discover() implementation.
        base_strategies = list(getattr(self._base_registry, "_strategies", []))
        additions = [
            TradeFlowImbalanceStrategy(self.trade_flow_ledger),
            TradeFlowLeadLagStrategy(self.trade_flow_ledger),
            CrossVenueResidualMeanReversionStrategy(),
            MultiHorizonMeanReversionStrategy(),
            VolatilityConditionedMeanReversionStrategy(),
            LiquidityConditionedMeanReversionStrategy(),
            BtcRelativeResidualMeanReversionStrategy(),
            OnChainFactorBreadthStrategy(self.fundamental_ledger),
        ]
        existing = {item.manifest.strategy_id for item in base_strategies}
        for strategy in additions:
            if strategy.manifest.strategy_id not in existing:
                base_strategies.append(strategy)
                existing.add(strategy.manifest.strategy_id)
        self._base_registry = AlphaStrategyRegistry(base_strategies)
        self.registry = AlphaStrategyRegistry([*base_strategies, self._cycle_strategy])

        # Lower only the broad discovery prefilter to the cheapest configured venue
        # economics. Candidate-specific repricing below is authoritative for research.
        self._expanded_settings = _ResearchCostSettingsView(
            self._expanded_settings,
            discovery_floor_bps=self._discovery_cost_floor_bps(),
        )

    def _discovery_cost_floor_bps(self) -> float:
        fees = [
            float(self.settings.coinbase_spot_taker_fee_bps),
            float(self.settings.kraken_spot_taker_fee_bps),
            float(self.settings.bybit_spot_taker_fee_bps),
            float(self.settings.bybit_derivatives_taker_fee_bps),
            float(self.settings.okx_spot_taker_fee_bps),
            float(self.settings.okx_derivatives_taker_fee_bps),
            float(self.settings.hyperliquid_perp_taker_fee_bps),
        ]
        usable = [item for item in fees if math.isfinite(item) and item >= 0.0]
        one_way = (
            min(usable)
            if usable
            else float(self.settings.alpha_research_cost_floor_bps) / 2.0
        )
        return max(
            1.0,
            2.0 * one_way + float(self.settings.alpha_execution_risk_floor_bps),
        )

    @staticmethod
    def _lane_for_candidate(candidate) -> str | None:
        return alpha_lane_for_family(candidate.family)

    def _primary_groups(self, candidate) -> set[str] | None:
        lane_id = self._lane_for_candidate(candidate)
        if lane_id not in self.VENUE_ANCHORED_SOURCE_LANES:
            return None
        group = source_group_for_venue(candidate.venue)
        return {group} if group is not None else None

    @staticmethod
    def _gate_from_lane(
        lane,
        *,
        required: list[str],
        primary_groups: set[str] | None,
    ) -> CandidateSourceSufficiency:
        admitted = [row for row in lane.sources if bool(row.get("admitted"))]
        covered = sorted(
            {
                str(cls)
                for row in admitted
                for cls in list(row.get("classes") or [])
                if str(cls) in required
            }
        )
        missing = [item for item in required if item not in covered]
        groups = sorted(
            {
                str(row.get("group") or "")
                for row in admitted
                if bool(row.get("authoritative")) and str(row.get("group") or "")
            }
        )
        normalized_primary = {
            str(item).strip().lower()
            for item in (primary_groups or set())
            if str(item).strip()
        }
        primary_ok = not normalized_primary or any(
            str(row.get("group") or "").strip().lower() in normalized_primary
            for row in admitted
        )
        research_eligible = bool(admitted) and primary_ok
        forward_test_eligible = research_eligible and not missing
        allocation_source_qualified = forward_test_eligible and len(groups) >= 2
        blockers: list[str] = []
        if not admitted:
            blockers.append("no fresh admitted authoritative source")
        if not primary_ok:
            blockers.append("candidate primary venue source is not freshly admitted")
        blockers.extend(f"missing evidence class:{item}" for item in missing)
        if forward_test_eligible and len(groups) < 2:
            blockers.append("independent-source redundancy remains required for allocation")
        return CandidateSourceSufficiency(
            lane_id=lane.lane_id,
            required_evidence_classes=required,
            covered_evidence_classes=covered,
            missing_evidence_classes=missing,
            admitted_source_groups=groups,
            primary_source_groups=sorted(normalized_primary),
            primary_group_satisfied=primary_ok,
            research_eligible=research_eligible,
            forward_test_eligible=forward_test_eligible,
            allocation_source_qualified=allocation_source_qualified,
            blockers=blockers,
        )

    def _source_gate(
        self,
        candidate,
        coverage: SourceCoverageSnapshot | None = None,
    ) -> CandidateSourceSufficiency | None:
        lane_id = self._lane_for_candidate(candidate)
        if lane_id is None or lane_id not in LANES:
            return None
        required = [str(item) for item in list(LANES[lane_id]["required"])]
        primary = self._primary_groups(candidate)
        if coverage is None:
            return self.source_plane.candidate_sufficiency(
                lane_id,
                required_evidence_classes=required,
                primary_groups=primary,
            )
        lane = next((row for row in coverage.lanes if row.lane_id == lane_id), None)
        if lane is None:
            return None
        return self._gate_from_lane(lane, required=required, primary_groups=primary)

    def _fallback_research_cost(self, candidate) -> float | None:
        fee_bps = self._one_way_fee_bps(candidate.venue, candidate.market_kind)
        if fee_bps is None:
            return None
        total_bps = (
            2.0 * float(fee_bps)
            + float(self.settings.alpha_execution_risk_floor_bps)
            + self.RESEARCH_FALLBACK_SLIPPAGE_BPS
        )
        return max(0.0, total_bps / 10_000.0)

    def discover(self, snapshot, *, total_capital_usd: float):
        rows = super().discover(snapshot, total_capital_usd=total_capital_usd)
        source_snapshot = self.source_plane.snapshot(now=snapshot.completed_at)
        repriced = []
        for candidate in rows:
            try:
                gate = self._source_gate(candidate, source_snapshot)
            except Exception:
                continue
            if gate is None or not gate.forward_test_eligible:
                continue

            book = self._snapshot_book(candidate, snapshot)
            current_cost = (
                self._cost_from_book(candidate, book)
                if book is not None
                else self._fallback_research_cost(candidate)
            )
            if current_cost is None:
                continue
            current_cost += self._holding_carry_cost(candidate)
            net = candidate.expected_gross_return - current_cost
            if net <= float(self.settings.alpha_min_current_net_return):
                continue

            item = candidate.model_copy(deep=True)
            item.estimated_cost_return = current_cost
            item.expected_net_return = net
            item.expected_profit_usd = item.notional_usd * net
            item.features.update(
                {
                    "research_cost_model": (
                        "candidate_visible_l2"
                        if book is not None
                        else "candidate_venue_fee_plus_fallback_slippage"
                    ),
                    "research_discovery_prefilter_bps": self._discovery_cost_floor_bps(),
                    "source_research_eligible": gate.research_eligible,
                    "source_forward_test_eligible": gate.forward_test_eligible,
                    "source_allocation_qualified": gate.allocation_source_qualified,
                    "source_group_count": len(gate.admitted_source_groups),
                    "allocation_authority": False,
                }
            )
            repriced.append(item)

        repriced.sort(
            key=lambda item: (item.expected_net_return, item.confidence_score),
            reverse=True,
        )
        return repriced

    @staticmethod
    def _continuous_wilson_lower(rate: float, total: float, z: float = 1.96) -> float | None:
        if total <= 0:
            return None
        p = max(0.0, min(1.0, float(rate)))
        denominator = 1.0 + z * z / total
        center = p + z * z / (2.0 * total)
        margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
        return max(0.0, (center - margin) / denominator)

    @staticmethod
    def _weighted_mean_lower(
        values: list[tuple[float, float]],
        effective_n: float,
    ) -> float | None:
        if not values or effective_n <= 0:
            return None
        total_weight = sum(weight for _, weight in values)
        if total_weight <= 0:
            return None
        mean = sum(value * weight for value, weight in values) / total_weight
        if effective_n < 2:
            return mean
        variance = sum(
            weight * (value - mean) ** 2 for value, weight in values
        ) / total_weight
        return mean - 1.96 * math.sqrt(max(0.0, variance) / effective_n)

    def _pooled_independent_outcomes(self, candidate):
        own = self._outcomes_for(candidate)
        all_rows = self.ledger.outcomes(
            strategy_id=candidate.strategy_id,
            direction=candidate.direction,
        )
        by_asset: dict[str, list] = defaultdict(list)
        for row in all_rows:
            by_asset[row.asset.upper()].append(row)
        independent_by_asset = {
            asset: self._independent_outcomes(rows)
            for asset, rows in by_asset.items()
        }
        own_asset = candidate.asset.upper()
        own = independent_by_asset.get(own_asset, own)
        other = [
            row
            for asset, rows in independent_by_asset.items()
            if asset != own_asset
            for row in rows
        ]
        return own, other, len(independent_by_asset)

    def qualification(self, candidate) -> AlphaQualification:
        own, other, asset_count = self._pooled_independent_outcomes(candidate)
        weight = max(
            0.0,
            min(
                0.50,
                float(
                    getattr(
                        self.settings,
                        "alpha_cross_asset_evidence_weight",
                        self.CROSS_ASSET_EVIDENCE_WEIGHT_DEFAULT,
                    )
                ),
            ),
        )
        weighted: list[tuple[object, float]] = [(row, 1.0) for row in own]
        if asset_count >= 2 and weight > 0:
            weighted.extend((row, weight) for row in other)

        total_weight = sum(item_weight for _, item_weight in weighted)
        effective_n_float = min(
            total_weight,
            float(len(own)) + weight * float(len(other)),
        )
        effective_n = max(0, int(math.floor(effective_n_float + 1e-9)))
        values = [
            (float(row.realized_net_return), item_weight)
            for row, item_weight in weighted
        ]
        mean = (
            sum(value * item_weight for value, item_weight in values) / total_weight
            if total_weight > 0
            else None
        )
        positive_weight = sum(
            item_weight
            for row, item_weight in weighted
            if float(row.realized_net_return) > 0
        )
        hit_rate = positive_weight / total_weight if total_weight > 0 else None
        hit_lower = (
            self._continuous_wilson_lower(hit_rate, effective_n_float)
            if hit_rate is not None
            else None
        )
        mean_lower = self._weighted_mean_lower(values, effective_n_float)

        regime_weighted: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for row, item_weight in weighted:
            regime_weighted[str(row.regime)].append(
                (float(row.realized_net_return), item_weight)
            )
        regime_means = {
            regime: sum(value * item_weight for value, item_weight in rows)
            / sum(item_weight for _, item_weight in rows)
            for regime, rows in regime_weighted.items()
            if rows and sum(item_weight for _, item_weight in rows) > 0
        }

        strategy_count = max(1, len(self.registry.manifests()))
        penalty = self.settings.alpha_multiple_testing_penalty_return * math.sqrt(
            math.log(strategy_count + 1.0)
        )
        required = self.settings.alpha_min_forward_mean_return + penalty
        blockers: list[str] = []
        local_minimum = max(
            self.CROSS_ASSET_LOCAL_SAMPLE_MINIMUM,
            int(getattr(self.settings, "alpha_cross_asset_min_local_samples", 3)),
        )
        if len(own) < local_minimum:
            blockers.append(
                "insufficient candidate-specific forward samples for cross-asset pooling"
            )
        if effective_n < self.settings.alpha_min_forward_samples:
            blockers.append("insufficient correlation-adjusted independent forward samples")
        if mean_lower is None or mean_lower <= required:
            blockers.append("forward net-return confidence lower bound is below hurdle")
        if hit_lower is None or hit_lower < self.settings.alpha_min_hit_rate_lower_bound:
            blockers.append("forward hit-rate confidence lower bound is below hurdle")
        if len(regime_means) < self.settings.alpha_min_regimes:
            blockers.append("insufficient regime coverage")
        elif any(
            value <= self.settings.alpha_min_regime_mean_return
            for value in regime_means.values()
        ):
            blockers.append("one or more observed regimes have non-qualifying mean return")

        raw_values = [float(row.realized_net_return) for row, _ in weighted]
        qualified = not blockers
        effective_positive = (
            min(
                effective_n,
                int(math.floor((hit_rate or 0.0) * effective_n + 1e-9)),
            )
            if effective_n > 0
            else 0
        )
        return AlphaQualification(
            strategy_id=candidate.strategy_id,
            family=candidate.family,
            asset=candidate.asset,
            direction=candidate.direction,
            sample_count=effective_n,
            positive_count=effective_positive,
            hit_rate=hit_rate,
            hit_rate_ci_lower=hit_lower,
            mean_realized_net_return=mean,
            mean_realized_net_return_ci_lower=mean_lower,
            p10_realized_net_return=_quantile(raw_values, 0.10),
            worst_realized_net_return=min(raw_values) if raw_values else None,
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

    async def promoted_candidates(self, snapshot, *, total_capital_usd: float):
        rows = await super().promoted_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
        )
        source_snapshot = self.source_plane.snapshot(now=snapshot.completed_at)
        qualified = []
        for candidate in rows:
            try:
                gate = self._source_gate(candidate, source_snapshot)
            except Exception:
                continue
            if gate is None or not gate.allocation_source_qualified:
                continue
            candidate.features.update(
                {
                    "source_allocation_qualified": True,
                    "source_admitted_group_count": len(gate.admitted_source_groups),
                    "research_cost_revalidated_at_promotion": True,
                }
            )
            qualified.append(candidate)
        qualified.sort(
            key=lambda item: (item.expected_net_return, item.expected_profit_usd),
            reverse=True,
        )
        return qualified

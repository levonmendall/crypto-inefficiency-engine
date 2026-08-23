from __future__ import annotations

from collections import Counter
from datetime import timedelta

from inefficiency_engine.alpha_funnel_projection import publish_alpha_funnel_projection
from inefficiency_engine.executable_alpha_factory import ExecutableExpandedAlphaFactoryService
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.models import MarketKind


ALPHA_RESEARCH_WORKER_ID = "shadow-research-auxiliary"
ALPHA_LANES = (
    "trend_momentum",
    "mean_reversion",
    "fundamental_onchain",
    "cross_sectional_relative_value",
    "event_driven",
    "microstructure",
)
# These strategies express an asset-level signal and then choose an execution venue.
# Preserve the signal but let current source/cost economics choose the venue instead of
# allowing a cheap discovery floor or lexical venue ordering to make that decision.
VENUE_COMPETITION_STRATEGIES = frozenset(
    {
        "time_series_momentum_v1",
        "mean_reversion_v1",
        "cross_sectional_relative_value_v1",
        "onchain_fundamental_composite_v1",
        "event_driven_surprise_v1",
    }
)
# Fast 24h-style signals must actually observe most of the named lookback. Historical
# backfill remains isolated and the slow cycle-aware strategy keeps its own tolerance.
HISTORY_COVERAGE_STRATEGIES = frozenset(
    {
        "time_series_momentum_v1",
        "mean_reversion_v1",
    }
)
MIN_NAMED_LOOKBACK_COVERAGE_FRACTION = 0.80
MAX_EXECUTION_VARIANT_REFERENCE_DIVERGENCE = 0.01


class AllLaneEvidenceFactoryService(ExecutableExpandedAlphaFactoryService):
    """Run alpha and non-alpha mechanism forward evidence on one bounded cadence.

    Alpha discovery is intentionally split into two stages here:
    1. raw strategy signals, before source/current-cost rejection;
    2. forward-test candidates after truthful history, source and current economics.

    This keeps research broad and observable while leaving every allocation,
    statistical, redundancy, settlement and profitability gate unchanged.
    """

    def __init__(self, core, store, *args, **kwargs):
        super().__init__(core, store, *args, **kwargs)
        self.mechanism_execution = GovernedMechanismExecutionService(core, store)
        self._last_alpha_discovery_diagnostics: dict[str, dict[str, object]] = {}

    @staticmethod
    def _diagnostic_seed() -> dict[str, object]:
        return {
            "raw_candidate_count": 0,
            "execution_variant_count": 0,
            "history_coverage_rejected_count": 0,
            "source_gate_rejected_count": 0,
            "cost_unavailable_count": 0,
            "net_hurdle_rejected_count": 0,
            "post_gate_candidate_count": 0,
            "emitted_candidate_count": 0,
            "best_gross_economics": None,
            "best_cost_economics": None,
            "best_net_economics": None,
            "required_net_economics": None,
            "gap_to_hurdle": None,
            "dominant_rejection_gate": "no_strategy_signal",
            "rejection_gate_counts": {},
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "paper_only": True,
        }

    def last_discovery_diagnostics(self) -> dict[str, dict[str, object]]:
        return {
            lane: dict(values)
            for lane, values in self._last_alpha_discovery_diagnostics.items()
        }

    def _history_coverage(
        self,
        candidate,
        history,
    ) -> tuple[bool, float | None, float]:
        lookback = max(0.25, float(candidate.lookback_hours))
        if candidate.strategy_id not in HISTORY_COVERAGE_STRATEGIES:
            return True, None, lookback
        key = (candidate.venue, candidate.asset.upper(), candidate.market_kind)
        cutoff = candidate.observed_at - timedelta(hours=lookback)
        rows = sorted(
            (
                item
                for item in history.get(key, [])
                if cutoff <= item.observed_at <= candidate.observed_at
            ),
            key=lambda item: item.observed_at,
        )
        if len(rows) < int(self.settings.alpha_min_history_points):
            return False, 0.0, lookback
        observed_span = max(
            0.0,
            (rows[-1].observed_at - rows[0].observed_at).total_seconds() / 3600.0,
        )
        required_span = lookback * MIN_NAMED_LOOKBACK_COVERAGE_FRACTION
        return observed_span >= required_span, observed_span, lookback

    def _execution_venue_variants(self, candidate, snapshot):
        if candidate.strategy_id not in VENUE_COMPETITION_STRATEGIES:
            return [candidate]
        if candidate.direction == "long":
            required_kind = MarketKind.SPOT
        elif candidate.direction == "short":
            required_kind = MarketKind.PERPETUAL
        else:
            return [candidate]

        variants = []
        reference = float(candidate.entry_reference_price)
        for quote in snapshot.market_quotes:
            if quote.asset.upper() != candidate.asset.upper():
                continue
            if quote.market_kind != required_kind or quote.mid <= 0:
                continue
            divergence = abs(float(quote.mid) / reference - 1.0) if reference > 0 else 0.0
            if divergence > MAX_EXECUTION_VARIANT_REFERENCE_DIVERGENCE:
                continue
            item = candidate.model_copy(deep=True)
            item.candidate_id = (
                f"{candidate.candidate_id}:execution-venue:{quote.venue}:{quote.market_kind.value}"
            )
            item.venue = quote.venue
            item.market_kind = quote.market_kind
            item.symbol = quote.symbol
            item.observed_at = quote.observed_at
            item.entry_reference_price = quote.mid
            item.conflict_keys = [
                key for key in item.conflict_keys if not key.startswith("alpha-instrument:")
            ]
            item.conflict_keys.append(f"alpha-instrument:{quote.venue}:{quote.symbol}")
            item.features.update(
                {
                    "signal_reference_venue": candidate.venue,
                    "signal_reference_symbol": candidate.symbol,
                    "execution_venue_variant": True,
                    "execution_reference_divergence": divergence,
                    "venue_selected_after_current_economics": True,
                }
            )
            variants.append(item)
        return variants or [candidate]

    @staticmethod
    def _competition_key(candidate) -> str:
        if candidate.strategy_id in VENUE_COMPETITION_STRATEGIES:
            return f"{candidate.strategy_id}:{candidate.asset.upper()}:{candidate.direction}"
        return candidate.candidate_id

    @staticmethod
    def _update_best(row: dict[str, object], *, gross: float, cost: float, net: float) -> None:
        current_net = row.get("best_net_economics")
        if current_net is None or net > float(current_net):
            row["best_gross_economics"] = gross
            row["best_cost_economics"] = cost
            row["best_net_economics"] = net

    def _finalize_diagnostics(
        self,
        diagnostics: dict[str, dict[str, object]],
        emitted,
    ) -> dict[str, dict[str, object]]:
        emitted_by_lane = Counter(
            lane
            for candidate in emitted
            if (lane := self._lane_for_candidate(candidate)) in diagnostics
        )
        required = float(self.settings.alpha_min_current_net_return)
        for lane, row in diagnostics.items():
            row["emitted_candidate_count"] = int(emitted_by_lane.get(lane, 0))
            row["required_net_economics"] = required
            best_net = row.get("best_net_economics")
            row["gap_to_hurdle"] = (
                float(best_net) - required if best_net is not None else None
            )
            rejection_counts = {
                "history_coverage": int(row["history_coverage_rejected_count"]),
                "source_gate": int(row["source_gate_rejected_count"]),
                "current_cost_unavailable": int(row["cost_unavailable_count"]),
                "net_return_hurdle": int(row["net_hurdle_rejected_count"]),
            }
            rejection_counts = {
                key: value for key, value in rejection_counts.items() if value > 0
            }
            row["rejection_gate_counts"] = rejection_counts
            if int(row["emitted_candidate_count"]) > 0:
                row["dominant_rejection_gate"] = "candidate_emitted"
            elif int(row["raw_candidate_count"]) <= 0:
                row["dominant_rejection_gate"] = "no_strategy_signal"
            elif rejection_counts:
                row["dominant_rejection_gate"] = max(
                    rejection_counts.items(), key=lambda item: (item[1], item[0])
                )[0]
            else:
                row["dominant_rejection_gate"] = "post_gate_candidate_not_selected"
        return diagnostics

    def discover(self, snapshot, *, total_capital_usd: float):
        # Bypass ExecutableExpandedAlphaFactoryService.discover so diagnostics see
        # strategy signals before source/current-cost rejection. The memory-bounded
        # discovery contract and all strategy signal thresholds remain unchanged.
        raw = MemoryBoundedExpandedAlphaFactoryService.discover(
            self,
            snapshot,
            total_capital_usd=total_capital_usd,
        )
        diagnostics = {lane: self._diagnostic_seed() for lane in ALPHA_LANES}
        history = self._history_for_snapshot(snapshot)
        source_snapshot = self.source_plane.snapshot(now=snapshot.completed_at)
        repriced = []

        for candidate in raw:
            lane = self._lane_for_candidate(candidate)
            if lane not in diagnostics:
                continue
            row = diagnostics[lane]
            row["raw_candidate_count"] = int(row["raw_candidate_count"]) + 1

            history_ok, observed_span, named_lookback = self._history_coverage(
                candidate,
                history,
            )
            if candidate.strategy_id in HISTORY_COVERAGE_STRATEGIES:
                row["named_lookback_hours"] = named_lookback
                row["minimum_required_history_span_hours"] = (
                    named_lookback * MIN_NAMED_LOOKBACK_COVERAGE_FRACTION
                )
                if observed_span is not None:
                    current_span = row.get("best_observed_history_span_hours")
                    if current_span is None or observed_span > float(current_span):
                        row["best_observed_history_span_hours"] = observed_span
            if not history_ok:
                row["history_coverage_rejected_count"] = (
                    int(row["history_coverage_rejected_count"]) + 1
                )
                continue

            variants = self._execution_venue_variants(candidate, snapshot)
            row["execution_variant_count"] = int(row["execution_variant_count"]) + len(variants)
            for variant in variants:
                try:
                    gate = self._source_gate(variant, source_snapshot)
                except Exception:
                    row["source_gate_rejected_count"] = int(row["source_gate_rejected_count"]) + 1
                    continue
                if gate is None or not gate.forward_test_eligible:
                    row["source_gate_rejected_count"] = int(row["source_gate_rejected_count"]) + 1
                    continue

                book = self._snapshot_book(variant, snapshot)
                current_cost = (
                    self._cost_from_book(variant, book)
                    if book is not None
                    else self._fallback_research_cost(variant)
                )
                if current_cost is None:
                    row["cost_unavailable_count"] = int(row["cost_unavailable_count"]) + 1
                    continue
                current_cost += self._holding_carry_cost(variant)
                net = float(variant.expected_gross_return) - float(current_cost)
                self._update_best(
                    row,
                    gross=float(variant.expected_gross_return),
                    cost=float(current_cost),
                    net=net,
                )
                if net <= float(self.settings.alpha_min_current_net_return):
                    row["net_hurdle_rejected_count"] = int(row["net_hurdle_rejected_count"]) + 1
                    continue

                item = variant.model_copy(deep=True)
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
                row["post_gate_candidate_count"] = int(row["post_gate_candidate_count"]) + 1
                repriced.append(item)

        # Asset-level signals compete across venues only after source and current-cost
        # reconstruction. One forward cohort remains per strategy/asset/direction.
        best_by_group = {}
        for candidate in repriced:
            key = self._competition_key(candidate)
            previous = best_by_group.get(key)
            if previous is None or (
                candidate.expected_net_return,
                candidate.confidence_score,
            ) > (
                previous.expected_net_return,
                previous.confidence_score,
            ):
                best_by_group[key] = candidate
        emitted = sorted(
            best_by_group.values(),
            key=lambda item: (item.expected_net_return, item.confidence_score),
            reverse=True,
        )
        self._last_alpha_discovery_diagnostics = self._finalize_diagnostics(
            diagnostics,
            emitted,
        )
        return emitted

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        alpha = await super().run_evidence_cycle(total_capital_usd=total_capital_usd)
        diagnostics = self.last_discovery_diagnostics()
        dashboard_projection_published = False
        try:
            dashboard_projection_published = publish_alpha_funnel_projection(
                self.store,
                diagnostics,
                observed_at=alpha.observed_at,
            )
        except Exception:
            # Projection failure cannot suppress evidence or become authority.
            dashboard_projection_published = False
        try:
            # The production dashboard already looks for this exact marker when
            # locating a successful alpha cycle. Emit it on the same research worker
            # id without claiming the entire disposable research process is complete.
            self.store.record_worker_heartbeat(
                worker_id=ALPHA_RESEARCH_WORKER_ID,
                state="running",
                cycle_id=alpha.cycle_id,
                observed_at=alpha.observed_at,
                detail={
                    "alpha_forward_evidence_cycle_id": alpha.cycle_id,
                    "alpha_candidate_count": alpha.candidate_count,
                    "alpha_signals_recorded": alpha.signals_recorded,
                    "alpha_outcomes_matured": alpha.outcomes_matured,
                    "alpha_discovery_funnel": diagnostics,
                    "raw_candidate_count": sum(
                        int(row.get("raw_candidate_count") or 0)
                        for row in diagnostics.values()
                    ),
                    "post_gate_candidate_count": sum(
                        int(row.get("post_gate_candidate_count") or 0)
                        for row in diagnostics.values()
                    ),
                    "alpha_dashboard_funnel_projected": dashboard_projection_published,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                    "live_execution_authority": False,
                },
            )
        except Exception:
            # Observability must never become investment authority or suppress the
            # already-completed forward evidence cycle.
            pass

        if getattr(self, "_mechanism_evidence_enabled", True):
            try:
                mechanism = await self.mechanism_execution.run_evidence_cycle(
                    total_capital_usd=total_capital_usd
                )
                self.store.record_worker_heartbeat(
                    worker_id="mechanism-forward-evidence",
                    state="success",
                    detail={
                        "trials_recorded": mechanism.trials_recorded,
                        "outcomes_matured": mechanism.outcomes_matured,
                        "current_specs": mechanism.current_specs,
                        "promoted_candidates": mechanism.promoted_candidates,
                        "by_mechanism": mechanism.by_mechanism,
                        "paper_only": True,
                        "live_execution_authority": False,
                    },
                )
            except Exception as exc:
                # Non-alpha mechanism evidence is isolated. A failure cannot reclassify
                # the already-completed alpha cycle or authorize any paper allocation.
                self.store.record_worker_heartbeat(
                    worker_id="mechanism-forward-evidence",
                    state="error",
                    error_type=type(exc).__name__,
                    detail={
                        "message": str(exc)[:500],
                        "paper_only": True,
                        "live_execution_authority": False,
                    },
                )
        return alpha

from __future__ import annotations

from inefficiency_engine.all_lane_alpha_factory import (
    ALPHA_LANES,
    HISTORY_COVERAGE_STRATEGIES,
    MIN_NAMED_LOOKBACK_COVERAGE_FRACTION,
    AllLaneEvidenceFactoryService,
)
from inefficiency_engine.candidate_observatory import (
    DIAGNOSTIC_SHADOW_MAX_GAP_TO_HURDLE,
    MAX_DIAGNOSTIC_SHADOW_SIGNALS_PER_CYCLE,
    OBSERVATORY_WORKER_ID,
    CandidateObservation,
    CandidateObservatoryLedger,
    build_observatory_snapshot,
    settle_diagnostic_shadows,
)
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService


class CandidateObservedAllLaneEvidenceFactoryService(AllLaneEvidenceFactoryService):
    """Preserve every alpha funnel decision while leaving authority gates unchanged."""

    def __init__(self, core, store, *args, **kwargs):
        super().__init__(core, store, *args, **kwargs)
        self.candidate_observatory = CandidateObservatoryLedger(store)
        self._last_candidate_observations: list[CandidateObservation] = []
        self._last_observatory_candidate_refs: dict[str, object] = {}
        self._last_source_groups_by_candidate: dict[str, list[str]] = {}
        self._last_discovery_snapshot = None

    def _observe(self, candidate, *, scan_id: str, lane: str, stage: str, signal_id: str,
                 cost: float | None = None, net: float | None = None,
                 source_groups: list[str] | None = None, blockers: list[str] | None = None,
                 selected: bool = False, shadow_eligible: bool = False) -> CandidateObservation:
        required = float(self.settings.alpha_min_current_net_return)
        return CandidateObservation(
            source_scan_id=scan_id, observed_at=candidate.observed_at, lane_id=lane,
            candidate_id=candidate.candidate_id, signal_candidate_id=signal_id,
            strategy_id=candidate.strategy_id, family=candidate.family, asset=candidate.asset,
            direction=candidate.direction, stage=stage, venue=candidate.venue,
            signal_reference_venue=str(candidate.features.get("signal_reference_venue") or candidate.venue),
            market_kind=candidate.market_kind.value, symbol=candidate.symbol,
            horizon_hours=float(candidate.horizon_hours), entry_reference_price=float(candidate.entry_reference_price),
            expected_gross_return=float(candidate.expected_gross_return),
            estimated_cost_return=float(cost) if cost is not None else None,
            expected_net_return=float(net) if net is not None else None,
            required_net_return=required, gap_to_hurdle=float(net) - required if net is not None else None,
            notional_usd=float(candidate.notional_usd),
            expected_profit_usd=float(candidate.notional_usd) * float(net) if net is not None else None,
            confidence_score=float(candidate.confidence_score), source_groups=list(source_groups or []),
            blockers=list(blockers or []), selected_for_forward_test=selected,
            diagnostic_shadow_eligible=shadow_eligible,
        )

    def discover(self, snapshot, *, total_capital_usd: float):
        self._last_candidate_observations = []
        self._last_observatory_candidate_refs = {}
        self._last_source_groups_by_candidate = {}
        self._last_discovery_snapshot = snapshot
        raw = MemoryBoundedExpandedAlphaFactoryService.discover(
            self, snapshot, total_capital_usd=total_capital_usd
        )
        diagnostics = {lane: self._diagnostic_seed() for lane in ALPHA_LANES}
        history = self._history_for_snapshot(snapshot)
        source_snapshot = self.source_plane.snapshot(now=snapshot.completed_at)
        repriced = []

        for candidate in raw:
            lane = self._lane_for_candidate(candidate)
            if lane not in diagnostics:
                continue
            diagnostic = diagnostics[lane]
            diagnostic["raw_candidate_count"] = int(diagnostic["raw_candidate_count"]) + 1
            self._last_candidate_observations.append(self._observe(
                candidate, scan_id=snapshot.scan_id, lane=lane, stage="raw_signal",
                signal_id=candidate.candidate_id,
            ))

            history_ok, observed_span, named_lookback = self._history_coverage(candidate, history)
            if candidate.strategy_id in HISTORY_COVERAGE_STRATEGIES:
                diagnostic["named_lookback_hours"] = named_lookback
                diagnostic["minimum_required_history_span_hours"] = (
                    named_lookback * MIN_NAMED_LOOKBACK_COVERAGE_FRACTION
                )
                if observed_span is not None:
                    prior = diagnostic.get("best_observed_history_span_hours")
                    if prior is None or observed_span > float(prior):
                        diagnostic["best_observed_history_span_hours"] = observed_span
            if not history_ok:
                diagnostic["history_coverage_rejected_count"] = int(diagnostic["history_coverage_rejected_count"]) + 1
                self._last_candidate_observations.append(self._observe(
                    candidate, scan_id=snapshot.scan_id, lane=lane, stage="history_coverage_rejected",
                    signal_id=candidate.candidate_id, blockers=["named lookback history coverage is insufficient"],
                ))
                continue

            variants = self._execution_venue_variants(candidate, snapshot)
            diagnostic["execution_variant_count"] = int(diagnostic["execution_variant_count"]) + len(variants)
            for variant in variants:
                self._last_observatory_candidate_refs[variant.candidate_id] = variant
                try:
                    gate = self._source_gate(variant, source_snapshot)
                except Exception as exc:
                    diagnostic["source_gate_rejected_count"] = int(diagnostic["source_gate_rejected_count"]) + 1
                    self._last_candidate_observations.append(self._observe(
                        variant, scan_id=snapshot.scan_id, lane=lane, stage="source_gate_rejected",
                        signal_id=candidate.candidate_id, blockers=[f"source gate error:{type(exc).__name__}"],
                    ))
                    continue
                if gate is None or not gate.forward_test_eligible:
                    diagnostic["source_gate_rejected_count"] = int(diagnostic["source_gate_rejected_count"]) + 1
                    self._last_candidate_observations.append(self._observe(
                        variant, scan_id=snapshot.scan_id, lane=lane, stage="source_gate_rejected",
                        signal_id=candidate.candidate_id,
                        source_groups=list(gate.admitted_source_groups) if gate is not None else [],
                        blockers=list(gate.blockers) if gate is not None else ["source gate unavailable"],
                    ))
                    continue

                book = self._snapshot_book(variant, snapshot)
                cost = self._cost_from_book(variant, book) if book is not None else self._fallback_research_cost(variant)
                if cost is None:
                    diagnostic["cost_unavailable_count"] = int(diagnostic["cost_unavailable_count"]) + 1
                    self._last_candidate_observations.append(self._observe(
                        variant, scan_id=snapshot.scan_id, lane=lane, stage="current_cost_unavailable",
                        signal_id=candidate.candidate_id, source_groups=list(gate.admitted_source_groups),
                        blockers=["current round-trip execution cost is unavailable"],
                    ))
                    continue
                cost += self._holding_carry_cost(variant)
                net = float(variant.expected_gross_return) - float(cost)
                self._update_best(diagnostic, gross=float(variant.expected_gross_return), cost=float(cost), net=net)
                if net <= float(self.settings.alpha_min_current_net_return):
                    diagnostic["net_hurdle_rejected_count"] = int(diagnostic["net_hurdle_rejected_count"]) + 1
                    shortfall = float(self.settings.alpha_min_current_net_return) - net
                    self._last_candidate_observations.append(self._observe(
                        variant, scan_id=snapshot.scan_id, lane=lane, stage="net_hurdle_rejected",
                        signal_id=candidate.candidate_id, cost=cost, net=net,
                        source_groups=list(gate.admitted_source_groups), blockers=["current net return is below hurdle"],
                        shadow_eligible=variant.direction in {"long", "short"} and shortfall <= DIAGNOSTIC_SHADOW_MAX_GAP_TO_HURDLE,
                    ))
                    continue

                item = variant.model_copy(deep=True)
                item.estimated_cost_return = cost
                item.expected_net_return = net
                item.expected_profit_usd = item.notional_usd * net
                item.features.update({
                    "research_cost_model": "candidate_visible_l2" if book is not None else "candidate_venue_fee_plus_fallback_slippage",
                    "research_discovery_prefilter_bps": self._discovery_cost_floor_bps(),
                    "source_research_eligible": gate.research_eligible,
                    "source_forward_test_eligible": gate.forward_test_eligible,
                    "source_allocation_qualified": gate.allocation_source_qualified,
                    "source_group_count": len(gate.admitted_source_groups), "allocation_authority": False,
                })
                diagnostic["post_gate_candidate_count"] = int(diagnostic["post_gate_candidate_count"]) + 1
                self._last_observatory_candidate_refs[item.candidate_id] = item
                self._last_source_groups_by_candidate[item.candidate_id] = list(gate.admitted_source_groups)
                repriced.append(item)

        best_by_group = {}
        for candidate in repriced:
            key = self._competition_key(candidate)
            previous = best_by_group.get(key)
            if previous is None or (candidate.expected_net_return, candidate.confidence_score) > (
                previous.expected_net_return, previous.confidence_score
            ):
                best_by_group[key] = candidate
        emitted = sorted(best_by_group.values(), key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        selected_ids = {candidate.candidate_id for candidate in emitted}
        for candidate in repriced:
            lane = self._lane_for_candidate(candidate)
            if lane not in diagnostics:
                continue
            selected = candidate.candidate_id in selected_ids
            self._last_candidate_observations.append(self._observe(
                candidate, scan_id=snapshot.scan_id, lane=lane,
                stage="forward_candidate_selected" if selected else "execution_variant_not_selected",
                signal_id=candidate.candidate_id.split(":execution-venue:", 1)[0],
                cost=float(candidate.estimated_cost_return), net=float(candidate.expected_net_return),
                source_groups=self._last_source_groups_by_candidate.get(candidate.candidate_id, []),
                blockers=[] if selected else ["better execution variant selected"], selected=selected,
                shadow_eligible=not selected and candidate.direction in {"long", "short"},
            ))
        self._last_alpha_discovery_diagnostics = self._finalize_diagnostics(diagnostics, emitted)
        return emitted

    def _qualification_projection(self) -> dict[tuple[str, str, str], dict[str, object]]:
        result = {}
        for candidate in self._last_observatory_candidate_refs.values():
            key = candidate.strategy_id, candidate.asset.upper(), candidate.direction
            if key in result:
                continue
            try:
                result[key] = self.qualification(candidate).model_dump(mode="json")
            except Exception:
                result[key] = {"sample_count": 0, "blockers": ["qualification projection unavailable"],
                               "statistically_qualified": False, "paper_allocation_authority": False}
        return result

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        alpha = await super().run_evidence_cycle(total_capital_usd=total_capital_usd)
        snapshot = self._last_discovery_snapshot
        if snapshot is None:
            return alpha
        try:
            matured = settle_diagnostic_shadows(self.candidate_observatory, snapshot)
            observations = self.candidate_observatory.record_observations(alpha.cycle_id, self._last_candidate_observations)
            eligible = sorted(
                (row for row in observations if row.diagnostic_shadow_eligible),
                key=lambda row: (
                    float(row.expected_net_return) if row.expected_net_return is not None else -1.0,
                    float(row.expected_gross_return), float(row.confidence_score),
                ), reverse=True,
            )[:MAX_DIAGNOSTIC_SHADOW_SIGNALS_PER_CYCLE]
            scheduled = sum(self.candidate_observatory.record_shadow_signal(row) for row in eligible)
            observatory = build_observatory_snapshot(
                cycle_id=alpha.cycle_id, observed_at=alpha.observed_at, source_scan_id=snapshot.scan_id,
                diagnostics=self.last_discovery_diagnostics(), observations=observations,
                qualifications=self._qualification_projection(),
                required_samples=max(1, int(self.settings.alpha_min_forward_samples)),
                research_capital_usd=float(total_capital_usd or self.settings.alpha_research_capital_usd),
                shadow_signals_recorded=scheduled, shadow_outcomes_matured=matured,
            )
            self.candidate_observatory.record_snapshot(observatory)
            try:
                self.store.record_worker_heartbeat(
                    worker_id=OBSERVATORY_WORKER_ID, state="success", cycle_id=alpha.cycle_id,
                    observed_at=alpha.observed_at, detail={
                        "candidate_event_count": observatory.candidate_event_count,
                        "raw_signal_count": observatory.raw_signal_count,
                        "forward_candidate_count": observatory.forward_candidate_count,
                        "diagnostic_shadow_signals_recorded": scheduled,
                        "diagnostic_shadow_outcomes_matured": matured,
                        "near_miss_count": len(observatory.near_misses),
                        "qualification_thresholds_unchanged": True, "allocation_authority": False, "paper_only": True,
                    },
                )
            except Exception:
                pass
        except Exception as exc:
            try:
                self.store.record_worker_heartbeat(
                    worker_id=OBSERVATORY_WORKER_ID, state="error", cycle_id=alpha.cycle_id,
                    observed_at=alpha.observed_at, error_type=type(exc).__name__, detail={
                        "observatory_failure_is_non_authoritative": True,
                        "qualification_thresholds_unchanged": True, "allocation_authority": False, "paper_only": True,
                    },
                )
            except Exception:
                pass
        return alpha

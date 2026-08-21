from __future__ import annotations

import statistics

from inefficiency_engine.evidence_velocity import source_group_for_venue
from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService
from inefficiency_engine.incremental_forward_sizing import forward_evidence_allocation_fraction
from inefficiency_engine.mechanism_execution import (
    FULL_FORWARD_TARGET,
    MAX_INCREMENTAL_DRAWDOWN,
    MIN_FORWARD_START,
    MIN_HIT_RATE,
    MechanismQualification,
    _max_drawdown,
    _mean_lower,
)
from inefficiency_engine.source_coverage import CandidateSourceSufficiency, SourceCoverageSnapshot
from inefficiency_engine.source_coverage_catalog import LANES


class GovernedMechanismExecutionService(ExecutableMechanismExecutionService):
    """Decouple forward learning from final allocation-grade source redundancy.

    A mechanism may open a paper research trial once its candidate has one fresh
    authoritative primary source and complete evidence classes. The existing
    two-independent-source contract remains mandatory before a candidate can be
    promoted into portfolio allocation. No threshold is weakened by this class.
    """

    @staticmethod
    def _primary_groups(venues: list[str], mechanism_id: str) -> set[str] | None:
        # Capital-location evidence is intentionally internal opportunity/transfer
        # telemetry, so a trading venue is not itself a required source group.
        if mechanism_id == "capital_location_settlement":
            return None
        groups = {
            group
            for venue in venues
            if (group := source_group_for_venue(venue)) is not None
        }
        return groups or None

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
            blockers.append("candidate primary venue/protocol source is not freshly admitted")
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
        *,
        mechanism_id: str,
        venues: list[str],
        coverage: SourceCoverageSnapshot | None = None,
    ) -> CandidateSourceSufficiency:
        required = [str(item) for item in list(LANES[mechanism_id]["required"])]
        primary = self._primary_groups(venues, mechanism_id)
        if coverage is None:
            return self.source_plane.candidate_sufficiency(
                mechanism_id,
                required_evidence_classes=required,
                primary_groups=primary,
            )
        lane = next((row for row in coverage.lanes if row.lane_id == mechanism_id), None)
        if lane is None:
            raise KeyError(mechanism_id)
        return self._gate_from_lane(lane, required=required, primary_groups=primary)

    @staticmethod
    def _semantic_economics_ready(mechanism_id: str, lane) -> bool:
        """Require truthful economics where raw evidence classes alone are insufficient.

        Yield is the clearest case: rate, capacity and exit-liquidity fields can all be
        present while protocol-loss/withdrawal economics are explicitly uncalibrated.
        Unknown risk must never be converted into zero risk simply because numeric
        placeholders exist. Other mechanism lanes construct research economics from
        their event/book surfaces. Liquidation is handled separately below because its
        current recovery shadows are useful learning evidence but not allocation-grade
        capture/settlement evidence.
        """

        if mechanism_id != "yield":
            return True
        required = set(str(item) for item in LANES["yield"]["required"])
        for source in list(lane.sources or []):
            if not bool(source.get("admitted")):
                continue
            classes = {str(item) for item in list(source.get("classes") or [])}
            if required.issubset(classes) and bool(source.get("economic_fields_complete")):
                return True
        return False

    @staticmethod
    def _liquidation_outcome_is_allocation_grade(row) -> bool:
        """Reject modeled capture/recovery shadows from capital qualification.

        Production liquidation settlement currently estimates capture probability from
        observation latency/event size and then marks the future price recovery. The
        outcome explicitly says ``capture_assumed=False`` and
        ``paper_capture_probability_model=True``. That is valuable forward research,
        but it is not empirical evidence that our order won selection, filled, and
        settled. Keep it in the append-only research ledger while excluding it from
        the 3->30 allocation gate.
        """

        if not bool(getattr(row, "settlement_evidence_complete", True)):
            return False
        detail = dict(getattr(row, "detail", {}) or {})
        if bool(detail.get("paper_capture_probability_model")):
            return False
        if detail.get("capture_assumed") is False:
            return False
        method = str(getattr(row, "settlement_method", "") or "")
        if method in {
            "observed_liquidation_price_to_recovery_mark",
            "latency_adjusted_capture_probability_recovery_shadow",
        }:
            return False
        return True

    def allocation_grade_outcomes(
        self,
        *,
        cohort_key: str | None = None,
        mechanism_id: str | None = None,
    ):
        rows = self.ledger.outcomes(
            cohort_key=cohort_key,
            mechanism_id=mechanism_id,
        )
        if mechanism_id != "liquidation_distress":
            return rows
        return [row for row in rows if self._liquidation_outcome_is_allocation_grade(row)]

    def qualification(self, cohort_key, mechanism_id):
        if mechanism_id != "liquidation_distress":
            return super().qualification(cohort_key, mechanism_id)

        outcomes = self.allocation_grade_outcomes(
            cohort_key=cohort_key,
            mechanism_id=mechanism_id,
        )
        values = [row.realized_net_return for row in outcomes]
        positive = sum(value > 0 for value in values)
        hit = positive / len(values) if values else None
        mean = statistics.fmean(values) if values else None
        lower = _mean_lower(values)
        drawdown = _max_drawdown(values) if values else None
        fraction = forward_evidence_allocation_fraction(
            len(values),
            full_target=FULL_FORWARD_TARGET,
        )
        blockers: list[str] = []
        if len(values) < MIN_FORWARD_START:
            blockers.append("fewer than three independent allocation-grade forward outcomes")
        if not values:
            blockers.append(
                "liquidation recovery shadows are research-only until empirical capture/selection and settlement evidence is connected"
            )
        if mean is None or mean <= 0:
            blockers.append("mean realized net return is non-positive")
        if hit is None or hit < MIN_HIT_RATE:
            blockers.append("forward hit rate is below 55%")
        if drawdown is None or drawdown > MAX_INCREMENTAL_DRAWDOWN:
            blockers.append("forward drawdown exceeds mechanism paper-risk limit")
        full = bool(
            len(values) >= FULL_FORWARD_TARGET
            and lower is not None
            and lower > 0
            and hit is not None
            and hit >= MIN_HIT_RATE
            and drawdown is not None
            and drawdown <= MAX_INCREMENTAL_DRAWDOWN
        )
        incremental = bool(
            MIN_FORWARD_START <= len(values) < FULL_FORWARD_TARGET
            and not blockers
            and fraction > 0
        )
        if len(values) >= FULL_FORWARD_TARGET and not full:
            blockers.append("full 30-outcome statistical gate is not satisfied")
        return MechanismQualification(
            mechanism_id=mechanism_id,
            cohort_key=cohort_key,
            sample_count=len(values),
            positive_count=positive,
            hit_rate=hit,
            mean_net_return=mean,
            mean_net_return_ci_lower=lower,
            max_drawdown=drawdown,
            allocation_fraction=1.0 if full else fraction if incremental else 0.0,
            incremental_eligible=incremental,
            fully_statistically_qualified=full,
            blockers=blockers,
        )

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        coverage = self.source_plane.snapshot(now=snapshot.completed_at)
        eligible = []
        for row in rows:
            try:
                lane = next(
                    item for item in coverage.lanes if item.lane_id == row.mechanism_id
                )
                gate = self._source_gate(
                    mechanism_id=row.mechanism_id,
                    venues=list(row.venues),
                    coverage=coverage,
                )
            except Exception:
                continue
            if not gate.forward_test_eligible:
                continue
            if not self._semantic_economics_ready(row.mechanism_id, lane):
                continue
            payload = dict(row.settlement_payload)
            payload["source_evidence_gate"] = {
                "research_eligible": gate.research_eligible,
                "forward_test_eligible": gate.forward_test_eligible,
                "allocation_source_qualified": gate.allocation_source_qualified,
                "covered_evidence_classes": gate.covered_evidence_classes,
                "missing_evidence_classes": gate.missing_evidence_classes,
                "admitted_source_groups": gate.admitted_source_groups,
                "primary_source_groups": gate.primary_source_groups,
                "semantic_economics_complete": True,
                "allocation_authority": False,
                "paper_only": True,
            }
            eligible.append(row.model_copy(update={"settlement_payload": payload}))
        return eligible

    def _candidate_from_spec(self, spec):
        # discover_specs stamps the current source decision onto the forward spec,
        # avoiding another full source-plane query inside the same evidence cycle.
        gate_payload = spec.settlement_payload.get("source_evidence_gate")
        if isinstance(gate_payload, dict):
            if not bool(gate_payload.get("allocation_source_qualified")):
                return None
            if not bool(gate_payload.get("semantic_economics_complete", True)):
                return None
            return super()._candidate_from_spec(spec)

        try:
            gate = self._source_gate(
                mechanism_id=spec.mechanism_id,
                venues=list(spec.venues),
            )
        except Exception:
            return None
        if not gate.allocation_source_qualified:
            return None
        return super()._candidate_from_spec(spec)

    def promoted_candidates(self, *, max_age_hours: float = 24.0):
        rows = super().promoted_candidates(max_age_hours=max_age_hours)
        coverage = self.source_plane.snapshot()
        qualified = []
        for row in rows:
            try:
                lane = next(
                    item for item in coverage.lanes if item.lane_id == row.mechanism_id
                )
                gate = self._source_gate(
                    mechanism_id=row.mechanism_id,
                    venues=list(row.venues),
                    coverage=coverage,
                )
            except Exception:
                continue
            if (
                gate.allocation_source_qualified
                and self._semantic_economics_ready(row.mechanism_id, lane)
            ):
                qualified.append(row)
        return qualified

from __future__ import annotations

import statistics

from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessMechanismExecutionService,
)
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
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


class YieldResearchShadowMechanismExecutionService(
    EvidenceVelocityLaneSuccessMechanismExecutionService
):
    """Let yield learn forward while keeping uncalibrated protocol risk out of capital.

    Morpho currently supplies authoritative rate, capacity and exit-liquidity fields,
    but the platform does not yet have an empirical protocol-loss calibration. The
    prior governed runtime therefore blocked yield before it could open any forward
    trial. That protected capital but also prevented the lane from accumulating the
    realized-yield/exit history needed for future calibration.

    This production research subclass separates those authorities:

    * source-complete yield observations may open paper research-shadow trials;
    * the shadow explicitly says protocol-risk economics are incomplete;
    * settled shadow outcomes are not allocation-grade evidence;
    * yield qualification always has zero allocation fraction until an explicitly
      calibrated economic path replaces the shadow flag;
    * every existing source/statistical/risk/settlement threshold remains unchanged.
    """

    @staticmethod
    def _semantic_economics_ready(mechanism_id: str, lane) -> bool:
        # Permit yield through the *research* discovery boundary only. The returned
        # spec is immediately stamped research-only below and cannot promote capital.
        if mechanism_id == "yield":
            return True
        return GovernedMechanismExecutionService._semantic_economics_ready(
            mechanism_id,
            lane,
        )

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        result = []
        for spec in rows:
            if spec.mechanism_id != "yield":
                result.append(spec)
                continue
            payload = dict(spec.settlement_payload)
            gate = dict(payload.get("source_evidence_gate") or {})
            gate.update(
                {
                    "semantic_economics_complete": False,
                    "research_shadow_only": True,
                    "protocol_risk_calibration_complete": False,
                    "allocation_authority": False,
                    "paper_only": True,
                }
            )
            payload["source_evidence_gate"] = gate
            payload.update(
                {
                    "yield_research_shadow": True,
                    "protocol_risk_calibration_complete": False,
                    "predicted_return_excludes_uncalibrated_protocol_loss": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                }
            )
            result.append(spec.model_copy(update={"settlement_payload": payload}))
        return result

    def _outcome(self, trial, settlement):
        if trial.mechanism_id != "yield" or not bool(
            trial.settlement_payload.get("yield_research_shadow")
        ):
            return super()._outcome(trial, settlement)

        # Bypass the Release-D wrapper for this one path so lane-success receives the
        # already-labeled research-only outcome rather than an unqualified copy.
        outcome = GovernedMechanismExecutionService._outcome(self, trial, settlement)
        detail = dict(outcome.detail or {})
        detail.update(
            {
                "yield_research_shadow": True,
                "protocol_risk_calibration_complete": False,
                "allocation_grade": False,
                "allocation_authority": False,
                "paper_only": True,
            }
        )
        outcome = outcome.model_copy(
            update={
                "settlement_evidence_complete": False,
                "detail": detail,
            }
        )
        self.lane_success.record_mechanism_outcome(
            trial,
            outcome,
            settlement_detail=detail,
        )
        return outcome

    @staticmethod
    def _yield_outcome_is_allocation_grade(row) -> bool:
        if not bool(getattr(row, "settlement_evidence_complete", True)):
            return False
        detail = dict(getattr(row, "detail", {}) or {})
        if bool(detail.get("yield_research_shadow")):
            return False
        if not bool(detail.get("protocol_risk_calibration_complete")):
            return False
        return bool(detail.get("allocation_grade", False))

    def allocation_grade_outcomes(
        self,
        *,
        cohort_key: str | None = None,
        mechanism_id: str | None = None,
    ):
        rows = self.raw_outcomes(
            cohort_key=cohort_key,
            mechanism_id=mechanism_id,
        )
        if mechanism_id == "yield":
            return [row for row in rows if self._yield_outcome_is_allocation_grade(row)]
        return super().allocation_grade_outcomes(
            cohort_key=cohort_key,
            mechanism_id=mechanism_id,
        )

    def qualification(self, cohort_key, mechanism_id):
        if mechanism_id != "yield":
            return super().qualification(cohort_key, mechanism_id)

        # Report the research-shadow statistics truthfully, but never convert them
        # into allocation authority while protocol-loss economics are uncalibrated.
        rows = self.raw_outcomes(cohort_key=cohort_key, mechanism_id="yield")
        values = [row.realized_net_return for row in rows]
        positive = sum(value > 0 for value in values)
        hit = positive / len(values) if values else None
        mean = statistics.fmean(values) if values else None
        lower = _mean_lower(values)
        drawdown = _max_drawdown(values) if values else None
        blockers = [
            "yield forward outcomes are research-only until empirical protocol-loss economics are calibrated",
        ]
        if len(values) < MIN_FORWARD_START:
            blockers.append("fewer than three independent yield research-shadow outcomes")
        if mean is None or mean <= 0:
            blockers.append("mean research-shadow net return is non-positive")
        if hit is None or hit < MIN_HIT_RATE:
            blockers.append("research-shadow forward hit rate is below 55%")
        if drawdown is None or drawdown > MAX_INCREMENTAL_DRAWDOWN:
            blockers.append("research-shadow drawdown exceeds mechanism paper-risk limit")
        if len(values) >= FULL_FORWARD_TARGET and (
            lower is None or lower <= 0
        ):
            blockers.append("30-outcome research-shadow confidence lower bound is non-positive")

        # Compute the informational evidence fraction for observability only; actual
        # allocation_fraction remains zero until protocol-risk calibration is real.
        _ = forward_evidence_allocation_fraction(
            len(values),
            full_target=FULL_FORWARD_TARGET,
        )
        return MechanismQualification(
            mechanism_id="yield",
            cohort_key=cohort_key,
            sample_count=len(values),
            positive_count=positive,
            hit_rate=hit,
            mean_net_return=mean,
            mean_net_return_ci_lower=lower,
            max_drawdown=drawdown,
            allocation_fraction=0.0,
            incremental_eligible=False,
            fully_statistically_qualified=False,
            blockers=blockers,
        )

    def _candidate_from_spec(self, spec):
        if spec.mechanism_id == "yield":
            gate = dict(spec.settlement_payload.get("source_evidence_gate") or {})
            if not bool(gate.get("semantic_economics_complete", False)):
                return None
            if bool(gate.get("research_shadow_only", False)):
                return None
        return super()._candidate_from_spec(spec)

    def promoted_candidates(self, *, max_age_hours: float = 24.0):
        rows = super().promoted_candidates(max_age_hours=max_age_hours)
        result = []
        for row in rows:
            if row.mechanism_id != "yield":
                result.append(row)
                continue
            gate = dict(row.settlement_payload.get("source_evidence_gate") or {})
            if bool(gate.get("semantic_economics_complete", False)) and not bool(
                gate.get("research_shadow_only", False)
            ):
                result.append(row)
        return result

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        cycle = await super().run_evidence_cycle(total_capital_usd=total_capital_usd)
        raw = self.raw_outcomes(mechanism_id="yield")
        allocation_grade = self.allocation_grade_outcomes(mechanism_id="yield")
        by_mechanism = dict(cycle.by_mechanism)
        row = dict(by_mechanism.get("yield") or {})
        row.update(
            {
                "research_shadow_outcome_count": len(raw),
                "allocation_grade_outcome_count": len(allocation_grade),
                "yield_research_shadow_active": True,
                "protocol_risk_calibration_complete": False,
                "allocation_requires_protocol_risk_calibration": True,
                "allocation_authority": False,
                "paper_only": True,
            }
        )
        by_mechanism["yield"] = row
        return cycle.model_copy(update={"by_mechanism": by_mechanism})

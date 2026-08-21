from __future__ import annotations

from inefficiency_engine.evidence_velocity import source_group_for_venue
from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService
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

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        coverage = self.source_plane.snapshot(now=snapshot.completed_at)
        eligible = []
        for row in rows:
            try:
                gate = self._source_gate(
                    mechanism_id=row.mechanism_id,
                    venues=list(row.venues),
                    coverage=coverage,
                )
            except Exception:
                continue
            if not gate.forward_test_eligible:
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
                gate = self._source_gate(
                    mechanism_id=row.mechanism_id,
                    venues=list(row.venues),
                    coverage=coverage,
                )
            except Exception:
                continue
            if gate.allocation_source_qualified:
                qualified.append(row)
        return qualified

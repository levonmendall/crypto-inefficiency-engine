from __future__ import annotations

from inefficiency_engine.evidence_velocity import source_group_for_venue
from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService
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

    def _source_gate(self, *, mechanism_id: str, venues: list[str]):
        required = [str(item) for item in list(LANES[mechanism_id]["required"])]
        return self.source_plane.candidate_sufficiency(
            mechanism_id,
            required_evidence_classes=required,
            primary_groups=self._primary_groups(venues, mechanism_id),
        )

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        eligible = []
        for row in rows:
            try:
                gate = self._source_gate(
                    mechanism_id=row.mechanism_id,
                    venues=list(row.venues),
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
        # This is the allocation boundary. Forward trials can learn with a single
        # authoritative source; portfolio candidates cannot bypass redundancy.
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
        qualified = []
        for row in rows:
            try:
                gate = self._source_gate(
                    mechanism_id=row.mechanism_id,
                    venues=list(row.venues),
                )
            except Exception:
                continue
            if gate.allocation_source_qualified:
                qualified.append(row)
        return qualified

from __future__ import annotations

from inefficiency_engine.executable_alpha_factory import ExecutableExpandedAlphaFactoryService
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService


class AllLaneEvidenceFactoryService(ExecutableExpandedAlphaFactoryService):
    """Run alpha and non-alpha mechanism forward evidence on one bounded cadence."""

    def __init__(self, core, store, *args, **kwargs):
        super().__init__(core, store, *args, **kwargs)
        self.mechanism_execution = GovernedMechanismExecutionService(core, store)

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        alpha = await super().run_evidence_cycle(total_capital_usd=total_capital_usd)
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

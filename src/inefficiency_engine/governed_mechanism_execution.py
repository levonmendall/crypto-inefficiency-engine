from __future__ import annotations

from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService


class GovernedMechanismExecutionService(ExecutableMechanismExecutionService):
    """Require decision-grade source coverage before opening a new mechanism trial."""

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        source_ready: dict[str, bool] = {}
        for row in rows:
            ready = source_ready.get(row.mechanism_id)
            if ready is None:
                try:
                    ready = self.source_plane.lane(row.mechanism_id).source_layer_sufficient
                except Exception:
                    ready = False
                source_ready[row.mechanism_id] = ready
        return [row for row in rows if source_ready.get(row.mechanism_id, False)]

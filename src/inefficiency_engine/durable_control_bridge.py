from __future__ import annotations

from datetime import timedelta

from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityBridgePublisher,
    candidate_has_canonical_settlement,
)
from inefficiency_engine.qualified_opportunity import QualifiedOpportunitySnapshot
from inefficiency_engine.qualified_opportunity_freshness import (
    _bridge_control_freshness_seconds,
    _candidate_freshness_seconds,
    _now,
)
from inefficiency_engine.unified_allocation import UnifiedPaperCandidate, _core_candidates


def _core_rejection_failures(snapshot, candidates) -> list[dict[str, object]]:
    """Explain observed core evidence that failed qualification, without relaxing it."""

    promoted_ids = {
        item.opportunity_id for item in candidates if item.opportunity_id is not None
    }
    opportunities = {item.id: item for item in snapshot.opportunities}
    failures: list[dict[str, object]] = []
    for execution in snapshot.executability:
        opportunity = opportunities.get(execution.opportunity_id)
        if opportunity is None or opportunity.id in promoted_ids:
            continue
        executable = [
            tier
            for tier in execution.tiers
            if tier.executable and tier.capital_required_usd > 0
        ]
        passing = [tier for tier in executable if tier.passes_return_hurdle]
        if executable and not passing:
            failures.append(
                {
                    "family": "core_cex",
                    "error_type": "EconomicQualificationRejected",
                    "reason": "executable core CEX evidence failed the unchanged return hurdle",
                    "opportunity_id": opportunity.id,
                    "executable_tier_count": len(executable),
                    "return_hurdle_pass_count": 0,
                }
            )
        elif not executable:
            failures.append(
                {
                    "family": "core_cex",
                    "error_type": "ExecutionQualificationRejected",
                    "reason": "observed core CEX evidence failed executable depth or capital qualification",
                    "opportunity_id": opportunity.id,
                    "executable_tier_count": 0,
                    "return_hurdle_pass_count": 0,
                }
            )
    return failures


class DurableControlQualifiedOpportunityBridgePublisher(
    CexDexFreshnessSeparatedQualifiedOpportunityBridgePublisher
):
    """Publish canonical candidates without any provider/network qualification call.

    The permanent control process is deliberately a durable-state process. Core CEX
    candidates are reconstructed from the latest persisted executable snapshot and
    alpha candidates are read from durable forward/research ledgers. CEX↔DEX live
    qualification remains owned by disposable research; this publisher carries a
    previously qualified CEX↔DEX candidate forward only while its own source evidence
    remains inside the unchanged candidate freshness window.

    The external control supervisor owns the 25-second hard deadline. Expose exact
    durable bridge substages to that supervisor before each potentially blocking
    operation so a killed executor leaves behind the precise production boundary
    instead of the coarse ``qualified_bridge_publication`` label. Reporting is
    diagnostic only and never changes qualification, freshness, or allocation state.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._control_stage_reporter = None

    def set_control_stage_reporter(self, reporter) -> None:
        """Attach the one-shot executor's lightweight status reporter."""

        self._control_stage_reporter = reporter
        alpha_factory = getattr(self.allocator, "alpha_factory", None)
        set_alpha_reporter = getattr(alpha_factory, "set_control_stage_reporter", None)
        if callable(set_alpha_reporter):
            set_alpha_reporter(self._report_alpha_control_stage)

    def _report_alpha_control_stage(self, stage: str) -> None:
        self._report_control_stage(f"alpha_promotion:{stage}")

    def _report_control_stage(self, stage: str) -> None:
        reporter = getattr(self, "_control_stage_reporter", None)
        if callable(reporter):
            reporter(f"qualified_bridge:{stage}")

    def _current_persisted_cex_dex(
        self,
        *,
        now,
        max_age_seconds: float,
    ) -> tuple[list[UnifiedPaperCandidate], list[dict[str, object]]]:
        previous = self.ledger.latest_active()
        if previous is None:
            return [], []

        rows: list[UnifiedPaperCandidate] = []
        for item in previous.candidates:
            if item.family != "cex_dex" or not candidate_has_canonical_settlement(item):
                continue
            source_at = item.source_observed_at
            if source_at is None:
                continue
            age = (now - source_at).total_seconds()
            if 0.0 <= age <= max_age_seconds:
                rows.append(item)

        failures = [
            dict(item)
            for item in previous.family_failures
            if isinstance(item, dict) and item.get("family") == "cex_dex"
        ]
        return rows, failures

    async def publish_latest(
        self,
        *,
        total_capital_usd: float,
    ) -> QualifiedOpportunitySnapshot | None:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")

        self._report_control_stage("source_scan_selection")
        snapshot = self._latest_scan()
        if snapshot is None:
            self._report_control_stage("source_scan_unavailable")
            return None

        self._report_control_stage("source_freshness_gate")
        now = _now()
        candidate_freshness = _candidate_freshness_seconds(self.core.settings)
        source_age = max(0.0, (now - snapshot.completed_at).total_seconds())
        if source_age > candidate_freshness:
            self._report_control_stage("source_scan_stale")
            return None

        rows: list[UnifiedPaperCandidate] = []
        failures: list[dict[str, object]] = []

        self._report_control_stage("core_cex_projection")
        try:
            core_rows = _core_candidates(snapshot.opportunities, snapshot.executability)
            rows.extend(core_rows)
            failures.extend(_core_rejection_failures(snapshot, core_rows))
        except Exception as exc:
            failures.append(
                {
                    "family": "core_cex",
                    "error_type": type(exc).__name__,
                    "reason": "core CEX durable bridge projection failed closed",
                }
            )

        self._report_control_stage("persisted_cex_dex_projection")
        persisted_cex_dex, persisted_failures = self._current_persisted_cex_dex(
            now=now,
            max_age_seconds=candidate_freshness,
        )
        rows.extend(persisted_cex_dex)
        failures.extend(persisted_failures)

        alpha_factory = self.allocator.alpha_factory
        if alpha_factory is not None:
            self._report_control_stage("alpha_promotion")
            try:
                rows.extend(
                    await self.allocator._alpha_family_candidates(
                        snapshot=snapshot,
                        total_capital_usd=total_capital_usd,
                    )
                )
                diagnostics = (
                    alpha_factory.durable_promotion_diagnostics()
                    if callable(getattr(alpha_factory, "durable_promotion_diagnostics", None))
                    else {}
                )
                missing_depth = int(
                    diagnostics.get("missing_current_executable_depth_count") or 0
                )
                if missing_depth > 0:
                    failures.append(
                        {
                            "family": "alpha",
                            "error_type": "MissingCurrentExecutableDepth",
                            "reason": (
                                "statistically eligible alpha candidates lacked a fresh matching "
                                "persisted order book; canonical control does not perform provider "
                                "fallback and leaves those candidates fail-closed"
                            ),
                            "candidate_count": missing_depth,
                            "provider_requests_used": 0,
                        }
                    )
            except Exception as exc:
                failures.append(
                    {
                        "family": "alpha",
                        "error_type": type(exc).__name__,
                        "reason": "alpha durable bridge projection failed closed",
                    }
                )

        self._report_control_stage("canonical_settlement_filter")
        deployable = [
            item
            for item in rows
            if item.allocation_eligible and candidate_has_canonical_settlement(item)
        ]
        deployable.sort(
            key=lambda item: (
                item.expected_return_on_reserved_capital,
                item.expected_profit_usd_per_deployment,
                -item.capital_required_usd,
            ),
            reverse=True,
        )
        result = QualifiedOpportunitySnapshot(
            observed_at=snapshot.completed_at,
            expires_at=now
            + timedelta(seconds=_bridge_control_freshness_seconds(self.core.settings)),
            source_scan_id=snapshot.scan_id,
            total_capital_usd=total_capital_usd,
            candidates=deployable,
            family_failures=failures,
        )
        self._report_control_stage("ledger_record")
        self.ledger.record(result)
        self._report_control_stage("complete")
        return result

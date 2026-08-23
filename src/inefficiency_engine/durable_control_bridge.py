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
    """

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

        snapshot = self._latest_scan()
        if snapshot is None:
            return None

        now = _now()
        candidate_freshness = _candidate_freshness_seconds(self.core.settings)
        source_age = max(0.0, (now - snapshot.completed_at).total_seconds())
        if source_age > candidate_freshness:
            return None

        rows: list[UnifiedPaperCandidate] = []
        failures: list[dict[str, object]] = []

        try:
            rows.extend(_core_candidates(snapshot.opportunities, snapshot.executability))
        except Exception as exc:
            failures.append(
                {
                    "family": "core_cex",
                    "error_type": type(exc).__name__,
                    "reason": "core CEX durable bridge projection failed closed",
                }
            )

        persisted_cex_dex, persisted_failures = self._current_persisted_cex_dex(
            now=now,
            max_age_seconds=candidate_freshness,
        )
        rows.extend(persisted_cex_dex)
        failures.extend(persisted_failures)

        alpha_factory = self.allocator.alpha_factory
        if alpha_factory is not None:
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
        self.ledger.record(result)
        return result

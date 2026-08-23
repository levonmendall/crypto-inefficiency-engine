from __future__ import annotations

from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService


class DurableControlAlphaFactoryService(DisposableExpandedAlphaFactoryService):
    """Alpha promotion surface for the canonical control process.

    The permanent control plane is a durable-state projection process. It must never
    perform provider acquisition while publishing the qualified-opportunity bridge.
    The inherited bounded alpha promotion already prefers order books carried by the
    persisted source snapshot; its only network escape hatch is
    ``_bounded_current_l2_cost`` when that snapshot lacks the exact book required by a
    candidate. Disable that escape hatch here so missing/stale executable depth fails
    the candidate closed instead of turning the control process into an exchange
    client.

    Disposable research keeps its existing bounded provider fallback in its own
    process. This subclass is used only by ``permanent_control_worker`` and therefore
    changes plumbing, not any economic/statistical/source/risk qualification hurdle.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._durable_missing_depth_count = 0

    async def _bounded_current_l2_cost(self, candidate):
        # Deliberately do not call the adapter registry. The caller already attempted
        # to locate a fresh matching book inside the current persisted ScanSnapshot.
        # No such book means allocation-grade current execution cost is unavailable.
        self._durable_missing_depth_count += 1
        return None

    async def _current_l2_cost(self, candidate):
        # Defense in depth for future refactors: any direct use of the old live-L2
        # promotion hook from the canonical control process must fail visibly rather
        # than silently reintroduce network I/O.
        raise RuntimeError("ProviderAccessForbiddenInCanonicalControl")

    async def refresh_l2_source_snapshot(self, quote_collector=None):
        raise RuntimeError("ProviderAcquisitionForbiddenInCanonicalControl")

    async def promoted_candidates(self, snapshot, *, total_capital_usd: float):
        self._durable_missing_depth_count = 0
        rows = await super().promoted_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
        )
        for candidate in rows:
            candidate.features.update(
                {
                    "canonical_control_durable_promotion": True,
                    "current_cost_source": "persisted_order_book",
                    "provider_requests_used_for_promotion": False,
                }
            )
        return rows

    def durable_promotion_diagnostics(self) -> dict[str, object]:
        return {
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "missing_current_executable_depth_count": self._durable_missing_depth_count,
            "missing_depth_policy": "fail_closed",
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

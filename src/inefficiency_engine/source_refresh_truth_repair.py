from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from inefficiency_engine import option_capacity
from inefficiency_engine import priority_source_collection as priority_sources
from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.provider_gap_collection import (
    ProviderAdmissionLedger,
    ProviderAdmissionObservation,
    ProviderProbeResult,
)
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService
from inefficiency_engine.remaining_source_transport_repair import (
    collect_deribit_option_capacity_resilient,
)
from inefficiency_engine.source_lane_repair_runtime import RemainingSourceLaneRepairService


PRESERVE_FRESH_DIRECT_SOURCES = {
    "aave-liquidations",
    "deribit-option-capacity",
}
DERIBIT_SHARED_RESULT_TTL_SECONDS = 30.0
DERIBIT_OPTION_EVIDENCE_TTL_SECONDS = 900.0

_RECORD_FAILURE_PATCH_MARKER = "_fresh_source_truth_failure_patch_installed"
_RECORD_PROBE_PATCH_MARKER = "_fresh_source_truth_probe_patch_installed"
_ADMISSION_PATCH_MARKER = "_fresh_deribit_admission_patch_installed"

_ORIGINAL_RECORD_FAILURE = priority_sources.PrioritySourceCollectionService._record_failure
_ORIGINAL_RECORD_PROBE = priority_sources.PrioritySourceCollectionService._record_probe
_ORIGINAL_ADMISSION_RECORD = ProviderAdmissionLedger.record

_DERIBIT_INFLIGHT: dict[tuple[int, int], asyncio.Task[SourceProbeResult]] = {}
_DERIBIT_SUCCESS_CACHE: dict[int, tuple[float, SourceProbeResult]] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _record_refresh_heartbeat(
    store,
    *,
    source_id: str,
    state: str,
    **detail: object,
) -> None:
    try:
        store.record_worker_heartbeat(
            worker_id=f"source-refresh-{source_id}",
            state=state,
            detail={
                "stage": "source_refresh_transport",
                "source_id": source_id,
                "refresh_failure_is_not_new_source_evidence": True,
                "source_freshness_thresholds_unchanged": True,
                "qualification_thresholds_unchanged": True,
                "paper_only": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                **detail,
            },
        )
    except Exception:
        # Refresh telemetry is advisory and must never affect evidence authority.
        pass


def _fresh_direct_truth(
    service: RemainingSourceLaneRepairService,
    *,
    source_id: str,
    lane_ids: list[str],
) -> dict[str, object] | None:
    """Return prior healthy direct evidence only while its real class TTL is valid."""

    try:
        latest = service.source_coverage.ledger.latest()
    except Exception:
        return None
    now = _now()
    ages: list[float] = []
    ttls: list[float] = []
    observed_values: list[str] = []
    for lane_id in lane_ids:
        row = latest.get((source_id, lane_id))
        if row is None or not bool(getattr(row, "healthy", False)):
            return None
        observed_at = getattr(row, "observed_at", None)
        if not isinstance(observed_at, datetime):
            return None
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        observed_at = observed_at.astimezone(timezone.utc)
        classes = list(getattr(row, "evidence_classes", ()) or ())
        try:
            ttl_seconds = float(service.source_coverage._freshness_seconds(classes))
        except Exception:
            return None
        age_seconds = max(0.0, (now - observed_at).total_seconds())
        if age_seconds > ttl_seconds:
            return None
        ages.append(age_seconds)
        ttls.append(ttl_seconds)
        observed_values.append(observed_at.isoformat())
    if not ages:
        return None
    return {
        "previous_success_observed_at": max(observed_values),
        "previous_success_age_seconds": max(ages),
        "evidence_freshness_ttl_seconds": min(ttls),
        "evidence_still_fresh": True,
    }


def _record_failure_preserving_fresh_truth(
    self: RemainingSourceLaneRepairService,
    source_id: str,
    lane_ids: list[str],
    source_reference: str,
    exc: Exception,
) -> None:
    """Do not let a failed refresh overwrite still-valid evidence for proven flaky sources."""

    if source_id in PRESERVE_FRESH_DIRECT_SOURCES:
        preserved = _fresh_direct_truth(self, source_id=source_id, lane_ids=lane_ids)
        if preserved is not None:
            _record_refresh_heartbeat(
                self.store,
                source_id=source_id,
                state="degraded",
                refresh_error_type=type(exc).__name__,
                refresh_error_message=str(exc)[:300],
                preserved_previous_source_observation=True,
                fail_closed_when_evidence_stales=True,
                **preserved,
            )
            return
    _ORIGINAL_RECORD_FAILURE(self, source_id, lane_ids, source_reference, exc)


def _record_probe_with_refresh_heartbeat(
    self: RemainingSourceLaneRepairService,
    probe: SourceProbeResult,
) -> None:
    _ORIGINAL_RECORD_PROBE(self, probe)
    if probe.source_id in PRESERVE_FRESH_DIRECT_SOURCES:
        _record_refresh_heartbeat(
            self.store,
            source_id=probe.source_id,
            state="success",
            refresh_error_type=None,
            item_count=probe.item_count,
            source_reference=probe.source_reference,
            preserved_previous_source_observation=False,
        )


def _copy_probe(probe: SourceProbeResult) -> SourceProbeResult:
    return probe.model_copy(deep=True)


async def collect_deribit_option_capacity_shared(store) -> SourceProbeResult:
    """Single-flight the identical Deribit option acquisition across runtime owners.

    Provider-gap admission, source coverage, and critical cadence all need the same
    bounded Deribit summary/order-book surface. One short-lived successful result can
    be reused by those owners without reissuing the same transport burst. The cache
    lifetime is only an acquisition de-duplication window; it does not extend the
    persisted 900-second option evidence freshness threshold.
    """

    store_key = id(store)
    now_mono = time.monotonic()
    cached = _DERIBIT_SUCCESS_CACHE.get(store_key)
    if cached is not None and now_mono - cached[0] <= DERIBIT_SHARED_RESULT_TTL_SECONDS:
        probe = _copy_probe(cached[1])
        probe.detail["shared_transport_result"] = True
        probe.detail["shared_result_cache_hit"] = True
        probe.detail["singleflight_joined"] = False
        probe.detail["shared_result_ttl_seconds"] = DERIBIT_SHARED_RESULT_TTL_SECONDS
        return probe

    loop = asyncio.get_running_loop()
    inflight_key = (store_key, id(loop))
    task = _DERIBIT_INFLIGHT.get(inflight_key)
    joined = bool(task is not None and not task.done())
    if not joined:
        task = asyncio.create_task(collect_deribit_option_capacity_resilient(store))
        _DERIBIT_INFLIGHT[inflight_key] = task
    assert task is not None
    try:
        probe = await asyncio.shield(task)
        if not joined:
            _DERIBIT_SUCCESS_CACHE[store_key] = (time.monotonic(), _copy_probe(probe))
        result = _copy_probe(probe)
        result.detail["shared_transport_result"] = True
        result.detail["shared_result_cache_hit"] = False
        result.detail["singleflight_joined"] = joined
        result.detail["shared_result_ttl_seconds"] = DERIBIT_SHARED_RESULT_TTL_SECONDS
        return result
    finally:
        if _DERIBIT_INFLIGHT.get(inflight_key) is task and task.done():
            _DERIBIT_INFLIGHT.pop(inflight_key, None)


async def _collect_deribit_options_via_shared_capacity(
    self: ResilientProviderGapCollectionService,
) -> ProviderProbeResult:
    probe = await collect_deribit_option_capacity_shared(self.store)
    quote_count = int(probe.detail.get("option_quote_greek_observation_count") or 0)
    if quote_count <= 0:
        raise ValueError(
            "Deribit shared capacity refresh produced no bounded executable option quotes with Greeks"
        )
    return ProviderProbeResult(
        mechanism_id="volatility",
        provider=self.DERIBIT_PROVIDER,
        item_count=quote_count,
        source_reference=probe.source_reference,
        detail={
            **probe.detail,
            "provider_gap_reuses_shared_capacity_collector": True,
            "same_deribit_order_books": True,
            "option_observation_count": quote_count,
            "provider_policy_unchanged": True,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        },
    )


def _record_admission_preserving_fresh_deribit(
    self: ProviderAdmissionLedger,
    observation: ProviderAdmissionObservation,
) -> str:
    """Keep a fresh successful Deribit admission authoritative across a refresh miss."""

    is_deribit_option = bool(
        observation.mechanism_id == "volatility"
        and observation.provider.startswith("deribit:public-option-order-book")
    )
    if is_deribit_option and not observation.healthy:
        try:
            previous = self.latest_by_provider("volatility").get(observation.provider)
        except Exception:
            previous = None
        if previous is not None and bool(previous.healthy):
            observed_at = previous.observed_at
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=timezone.utc)
            age_seconds = max(
                0.0,
                (_now() - observed_at.astimezone(timezone.utc)).total_seconds(),
            )
            if age_seconds <= DERIBIT_OPTION_EVIDENCE_TTL_SECONDS:
                _record_refresh_heartbeat(
                    self.store,
                    source_id="deribit-options",
                    state="degraded",
                    refresh_error_type=observation.error_type,
                    refresh_error_message=str(observation.detail.get("message") or "")[:300],
                    preserved_previous_provider_admission=True,
                    previous_success_observed_at=observed_at.isoformat(),
                    previous_success_age_seconds=age_seconds,
                    evidence_freshness_ttl_seconds=DERIBIT_OPTION_EVIDENCE_TTL_SECONDS,
                    fail_closed_when_evidence_stales=True,
                )
                return previous.admission_id

    admission_id = _ORIGINAL_ADMISSION_RECORD(self, observation)
    if is_deribit_option and observation.healthy:
        _record_refresh_heartbeat(
            self.store,
            source_id="deribit-options",
            state="success",
            refresh_error_type=None,
            item_count=observation.item_count,
            source_reference=observation.source_reference,
            preserved_previous_provider_admission=False,
        )
    return admission_id


def install_source_refresh_truth_repair() -> None:
    """Install fail-soft refresh telemetry without weakening evidence freshness."""

    # One shared acquisition result feeds every Deribit option owner. This removes the
    # race where a successful quote/Greek fetch could be followed seconds later by an
    # independent capacity timeout from the identical first-party book surface.
    option_capacity.collect_deribit_option_capacity = collect_deribit_option_capacity_shared
    priority_sources.collect_deribit_option_capacity = collect_deribit_option_capacity_shared
    recovery_v2.collect_deribit_option_capacity = collect_deribit_option_capacity_shared
    ResilientProviderGapCollectionService._collect_deribit_options = (
        _collect_deribit_options_via_shared_capacity
    )

    service = RemainingSourceLaneRepairService
    if not bool(getattr(service, _RECORD_FAILURE_PATCH_MARKER, False)):
        service._record_failure = _record_failure_preserving_fresh_truth
        setattr(service, _RECORD_FAILURE_PATCH_MARKER, True)
    if not bool(getattr(service, _RECORD_PROBE_PATCH_MARKER, False)):
        service._record_probe = _record_probe_with_refresh_heartbeat
        setattr(service, _RECORD_PROBE_PATCH_MARKER, True)

    if not bool(getattr(ProviderAdmissionLedger, _ADMISSION_PATCH_MARKER, False)):
        ProviderAdmissionLedger.record = _record_admission_preserving_fresh_deribit
        setattr(ProviderAdmissionLedger, _ADMISSION_PATCH_MARKER, True)

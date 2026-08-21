from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel


ProviderConnectivityState = Literal["healthy", "degraded", "missing", "stale"]
SourceSufficiencyState = Literal[
    "sufficient",
    "provider_gap",
    "evidence_class_gap",
    "redundancy_gap",
    "stale",
]
SourceHeadlineState = Literal["ready", "source_gap", "degraded", "provider_gap"]


class _LaneSourceLike(Protocol):
    source_layer_sufficient: bool
    healthy_source_count: int
    source_redundancy_satisfied: bool
    missing_evidence_classes: list[str]
    sources: list[dict[str, object]]


class LaneSourceDimensions(BaseModel):
    """Independent read-model dimensions for one profit lane's source layer.

    Provider connectivity answers whether an authoritative provider is currently
    usable. Research/forward eligibility describe whether the lane may continue
    evidence accumulation. Allocation source qualification remains the original
    decision-grade two-source contract. None of these dimensions independently
    grants portfolio authority.
    """

    provider_connectivity_state: ProviderConnectivityState
    source_sufficiency_state: SourceSufficiencyState
    source_headline_state: SourceHeadlineState
    provider_ready: bool
    research_eligible: bool = False
    forward_test_eligible: bool = False
    allocation_source_qualified: bool = False
    source_layer_sufficient: bool


def classify_lane_source_dimensions(lane: _LaneSourceLike) -> LaneSourceDimensions:
    sources = list(getattr(lane, "sources", []) or [])
    admitted = [row for row in sources if bool(row.get("admitted"))]
    provider_ready = bool(admitted or int(getattr(lane, "healthy_source_count", 0)) > 0)

    # Credential-gated optional sources and never-observed sources are not provider
    # failures. A fresh admitted source can coexist with another failed/stale source;
    # that is degraded connectivity, not a provider gap.
    observed_states = {
        str(row.get("state") or "")
        for row in sources
        if str(row.get("state") or "") not in {"", "credential_required", "unobserved"}
    }
    has_failed = "failed" in observed_states
    has_stale = "stale" in observed_states

    if provider_ready:
        connectivity: ProviderConnectivityState = (
            "degraded" if has_failed or has_stale else "healthy"
        )
    elif has_stale:
        connectivity = "stale"
    elif has_failed:
        connectivity = "degraded"
    else:
        connectivity = "missing"

    source_layer_sufficient = bool(getattr(lane, "source_layer_sufficient", False))
    research_eligible = bool(getattr(lane, "research_eligible", provider_ready))
    forward_test_eligible = bool(
        getattr(
            lane,
            "forward_test_eligible",
            research_eligible and not list(getattr(lane, "missing_evidence_classes", []) or []),
        )
    )
    allocation_source_qualified = bool(
        getattr(lane, "allocation_source_qualified", source_layer_sufficient)
    )
    missing_classes = list(getattr(lane, "missing_evidence_classes", []) or [])
    redundancy_satisfied = bool(getattr(lane, "source_redundancy_satisfied", False))

    if allocation_source_qualified:
        sufficiency: SourceSufficiencyState = "sufficient"
    elif not provider_ready and connectivity == "stale":
        sufficiency = "stale"
    elif not provider_ready:
        sufficiency = "provider_gap"
    elif missing_classes:
        sufficiency = "evidence_class_gap"
    elif not redundancy_satisfied:
        sufficiency = "redundancy_gap"
    else:
        sufficiency = "evidence_class_gap"

    if allocation_source_qualified:
        headline: SourceHeadlineState = "ready"
    elif connectivity in {"degraded", "stale"}:
        headline = "degraded"
    elif sufficiency == "provider_gap":
        headline = "provider_gap"
    else:
        headline = "source_gap"

    return LaneSourceDimensions(
        provider_connectivity_state=connectivity,
        source_sufficiency_state=sufficiency,
        source_headline_state=headline,
        provider_ready=provider_ready,
        research_eligible=research_eligible,
        forward_test_eligible=forward_test_eligible,
        allocation_source_qualified=allocation_source_qualified,
        source_layer_sufficient=source_layer_sufficient,
    )

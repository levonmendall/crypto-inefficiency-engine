from __future__ import annotations
from pydantic import BaseModel, Field

class SourceProbeResult(BaseModel):
    source_id: str
    item_count: int = Field(ge=0)
    source_reference: str
    evidence_by_lane: dict[str, list[str]]
    authoritative: bool = True
    commercial_use_permitted: bool = True
    point_in_time: bool = True
    economic_fields_complete: bool = False
    forward_testable_evidence: bool = False
    detail: dict[str, object] = Field(default_factory=dict)

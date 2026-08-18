from __future__ import annotations

from pydantic import BaseModel

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService


class ReplayResult(BaseModel):
    scan_id: str
    recorded_opportunity_ids: list[str]
    replayed_opportunity_ids: list[str]
    deterministic_match: bool


def replay_scan(store: EvidenceStore, service: OpportunityService, scan_id: str) -> ReplayResult:
    snapshot = store.load_scan(scan_id)
    replay_service = service
    if snapshot.analysis_config:
        replay_service = OpportunityService(settings=Settings(**snapshot.analysis_config))
    replayed = replay_service.analyze(snapshot.funding_quotes, snapshot.market_quotes)
    recorded_ids = [item.id for item in snapshot.opportunities]
    replayed_ids = [item.id for item in replayed]
    return ReplayResult(
        scan_id=scan_id,
        recorded_opportunity_ids=recorded_ids,
        replayed_opportunity_ids=replayed_ids,
        deterministic_match=recorded_ids == replayed_ids,
    )

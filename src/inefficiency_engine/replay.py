from __future__ import annotations

from pydantic import BaseModel

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.service import OpportunityService


class ReplayResult(BaseModel):
    scan_id: str
    recorded_opportunity_ids: list[str]
    replayed_opportunity_ids: list[str]
    deterministic_match: bool
    execution_deterministic_match: bool | None = None


def replay_scan(store: EvidenceStore, service: OpportunityService, scan_id: str) -> ReplayResult:
    snapshot = store.load_scan(scan_id)
    replay_service = service
    replay_settings = service.settings
    if snapshot.analysis_config:
        replay_settings = Settings(**snapshot.analysis_config)
        replay_service = OpportunityService(settings=replay_settings)
    replayed = replay_service.analyze(snapshot.funding_quotes, snapshot.market_quotes)
    recorded_ids = [item.id for item in snapshot.opportunities]
    replayed_ids = [item.id for item in replayed]

    execution_match: bool | None = None
    if snapshot.executability:
        replay_time = snapshot.executability[0].observed_at
        replayed_by_id = {item.id: item for item in replayed}
        replayed_executability = [
            qualify_opportunity(
                replayed_by_id[item.opportunity_id],
                snapshot.order_books,
                replay_settings,
                notionals_usd=replay_settings.capital_tiers_usd,
                now=replay_time,
            )
            for item in snapshot.executability
            if item.opportunity_id in replayed_by_id
        ]
        execution_match = (
            [item.model_dump(mode="json") for item in snapshot.executability]
            == [item.model_dump(mode="json") for item in replayed_executability]
        )

    return ReplayResult(
        scan_id=scan_id,
        recorded_opportunity_ids=recorded_ids,
        replayed_opportunity_ids=replayed_ids,
        deterministic_match=recorded_ids == replayed_ids,
        execution_deterministic_match=execution_match,
    )

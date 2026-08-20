from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.bounded_research_closure import MemoryBoundedResearchClosureService
from inefficiency_engine.research_mechanisms import CapitalLocationResearchService


class ResearchClosureCycleSummary(BaseModel):
    summary_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    rejection_funnels: dict[str, dict[str, object]]
    capital_location_forward: dict[str, object]
    maker_shadow: dict[str, object]
    canonical_capabilities: dict[str, bool]
    provider_admission: dict[str, dict[str, object]]
    paper_only: bool = True
    live_execution_authority: bool = False


class ResearchClosureSummaryLedger:
    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.table = Table(
            "research_closure_cycle_summaries",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("summary_id", String(64), nullable=False, unique=True),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_research_closure_observed", self.table.c.observed_at)
        metadata.create_all(store.engine)

    def record(self, row: ResearchClosureCycleSummary) -> None:
        raw = json.dumps(row.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        lineage = hashlib.sha256(raw.encode()).hexdigest()
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.table.c.summary_id).where(self.table.c.summary_id == row.summary_id)
            ).scalar_one_or_none()
            if exists is None:
                db.execute(insert(self.table), {
                    "summary_id": row.summary_id,
                    "observed_at": row.observed_at.isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })


def _provider_admission(latest_operating) -> dict[str, dict[str, object]]:
    by_id = {row.mechanism_id: row for row in (latest_operating.mechanisms if latest_operating else [])}
    contracts = {
        "fundamental_onchain": "FundamentalFactorObservation",
        "event_driven": "EventObservation",
        "yield": "YieldObservation",
        "volatility": "OptionQuoteObservation",
        "liquidation_distress": "DistressOpportunityObservation",
    }
    result: dict[str, dict[str, object]] = {}
    for mechanism_id, contract in contracts.items():
        row = by_id.get(mechanism_id)
        connected = bool(row and row.provider_ready and row.authoritative_observation_count > 0)
        result[mechanism_id] = {
            "observation_contract": contract,
            "admission_contract_ready": True,
            "authoritative_provider_connected": connected,
            "commercial_permission_must_be_explicit": True,
            "point_in_time_lineage_required": True,
            "fail_closed_until_connected": True,
        }
    return result


def _canonical_capabilities() -> dict[str, bool]:
    """Single runtime truth for capabilities already implemented in the allocator.

    These facts mirror the settlement methods enforced by AllocationForwardCertificationService;
    they are not inferred from whether a profitable trial has happened yet.
    """

    return {
        "spot_forward_settlement": True,
        "perpetual_short_observed_funding_settlement": True,
        "realized_two_leg_cex_settlement": True,
        "capital_location_forward_testing": True,
        "maker_shadow_cross_through_learning": True,
        "maker_queue_priority_observable": False,
        "live_execution_authority": False,
    }


async def run_research_closure_cycle(
    *,
    service,
    store,
    alpha_factory,
    operating_certification,
    total_capital_usd: float,
) -> ResearchClosureCycleSummary | None:
    """Persist diagnostics/forward cohorts after an operating certification cycle.

    The function consumes the newest already-persisted scan. It does not trigger a
    second provider scan. Rejection analysis retains only running best candidates,
    so broad discovery does not create a quadratic diagnostics working set.
    """

    del alpha_factory
    with store.engine.connect() as db:
        scan_id = db.execute(
            select(store.scans.c.scan_id).order_by(store.scans.c.completed_at.desc()).limit(1)
        ).scalar_one_or_none()
    if scan_id is None:
        return None
    snapshot = store.load_scan(str(scan_id))
    latest_operating = operating_certification.ledger.latest()
    microstructure_emitted_count = 0
    if latest_operating is not None:
        for row in latest_operating.mechanisms:
            if row.mechanism_id == "microstructure":
                microstructure_emitted_count = row.current_candidate_count
                break

    closure = MemoryBoundedResearchClosureService(store, service.settings)
    rejection_rows = closure.record_rejection_funnels(
        market_quotes=snapshot.market_quotes,
        funding_quotes=snapshot.funding_quotes,
        opportunities=snapshot.opportunities,
        order_books=snapshot.order_books,
        microstructure_emitted_count=microstructure_emitted_count,
        observed_at=snapshot.completed_at,
    )

    location_plan = CapitalLocationResearchService(store).plan(
        reserve_capital_usd=total_capital_usd
    )
    location = closure.run_capital_location_forward_cycle(
        location_plan,
        now=snapshot.completed_at,
        horizon_hours=max(1.0, float(getattr(service.settings, "default_holding_hours", 24.0)) / 24.0),
    )
    maker = closure.run_maker_shadow_cycle(
        snapshot.order_books,
        now=snapshot.completed_at,
        horizon_seconds=max(30.0, float(getattr(service.settings, "shadow_cycle_interval_seconds", 30.0))),
    )

    summary = ResearchClosureCycleSummary(
        observed_at=snapshot.completed_at,
        rejection_funnels={
            key: value.model_dump(mode="json") for key, value in rejection_rows.items()
        },
        capital_location_forward=location,
        maker_shadow=maker,
        canonical_capabilities=_canonical_capabilities(),
        provider_admission=_provider_admission(latest_operating),
    )
    ResearchClosureSummaryLedger(store).record(summary)
    return summary

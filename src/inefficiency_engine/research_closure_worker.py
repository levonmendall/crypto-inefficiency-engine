from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.bounded_capital_location import (
    MemoryBoundedCapitalLocationResearchService,
)
from inefficiency_engine.bounded_research_closure import MemoryBoundedResearchClosureService


RESEARCH_CLOSURE_WORKER_ID = "research-closure-diagnostic-loop"


class ResearchClosureCycleSummary(BaseModel):
    summary_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    source_scan_id: str
    source_order_book_count: int = 0
    usable_order_book_count: int = 0
    rejection_funnels: dict[str, dict[str, object]]
    capital_location_forward: dict[str, object]
    maker_shadow: dict[str, object]
    canonical_capabilities: dict[str, bool]
    provider_admission: dict[str, dict[str, object]]
    diagnostic_errors: dict[str, str] = Field(default_factory=dict)
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


def _record_runtime_heartbeat(store, *, state: str, error_type: str | None = None, detail=None) -> None:
    """Publish closure liveness without allowing telemetry failure to block research."""
    try:
        store.record_worker_heartbeat(
            worker_id=RESEARCH_CLOSURE_WORKER_ID,
            state=state,
            error_type=error_type,
            detail={"paper_only": True, **(detail or {})},
        )
    except Exception:
        pass


def _empty_location_summary() -> dict[str, object]:
    return {
        "available": False,
        "trial_count": 0,
        "outcome_count": 0,
        "mean_incremental_option_value": None,
        "positive_incremental_rate": None,
        "transfer_evidence_complete": False,
        "decision_grade": False,
    }


def _empty_maker_summary() -> dict[str, object]:
    return {
        "available": False,
        "trial_count": 0,
        "outcome_count": 0,
        "crossed_through_count": 0,
        "queue_fill_confirmed_count": 0,
        "adverse_selection_observation_count": 0,
        "queue_position_observable": False,
        "decision_grade": False,
    }


async def run_research_closure_cycle(
    *,
    service,
    store,
    alpha_factory,
    operating_certification,
    total_capital_usd: float,
) -> ResearchClosureCycleSummary | None:
    """Persist a fail-contained research-closure checkpoint from the newest scan.

    The v3.6.0 implementation treated rejection diagnostics, capital-location
    research, maker shadow learning, and summary publication as one all-or-nothing
    operation. A single research-only substage could therefore prevent every closure
    diagnostic from becoming visible while the surrounding worker still reported a
    healthy core cycle. This version isolates each substage, records a compact summary
    even when one substage degrades, and publishes a dedicated closure heartbeat.
    """

    del alpha_factory
    with store.engine.connect() as db:
        scan_id = db.execute(
            select(store.scans.c.scan_id).order_by(store.scans.c.completed_at.desc()).limit(1)
        ).scalar_one_or_none()
    if scan_id is None:
        _record_runtime_heartbeat(
            store,
            state="degraded",
            error_type="NoPersistedScan",
            detail={"summary_recorded": False},
        )
        return None

    scan_id_text = str(scan_id)
    _record_runtime_heartbeat(
        store,
        state="running",
        detail={"source_scan_id": scan_id_text, "summary_recorded": False},
    )

    try:
        snapshot = store.load_scan(scan_id_text)
    except Exception as exc:
        _record_runtime_heartbeat(
            store,
            state="error",
            error_type=type(exc).__name__,
            detail={"source_scan_id": scan_id_text, "stage": "load_scan", "summary_recorded": False},
        )
        raise

    diagnostic_errors: dict[str, str] = {}
    latest_operating = None
    try:
        latest_operating = operating_certification.ledger.latest()
    except Exception as exc:
        diagnostic_errors["operating_snapshot"] = type(exc).__name__

    microstructure_emitted_count = 0
    if latest_operating is not None:
        for row in latest_operating.mechanisms:
            if row.mechanism_id == "microstructure":
                microstructure_emitted_count = row.current_candidate_count
                break

    # Current OrderBookSnapshot validation normally guarantees both sides. Keep the
    # guard at the research boundary as protection against legacy/injected payloads.
    usable_order_books = [book for book in snapshot.order_books if book.bids and book.asks]

    closure = None
    try:
        closure = MemoryBoundedResearchClosureService(store, service.settings)
    except Exception as exc:
        diagnostic_errors["closure_ledger"] = type(exc).__name__

    rejection_rows: dict[str, object] = {}
    if closure is not None:
        try:
            rejection_rows = closure.record_rejection_funnels(
                market_quotes=snapshot.market_quotes,
                funding_quotes=snapshot.funding_quotes,
                opportunities=snapshot.opportunities,
                order_books=usable_order_books,
                microstructure_emitted_count=microstructure_emitted_count,
                observed_at=snapshot.completed_at,
            )
        except Exception as exc:
            diagnostic_errors["rejection_funnels"] = type(exc).__name__

    location = _empty_location_summary()
    if closure is not None:
        try:
            location_plan = MemoryBoundedCapitalLocationResearchService(
                store,
                history_hours=max(1.0, float(getattr(service.settings, "alpha_history_hours", 72.0))),
                max_history_records=5_000,
            ).plan(
                reserve_capital_usd=total_capital_usd,
                now=snapshot.completed_at,
            )
            location = {
                "available": True,
                **closure.run_capital_location_forward_cycle(
                    location_plan,
                    now=snapshot.completed_at,
                    horizon_hours=max(
                        1.0,
                        float(getattr(service.settings, "default_holding_hours", 24.0)) / 24.0,
                    ),
                ),
            }
        except Exception as exc:
            diagnostic_errors["capital_location_forward"] = type(exc).__name__

    maker = _empty_maker_summary()
    if closure is not None:
        try:
            maker = {
                "available": True,
                **closure.run_maker_shadow_cycle(
                    usable_order_books,
                    now=snapshot.completed_at,
                    horizon_seconds=max(
                        30.0,
                        float(getattr(service.settings, "shadow_cycle_interval_seconds", 30.0)),
                    ),
                ),
            }
        except Exception as exc:
            diagnostic_errors["maker_shadow"] = type(exc).__name__

    summary = ResearchClosureCycleSummary(
        observed_at=snapshot.completed_at,
        source_scan_id=scan_id_text,
        source_order_book_count=len(snapshot.order_books),
        usable_order_book_count=len(usable_order_books),
        rejection_funnels={
            key: value.model_dump(mode="json")
            for key, value in rejection_rows.items()
            if hasattr(value, "model_dump")
        },
        capital_location_forward=location,
        maker_shadow=maker,
        canonical_capabilities=_canonical_capabilities(),
        provider_admission=_provider_admission(latest_operating),
        diagnostic_errors=diagnostic_errors,
    )

    try:
        ResearchClosureSummaryLedger(store).record(summary)
    except Exception as exc:
        _record_runtime_heartbeat(
            store,
            state="error",
            error_type=type(exc).__name__,
            detail={
                "source_scan_id": scan_id_text,
                "stage": "summary_persistence",
                "summary_recorded": False,
                "diagnostic_errors": diagnostic_errors,
            },
        )
        raise

    _record_runtime_heartbeat(
        store,
        state="degraded" if diagnostic_errors else "success",
        error_type=next(iter(diagnostic_errors.values()), None),
        detail={
            "source_scan_id": scan_id_text,
            "summary_id": summary.summary_id,
            "summary_observed_at": summary.observed_at.isoformat(),
            "summary_recorded": True,
            "source_order_book_count": len(snapshot.order_books),
            "usable_order_book_count": len(usable_order_books),
            "diagnostic_errors": diagnostic_errors,
        },
    )
    return summary

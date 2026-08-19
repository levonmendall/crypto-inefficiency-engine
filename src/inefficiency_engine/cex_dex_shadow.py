from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from statistics import fmean

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, func, insert, select

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.evidence import EvidenceStore


SleepFn = Callable[[float], Awaitable[None]]


def composite_edge_key(evidence: CexDexCompositeEvidence) -> str:
    raw = "|".join(
        (
            evidence.asset.upper(),
            evidence.route_direction,
            f"{evidence.target_notional_usd:.8f}",
            evidence.cex_venue.lower(),
            evidence.cex_symbol.upper(),
            evidence.cex_quote_currency.upper(),
            evidence.route_quote_currency.upper(),
        )
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


class CexDexCompositeEdgeRecord(BaseModel):
    record_id: str
    cycle_id: str
    phase: str
    horizon_seconds: float = Field(ge=0)
    composite_key: str
    observed_at: datetime
    evidence: CexDexCompositeEvidence
    paper_only: bool = True


class CexDexCompositeEdgeObservation(BaseModel):
    cycle_id: str
    composite_key: str
    asset: str
    route_direction: str
    target_notional_usd: float = Field(gt=0)
    cex_venue: str
    cex_symbol: str
    horizon_seconds: float = Field(ge=0)
    initial_evidence_id: str
    verification_evidence_id: str | None = None
    initial_net_edge_bps: float
    verification_net_edge_bps: float | None = None
    net_edge_change_bps: float | None = None
    adverse_deterioration_bps: float | None = Field(default=None, ge=0)
    retained_edge_fraction: float | None = None
    initial_above_hurdle: bool
    verification_above_hurdle: bool | None = None
    hurdle_survived: bool = False
    survived: bool
    failure_type: str | None = None
    verified_at: datetime
    capacity_claimed: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class CexDexCompositeEdgeShadowCycle(BaseModel):
    cycle_id: str
    started_at: datetime
    completed_at: datetime
    horizons_seconds: list[float]
    min_net_edge_bps: float
    initial_evidence_count: int
    records: list[CexDexCompositeEdgeRecord] = Field(default_factory=list)
    observations: list[CexDexCompositeEdgeObservation] = Field(default_factory=list)
    capacity_claimed: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


def _json(value: BaseModel) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


class CexDexCompositeEdgeLedger:
    """Append-only persistence for fully reconstructed CEX↔DEX edge observations."""

    def __init__(self, evidence_store: EvidenceStore):
        self.engine = evidence_store.engine
        self.metadata = MetaData()
        self.records = Table(
            "cex_dex_composite_edge_records",
            self.metadata,
            Column("record_id", String(64), primary_key=True),
            Column("cycle_id", String(64), nullable=False),
            Column("phase", Text, nullable=False),
            Column("horizon_seconds", Text, nullable=False),
            Column("composite_key", String(64), nullable=False),
            Column("asset", Text, nullable=False),
            Column("direction", Text, nullable=False),
            Column("target_notional_usd", Text, nullable=False),
            Column("cex_venue", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.cycles = Table(
            "cex_dex_composite_edge_shadow_cycles",
            self.metadata,
            Column("cycle_id", String(64), primary_key=True),
            Column("started_at", Text, nullable=False),
            Column("completed_at", Text, nullable=False),
            Column("initial_evidence_count", Integer, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_cex_dex_edge_record_cycle", self.records.c.cycle_id)
        Index("ix_cex_dex_edge_record_key", self.records.c.composite_key)
        Index("ix_cex_dex_edge_record_observed", self.records.c.observed_at)
        Index("ix_cex_dex_edge_cycle_completed", self.cycles.c.completed_at)
        self.metadata.create_all(self.engine)

    def record_cycle(self, cycle: CexDexCompositeEdgeShadowCycle) -> str:
        cycle_payload = _json(cycle)
        rows: list[dict[str, object]] = []
        for record in cycle.records:
            payload = _json(record)
            evidence = record.evidence
            rows.append(
                {
                    "record_id": record.record_id,
                    "cycle_id": record.cycle_id,
                    "phase": record.phase,
                    "horizon_seconds": str(record.horizon_seconds),
                    "composite_key": record.composite_key,
                    "asset": evidence.asset,
                    "direction": evidence.route_direction,
                    "target_notional_usd": str(evidence.target_notional_usd),
                    "cex_venue": evidence.cex_venue,
                    "observed_at": record.observed_at.isoformat(),
                    "payload_json": payload,
                    "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(),
                }
            )
        with self.engine.begin() as db:
            db.execute(
                insert(self.cycles),
                {
                    "cycle_id": cycle.cycle_id,
                    "started_at": cycle.started_at.isoformat(),
                    "completed_at": cycle.completed_at.isoformat(),
                    "initial_evidence_count": cycle.initial_evidence_count,
                    "payload_json": cycle_payload,
                    "lineage_hash": hashlib.sha256(cycle_payload.encode()).hexdigest(),
                },
            )
            if rows:
                db.execute(insert(self.records), rows)
        return cycle.cycle_id

    def load_cycle(self, cycle_id: str) -> CexDexCompositeEdgeShadowCycle:
        with self.engine.connect() as db:
            payload = db.execute(
                select(self.cycles.c.payload_json).where(self.cycles.c.cycle_id == cycle_id)
            ).scalar_one_or_none()
        if payload is None:
            raise KeyError(f"unknown CEX↔DEX composite edge cycle_id: {cycle_id}")
        return CexDexCompositeEdgeShadowCycle.model_validate_json(payload)

    def load_records(self, cycle_id: str) -> list[CexDexCompositeEdgeRecord]:
        with self.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(self.records.c.payload_json)
                    .where(self.records.c.cycle_id == cycle_id)
                    .order_by(self.records.c.observed_at, self.records.c.record_id)
                ).scalars()
            )
        return [CexDexCompositeEdgeRecord.model_validate_json(payload) for payload in payloads]

    def summary(self) -> dict[str, object]:
        with self.engine.connect() as db:
            payloads = list(db.execute(select(self.cycles.c.payload_json).order_by(self.cycles.c.completed_at)).scalars())
            record_count = int(db.execute(select(func.count()).select_from(self.records)).scalar_one())
        cycles = [CexDexCompositeEdgeShadowCycle.model_validate_json(payload) for payload in payloads]
        observations = [observation for cycle in cycles for observation in cycle.observations]
        deteriorations = [
            observation.adverse_deterioration_bps
            for observation in observations
            if observation.adverse_deterioration_bps is not None
        ]
        return {
            "cycle_count": len(cycles),
            "record_count": record_count,
            "observation_count": len(observations),
            "matched_count": sum(observation.survived for observation in observations),
            "missing_count": sum(not observation.survived for observation in observations),
            "hurdle_survived_count": sum(observation.hurdle_survived for observation in observations),
            "mean_adverse_deterioration_bps": fmean(deteriorations) if deteriorations else None,
            "capacity_claimed": False,
            "allocation_eligible": False,
            "executable_eligible": False,
            "paper_only": True,
        }


class CexDexCompositeEdgeShadowService:
    def __init__(
        self,
        composite_service: CexDexCompositeEvidenceService,
        *,
        evidence_store: EvidenceStore | None = None,
        sleep: SleepFn = asyncio.sleep,
    ):
        self.composite_service = composite_service
        self.settings = composite_service.settings
        store = evidence_store
        if store is None:
            core = getattr(composite_service, "core", None)
            store = getattr(core, "evidence_store", None)
        self.ledger = CexDexCompositeEdgeLedger(store) if store is not None else None
        self.sleep = sleep

    @staticmethod
    def _record(
        cycle_id: str,
        phase: str,
        horizon_seconds: float,
        evidence: CexDexCompositeEvidence,
    ) -> CexDexCompositeEdgeRecord:
        key = composite_edge_key(evidence)
        raw = f"{cycle_id}:{phase}:{horizon_seconds}:{evidence.evidence_id}:{key}"
        return CexDexCompositeEdgeRecord(
            record_id=hashlib.sha256(raw.encode()).hexdigest()[:32],
            cycle_id=cycle_id,
            phase=phase,
            horizon_seconds=horizon_seconds,
            composite_key=key,
            observed_at=evidence.observed_at,
            evidence=evidence,
            paper_only=True,
        )

    @staticmethod
    def _index(rows: list[CexDexCompositeEvidence]) -> dict[str, CexDexCompositeEvidence]:
        indexed: dict[str, CexDexCompositeEvidence] = {}
        for row in rows:
            indexed.setdefault(composite_edge_key(row), row)
        return indexed

    async def run_cycle(
        self,
        *,
        horizons_seconds: tuple[float, ...] | None = None,
    ) -> CexDexCompositeEdgeShadowCycle:
        horizons = tuple(
            sorted(
                set(
                    max(0.0, value)
                    for value in (horizons_seconds or self.settings.shadow_horizons_seconds)
                )
            )
        )
        if not horizons:
            horizons = (max(0.0, self.settings.shadow_delay_seconds),)

        cycle_id = uuid.uuid4().hex
        started_at = datetime.now(timezone.utc)
        initial_probe = await self.composite_service.probe()
        initial_rows = list(initial_probe.evidence)
        initial_index = self._index(initial_rows)
        records = [self._record(cycle_id, "initial", 0.0, row) for row in initial_rows]
        observations: list[CexDexCompositeEdgeObservation] = []
        min_net_edge_bps = self.settings.dex_statistical_min_net_edge_bps

        elapsed = 0.0
        for horizon in horizons:
            wait = max(0.0, horizon - elapsed)
            if wait > 0:
                await self.sleep(wait)
            elapsed = horizon
            verification_probe = await self.composite_service.probe()
            verification_rows = list(verification_probe.evidence)
            verification_index = self._index(verification_rows)
            records.extend(self._record(cycle_id, "verification", horizon, row) for row in verification_rows)
            verified_at = verification_probe.observed_at

            for key, initial in initial_index.items():
                verification = verification_index.get(key)
                initial_above = initial.net_research_edge_bps >= min_net_edge_bps
                if verification is None:
                    observations.append(
                        CexDexCompositeEdgeObservation(
                            cycle_id=cycle_id,
                            composite_key=key,
                            asset=initial.asset,
                            route_direction=initial.route_direction,
                            target_notional_usd=initial.target_notional_usd,
                            cex_venue=initial.cex_venue,
                            cex_symbol=initial.cex_symbol,
                            horizon_seconds=horizon,
                            initial_evidence_id=initial.evidence_id,
                            initial_net_edge_bps=initial.net_research_edge_bps,
                            initial_above_hurdle=initial_above,
                            survived=False,
                            failure_type="CompositeMissing",
                            verified_at=verified_at,
                        )
                    )
                    continue

                verification_edge = verification.net_research_edge_bps
                verification_above = verification_edge >= min_net_edge_bps
                delta = verification_edge - initial.net_research_edge_bps
                retained_fraction = (
                    verification_edge / initial.net_research_edge_bps
                    if initial.net_research_edge_bps > 0
                    else None
                )
                observations.append(
                    CexDexCompositeEdgeObservation(
                        cycle_id=cycle_id,
                        composite_key=key,
                        asset=initial.asset,
                        route_direction=initial.route_direction,
                        target_notional_usd=initial.target_notional_usd,
                        cex_venue=initial.cex_venue,
                        cex_symbol=initial.cex_symbol,
                        horizon_seconds=horizon,
                        initial_evidence_id=initial.evidence_id,
                        verification_evidence_id=verification.evidence_id,
                        initial_net_edge_bps=initial.net_research_edge_bps,
                        verification_net_edge_bps=verification_edge,
                        net_edge_change_bps=delta,
                        adverse_deterioration_bps=max(0.0, -delta),
                        retained_edge_fraction=retained_fraction,
                        initial_above_hurdle=initial_above,
                        verification_above_hurdle=verification_above,
                        hurdle_survived=initial_above and verification_above,
                        survived=True,
                        verified_at=verified_at,
                    )
                )

        cycle = CexDexCompositeEdgeShadowCycle(
            cycle_id=cycle_id,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            horizons_seconds=list(horizons),
            min_net_edge_bps=min_net_edge_bps,
            initial_evidence_count=len(initial_rows),
            records=records,
            observations=observations,
            capacity_claimed=False,
            allocation_eligible=False,
            executable_eligible=False,
            paper_only=True,
        )
        if self.ledger is not None:
            self.ledger.record_cycle(cycle)
        return cycle

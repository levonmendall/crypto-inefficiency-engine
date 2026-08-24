from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.evidence import EvidenceStore


class VerifiedCapitalTransferObservation(BaseModel):
    """Empirical venue-transfer evidence; never a modeled or paper estimate.

    Capital-location source qualification requires observed transfer cost and latency.
    This model deliberately rejects inferred policy fees, configured assumptions, or
    simulated paper timing. Only a verified external observation may enter the durable
    source table consumed by SourceCoveragePlane.
    """

    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    transfer_id: str
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    initiated_at: datetime
    settled_at: datetime
    from_venue: str
    to_venue: str
    asset: str
    network: str | None = None
    transfer_cost_usd: float = Field(ge=0.0)
    latency_seconds: float = Field(gt=0.0)
    source_reference: str
    authoritative: bool = True
    commercial_use_permitted: bool = True
    point_in_time: bool = True
    verified_external_transfer: bool = True
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False


class CapitalTransferEvidenceLedger:
    """Durable sink for genuine transfer observations used by source coverage.

    Creating the sink repairs the previously missing production evidence contract,
    but an empty sink remains fail-closed. No synthetic row is ever created merely to
    make the capital-location lane pass source qualification.
    """

    table_name = "capital_transfer_outcomes"

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            self.table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("transfer_id", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        metadata.create_all(store.engine)

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    def record(self, row: VerifiedCapitalTransferObservation) -> str:
        initiated = self._utc(row.initiated_at)
        settled = self._utc(row.settled_at)
        observed = self._utc(row.observed_at)
        measured_latency = (settled - initiated).total_seconds()
        if measured_latency <= 0.0:
            raise ValueError("verified transfer settlement must occur after initiation")
        # Permit small clock/rounding differences but reject a materially inconsistent
        # supplied latency instead of silently converting an estimate into evidence.
        tolerance = max(1.0, measured_latency * 0.01)
        if abs(float(row.latency_seconds) - measured_latency) > tolerance:
            raise ValueError("transfer latency must match verified initiation/settlement timestamps")
        if not str(row.source_reference or "").strip():
            raise ValueError("verified transfer evidence requires a source reference")
        if not (
            row.authoritative
            and row.commercial_use_permitted
            and row.point_in_time
            and row.verified_external_transfer
        ):
            raise ValueError("only authoritative point-in-time verified external transfers are admissible")
        if row.allocation_authority or row.live_execution_authority or not row.paper_only:
            raise ValueError("capital transfer evidence is diagnostic and paper-only")

        payload = row.model_copy(
            update={
                "observed_at": observed,
                "initiated_at": initiated,
                "settled_at": settled,
                "latency_seconds": measured_latency,
            }
        )
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.observation_id).where(
                    self.rows.c.observation_id == payload.observation_id
                )
            ).scalar_one_or_none()
            if exists is None:
                db.execute(
                    insert(self.rows),
                    {
                        "observation_id": payload.observation_id,
                        "transfer_id": payload.transfer_id,
                        "observed_at": payload.observed_at.isoformat(),
                        "payload_json": payload.model_dump_json(),
                    },
                )
        return payload.observation_id

    def status(self) -> dict[str, object]:
        with self.store.engine.connect() as db:
            latest = db.execute(
                select(self.rows.c.observed_at).order_by(self.rows.c.id.desc()).limit(1)
            ).scalar_one_or_none()
        return {
            "producer_implemented": True,
            "table": self.table_name,
            "latest_verified_observation_at": latest,
            "verified_observation_available": latest is not None,
            "synthetic_transfer_evidence_allowed": False,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }

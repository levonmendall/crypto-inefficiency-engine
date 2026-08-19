from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_PORTFOLIO_ID,
    CanonicalPaperPortfolioSnapshot,
)
from inefficiency_engine.evidence import EvidenceStore


ValuationStatus = Literal["cash_only", "fresh", "partial", "stale", "unavailable"]
IntegrityCycleStatus = Literal["accounting_only", "success", "degraded", "failed"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PortfolioIntegritySnapshot(BaseModel):
    integrity_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    portfolio_id: str = CANONICAL_PORTFOLIO_ID
    observed_at: datetime = Field(default_factory=_now)
    account_snapshot_at: datetime
    market_evidence_at: datetime | None = None
    valuation_status: ValuationStatus
    cycle_status: IntegrityCycleStatus
    fallback_snapshot: bool = False
    cycle_error_type: str | None = None
    stale_position_count: int = Field(default=0, ge=0)
    settlement_evidence_blocked_count: int = Field(default=0, ge=0)
    open_position_count: int = Field(default=0, ge=0)
    allocation_family_failures: list[dict[str, object]] = Field(default_factory=list)
    market_snapshot_id: str | None = None
    paper_only: bool = True
    live_execution_authority: bool = False


class PortfolioIntegrityLedger:
    """Append-only provenance for canonical paper-account observations.

    Accounting snapshots and valuation evidence are deliberately separate. A new
    account timestamp cannot imply that open positions were re-priced unless this
    ledger records fresh market evidence for that cycle.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "canonical_paper_portfolio_integrity",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("integrity_id", String(64), nullable=False, unique=True),
            Column("portfolio_id", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("previous_lineage_hash", String(64)),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_canonical_portfolio_integrity_time", self.rows.c.portfolio_id, self.rows.c.observed_at)
        metadata.create_all(store.engine)

    def _last_hash(self) -> str | None:
        with self.store.engine.connect() as db:
            return db.execute(
                select(self.rows.c.lineage_hash)
                .where(self.rows.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.rows.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()

    def record(self, snapshot: PortfolioIntegritySnapshot) -> str:
        if snapshot.portfolio_id != CANONICAL_PORTFOLIO_ID:
            raise ValueError("only the canonical paper portfolio is supported")
        raw = json.dumps(snapshot.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        previous = self._last_hash()
        lineage = hashlib.sha256(f"{previous or ''}|{raw}".encode()).hexdigest()
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.integrity_id).where(self.rows.c.integrity_id == snapshot.integrity_id)
            ).scalar_one_or_none()
            if exists is None:
                db.execute(insert(self.rows), {
                    "integrity_id": snapshot.integrity_id,
                    "portfolio_id": snapshot.portfolio_id,
                    "observed_at": snapshot.observed_at.isoformat(),
                    "payload_json": raw,
                    "previous_lineage_hash": previous,
                    "lineage_hash": lineage,
                })
        return snapshot.integrity_id

    def latest(self) -> PortfolioIntegritySnapshot | None:
        with self.store.engine.connect() as db:
            payload = db.execute(
                select(self.rows.c.payload_json)
                .where(self.rows.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.rows.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        return PortfolioIntegritySnapshot.model_validate_json(payload) if payload else None

    def history(self, *, limit: int = 100) -> list[PortfolioIntegritySnapshot]:
        bounded = max(1, min(1000, int(limit)))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.rows.c.payload_json)
                .where(self.rows.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.rows.c.id.desc())
                .limit(bounded)
            ).scalars())
        return [PortfolioIntegritySnapshot.model_validate_json(payload) for payload in payloads]

    def ensure_initial(self, account: CanonicalPaperPortfolioSnapshot) -> PortfolioIntegritySnapshot:
        existing = self.latest()
        if existing is not None:
            return existing
        status: ValuationStatus = "cash_only" if account.open_position_count == 0 else "unavailable"
        initial = PortfolioIntegritySnapshot(
            observed_at=account.observed_at,
            account_snapshot_at=account.observed_at,
            market_evidence_at=None,
            valuation_status=status,
            cycle_status="accounting_only",
            stale_position_count=account.open_position_count,
            open_position_count=account.open_position_count,
        )
        self.record(initial)
        return initial

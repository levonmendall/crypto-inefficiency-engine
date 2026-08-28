from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, MetaData, String, Table, Text, create_engine, func, insert, select, text
from sqlalchemy.engine import Engine

from inefficiency_engine.dex_frontier import DexRouteSizeFrontier, summarize_size_frontiers
from inefficiency_engine.dex_shadow import DexRouteQuoteRecord, DexRouteShadowCycle, summarize_route_cycles
from inefficiency_engine.models import FundingQuote, MarketQuote, Opportunity, OpportunityExecutability, OrderBookSnapshot, ShadowCycle
from inefficiency_engine.local_storage import local_storage_paths


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: BaseModel | dict[str, object]) -> str:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def lineage_hash(value: BaseModel | dict[str, object]) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


class ProviderStatus(BaseModel):
    provider: str
    ok: bool
    observed_at: datetime = Field(default_factory=_now)
    item_count: int = 0
    error_type: str | None = None


class ScanSnapshot(BaseModel):
    scan_id: str
    started_at: datetime
    completed_at: datetime
    providers: list[ProviderStatus]
    funding_quotes: list[FundingQuote]
    market_quotes: list[MarketQuote]
    opportunities: list[Opportunity]
    order_books: list[OrderBookSnapshot] = Field(default_factory=list)
    executability: list[OpportunityExecutability] = Field(default_factory=list)
    analysis_config: dict[str, object] = Field(default_factory=dict)


class WorkerHeartbeat(BaseModel):
    worker_id: str
    observed_at: datetime = Field(default_factory=_now)
    state: str
    cycle_id: str | None = None
    scan_id: str | None = None
    error_type: str | None = None
    detail: dict[str, object] = Field(default_factory=dict)


@dataclass(frozen=True)
class PersistedCounts:
    scans: int
    provider_statuses: int
    funding_quotes: int
    market_quotes: int
    opportunities: int
    order_books: int
    executability: int
    shadow_cycles: int
    worker_heartbeats: int = 0
    dex_route_quotes: int = 0
    dex_route_shadow_cycles: int = 0
    dex_route_size_frontiers: int = 0


def evidence_location_from_env(fallback_path: str | Path | None = None) -> str | Path | None:
    explicit = os.getenv("CIE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if explicit:
        return explicit
    if os.getenv("CIE_STORAGE_ROOT"):
        return local_storage_paths().metadata_db
    return fallback_path


def build_evidence_store(fallback_path: str | Path | None = None) -> EvidenceStore | None:
    location = evidence_location_from_env(fallback_path)
    return None if location is None else EvidenceStore(location)


def _database_url(location: str | Path) -> str:
    raw = str(location)
    if raw.startswith("postgres://"):
        return "postgresql+psycopg://" + raw[11:]
    if raw.startswith("postgresql://"):
        return "postgresql+psycopg://" + raw[13:]
    if raw.startswith(("postgresql+psycopg://", "sqlite://")):
        return raw
    path = Path(raw).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{path}"


class EvidenceStore:
    """Append-only evidence ledger using SQLite locally or PostgreSQL in production."""

    def __init__(self, location: str | Path):
        url = _database_url(location)
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if url.startswith("sqlite:"):
            kwargs["connect_args"] = {"check_same_thread": False}
        self.engine: Engine = create_engine(url, **kwargs)
        self.backend = self.engine.url.get_backend_name()
        self.safe_database_url = self.engine.url.render_as_string(hide_password=True)
        self.metadata = MetaData()
        self._schema()
        self.metadata.create_all(self.engine)
        if self.backend == "sqlite":
            with self.engine.begin() as db:
                db.execute(text("PRAGMA journal_mode=WAL"))
                db.execute(text("PRAGMA foreign_keys=ON"))
                db.execute(text("PRAGMA synchronous=FULL"))
                db.execute(text("PRAGMA busy_timeout=30000"))

    def _schema(self) -> None:
        self.scans = Table(
            "scans", self.metadata,
            Column("scan_id", String(64), primary_key=True),
            Column("started_at", Text, nullable=False), Column("completed_at", Text, nullable=False),
            Column("created_at", Text, nullable=False), Column("analysis_config_json", Text, nullable=False),
        )

        def payload_table(name: str, *extra: Column) -> Table:
            return Table(
                name, self.metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("scan_id", String(64), ForeignKey("scans.scan_id"), nullable=False),
                *extra,
                Column("observed_at", Text, nullable=False), Column("payload_json", Text, nullable=False),
                Column("lineage_hash", String(64), nullable=False),
            )

        self.provider_statuses = payload_table(
            "provider_statuses", Column("provider", Text, nullable=False), Column("ok", Boolean, nullable=False),
            Column("item_count", Integer, nullable=False), Column("error_type", Text),
        )
        self.funding_quotes = payload_table("funding_quotes", Column("venue", Text, nullable=False), Column("asset", Text, nullable=False))
        self.market_quotes = payload_table("market_quotes", Column("venue", Text, nullable=False), Column("asset", Text, nullable=False))
        self.opportunities = payload_table(
            "opportunities", Column("opportunity_id", Text, nullable=False), Column("strategy", Text, nullable=False), Column("asset", Text, nullable=False),
        )
        self.order_books = payload_table(
            "order_books", Column("venue", Text, nullable=False), Column("asset", Text, nullable=False), Column("market_kind", Text, nullable=False),
        )
        self.executability = payload_table("executability", Column("opportunity_id", Text, nullable=False), Column("asset", Text, nullable=False))
        self.shadow_cycles = Table(
            "shadow_cycles", self.metadata,
            Column("cycle_id", String(64), primary_key=True), Column("started_at", Text, nullable=False), Column("completed_at", Text, nullable=False),
            Column("initial_scan_id", String(64), nullable=False), Column("verification_scan_id", String(64), nullable=False),
            Column("payload_json", Text, nullable=False), Column("lineage_hash", String(64), nullable=False),
        )
        self.worker_heartbeats = Table(
            "worker_heartbeats", self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True), Column("worker_id", Text, nullable=False),
            Column("observed_at", Text, nullable=False), Column("state", Text, nullable=False), Column("cycle_id", Text), Column("scan_id", Text),
            Column("error_type", Text), Column("payload_json", Text, nullable=False), Column("lineage_hash", String(64), nullable=False),
        )
        self.dex_route_quotes = Table(
            "dex_route_quotes", self.metadata,
            Column("record_id", String(64), primary_key=True),
            Column("cycle_id", String(64), nullable=False),
            Column("phase", Text, nullable=False),
            Column("horizon_seconds", Text, nullable=False),
            Column("route_signature", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("direction", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.dex_route_shadow_cycles = Table(
            "dex_route_shadow_cycles", self.metadata,
            Column("cycle_id", String(64), primary_key=True),
            Column("started_at", Text, nullable=False),
            Column("completed_at", Text, nullable=False),
            Column("initial_quote_count", Integer, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.dex_route_size_frontiers = Table(
            "dex_route_size_frontiers", self.metadata,
            Column("frontier_id", String(64), primary_key=True),
            Column("asset", Text, nullable=False),
            Column("direction", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        for name, table, column in [
            ("ix_funding_scan", self.funding_quotes, "scan_id"), ("ix_market_scan", self.market_quotes, "scan_id"),
            ("ix_opportunity_scan", self.opportunities, "scan_id"), ("ix_order_book_scan", self.order_books, "scan_id"),
            ("ix_executability_scan", self.executability, "scan_id"), ("ix_shadow_completed", self.shadow_cycles, "completed_at"),
            ("ix_worker_heartbeat_observed", self.worker_heartbeats, "observed_at"), ("ix_worker_heartbeat_worker", self.worker_heartbeats, "worker_id"),
            ("ix_dex_route_quote_cycle", self.dex_route_quotes, "cycle_id"),
            ("ix_dex_route_quote_signature", self.dex_route_quotes, "route_signature"),
            ("ix_dex_route_quote_observed", self.dex_route_quotes, "observed_at"),
            ("ix_dex_route_shadow_completed", self.dex_route_shadow_cycles, "completed_at"),
            ("ix_dex_route_frontier_observed", self.dex_route_size_frontiers, "observed_at"),
            ("ix_dex_route_frontier_asset", self.dex_route_size_frontiers, "asset"),
        ]:
            Index(name, getattr(table.c, column))

    @staticmethod
    def _payload_rows(scan_id: str, values: list[BaseModel], extra) -> list[dict[str, object]]:
        rows = []
        for value in values:
            payload = _json(value)
            rows.append({
                "scan_id": scan_id, "observed_at": value.observed_at.isoformat(), "payload_json": payload,
                "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(), **extra(value),
            })
        return rows

    def ping(self) -> bool:
        with self.engine.connect() as db:
            return db.execute(text("SELECT 1")).scalar_one() == 1

    def record_scan(self, *, funding_quotes: list[FundingQuote], market_quotes: list[MarketQuote], opportunities: list[Opportunity],
                    providers: list[ProviderStatus], started_at: datetime, completed_at: datetime, scan_id: str | None = None,
                    analysis_config: dict[str, object] | None = None, order_books: list[OrderBookSnapshot] | None = None,
                    executability: list[OpportunityExecutability] | None = None) -> str:
        scan_id = scan_id or uuid.uuid4().hex
        if os.getenv("CIE_MARKET_HISTORY_BACKEND", "").strip().lower() == "parquet":
            # Commit immutable history before its relational compatibility
            # projection. Retry is idempotent by lineage hash.
            from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory

            PartitionedMarketHistory().append(market_quotes)
        batches = [
            (self.provider_statuses, self._payload_rows(scan_id, providers, lambda x: {"provider": x.provider, "ok": x.ok, "item_count": x.item_count, "error_type": x.error_type})),
            (self.funding_quotes, self._payload_rows(scan_id, funding_quotes, lambda x: {"venue": x.venue, "asset": x.asset})),
            (self.market_quotes, self._payload_rows(scan_id, market_quotes, lambda x: {"venue": x.venue, "asset": x.asset})),
            (self.opportunities, self._payload_rows(scan_id, opportunities, lambda x: {"opportunity_id": x.id, "strategy": x.strategy.value, "asset": x.asset})),
            (self.order_books, self._payload_rows(scan_id, order_books or [], lambda x: {"venue": x.venue, "asset": x.asset, "market_kind": x.market_kind.value})),
            (self.executability, self._payload_rows(scan_id, executability or [], lambda x: {"opportunity_id": x.opportunity_id, "asset": x.asset})),
        ]
        with self.engine.begin() as db:
            db.execute(insert(self.scans), {"scan_id": scan_id, "started_at": started_at.isoformat(), "completed_at": completed_at.isoformat(),
                                           "created_at": _now().isoformat(), "analysis_config_json": json.dumps(analysis_config or {}, sort_keys=True)})
            for table, rows in batches:
                if rows:
                    db.execute(insert(table), rows)
        return scan_id

    def _payloads(self, table: Table, scan_id: str) -> list[str]:
        with self.engine.connect() as db:
            return list(db.execute(select(table.c.payload_json).where(table.c.scan_id == scan_id).order_by(table.c.id)).scalars())

    def load_scan(self, scan_id: str) -> ScanSnapshot:
        with self.engine.connect() as db:
            scan = db.execute(select(self.scans).where(self.scans.c.scan_id == scan_id)).mappings().first()
        if scan is None:
            raise KeyError(f"unknown scan_id: {scan_id}")
        return ScanSnapshot(
            scan_id=scan_id, started_at=datetime.fromisoformat(scan["started_at"]), completed_at=datetime.fromisoformat(scan["completed_at"]),
            providers=[ProviderStatus.model_validate_json(x) for x in self._payloads(self.provider_statuses, scan_id)],
            funding_quotes=[FundingQuote.model_validate_json(x) for x in self._payloads(self.funding_quotes, scan_id)],
            market_quotes=[MarketQuote.model_validate_json(x) for x in self._payloads(self.market_quotes, scan_id)],
            opportunities=[Opportunity.model_validate_json(x) for x in self._payloads(self.opportunities, scan_id)],
            order_books=[OrderBookSnapshot.model_validate_json(x) for x in self._payloads(self.order_books, scan_id)],
            executability=[OpportunityExecutability.model_validate_json(x) for x in self._payloads(self.executability, scan_id)],
            analysis_config=json.loads(scan["analysis_config_json"]),
        )

    def record_shadow_cycle(self, cycle: ShadowCycle) -> str:
        payload = _json(cycle)
        with self.engine.begin() as db:
            db.execute(insert(self.shadow_cycles), {"cycle_id": cycle.cycle_id, "started_at": cycle.started_at.isoformat(), "completed_at": cycle.completed_at.isoformat(),
                                                    "initial_scan_id": cycle.initial_scan_id, "verification_scan_id": cycle.verification_scan_id,
                                                    "payload_json": payload, "lineage_hash": hashlib.sha256(payload.encode()).hexdigest()})
        return cycle.cycle_id

    def load_shadow_cycle(self, cycle_id: str) -> ShadowCycle:
        with self.engine.connect() as db:
            payload = db.execute(select(self.shadow_cycles.c.payload_json).where(self.shadow_cycles.c.cycle_id == cycle_id)).scalar_one_or_none()
        if payload is None:
            raise KeyError(f"unknown cycle_id: {cycle_id}")
        return ShadowCycle.model_validate_json(payload)

    def shadow_summary(self) -> dict[str, object]:
        with self.engine.connect() as db:
            payloads = list(db.execute(select(self.shadow_cycles.c.payload_json).order_by(self.shadow_cycles.c.completed_at)).scalars())
        observations = [obs for payload in payloads for obs in ShadowCycle.model_validate_json(payload).observations]
        survived = sum(obs.survived for obs in observations)
        outcomes: dict[str, int] = {}
        for obs in observations:
            outcomes[obs.outcome.value] = outcomes.get(obs.outcome.value, 0) + 1
        return {"cycle_count": len(payloads), "observation_count": len(observations), "survived_count": survived,
                "survival_rate": survived / len(observations) if observations else None, "outcomes": outcomes}

    def record_dex_route_shadow_cycle(self, cycle: DexRouteShadowCycle, records: list[DexRouteQuoteRecord]) -> str:
        cycle_payload = _json(cycle)
        rows: list[dict[str, object]] = []
        for record in records:
            payload = _json(record)
            rows.append({
                "record_id": record.record_id, "cycle_id": record.cycle_id, "phase": record.phase,
                "horizon_seconds": str(record.horizon_seconds), "route_signature": record.route_signature,
                "asset": record.quote.asset, "direction": record.quote.direction, "observed_at": record.observed_at.isoformat(),
                "payload_json": payload, "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(),
            })
        with self.engine.begin() as db:
            db.execute(insert(self.dex_route_shadow_cycles), {
                "cycle_id": cycle.cycle_id, "started_at": cycle.started_at.isoformat(), "completed_at": cycle.completed_at.isoformat(),
                "initial_quote_count": cycle.initial_quote_count, "payload_json": cycle_payload,
                "lineage_hash": hashlib.sha256(cycle_payload.encode()).hexdigest(),
            })
            if rows:
                db.execute(insert(self.dex_route_quotes), rows)
        return cycle.cycle_id

    def load_dex_route_shadow_cycle(self, cycle_id: str) -> DexRouteShadowCycle:
        with self.engine.connect() as db:
            payload = db.execute(select(self.dex_route_shadow_cycles.c.payload_json).where(
                self.dex_route_shadow_cycles.c.cycle_id == cycle_id)).scalar_one_or_none()
        if payload is None:
            raise KeyError(f"unknown DEX route cycle_id: {cycle_id}")
        return DexRouteShadowCycle.model_validate_json(payload)

    def load_dex_route_quote_records(self, cycle_id: str) -> list[DexRouteQuoteRecord]:
        with self.engine.connect() as db:
            payloads = list(db.execute(select(self.dex_route_quotes.c.payload_json).where(
                self.dex_route_quotes.c.cycle_id == cycle_id).order_by(
                self.dex_route_quotes.c.observed_at, self.dex_route_quotes.c.record_id)).scalars())
        return [DexRouteQuoteRecord.model_validate_json(payload) for payload in payloads]

    def dex_route_shadow_summary(self) -> dict[str, object]:
        with self.engine.connect() as db:
            payloads = list(db.execute(select(self.dex_route_shadow_cycles.c.payload_json).order_by(
                self.dex_route_shadow_cycles.c.completed_at)).scalars())
        return summarize_route_cycles([DexRouteShadowCycle.model_validate_json(payload) for payload in payloads])

    def record_dex_route_size_frontiers(self, frontiers: list[DexRouteSizeFrontier]) -> list[str]:
        rows = []
        for frontier in frontiers:
            payload = _json(frontier)
            rows.append({
                "frontier_id": frontier.frontier_id,
                "asset": frontier.asset,
                "direction": frontier.direction,
                "observed_at": frontier.observed_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(),
            })
        if rows:
            with self.engine.begin() as db:
                db.execute(insert(self.dex_route_size_frontiers), rows)
        return [frontier.frontier_id for frontier in frontiers]

    def load_dex_route_size_frontier(self, frontier_id: str) -> DexRouteSizeFrontier:
        with self.engine.connect() as db:
            payload = db.execute(select(self.dex_route_size_frontiers.c.payload_json).where(
                self.dex_route_size_frontiers.c.frontier_id == frontier_id)).scalar_one_or_none()
        if payload is None:
            raise KeyError(f"unknown DEX route frontier_id: {frontier_id}")
        return DexRouteSizeFrontier.model_validate_json(payload)

    def dex_route_size_frontier_summary(self) -> dict[str, object]:
        with self.engine.connect() as db:
            payloads = list(db.execute(select(self.dex_route_size_frontiers.c.payload_json).order_by(
                self.dex_route_size_frontiers.c.observed_at)).scalars())
        return summarize_size_frontiers([DexRouteSizeFrontier.model_validate_json(payload) for payload in payloads])

    def record_worker_heartbeat(self, *, worker_id: str, state: str, cycle_id: str | None = None, scan_id: str | None = None,
                                error_type: str | None = None, detail: dict[str, object] | None = None,
                                observed_at: datetime | None = None) -> WorkerHeartbeat:
        heartbeat = WorkerHeartbeat(worker_id=worker_id, observed_at=observed_at or _now(), state=state, cycle_id=cycle_id,
                                    scan_id=scan_id, error_type=error_type, detail=detail or {})
        payload = _json(heartbeat)
        with self.engine.begin() as db:
            db.execute(insert(self.worker_heartbeats), {"worker_id": worker_id, "observed_at": heartbeat.observed_at.isoformat(), "state": state,
                                                       "cycle_id": cycle_id, "scan_id": scan_id, "error_type": error_type, "payload_json": payload,
                                                       "lineage_hash": hashlib.sha256(payload.encode()).hexdigest()})
        return heartbeat

    def latest_worker_heartbeat(self, worker_id: str | None = None) -> WorkerHeartbeat | None:
        query = select(self.worker_heartbeats.c.payload_json)
        if worker_id:
            query = query.where(self.worker_heartbeats.c.worker_id == worker_id)
        with self.engine.connect() as db:
            payload = db.execute(query.order_by(self.worker_heartbeats.c.id.desc()).limit(1)).scalar_one_or_none()
        return WorkerHeartbeat.model_validate_json(payload) if payload else None

    def worker_health(self, *, stale_after_seconds: float = 180.0, now: datetime | None = None) -> dict[str, object]:
        latest = self.latest_worker_heartbeat()
        if latest is None:
            return {"healthy": False, "reason": "no worker heartbeat recorded", "backend": self.backend, "database_ok": self.ping(), "latest_heartbeat": None}
        age = max(0.0, ((now or _now()) - latest.observed_at).total_seconds())
        healthy = latest.state not in {"error", "stopped"} and age <= stale_after_seconds
        return {"healthy": healthy, "reason": None if healthy else (f"heartbeat stale by {age:.1f}s" if age > stale_after_seconds else f"worker state={latest.state}"),
                "backend": self.backend, "database_ok": self.ping(), "heartbeat_age_seconds": age, "latest_heartbeat": latest.model_dump(mode="json")}

    def counts(self) -> PersistedCounts:
        tables = [
            self.scans, self.provider_statuses, self.funding_quotes, self.market_quotes, self.opportunities,
            self.order_books, self.executability, self.shadow_cycles, self.worker_heartbeats,
            self.dex_route_quotes, self.dex_route_shadow_cycles, self.dex_route_size_frontiers,
        ]
        with self.engine.connect() as db:
            values = [int(db.execute(select(func.count()).select_from(table)).scalar_one()) for table in tables]
        return PersistedCounts(*values)

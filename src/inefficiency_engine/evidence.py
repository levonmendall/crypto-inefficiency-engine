from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

from inefficiency_engine.models import FundingQuote, MarketQuote, Opportunity, OpportunityExecutability, OrderBookSnapshot, ShadowCycle


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(model: BaseModel | dict[str, object]) -> str:
    if isinstance(model, BaseModel):
        payload = model.model_dump(mode="json")
    else:
        payload = model
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def lineage_hash(model: BaseModel | dict[str, object]) -> str:
    return hashlib.sha256(_canonical_json(model).encode()).hexdigest()


class ProviderStatus(BaseModel):
    provider: str
    ok: bool
    observed_at: datetime = Field(default_factory=_utc_now)
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


class EvidenceStore:
    """Append-only SQLite evidence ledger for point-in-time market observations."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS scans (
                    scan_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    analysis_config_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS provider_statuses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT NOT NULL REFERENCES scans(scan_id),
                    provider TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    error_type TEXT,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS funding_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT NOT NULL REFERENCES scans(scan_id),
                    venue TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS market_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT NOT NULL REFERENCES scans(scan_id),
                    venue TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT NOT NULL REFERENCES scans(scan_id),
                    opportunity_id TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS order_books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT NOT NULL REFERENCES scans(scan_id),
                    venue TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    market_kind TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS executability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT NOT NULL REFERENCES scans(scan_id),
                    opportunity_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS shadow_cycles (
                    cycle_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    initial_scan_id TEXT NOT NULL,
                    verification_scan_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    lineage_hash TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_funding_scan ON funding_quotes(scan_id);
                CREATE INDEX IF NOT EXISTS ix_market_scan ON market_quotes(scan_id);
                CREATE INDEX IF NOT EXISTS ix_opportunity_scan ON opportunities(scan_id);
                CREATE INDEX IF NOT EXISTS ix_order_book_scan ON order_books(scan_id);
                CREATE INDEX IF NOT EXISTS ix_executability_scan ON executability(scan_id);
                CREATE INDEX IF NOT EXISTS ix_shadow_completed ON shadow_cycles(completed_at);
                """
            )

    def record_scan(
        self,
        *,
        funding_quotes: list[FundingQuote],
        market_quotes: list[MarketQuote],
        opportunities: list[Opportunity],
        providers: list[ProviderStatus],
        started_at: datetime,
        completed_at: datetime,
        scan_id: str | None = None,
        analysis_config: dict[str, object] | None = None,
        order_books: list[OrderBookSnapshot] | None = None,
        executability: list[OpportunityExecutability] | None = None,
    ) -> str:
        scan_id = scan_id or uuid.uuid4().hex
        with self._connect() as db:
            db.execute(
                "INSERT INTO scans(scan_id, started_at, completed_at, created_at, analysis_config_json) VALUES (?, ?, ?, ?, ?)",
                (scan_id, started_at.isoformat(), completed_at.isoformat(), _utc_now().isoformat(), json.dumps(analysis_config or {}, sort_keys=True)),
            )
            for status in providers:
                payload = _canonical_json(status)
                db.execute(
                    """INSERT INTO provider_statuses
                    (scan_id, provider, ok, observed_at, item_count, error_type, payload_json, lineage_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (scan_id, status.provider, int(status.ok), status.observed_at.isoformat(), status.item_count, status.error_type, payload, hashlib.sha256(payload.encode()).hexdigest()),
                )
            for quote in funding_quotes:
                payload = _canonical_json(quote)
                db.execute("INSERT INTO funding_quotes(scan_id, venue, asset, observed_at, payload_json, lineage_hash) VALUES (?, ?, ?, ?, ?, ?)", (scan_id, quote.venue, quote.asset, quote.observed_at.isoformat(), payload, hashlib.sha256(payload.encode()).hexdigest()))
            for quote in market_quotes:
                payload = _canonical_json(quote)
                db.execute("INSERT INTO market_quotes(scan_id, venue, asset, observed_at, payload_json, lineage_hash) VALUES (?, ?, ?, ?, ?, ?)", (scan_id, quote.venue, quote.asset, quote.observed_at.isoformat(), payload, hashlib.sha256(payload.encode()).hexdigest()))
            for opportunity in opportunities:
                payload = _canonical_json(opportunity)
                db.execute(
                    """INSERT INTO opportunities
                    (scan_id, opportunity_id, strategy, asset, observed_at, payload_json, lineage_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (scan_id, opportunity.id, opportunity.strategy.value, opportunity.asset, opportunity.observed_at.isoformat(), payload, hashlib.sha256(payload.encode()).hexdigest()),
                )
            for book in order_books or []:
                payload = _canonical_json(book)
                db.execute(
                    """INSERT INTO order_books
                    (scan_id, venue, asset, market_kind, observed_at, payload_json, lineage_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (scan_id, book.venue, book.asset, book.market_kind.value, book.observed_at.isoformat(), payload, hashlib.sha256(payload.encode()).hexdigest()),
                )
            for qualification in executability or []:
                payload = _canonical_json(qualification)
                db.execute(
                    """INSERT INTO executability
                    (scan_id, opportunity_id, asset, observed_at, payload_json, lineage_hash)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (scan_id, qualification.opportunity_id, qualification.asset, qualification.observed_at.isoformat(), payload, hashlib.sha256(payload.encode()).hexdigest()),
                )
        return scan_id

    def load_scan(self, scan_id: str) -> ScanSnapshot:
        with self._connect() as db:
            scan = db.execute("SELECT * FROM scans WHERE scan_id = ?", (scan_id,)).fetchone()
            if scan is None:
                raise KeyError(f"unknown scan_id: {scan_id}")
            provider_rows = db.execute("SELECT payload_json FROM provider_statuses WHERE scan_id = ? ORDER BY id", (scan_id,)).fetchall()
            funding_rows = db.execute("SELECT payload_json FROM funding_quotes WHERE scan_id = ? ORDER BY id", (scan_id,)).fetchall()
            market_rows = db.execute("SELECT payload_json FROM market_quotes WHERE scan_id = ? ORDER BY id", (scan_id,)).fetchall()
            opportunity_rows = db.execute("SELECT payload_json FROM opportunities WHERE scan_id = ? ORDER BY id", (scan_id,)).fetchall()
            order_book_rows = db.execute("SELECT payload_json FROM order_books WHERE scan_id = ? ORDER BY id", (scan_id,)).fetchall()
            executability_rows = db.execute("SELECT payload_json FROM executability WHERE scan_id = ? ORDER BY id", (scan_id,)).fetchall()

        return ScanSnapshot(
            scan_id=scan_id,
            started_at=datetime.fromisoformat(scan["started_at"]),
            completed_at=datetime.fromisoformat(scan["completed_at"]),
            providers=[ProviderStatus.model_validate_json(row["payload_json"]) for row in provider_rows],
            funding_quotes=[FundingQuote.model_validate_json(row["payload_json"]) for row in funding_rows],
            market_quotes=[MarketQuote.model_validate_json(row["payload_json"]) for row in market_rows],
            opportunities=[Opportunity.model_validate_json(row["payload_json"]) for row in opportunity_rows],
            order_books=[OrderBookSnapshot.model_validate_json(row["payload_json"]) for row in order_book_rows],
            executability=[OpportunityExecutability.model_validate_json(row["payload_json"]) for row in executability_rows],
            analysis_config=json.loads(scan["analysis_config_json"]),
        )

    def record_shadow_cycle(self, cycle: ShadowCycle) -> str:
        payload = _canonical_json(cycle)
        with self._connect() as db:
            db.execute(
                """INSERT INTO shadow_cycles
                (cycle_id, started_at, completed_at, initial_scan_id, verification_scan_id, payload_json, lineage_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (cycle.cycle_id, cycle.started_at.isoformat(), cycle.completed_at.isoformat(), cycle.initial_scan_id, cycle.verification_scan_id, payload, hashlib.sha256(payload.encode()).hexdigest()),
            )
        return cycle.cycle_id

    def load_shadow_cycle(self, cycle_id: str) -> ShadowCycle:
        with self._connect() as db:
            row = db.execute("SELECT payload_json FROM shadow_cycles WHERE cycle_id = ?", (cycle_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown cycle_id: {cycle_id}")
        return ShadowCycle.model_validate_json(row["payload_json"])

    def shadow_summary(self) -> dict[str, object]:
        with self._connect() as db:
            rows = db.execute("SELECT payload_json FROM shadow_cycles ORDER BY completed_at").fetchall()
        cycles = [ShadowCycle.model_validate_json(row["payload_json"]) for row in rows]
        observations = [obs for cycle in cycles for obs in cycle.observations]
        survived = sum(1 for obs in observations if obs.survived)
        return {
            "cycle_count": len(cycles),
            "observation_count": len(observations),
            "survived_count": survived,
            "survival_rate": (survived / len(observations)) if observations else None,
        }

    def counts(self) -> PersistedCounts:
        with self._connect() as db:
            def count(table: str) -> int:
                return int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

            return PersistedCounts(
                scans=count("scans"),
                provider_statuses=count("provider_statuses"),
                funding_quotes=count("funding_quotes"),
                market_quotes=count("market_quotes"),
                opportunities=count("opportunities"),
                order_books=count("order_books"),
                executability=count("executability"),
                shadow_cycles=count("shadow_cycles"),
            )

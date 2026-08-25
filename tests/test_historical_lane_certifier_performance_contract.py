from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine, event, insert

from inefficiency_engine.candidate_observatory_backfill_supervisor import (
    BACKFILL_COVERAGE_DEADLINE_SECONDS,
)
from inefficiency_engine.historical_raw_lane_evidence import recover_raw_lane_history


START = datetime(2026, 8, 21, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc)


def test_all_lane_certifier_has_bounded_database_aggregation_lease():
    assert BACKFILL_COVERAGE_DEADLINE_SECONDS == 90.0
    assert BACKFILL_COVERAGE_DEADLINE_SECONDS < 300.0


def test_catalog_history_uses_one_market_range_scan_for_all_venue_sources():
    engine = create_engine("sqlite://")
    metadata = MetaData()
    market = Table(
        "market_quotes",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("venue", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    metadata.create_all(engine)
    early = START + timedelta(hours=1)
    late = END - timedelta(hours=1)
    with engine.begin() as db:
        db.execute(
            insert(market),
            [
                {"venue": venue, "observed_at": observed_at.isoformat()}
                for venue in ("Coinbase", "Bybit", "OKX")
                for observed_at in (early, late)
            ],
        )

    range_scans: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        normalized = str(statement).lower()
        if "from market_quotes" in normalized and "count(" in normalized:
            range_scans.append(normalized)

    event.listen(engine, "before_cursor_execute", capture)
    try:
        history = recover_raw_lane_history(
            SimpleNamespace(engine=engine),
            start=START,
            boundary=END,
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture)

    assert len(range_scans) == 1
    trend_sources = history["trend_momentum"]["source_ids"]
    assert {"coinbase-market", "bybit-market", "okx-market"} <= trend_sources
    assert {"market_history", "execution_costs"} <= history["trend_momentum"]["evidence_classes"]

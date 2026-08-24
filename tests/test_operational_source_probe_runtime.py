from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import event, inspect

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.operational_source_probe_runtime import (
    PERMANENT_SOURCE_WORKER_ID,
    _current_source_scan_candidate,
)
from inefficiency_engine.source_coverage import SourceCoveragePlane


def _quote(*, venue: str, observed_at: datetime, mid: float = 100.0) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        mid=mid,
        observed_at=observed_at,
        source="test",
    )


def _scan(store: EvidenceStore, quote: MarketQuote) -> str:
    return store.record_scan(
        funding_quotes=[],
        market_quotes=[quote],
        opportunities=[],
        providers=[],
        order_books=[],
        executability=[],
        started_at=quote.observed_at,
        completed_at=quote.observed_at + timedelta(milliseconds=1),
    )


def _spec() -> dict[str, object]:
    return {
        "id": "coinbase-market",
        "name": "Coinbase market data",
        "classes": ["market_quotes"],
        "group": "coinbase",
        "tier": "first_party",
        "table": ("market_quotes", "venue", "Coinbase"),
    }


def test_probe_reads_only_latest_successful_executable_source_scan(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 6, 30, tzinfo=timezone.utc)
    authoritative_scan = _scan(store, _quote(venue="Coinbase", observed_at=base, mid=100.0))
    # A later historical row that was never published as a successful permanent-source
    # cycle must not silently replace the current executable source boundary.
    _scan(store, _quote(venue="Coinbase", observed_at=base + timedelta(minutes=5), mid=101.0))
    store.record_worker_heartbeat(
        worker_id=PERMANENT_SOURCE_WORKER_ID,
        state="success",
        scan_id=authoritative_scan,
        detail={"paper_only": True},
    )

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(str(statement).lower())

    event.listen(store.engine, "before_cursor_execute", capture)
    try:
        candidate = _current_source_scan_candidate(
            SourceCoveragePlane(store),
            _spec(),
            set(inspect(store.engine).get_table_names()),
        )
    finally:
        event.remove(store.engine, "before_cursor_execute", capture)

    assert candidate is not None
    assert candidate["observed_at"] == base.isoformat()
    assert authoritative_scan in str(candidate["source_reference"])
    quote_queries = [sql for sql in statements if "market_quotes" in sql]
    assert quote_queries
    assert all("scan_id" in sql for sql in quote_queries)
    assert all("order by observed_at" not in sql for sql in quote_queries)


def test_missing_venue_in_current_source_scan_fails_closed(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 6, 30, tzinfo=timezone.utc)
    # Historical Coinbase evidence exists, but the current successful executable scan
    # contains only Kraken. The operational probe must not fall back into history.
    _scan(store, _quote(venue="Coinbase", observed_at=base))
    current_scan = _scan(
        store,
        _quote(venue="Kraken", observed_at=base + timedelta(minutes=1)),
    )
    store.record_worker_heartbeat(
        worker_id=PERMANENT_SOURCE_WORKER_ID,
        state="success",
        scan_id=current_scan,
        detail={"paper_only": True},
    )

    candidate = _current_source_scan_candidate(
        SourceCoveragePlane(store),
        _spec(),
        set(inspect(store.engine).get_table_names()),
    )

    assert candidate is None


def test_missing_successful_source_scan_fails_closed(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 6, 30, tzinfo=timezone.utc)
    _scan(store, _quote(venue="Coinbase", observed_at=base))

    candidate = _current_source_scan_candidate(
        SourceCoveragePlane(store),
        _spec(),
        set(inspect(store.engine).get_table_names()),
    )

    assert candidate is None

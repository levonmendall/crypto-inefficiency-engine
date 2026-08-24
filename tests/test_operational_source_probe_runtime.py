from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import event, inspect

from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.operational_source_probe_runtime import (
    PERMANENT_SOURCE_WORKER_ID,
    _CATALOG_PROVIDER_IDS,
    _current_source_scan_candidate,
    _latest_catalog_provider_rows,
)
from inefficiency_engine.source_coverage import SourceCoveragePlane
from inefficiency_engine.source_coverage_catalog import SOURCES


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


def _provider_scan(
    store: EvidenceStore,
    *,
    provider: str,
    observed_at: datetime,
    ok: bool = True,
    item_count: int = 1,
) -> str:
    return store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[],
        providers=[
            ProviderStatus(
                provider=provider,
                ok=ok,
                item_count=item_count,
                observed_at=observed_at,
            )
        ],
        order_books=[],
        executability=[],
        started_at=observed_at,
        completed_at=observed_at + timedelta(milliseconds=1),
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


def test_provider_status_reconciliation_uses_bounded_indexed_catalog_seeks(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 6, 30, tzinfo=timezone.utc)

    # Large unrelated history must never be scanned merely to reconstruct the source
    # providers that are actually referenced by the canonical source catalog.
    for offset in range(40):
        _provider_scan(
            store,
            provider="diagnostic-provider-not-in-source-contract",
            observed_at=base + timedelta(seconds=offset),
        )
    _provider_scan(
        store,
        provider="coinbase-exchange:ticker",
        observed_at=base + timedelta(minutes=1),
        ok=False,
        item_count=0,
    )
    newest = base + timedelta(minutes=2)
    _provider_scan(
        store,
        provider="coinbase-exchange:ticker",
        observed_at=newest,
        ok=True,
        item_count=7,
    )
    _provider_scan(
        store,
        provider="okx-v5:market:ticker",
        observed_at=base + timedelta(minutes=3),
        ok=True,
        item_count=9,
    )

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(str(statement).lower())

    plane = SourceCoveragePlane(store)
    available = set(inspect(store.engine).get_table_names())
    event.listen(store.engine, "before_cursor_execute", capture)
    try:
        rows = _latest_catalog_provider_rows(plane, available)
        statement_count_after_first_read = len(statements)
        cached_rows = _latest_catalog_provider_rows(plane, available)
    finally:
        event.remove(store.engine, "before_cursor_execute", capture)

    by_provider = {str(row["provider"]): row for row in rows}
    assert set(by_provider) == {
        "coinbase-exchange:ticker",
        "okx-v5:market:ticker",
    }
    assert by_provider["coinbase-exchange:ticker"]["observed_at"] == newest.isoformat()
    assert by_provider["coinbase-exchange:ticker"]["ok"] is True
    assert by_provider["coinbase-exchange:ticker"]["item_count"] == 7
    assert cached_rows == rows
    assert len(statements) == statement_count_after_first_read

    provider_queries = [
        sql for sql in statements if "select" in sql and "provider_statuses" in sql
    ]
    assert provider_queries
    assert len(provider_queries) <= len(_CATALOG_PROVIDER_IDS)
    assert all("provider_statuses.provider =" in sql for sql in provider_queries)
    assert all("order by provider_statuses.id desc" in sql for sql in provider_queries)
    assert all("limit" in sql for sql in provider_queries)
    assert all("group by" not in sql for sql in provider_queries)
    assert all("limit 1000" not in sql for sql in provider_queries)


def test_catalog_provider_seek_set_is_exactly_source_contract_provider_ids():
    expected = {
        str(provider)
        for source in SOURCES
        for provider in list(source.get("provider") or [])
        if str(provider)
    }
    assert set(_CATALOG_PROVIDER_IDS) == expected

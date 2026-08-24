from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import event

from inefficiency_engine.config import Settings
from inefficiency_engine.durable_control_alpha import DurableControlAlphaFactoryService
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.memory_bounded_qualified_opportunity import (
    PERMANENT_SOURCE_WORKER_ID,
    MemoryBoundedQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityLeg,
    Side,
    Strategy,
)


def _quote(
    *,
    venue: str,
    asset: str,
    observed_at: datetime,
    mid: float = 100.0,
) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot",
        mid=mid,
        observed_at=observed_at,
        source="test",
    )


def _source_scan(
    store: EvidenceStore,
    *,
    observed_at: datetime,
    permanent: bool,
    venue: str = "Coinbase",
    asset: str = "BTC",
) -> str:
    return store.record_scan(
        funding_quotes=[],
        market_quotes=[_quote(venue=venue, asset=asset, observed_at=observed_at)],
        opportunities=[],
        providers=[],
        order_books=[],
        executability=[],
        started_at=observed_at,
        completed_at=observed_at + timedelta(milliseconds=1),
        analysis_config={"permanent_source_plane": permanent},
    )


def _opportunity(observed_at: datetime) -> Opportunity:
    return Opportunity(
        id="bridge-test-opportunity",
        strategy=Strategy.CEX_SPOT_DISLOCATION,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue="Coinbase",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.LONG,
                symbol="BTC-USD",
                quote_currency="USD",
                contract_key="spot",
                reference_price=100.0,
            ),
            OpportunityLeg(
                venue="Kraken",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.SHORT,
                symbol="BTC-USD",
                quote_currency="USD",
                contract_key="spot",
                reference_price=100.2,
            ),
        ],
        gross_edge_bps_per_hour=20.0,
        modeled_cost_bps=2.0,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=1.0,
        net_edge_bps_per_hour=17.0,
        net_annualized_return=0.25,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=5),
        paper_only=True,
    )


def test_bridge_selects_exact_successful_source_heartbeat_without_scan_history_walk(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc)
    authoritative_scan = _source_scan(store, observed_at=base, permanent=True)
    # A newer quote-bearing row must not displace the permanent source boundary.
    _source_scan(
        store,
        observed_at=base + timedelta(minutes=1),
        permanent=False,
        venue="Kraken",
    )
    store.record_worker_heartbeat(
        worker_id=PERMANENT_SOURCE_WORKER_ID,
        state="success",
        scan_id=authoritative_scan,
        detail={"paper_only": True},
    )

    bridge = MemoryBoundedQualifiedOpportunityBridgePublisher(
        SimpleNamespace(),
        store,
        SimpleNamespace(),
    )
    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(str(statement).lower())

    event.listen(store.engine, "before_cursor_execute", capture)
    try:
        with store.engine.connect() as db:
            row, config = bridge._select_full_scan(db)
    finally:
        event.remove(store.engine, "before_cursor_execute", capture)

    assert row is not None
    assert str(row["scan_id"]) == authoritative_scan
    assert config["bridge_source_selection"] == "permanent_source_heartbeat"
    assert any("worker_heartbeats" in sql and "order by worker_heartbeats.id desc" in sql for sql in statements)
    assert not any("order by scans.completed_at desc" in sql for sql in statements)


def test_bridge_does_not_load_full_empirical_latency_history_when_depth_fails_closed(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    observed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    scan_id = _source_scan(store, observed_at=observed_at, permanent=True)
    store.record_worker_heartbeat(
        worker_id=PERMANENT_SOURCE_WORKER_ID,
        state="success",
        scan_id=scan_id,
        detail={"paper_only": True},
    )

    class Core:
        settings = Settings()

        @staticmethod
        def analyze(_funding_quotes, _market_quotes):
            return [_opportunity(observed_at)]

        @staticmethod
        def empirical_latency_resolver():
            raise AssertionError("empirical shadow history must stay lazy")

    bridge = MemoryBoundedQualifiedOpportunityBridgePublisher(
        Core(),
        store,
        SimpleNamespace(),
    )
    snapshot = bridge._latest_scan()

    assert snapshot is not None
    assert snapshot.scan_id == scan_id
    assert len(snapshot.opportunities) == 1
    assert len(snapshot.executability) == 1
    assert snapshot.analysis_config["bridge_projection_synthesized_executability"] is True
    assert snapshot.analysis_config["bridge_projection_empirical_latency_history_loaded"] is False
    assert snapshot.analysis_config["bridge_projection_provider_requests"] == 0


def test_durable_control_short_history_is_current_cohort_only_and_cached(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    completed_at = datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc)
    relevant = _quote(
        venue="Coinbase",
        asset="BTC",
        observed_at=completed_at - timedelta(minutes=30),
    )
    unrelated = _quote(
        venue="Bybit",
        asset="DOGE",
        observed_at=completed_at - timedelta(minutes=20),
    )
    store.record_scan(
        funding_quotes=[],
        market_quotes=[relevant, unrelated],
        opportunities=[],
        providers=[],
        order_books=[],
        executability=[],
        started_at=completed_at - timedelta(minutes=30),
        completed_at=completed_at - timedelta(minutes=20),
    )

    current = _quote(
        venue="Coinbase",
        asset="BTC",
        observed_at=completed_at,
        mid=101.0,
    )
    snapshot = ScanSnapshot(
        scan_id="current-source-scan",
        started_at=completed_at - timedelta(seconds=1),
        completed_at=completed_at,
        providers=[],
        funding_quotes=[],
        market_quotes=[current],
        opportunities=[],
        order_books=[],
        executability=[],
        analysis_config={},
    )

    service = DurableControlAlphaFactoryService.__new__(DurableControlAlphaFactoryService)
    service.store = store
    service._durable_history_cache_key = None
    service._durable_history_cache = {}
    service._durable_history_cache_hits = 0
    service._durable_history_query_count = 0
    service._effective_history_hours = lambda: 24.0

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(str(statement).lower())

    event.listen(store.engine, "before_cursor_execute", capture)
    try:
        first = service._history_for_snapshot(snapshot)
        statement_count = len(statements)
        second = service._history_for_snapshot(snapshot)
    finally:
        event.remove(store.engine, "before_cursor_execute", capture)

    key = ("Coinbase", "BTC", MarketKind.SPOT)
    assert list(first) == [key]
    assert [item.mid for item in first[key]] == [100.0]
    assert second == first
    assert len(statements) == statement_count
    assert service._durable_history_query_count == 1
    assert service._durable_history_cache_hits == 1

    history_queries = [
        sql for sql in statements if "market_quotes" in sql and "payload_json" in sql
    ]
    assert history_queries
    assert all("market_quotes.venue =" in sql for sql in history_queries)
    assert all("upper(market_quotes.asset)" in sql for sql in history_queries)

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import event, select

from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.durable_control_cycle_history import (
    CONTROL_CYCLE_HISTORY_ROWS,
    advance_durable_control_cycle_history_cache,
    load_durable_control_cycle_history,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote


class _Factory:
    def __init__(self, store: EvidenceStore, *, effective_history_hours: float = 24.0):
        self.store = store
        self._expanded_settings = SimpleNamespace()
        self._effective_hours = effective_history_hours

    def _effective_history_hours(self) -> float:
        return self._effective_hours

    @staticmethod
    def _current_keys(snapshot):
        return {
            (quote.venue, quote.asset.upper(), quote.market_kind)
            for quote in snapshot.market_quotes
        }


def _quote(
    observed_at: datetime,
    *,
    mid: float,
    venue: str = "Coinbase",
    asset: str = "BTC",
    kind: MarketKind = MarketKind.SPOT,
) -> MarketQuote:
    suffix = "PERP" if kind == MarketKind.PERPETUAL else "USD"
    return MarketQuote(
        venue=venue,
        asset=asset,
        market_kind=kind,
        symbol=f"{asset}-{suffix}",
        mid=mid,
        observed_at=observed_at,
        source="test",
    )


def _snapshot(at: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        scan_id=f"scan-{int(at.timestamp())}",
        completed_at=at,
        market_quotes=[
            _quote(at, mid=999.0),
            _quote(at, mid=998.0, kind=MarketKind.PERPETUAL),
        ],
    )


def _record(store: EvidenceStore, quotes: list[MarketQuote], *, completed_at: datetime) -> None:
    store.record_scan(
        funding_quotes=[],
        market_quotes=quotes,
        opportunities=[],
        providers=[],
        started_at=completed_at - timedelta(seconds=1),
        completed_at=completed_at,
    )


def test_cycle_history_cache_matches_filter_before_daily_rank(monkeypatch, tmp_path):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "cycle-history-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 72.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )

    store = EvidenceStore(tmp_path / "evidence.db")
    factory = _Factory(store)
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    quotes = [
        _quote(datetime(2026, 8, 21, 13, 0, tzinfo=timezone.utc), mid=101.0),
        _quote(datetime(2026, 8, 21, 14, 0, tzinfo=timezone.utc), mid=102.0),
        _quote(datetime(2026, 8, 21, 15, 0, tzinfo=timezone.utc), mid=103.0),
        _quote(datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc), mid=201.0),
        _quote(
            datetime(2026, 8, 22, 9, 0, tzinfo=timezone.utc),
            mid=202.0,
            kind=MarketKind.PERPETUAL,
        ),
        _quote(datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc), mid=203.0),
        _quote(datetime(2026, 8, 23, 8, 0, tzinfo=timezone.utc), mid=301.0),
        _quote(datetime(2026, 8, 23, 10, 0, tzinfo=timezone.utc), mid=302.0),
        _quote(datetime(2026, 8, 23, 11, 0, tzinfo=timezone.utc), mid=303.0),
        _quote(datetime(2026, 8, 23, 12, 30, tzinfo=timezone.utc), mid=304.0),
        _quote(datetime(2026, 8, 23, 14, 0, tzinfo=timezone.utc), mid=305.0),
        _quote(
            datetime(2026, 8, 22, 11, 0, tzinfo=timezone.utc),
            mid=9999.0,
            venue="OKX",
        ),
    ]
    _record(store, quotes, completed_at=now)

    snapshot = _snapshot(now)
    progress = advance_durable_control_cycle_history_cache(factory, snapshot)
    assert progress["complete"] is True
    assert progress["legacy_window_query_avoided"] is True
    assert progress["filter_before_daily_rank_preserved"] is True
    assert progress["boundary_raw_day_materialization_avoided"] is True
    assert progress["bucket_payload_fetch_bounded"] is True

    history = load_durable_control_cycle_history(factory, snapshot)
    assert history is not None
    spot = history[("Coinbase", "BTC", MarketKind.SPOT)]
    perp = history.get(("Coinbase", "BTC", MarketKind.PERPETUAL), [])

    assert [row.mid for row in spot if row.observed_at.date().isoformat() == "2026-08-21"] == [
        102.0,
        103.0,
    ]
    assert [row.mid for row in spot if row.observed_at.date().isoformat() == "2026-08-22"] == [
        203.0
    ]
    assert [row.mid for row in perp] == [202.0]
    # The boundary query applies the 12:00 cutoff before ranking, so later 12:30/14:00
    # rows never enter the compact bucket and cannot suppress eligible 10:00/11:00 rows.
    assert [row.mid for row in spot if row.observed_at.date().isoformat() == "2026-08-23"] == [
        302.0,
        303.0,
    ]

    with store.engine.connect() as db:
        boundary_rows = list(
            db.execute(
                select(CONTROL_CYCLE_HISTORY_ROWS.c.source_id)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == "cycle-history-test")
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.venue == "Coinbase")
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.asset == "BTC")
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.day == "2026-08-23")
            )
        )
    assert len(boundary_rows) == 2


def test_cycle_history_bootstrap_is_bounded_checkpointed_and_fail_closed(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "cycle-history-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 240.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )
    import inefficiency_engine.durable_control_cycle_history as cache_runtime

    monkeypatch.setattr(cache_runtime, "_bucket_query_budget", lambda: 2)
    store = EvidenceStore(tmp_path / "evidence.db")
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    quotes = [
        _quote(now - timedelta(days=day, hours=2), mid=100.0 + day)
        for day in range(1, 11)
    ]
    _record(store, list(reversed(quotes)), completed_at=now)

    first_factory = _Factory(store)
    snapshot = _snapshot(now)
    first = advance_durable_control_cycle_history_cache(first_factory, snapshot)
    assert first["complete"] is False
    assert first["bucket_queries"] <= first["query_budget"]
    assert first["checkpoint_writes"] >= first["bucket_queries"]
    assert first["durable_checkpoint_persisted"] is True
    assert load_durable_control_cycle_history(first_factory, snapshot) is None

    factory = _Factory(store)
    attempts = 1
    progress = first
    while not progress["complete"] and attempts < 10:
        progress = advance_durable_control_cycle_history_cache(factory, snapshot)
        attempts += 1
    assert progress["complete"] is True
    assert attempts > 1
    assert load_durable_control_cycle_history(factory, snapshot) is not None
    assert progress["qualification_thresholds_unchanged"] is True
    assert progress["paper_only"] is True


def test_boundary_refreshes_for_later_cutoff_same_utc_day(monkeypatch, tmp_path):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "cycle-history-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 72.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )

    store = EvidenceStore(tmp_path / "evidence.db")
    factory = _Factory(store)
    first_now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    _record(
        store,
        [
            _quote(datetime(2026, 8, 23, 10, 0, tzinfo=timezone.utc), mid=10.0),
            _quote(datetime(2026, 8, 23, 11, 0, tzinfo=timezone.utc), mid=11.0),
            _quote(datetime(2026, 8, 23, 12, 30, tzinfo=timezone.utc), mid=12.5),
            _quote(datetime(2026, 8, 23, 14, 0, tzinfo=timezone.utc), mid=14.0),
        ],
        completed_at=first_now,
    )
    assert advance_durable_control_cycle_history_cache(factory, _snapshot(first_now))["complete"]

    later_snapshot = _snapshot(first_now + timedelta(hours=1))
    # Exact-cutoff compact rows are intentionally not reused across a different source
    # snapshot. The cache fails closed until the later cutoff is refreshed.
    assert load_durable_control_cycle_history(factory, later_snapshot) is None
    refreshed = advance_durable_control_cycle_history_cache(factory, later_snapshot)
    assert refreshed["complete"] is True
    history = load_durable_control_cycle_history(factory, later_snapshot)
    assert history is not None
    spot = history[("Coinbase", "BTC", MarketKind.SPOT)]
    boundary = [row.mid for row in spot if row.observed_at.date().isoformat() == "2026-08-23"]
    assert boundary == [11.0, 12.5]


def test_bucket_seek_ranks_ids_before_fetching_payloads(monkeypatch, tmp_path):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "cycle-history-query-shape-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 72.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )
    store = EvidenceStore(tmp_path / "evidence.db")
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    _record(
        store,
        [
            _quote(datetime(2026, 8, 23, hour, 0, tzinfo=timezone.utc), mid=float(hour))
            for hour in range(1, 15)
        ],
        completed_at=now,
    )
    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        if "market_quotes" in statement and statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(store.engine, "before_cursor_execute", capture)
    try:
        progress = advance_durable_control_cycle_history_cache(_Factory(store), _snapshot(now))
    finally:
        event.remove(store.engine, "before_cursor_execute", capture)

    assert progress["complete"] is True
    candidate_queries = [statement for statement in statements if "observed_at" in statement]
    assert candidate_queries
    first = candidate_queries[0].lower()
    assert "payload_json" not in first
    assert "order by market_quotes.id desc" in first
    assert "limit" in first
    assert any("payload_json" in statement.lower() and " in (" in statement.lower() for statement in statements)


def test_cycle_history_query_budget_is_hard_and_round_robin(monkeypatch, tmp_path):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "cycle-history-budget-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 72.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )
    import inefficiency_engine.durable_control_cycle_history as cache_runtime

    monkeypatch.setattr(cache_runtime, "_bucket_query_budget", lambda: 1)
    store = EvidenceStore(tmp_path / "evidence.db")
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    snapshot = SimpleNamespace(
        scan_id="budget-scan",
        completed_at=now,
        market_quotes=[
            _quote(now, mid=100.0, venue="Coinbase", asset="BTC"),
            _quote(now, mid=200.0, venue="OKX", asset="ETH"),
        ],
    )

    first = advance_durable_control_cycle_history_cache(_Factory(store), snapshot)
    assert first["complete"] is False
    assert first["bucket_queries"] == 1
    assert first["query_budget"] == 1
    assert first["next_pair_index"] == 1
    assert first["checkpoint_writes"] >= 1

    second = advance_durable_control_cycle_history_cache(_Factory(store), snapshot)
    assert second["bucket_queries"] == 1
    assert second["query_budget"] == 1
    assert second["next_pair_index"] == 0
    assert second["checkpoint_writes"] >= 1


def test_cycle_history_time_budget_checkpoints_before_return(monkeypatch, tmp_path):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "cycle-history-time-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 240.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )
    import inefficiency_engine.durable_control_cycle_history as cache_runtime

    monkeypatch.setattr(cache_runtime, "_bucket_query_budget", lambda: 10)
    original_monotonic = cache_runtime.time.monotonic
    ticks = iter([0.0, 0.0, 9.0, 9.0, 9.0])
    monkeypatch.setattr(cache_runtime.time, "monotonic", lambda: next(ticks))

    store = EvidenceStore(tmp_path / "evidence.db")
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    snapshot = _snapshot(now)
    first = advance_durable_control_cycle_history_cache(_Factory(store), snapshot)

    assert first["complete"] is False
    assert first["bucket_queries"] == 1
    assert first["stopped_for_time_budget"] is True
    assert first["durable_checkpoint_persisted"] is True
    assert first["checkpoint_writes"] >= first["bucket_queries"]
    assert first["cached_pair_count"] == 0

    monkeypatch.setattr(cache_runtime.time, "monotonic", original_monotonic)
    monkeypatch.setattr(cache_runtime, "_bucket_query_budget", lambda: 1)
    second = advance_durable_control_cycle_history_cache(_Factory(store), snapshot)
    assert second["bucket_queries"] == 1
    assert second["boundary_rows_retained"] == 0
    assert second["cached_pair_count"] == 1

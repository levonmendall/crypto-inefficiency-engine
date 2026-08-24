from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.durable_control_cache import save_control_cache_checkpoint
from inefficiency_engine.durable_control_cycle_history import (
    _CACHE_KEY,
    _fresh_checkpoint,
    _pair_token,
)
from inefficiency_engine.durable_control_cycle_history_target_bridge_runtime import (
    advance_durable_control_cycle_history_cache as advance_and_pin,
)
from inefficiency_engine.durable_control_cycle_history_target_runtime import (
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
    venue: str,
    kind: MarketKind,
    mid: float,
) -> MarketQuote:
    suffix = "PERP" if kind == MarketKind.PERPETUAL else "USD"
    return MarketQuote(
        venue=venue,
        asset="BTC",
        market_kind=kind,
        symbol=f"BTC-{suffix}",
        mid=mid,
        observed_at=observed_at,
        source="test",
    )


def _record_source_scan(
    store: EvidenceStore,
    *,
    scan_id: str,
    completed_at: datetime,
):
    quotes = [
        _quote(
            completed_at,
            venue="Coinbase",
            kind=MarketKind.SPOT,
            mid=100.0,
        ),
        _quote(
            completed_at,
            venue="OKX",
            kind=MarketKind.PERPETUAL,
            mid=101.0,
        ),
    ]
    store.record_scan(
        funding_quotes=[],
        market_quotes=quotes,
        opportunities=[],
        providers=[],
        order_books=[],
        executability=[],
        started_at=completed_at - timedelta(seconds=1),
        completed_at=completed_at,
        scan_id=scan_id,
    )
    return store.load_scan(scan_id)


def _configure(monkeypatch) -> None:
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "frozen-cycle-history-test")
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "required_history_hours",
        classmethod(lambda cls, settings: 48.0),
    )
    monkeypatch.setattr(
        CycleAwareMultiHorizonTrendStrategy,
        "rows_per_day",
        classmethod(lambda cls, settings: 2),
    )


def test_working_target_remains_frozen_while_source_snapshot_advances(monkeypatch, tmp_path):
    _configure(monkeypatch)
    import inefficiency_engine.durable_control_cycle_history_target_runtime as runtime

    monkeypatch.setattr(runtime, "_bucket_query_budget", lambda: 1)
    store = EvidenceStore(tmp_path / "evidence.db")
    factory = _Factory(store)
    first_at = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    first_snapshot = _record_source_scan(
        store,
        scan_id="source-a",
        completed_at=first_at,
    )
    later_snapshot = _record_source_scan(
        store,
        scan_id="source-b",
        completed_at=first_at + timedelta(minutes=1),
    )

    first = advance_durable_control_cycle_history_cache(factory, first_snapshot)
    assert first["complete"] is False
    assert first["working_target_scan_id"] == "source-a"
    assert first["target_frozen_across_executors"] is True

    second = advance_durable_control_cycle_history_cache(factory, later_snapshot)
    assert second["complete"] is False
    assert second["working_target_scan_id"] == "source-a"
    assert second["working_target_completed_at"] == first_at.isoformat()

    progress = second
    attempts = 2
    while not progress["complete"] and attempts < 10:
        progress = advance_durable_control_cycle_history_cache(factory, later_snapshot)
        attempts += 1

    assert progress["complete"] is True
    assert progress["serving_scan_id"] == "source-a"
    assert progress["promoted_working_target"] is True
    assert load_durable_control_cycle_history(factory, first_snapshot) is not None
    assert load_durable_control_cycle_history(factory, later_snapshot) is None
    assert progress["qualification_thresholds_unchanged"] is True
    assert progress["paper_only"] is True


def test_certified_target_serves_while_new_boundary_builds_then_promotes(monkeypatch, tmp_path):
    _configure(monkeypatch)
    import inefficiency_engine.durable_control_cycle_history_target_runtime as runtime

    monkeypatch.setattr(runtime, "_bucket_query_budget", lambda: 16)
    store = EvidenceStore(tmp_path / "evidence.db")
    factory = _Factory(store)
    first_at = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    first_snapshot = _record_source_scan(
        store,
        scan_id="source-a",
        completed_at=first_at,
    )
    first = advance_durable_control_cycle_history_cache(factory, first_snapshot)
    assert first["complete"] is True
    assert first["serving_scan_id"] == "source-a"

    second_at = first_at + timedelta(minutes=1)
    _record_source_scan(store, scan_id="source-b", completed_at=second_at)
    monkeypatch.setattr(runtime, "_bucket_query_budget", lambda: 1)

    current = store.load_scan("source-b")
    rolling = advance_and_pin(factory, current)
    assert rolling["complete"] is True
    assert rolling["rolling_refresh_in_progress"] is True
    assert rolling["serving_scan_id"] == "source-a"
    assert rolling["working_target_scan_id"] == "source-b"
    assert rolling["double_buffered_boundary"] is True
    assert rolling["partial_working_target_authoritative"] is False
    assert rolling["serving_snapshot_pinned_in_place"] is True
    assert current.scan_id == "source-a"
    assert current.completed_at == first_at

    current = store.load_scan("source-b")
    promoted = advance_and_pin(factory, current)
    assert promoted["complete"] is True
    assert promoted["promoted_working_target"] is True
    assert promoted["rolling_refresh_in_progress"] is False
    assert promoted["serving_scan_id"] == "source-b"
    assert current.scan_id == "source-b"
    assert current.completed_at == second_at
    assert load_durable_control_cycle_history(factory, current) is not None


def test_legacy_v3_stable_progress_is_preserved_during_target_migration(monkeypatch, tmp_path):
    _configure(monkeypatch)
    import inefficiency_engine.durable_control_cycle_history_target_runtime as runtime

    monkeypatch.setattr(runtime, "_bucket_query_budget", lambda: 1)
    store = EvidenceStore(tmp_path / "evidence.db")
    factory = _Factory(store)
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    snapshot = _record_source_scan(store, scan_id="source-a", completed_at=now)

    checkpoint = _fresh_checkpoint(factory)
    stable_end_day = (now - timedelta(hours=24)).date() - timedelta(days=1)
    checkpoint["pair_completed_through"] = {
        _pair_token("Coinbase", "BTC"): stable_end_day.isoformat(),
        _pair_token("OKX", "BTC"): stable_end_day.isoformat(),
    }
    assert save_control_cache_checkpoint(
        store,
        cache_key=_CACHE_KEY,
        payload=checkpoint,
        complete=False,
    )

    progress = advance_durable_control_cycle_history_cache(factory, snapshot)
    assert progress["legacy_stable_progress_preserved"] is True
    assert progress["cached_pair_count"] == 2
    assert progress["stable_rows_retained"] == 0
    assert progress["boundary_rows_retained"] >= 0
    assert progress["working_target_scan_id"] == "source-a"
    assert progress["complete"] is False

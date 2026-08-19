from datetime import datetime, timedelta, timezone

from inefficiency_engine.alpha_coverage_strategies import (
    CrossSectionalRelativeValueStrategy,
    EventDrivenStrategy,
    EventLedger,
    EventObservation,
    MicrostructureImbalanceStrategy,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.expanded_alpha_factory import _ExpandedSettingsView
from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


def quote(asset: str, price: float, at: datetime, *, venue: str = "Coinbase", kind: MarketKind = MarketKind.SPOT) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset=asset,
        market_kind=kind,
        symbol=f"{asset}-USD" if kind == MarketKind.SPOT else asset,
        bid=price * 0.9999,
        ask=price * 1.0001,
        mid=price,
        observed_at=at,
        source="test",
    )


def scan(quotes: list[MarketQuote], *, books: list[OrderBookSnapshot] | None = None) -> ScanSnapshot:
    return ScanSnapshot(
        scan_id="coverage-alpha",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=quotes,
        opportunities=[],
        order_books=books or [],
    )


def history(asset: str, start: float, end: float) -> list[MarketQuote]:
    rows: list[MarketQuote] = []
    for index in range(12):
        fraction = index / 11.0
        price = start + (end - start) * fraction
        rows.append(quote(asset, price, NOW - timedelta(hours=44 - index * 4)))
    rows[-1] = quote(asset, end, NOW)
    return rows


def test_cross_sectional_relative_value_emits_forward_candidate_from_multi_asset_dispersion():
    settings = _ExpandedSettingsView(Settings(alpha_research_cost_floor_bps=5.0, alpha_min_history_points=8))
    btc = history("BTC", 60000.0, 72000.0)
    eth = history("ETH", 3000.0, 3030.0)
    sol = history("SOL", 150.0, 138.0)
    current = [btc[-1], eth[-1], sol[-1]]
    histories = {
        ("Coinbase", "BTC", MarketKind.SPOT): btc,
        ("Coinbase", "ETH", MarketKind.SPOT): eth,
        ("Coinbase", "SOL", MarketKind.SPOT): sol,
    }

    rows = CrossSectionalRelativeValueStrategy().discover(
        scan(current), histories, settings, total_capital_usd=100000.0  # type: ignore[arg-type]
    )

    assert rows
    assert any(row.asset == "BTC" and row.direction == "long" for row in rows)
    assert all(row.family == "cross_sectional_relative_value" for row in rows)
    assert all(row.expected_net_return > 0 for row in rows)
    assert all(row.paper_allocation_eligible is False for row in rows)


def test_microstructure_alpha_uses_visible_l2_without_maker_fill_assumption():
    settings = _ExpandedSettingsView(Settings(alpha_research_cost_floor_bps=5.0, alpha_min_history_points=4))
    current = quote("BTC", 60000.0, NOW)
    book = OrderBookSnapshot(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        bids=[
            OrderBookLevel(price=59994.0, size=2.0),
            OrderBookLevel(price=59990.0, size=1.5),
            OrderBookLevel(price=59985.0, size=1.0),
        ],
        asks=[
            OrderBookLevel(price=60006.0, size=0.15),
            OrderBookLevel(price=60010.0, size=0.10),
            OrderBookLevel(price=60015.0, size=0.10),
        ],
        observed_at=NOW,
        source="test",
    )
    hist = [quote("BTC", 59800.0 + index * 20.0, NOW - timedelta(hours=5 - index)) for index in range(6)]
    hist[-1] = current

    rows = MicrostructureImbalanceStrategy().discover(
        scan([current], books=[book]),
        {("Coinbase", "BTC", MarketKind.SPOT): hist},
        settings,  # type: ignore[arg-type]
        total_capital_usd=100000.0,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.family == "microstructure_orderflow"
    assert row.direction == "long"
    assert row.features["l2_imbalance"] > 0
    assert row.features["maker_fill_assumed"] is False
    assert row.paper_allocation_eligible is False


def test_event_alpha_fails_closed_until_event_source_is_authoritative_and_commercial(tmp_path):
    store = EvidenceStore(tmp_path / "event.sqlite3")
    ledger = EventLedger(store)
    strategy = EventDrivenStrategy(ledger)
    settings = _ExpandedSettingsView(Settings(alpha_research_cost_floor_bps=5.0))
    current = quote("BTC", 60000.0, NOW)

    ledger.record(EventObservation(
        provider="unqualified-feed",
        asset="BTC",
        event_type="protocol_upgrade",
        known_at=NOW - timedelta(hours=1),
        event_at=NOW + timedelta(hours=2),
        observed_at=NOW,
        surprise_score=0.9,
        confidence=0.9,
        authoritative=False,
        commercial_use_permitted=True,
    ))
    assert strategy.discover(
        scan([current]),
        {("Coinbase", "BTC", MarketKind.SPOT): [current]},
        settings,  # type: ignore[arg-type]
        total_capital_usd=100000.0,
    ) == []

    ledger.record(EventObservation(
        provider="authoritative-feed",
        asset="BTC",
        event_type="protocol_upgrade",
        known_at=NOW - timedelta(minutes=30),
        event_at=NOW + timedelta(hours=2),
        observed_at=NOW,
        surprise_score=0.9,
        confidence=0.9,
        authoritative=True,
        commercial_use_permitted=True,
    ))
    rows = strategy.discover(
        scan([current]),
        {("Coinbase", "BTC", MarketKind.SPOT): [current]},
        settings,  # type: ignore[arg-type]
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    assert rows[0].family == "event_driven"
    assert rows[0].direction == "long"
    assert rows[0].paper_allocation_eligible is False

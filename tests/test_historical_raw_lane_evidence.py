from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, create_engine, insert

from inefficiency_engine.historical_raw_lane_evidence import recover_raw_lane_history


START = datetime(2026, 8, 21, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc)


def _store_with_tables():
    engine = create_engine("sqlite://")
    metadata = MetaData()
    market = Table(
        "market_quotes", metadata,
        Column("id", Integer, primary_key=True),
        Column("venue", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    books = Table(
        "order_books", metadata,
        Column("id", Integer, primary_key=True),
        Column("venue", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    events = Table(
        "source_event_observations", metadata,
        Column("id", Integer, primary_key=True),
        Column("lane_id", Text, nullable=False),
        Column("source_id", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    mechanism = Table(
        "mechanism_research_observations", metadata,
        Column("id", Integer, primary_key=True),
        Column("mechanism", Text, nullable=False),
        Column("provider", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    capacity = Table(
        "option_capacity_observations", metadata,
        Column("id", Integer, primary_key=True),
        Column("observed_at", Text, nullable=False),
    )
    fundamental = Table(
        "alpha_fundamental_observations", metadata,
        Column("id", Integer, primary_key=True),
        Column("provider", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    metadata.create_all(engine)
    return SimpleNamespace(engine=engine), market, books, events, mechanism, capacity, fundamental


def _times():
    return START + timedelta(hours=1), END - timedelta(hours=1)


def test_raw_market_and_event_ledgers_recover_nonzero_lane_history():
    store, market, books, events, mechanism, capacity, fundamental = _store_with_tables()
    early, late = _times()
    with store.engine.begin() as db:
        db.execute(insert(market), [
            {"venue": "Coinbase", "observed_at": early.isoformat()},
            {"venue": "Coinbase", "observed_at": late.isoformat()},
        ])
        db.execute(insert(books), [
            {"venue": "Coinbase", "observed_at": early.isoformat()},
            {"venue": "Coinbase", "observed_at": late.isoformat()},
        ])
        db.execute(insert(events), [
            {"lane_id": lane, "source_id": "public-trade-flow", "observed_at": at.isoformat()}
            for lane in ("microstructure", "liquidity_provision")
            for at in (early, late)
        ])

    history = recover_raw_lane_history(store, start=START, boundary=END)
    assert history["trend_momentum"]["source_count"] > 0
    assert {"market_history", "execution_costs"} <= history["trend_momentum"]["evidence_classes"]
    assert {"order_book", "trade_flow"} <= history["microstructure"]["evidence_classes"]
    assert {"order_book", "trade_flow"} <= history["liquidity_provision"]["evidence_classes"]
    assert "market_quotes" in history["trend_momentum"]["source_ledgers"]
    assert "source_event_observations" in history["microstructure"]["source_ledgers"]


def test_raw_research_ledgers_recover_yield_volatility_and_fundamental_classes():
    store, market, books, events, mechanism, capacity, fundamental = _store_with_tables()
    early, late = _times()
    with store.engine.begin() as db:
        db.execute(insert(mechanism), [
            {"mechanism": "yield", "provider": "morpho:graphql-markets", "observed_at": early.isoformat()},
            {"mechanism": "yield", "provider": "morpho:graphql-markets", "observed_at": late.isoformat()},
            {"mechanism": "volatility", "provider": "deribit:public-option-order-book", "observed_at": early.isoformat()},
            {"mechanism": "volatility", "provider": "deribit:public-option-order-book", "observed_at": late.isoformat()},
        ])
        db.execute(insert(capacity), [
            {"observed_at": early.isoformat()},
            {"observed_at": late.isoformat()},
        ])
        db.execute(insert(fundamental), [
            {"provider": "ethereum+morpho:composite", "observed_at": early.isoformat()},
            {"provider": "ethereum+morpho:composite", "observed_at": late.isoformat()},
        ])

    history = recover_raw_lane_history(store, start=START, boundary=END)
    assert {"yield_rate", "capacity", "exit_liquidity"} <= history["yield"]["evidence_classes"]
    assert {"option_quotes", "option_greeks", "option_depth", "option_capacity"} <= history["volatility"]["evidence_classes"]
    assert {"chain_activity", "protocol_fundamentals"} <= history["fundamental_onchain"]["evidence_classes"]


def test_raw_reconstruction_never_fabricates_transfer_classes():
    store, market, books, events, mechanism, capacity, fundamental = _store_with_tables()
    history = recover_raw_lane_history(store, start=START, boundary=END)
    lane = history["capital_location_settlement"]
    assert "transfer_costs" not in lane["evidence_classes"]
    assert "transfer_latency" not in lane["evidence_classes"]

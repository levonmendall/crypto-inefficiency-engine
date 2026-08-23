from datetime import datetime, timezone

from sqlalchemy import func, select

from inefficiency_engine.coinbase_trade_flow import (
    _persist_trade_events_bulk,
    parse_coinbase_product_trades,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation


def test_coinbase_trade_parser_inverts_documented_maker_side_to_aggressor():
    observed_at = datetime(2026, 8, 22, 21, 0, tzinfo=timezone.utc)
    rows = parse_coinbase_product_trades(
        [
            {
                "trade_id": 101,
                "time": "2026-08-22T20:59:59.000Z",
                "price": "60000.0",
                "size": "0.25",
                "side": "sell",
            },
            {
                "trade_id": 102,
                "time": "2026-08-22T20:59:58.000Z",
                "price": "59999.0",
                "size": "0.10",
                "side": "buy",
            },
        ],
        product_id="BTC-USD",
        observed_at=observed_at,
    )

    assert len(rows) == 2
    assert rows[0]["asset"] == "BTC"
    assert rows[0]["maker_side"] == "sell"
    assert rows[0]["aggressor_side"] == "buy"
    assert rows[0]["notional_usd"] == 15000.0
    assert rows[1]["maker_side"] == "buy"
    assert rows[1]["aggressor_side"] == "sell"


def test_coinbase_trade_parser_fails_closed_on_wrong_payload_shape():
    try:
        parse_coinbase_product_trades(
            {"trades": []},
            product_id="BTC-USD",
            observed_at=datetime.now(timezone.utc),
        )
    except ValueError as exc:
        assert "must be a list" in str(exc)
    else:
        raise AssertionError("invalid Coinbase trade payload did not fail closed")


def test_trade_flow_bulk_persistence_is_idempotent_and_bounded(tmp_path):
    store = EvidenceStore(tmp_path / "trade-flow.sqlite")
    coverage = SourceCoveragePlane(store)
    observed_at = datetime(2026, 8, 22, 21, 0, tzinfo=timezone.utc)
    observations = [
        SourceEventObservation(
            event_id=f"trade-{index}",
            lane_id="microstructure",
            source_id="public-trade-flow",
            event_type="public_trade",
            event_at=observed_at,
            observed_at=observed_at,
            asset="BTC",
            payload={"index": index},
        )
        for index in range(600)
    ]

    assert _persist_trade_events_bulk(coverage, observations) == 600
    assert _persist_trade_events_bulk(coverage, observations) == 0

    with store.engine.connect() as db:
        count = db.execute(select(func.count()).select_from(coverage.events.rows)).scalar_one()
    assert count == 600

from datetime import datetime, timezone

from inefficiency_engine.coinbase_trade_flow import parse_coinbase_product_trades


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

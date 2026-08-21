from __future__ import annotations

from datetime import timezone

from inefficiency_engine.priority_source_parsers import (
    AAVE_LIQUIDATION_TOPIC,
    parse_aave_liquidation_log,
    parse_bybit_liquidation_message,
    parse_bybit_option_symbol,
    parse_morpho_markets,
    parse_okx_option_symbol,
    parse_snapshot_proposals,
)


def test_parse_bybit_liquidation_message():
    rows = parse_bybit_liquidation_message({
        "topic": "allLiquidation.BTCUSDT",
        "data": [{"T": 1787267000000, "s": "BTCUSDT", "S": "Sell", "v": "0.25", "p": "64000"}],
    })
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["quantity"] == 0.25
    assert rows[0]["price"] == 64000.0
    assert rows[0]["event_at"].tzinfo == timezone.utc


def test_parse_aave_liquidation_log_keeps_raw_economics_without_inference():
    address = lambda suffix: "0x" + "0" * 24 + suffix.lower().replace("0x", "")
    words = [
        f"{1000:064x}",
        f"{2500:064x}",
        "0" * 24 + "44" * 20,
        f"{1:064x}",
    ]
    row = parse_aave_liquidation_log({
        "topics": [AAVE_LIQUIDATION_TOPIC, address("0x" + "11" * 20), address("0x" + "22" * 20), address("0x" + "33" * 20)],
        "data": "0x" + "".join(words),
        "transactionHash": "0xabc",
        "logIndex": "0x1",
        "blockNumber": "0x10",
    })
    assert row is not None
    assert row["debt_to_cover_raw"] == 1000
    assert row["liquidated_collateral_raw"] == 2500
    assert row["receive_a_token"] is True
    assert "capture_probability" not in row
    assert "settlement_probability" not in row


def test_snapshot_parser_preserves_timestamped_identity():
    rows = parse_snapshot_proposals({"data":{"proposals":[{
        "id":"proposal-1","title":"Upgrade","state":"active","created":1787260000,"start":1787260500,
        "space":{"id":"aave.eth","symbol":"AAVE"},
    }]}})
    assert rows[0]["id"] == "proposal-1"
    assert rows[0]["space_symbol"] == "AAVE"
    assert rows[0]["event_at"].tzinfo == timezone.utc


def test_morpho_parser_requires_rate_capacity_and_liquidity():
    rows = parse_morpho_markets({"data":{"markets":{"items":[
        {"uniqueKey":"m1","loanAsset":{"symbol":"USDC"},"state":{"supplyApy":0.05,"supplyAssetsUsd":1000000,"liquidityAssetsUsd":250000}},
        {"uniqueKey":"bad","loanAsset":{"symbol":"DAI"},"state":{"supplyApy":0.04,"supplyAssetsUsd":1000000,"liquidityAssetsUsd":0}},
    ]}}})
    assert rows == [{"market_id":"m1","asset":"USDC","supply_apy":0.05,"supply_usd":1000000.0,"liquidity_usd":250000.0}]


def test_option_symbol_parsers_cover_bybit_and_okx():
    bybit = parse_bybit_option_symbol("BTC-30AUG26-65000-C")
    okx = parse_okx_option_symbol("ETH-USD-260830-4000-P")
    assert bybit is not None and bybit[0] == "BTC" and bybit[2] == 65000 and bybit[3] == "call"
    assert okx is not None and okx[0] == "ETH" and okx[2] == 4000 and okx[3] == "put"

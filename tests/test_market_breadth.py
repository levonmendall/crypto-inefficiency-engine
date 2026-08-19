from datetime import datetime, timedelta, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.futures_basis import FuturesBasisDetector
from inefficiency_engine.detectors.spot_dislocation import CexSpotDislocationDetector
from inefficiency_engine.evidence import ProviderStatus
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import MarketKind, MarketQuote, Opportunity, OpportunityLeg, OrderBookLevel, OrderBookSnapshot, Side, Strategy
from inefficiency_engine.service import _books_for_opportunity, _provider_failure_affects


NOW = datetime(2026, 8, 19, 0, 20, tzinfo=timezone.utc)


def cfg(**overrides):
    values = dict(
        min_net_annualized_return=0.0,
        pair_roundtrip_cost_bps=0.0,
        safety_buffer_bps_per_hour=0.0,
        collateral_opportunity_cost_annual=0.0,
        coinbase_spot_taker_fee_bps=0.0,
        kraken_spot_taker_fee_bps=0.0,
        bybit_spot_taker_fee_bps=0.0,
        bybit_derivatives_taker_fee_bps=0.0,
        hedge_recovery_buffer_bps=0.0,
        latency_risk_bps_per_second=0.0,
    )
    values.update(overrides)
    return Settings(**values)


def test_futures_basis_detects_same_quote_cash_and_carry():
    expiry = NOW + timedelta(days=30)
    quotes = [
        MarketQuote(
            venue="Bybit", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTCUSDT",
            quote_currency="USDT", contract_key="spot", bid=99990, ask=100000, mid=99995,
            observed_at=NOW, source="spot",
        ),
        MarketQuote(
            venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE, symbol="BTCUSDT-18SEP26",
            quote_currency="USDT", contract_key="expiry-test", expires_at=expiry,
            bid=102000, ask=102010, mid=102005, observed_at=NOW, source="future",
        ),
    ]

    opportunities = FuturesBasisDetector(cfg()).detect(quotes)

    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity.strategy == Strategy.FUTURES_BASIS
    assert opportunity.legs[1].contract_key == "expiry-test"
    assert opportunity.holding_hours == 30 * 24


def test_futures_basis_rejects_quote_currency_mismatch():
    expiry = NOW + timedelta(days=30)
    quotes = [
        MarketQuote(venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTC-USD", quote_currency="USD", mid=100000, observed_at=NOW, source="spot"),
        MarketQuote(venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE, symbol="BTCUSDT-X", quote_currency="USDT", contract_key="x", expires_at=expiry, mid=103000, observed_at=NOW, source="future"),
    ]
    assert FuturesBasisDetector(cfg()).detect(quotes) == []


def test_cex_spot_dislocation_requires_same_quote_and_fails_closed_without_borrow():
    quotes = [
        MarketQuote(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD", quote_currency="USD", bid=99, ask=100, mid=99.5, observed_at=NOW, source="coinbase"),
        MarketQuote(venue="Kraken", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH/USD", quote_currency="USD", bid=103, ask=104, mid=103.5, observed_at=NOW, source="kraken"),
        MarketQuote(venue="Bybit", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETHUSDT", quote_currency="USDT", bid=110, ask=111, mid=110.5, observed_at=NOW, source="bybit"),
    ]
    detector = CexSpotDislocationDetector(cfg())
    opportunities = detector.detect(quotes)
    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert [leg.venue for leg in opportunity.legs] == ["Coinbase", "Kraken"]

    books = [
        OrderBookSnapshot(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD", quote_currency="USD", contract_key="spot", bids=[OrderBookLevel(price=99, size=100)], asks=[OrderBookLevel(price=100, size=100)], observed_at=NOW, source="fixture"),
        OrderBookSnapshot(venue="Kraken", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH/USD", quote_currency="USD", contract_key="spot", bids=[OrderBookLevel(price=103, size=100)], asks=[OrderBookLevel(price=104, size=100)], observed_at=NOW, source="fixture"),
    ]
    tier = qualify_opportunity(opportunity, books, cfg(), notionals_usd=(1000.0,), now=NOW).tiers[0]
    assert tier.executable is False
    assert "spot short borrow cost unavailable" in (tier.rejection_reason or "")


def test_contract_specific_book_selection_keeps_futures_separate():
    expiry_a = NOW + timedelta(days=30)
    expiry_b = NOW + timedelta(days=90)
    opportunity = Opportunity(
        id="future-op", strategy=Strategy.FUTURES_BASIS, asset="BTC",
        legs=[
            OpportunityLeg(venue="Bybit", asset="BTC", market_kind=MarketKind.SPOT, side=Side.LONG, symbol="BTCUSDT", quote_currency="USDT", contract_key="spot"),
            OpportunityLeg(venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE, side=Side.SHORT, symbol="BTCUSDT-A", quote_currency="USDT", contract_key="expiry-a", expires_at=expiry_a),
        ],
        gross_edge_bps_per_hour=1, modeled_cost_bps=0, holding_hours=720,
        safety_buffer_bps_per_hour=0, net_edge_bps_per_hour=1, net_annualized_return=0.5,
        observed_at=NOW, expires_at=NOW + timedelta(minutes=1),
    )
    books = [
        OrderBookSnapshot(venue="Bybit", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTCUSDT", quote_currency="USDT", contract_key="spot", bids=[OrderBookLevel(price=99900, size=10)], asks=[OrderBookLevel(price=100000, size=10)], observed_at=NOW, source="fixture"),
        OrderBookSnapshot(venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE, symbol="BTCUSDT-A", quote_currency="USDT", contract_key="expiry-a", expires_at=expiry_a, bids=[OrderBookLevel(price=102000, size=10)], asks=[OrderBookLevel(price=102100, size=10)], observed_at=NOW, source="fixture"),
        OrderBookSnapshot(venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE, symbol="BTCUSDT-B", quote_currency="USDT", contract_key="expiry-b", expires_at=expiry_b, bids=[OrderBookLevel(price=120000, size=10)], asks=[OrderBookLevel(price=120100, size=10)], observed_at=NOW, source="fixture"),
    ]

    selected = _books_for_opportunity(opportunity, books)
    assert [book.symbol for book in selected] == ["BTCUSDT", "BTCUSDT-A"]


def test_provider_failure_only_poison_opportunities_using_that_venue():
    coinbase_hl = Opportunity(
        id="a", strategy=Strategy.SPOT_PERP_BASIS, asset="BTC",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="BTC", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=1, modeled_cost_bps=0, holding_hours=1, safety_buffer_bps_per_hour=0,
        net_edge_bps_per_hour=1, net_annualized_return=1, observed_at=NOW, expires_at=NOW + timedelta(minutes=1),
    )
    bybit = coinbase_hl.model_copy(update={
        "id": "b",
        "legs": [
            OpportunityLeg(venue="Bybit", asset="BTC", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE, side=Side.SHORT, contract_key="expiry-a", expires_at=NOW + timedelta(days=30)),
        ],
    })
    statuses = [ProviderStatus(provider="bybit-v5:market-snapshot", ok=False, error_type="TimeoutError")]
    assert _provider_failure_affects(coinbase_hl, statuses) is False
    assert _provider_failure_affects(bybit, statuses) is True

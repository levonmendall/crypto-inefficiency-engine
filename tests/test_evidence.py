from datetime import datetime, timezone

from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.models import FundingQuote
from inefficiency_engine.replay import replay_scan
from inefficiency_engine.service import OpportunityService


def test_append_only_scan_persistence_and_replay(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.sqlite3")
    service = OpportunityService(evidence_store=store)
    now = datetime.now(timezone.utc)
    funding = [
        FundingQuote(venue="A", asset="BTC", rate=-0.0004, interval_hours=8, observed_at=now, source="test"),
        FundingQuote(venue="B", asset="BTC", rate=0.0012, interval_hours=8, observed_at=now, source="test"),
    ]
    opportunities = service.analyze(funding, [])
    providers = [ProviderStatus(provider="test", ok=True, item_count=2, observed_at=now)]

    first = store.record_scan(
        funding_quotes=funding,
        market_quotes=[],
        opportunities=opportunities,
        providers=providers,
        started_at=now,
        completed_at=now,
    )
    second = store.record_scan(
        funding_quotes=funding,
        market_quotes=[],
        opportunities=opportunities,
        providers=providers,
        started_at=now,
        completed_at=now,
    )

    assert first != second
    counts = store.counts()
    assert counts.scans == 2
    assert counts.funding_quotes == 4
    assert counts.provider_statuses == 2
    loaded = store.load_scan(first)
    assert [q.venue for q in loaded.funding_quotes] == ["A", "B"]
    assert replay_scan(store, service, first).deterministic_match is True


def test_execution_evidence_round_trip_and_replay(tmp_path):
    from inefficiency_engine.config import Settings
    from inefficiency_engine.execution import qualify_opportunity
    from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot

    store = EvidenceStore(tmp_path / "execution-evidence.sqlite3")
    settings = Settings(
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        max_order_book_skew_seconds=2.0,
    )
    service = OpportunityService(settings=settings, evidence_store=store)
    now = datetime.now(timezone.utc)
    market = [
        MarketQuote(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD", mid=100.0, observed_at=now, source="test"),
        MarketQuote(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH", mid=102.0, observed_at=now, source="test"),
    ]
    opportunities = service.analyze([], market)
    books = [
        OrderBookSnapshot(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
            bids=[OrderBookLevel(price=99.9, size=100)], asks=[OrderBookLevel(price=100.0, size=100)], observed_at=now, source="test",
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
            bids=[OrderBookLevel(price=102.0, size=100)], asks=[OrderBookLevel(price=102.1, size=100)], observed_at=now, source="test",
        ),
    ]
    executability = [qualify_opportunity(opportunities[0], books, settings, now=now)]
    scan_id = store.record_scan(
        funding_quotes=[],
        market_quotes=market,
        opportunities=opportunities,
        providers=[ProviderStatus(provider="test", ok=True, item_count=2, observed_at=now)],
        started_at=now,
        completed_at=now,
        analysis_config=__import__("dataclasses").asdict(settings),
        order_books=books,
        executability=executability,
    )

    loaded = store.load_scan(scan_id)
    assert len(loaded.order_books) == 2
    assert len(loaded.executability) == 1
    assert store.counts().order_books == 2
    assert store.counts().executability == 1
    result = replay_scan(store, service, scan_id)
    assert result.deterministic_match is True
    assert result.execution_deterministic_match is True

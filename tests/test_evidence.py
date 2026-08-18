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

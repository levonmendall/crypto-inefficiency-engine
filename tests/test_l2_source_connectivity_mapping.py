from datetime import datetime, timezone

from inefficiency_engine.dashboard_source_connectivity import read_source_connectivity
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus


def test_l2_provider_statuses_make_source_connectivity_observable(tmp_path):
    store = EvidenceStore(tmp_path / "l2-connectivity.sqlite")
    observed_at = datetime(2026, 8, 23, 6, 30, tzinfo=timezone.utc)
    store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[],
        providers=[
            ProviderStatus(
                provider="coinbase-exchange:book-level2:BTC",
                ok=True,
                item_count=1,
                observed_at=observed_at,
            ),
            ProviderStatus(
                provider="okx-v5:market:books:BTC-USDT-SWAP",
                ok=True,
                item_count=1,
                observed_at=observed_at,
            ),
            ProviderStatus(
                provider="hyperliquid:l2Book:BTC",
                ok=True,
                item_count=1,
                observed_at=observed_at,
            ),
        ],
        started_at=observed_at,
        completed_at=observed_at,
        order_books=[],
    )

    payload = read_source_connectivity(store, now=observed_at)
    rows = {row["source_id"]: row for row in payload["sources"]}

    assert rows["coinbase-l2"]["state"] == "healthy"
    assert rows["okx-l2"]["state"] == "healthy"
    assert rows["hyperliquid-l2"]["state"] == "healthy"
    assert rows["coinbase-l2"]["source_reference"].startswith("provider_status:")

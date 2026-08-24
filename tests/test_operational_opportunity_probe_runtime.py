from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import event, insert, inspect

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, Opportunity, OpportunityLeg, Side, Strategy
from inefficiency_engine.operational_source_probe_runtime import (
    _current_shadow_opportunity_candidate,
    install_current_source_scan_probe_runtime,
)
from inefficiency_engine.source_coverage import SourceCoveragePlane


def _opportunity(*, opportunity_id: str, observed_at: datetime) -> Opportunity:
    return Opportunity(
        id=opportunity_id,
        strategy=Strategy.CEX_SPOT_DISLOCATION,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue="Coinbase",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.LONG,
                symbol="BTC-USD",
                reference_price=100.0,
            ),
            OpportunityLeg(
                venue="Kraken",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.SHORT,
                symbol="XBT/USD",
                reference_price=100.2,
            ),
        ],
        gross_edge_bps_per_hour=20.0,
        modeled_cost_bps=2.0,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=1.0,
        net_edge_bps_per_hour=17.0,
        net_annualized_return=0.25,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=5),
        paper_only=True,
    )


def _scan(
    store: EvidenceStore,
    *,
    observed_at: datetime,
    opportunity: Opportunity | None,
) -> str:
    return store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[opportunity] if opportunity is not None else [],
        providers=[],
        order_books=[],
        executability=[],
        started_at=observed_at,
        completed_at=observed_at + timedelta(milliseconds=1),
    )


def _shadow_boundary(
    store: EvidenceStore,
    *,
    cycle_id: str,
    verification_scan_id: str,
    completed_at: datetime,
) -> None:
    with store.engine.begin() as db:
        db.execute(
            insert(store.shadow_cycles),
            {
                "cycle_id": cycle_id,
                "started_at": (completed_at - timedelta(seconds=1)).isoformat(),
                "completed_at": completed_at.isoformat(),
                "initial_scan_id": verification_scan_id,
                "verification_scan_id": verification_scan_id,
                "payload_json": "{}",
                "lineage_hash": "0" * 64,
            },
        )


def _spec() -> dict[str, object]:
    return {
        "id": "internal-opportunity-history",
        "name": "Durable opportunity history",
        "classes": ["venue_opportunity_history"],
        "group": "internal-opportunities",
        "tier": "internal",
        "table": ("opportunities", None, None),
    }


def test_opportunity_probe_reads_only_latest_completed_shadow_verification_scan(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc)

    old_scan = _scan(
        store,
        observed_at=base,
        opportunity=_opportunity(opportunity_id="old", observed_at=base),
    )
    authoritative_at = base + timedelta(minutes=2)
    authoritative_scan = _scan(
        store,
        observed_at=authoritative_at,
        opportunity=_opportunity(
            opportunity_id="authoritative",
            observed_at=authoritative_at,
        ),
    )
    # Later durable opportunity history that is not the verification scan of a
    # completed shadow cycle must not replace the stable operational boundary.
    _scan(
        store,
        observed_at=base + timedelta(minutes=10),
        opportunity=_opportunity(
            opportunity_id="orphan-later-history",
            observed_at=base + timedelta(minutes=10),
        ),
    )
    _shadow_boundary(
        store,
        cycle_id="cycle-old",
        verification_scan_id=old_scan,
        completed_at=base + timedelta(minutes=1),
    )
    _shadow_boundary(
        store,
        cycle_id="cycle-current",
        verification_scan_id=authoritative_scan,
        completed_at=base + timedelta(minutes=3),
    )

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(str(statement).lower())

    plane = SourceCoveragePlane(store)
    available = set(inspect(store.engine).get_table_names())
    event.listen(store.engine, "before_cursor_execute", capture)
    try:
        candidate = _current_shadow_opportunity_candidate(plane, _spec(), available)
        statement_count_after_first_read = len(statements)
        cached_candidate = _current_shadow_opportunity_candidate(plane, _spec(), available)
    finally:
        event.remove(store.engine, "before_cursor_execute", capture)

    assert candidate is not None
    assert candidate["observed_at"] == authoritative_at.isoformat()
    assert authoritative_scan in str(candidate["source_reference"])
    assert cached_candidate == candidate
    assert len(statements) == statement_count_after_first_read

    shadow_queries = [sql for sql in statements if "shadow_cycles" in sql and "select" in sql]
    assert shadow_queries
    assert all("order by shadow_cycles.completed_at desc" in sql for sql in shadow_queries)
    assert all("limit" in sql for sql in shadow_queries)

    opportunity_queries = [
        sql for sql in statements if "opportunities" in sql and "select" in sql
    ]
    assert opportunity_queries
    assert all("opportunities.scan_id =" in sql for sql in opportunity_queries)
    assert all("order by opportunities.observed_at" not in sql for sql in opportunity_queries)


def test_latest_completed_shadow_scan_without_opportunities_fails_closed(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc)

    historical_scan = _scan(
        store,
        observed_at=base,
        opportunity=_opportunity(opportunity_id="historical", observed_at=base),
    )
    _shadow_boundary(
        store,
        cycle_id="cycle-historical",
        verification_scan_id=historical_scan,
        completed_at=base + timedelta(minutes=1),
    )
    empty_current_scan = _scan(
        store,
        observed_at=base + timedelta(minutes=2),
        opportunity=None,
    )
    _shadow_boundary(
        store,
        cycle_id="cycle-empty-current",
        verification_scan_id=empty_current_scan,
        completed_at=base + timedelta(minutes=3),
    )

    candidate = _current_shadow_opportunity_candidate(
        SourceCoveragePlane(store),
        _spec(),
        set(inspect(store.engine).get_table_names()),
    )

    assert candidate is None


def test_operational_install_routes_opportunity_history_to_bounded_shadow_probe(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    base = datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc)
    scan_id = _scan(
        store,
        observed_at=base,
        opportunity=_opportunity(opportunity_id="current", observed_at=base),
    )
    _shadow_boundary(
        store,
        cycle_id="cycle-current",
        verification_scan_id=scan_id,
        completed_at=base + timedelta(minutes=1),
    )

    install_current_source_scan_probe_runtime()
    plane = SourceCoveragePlane(store)
    candidate = plane._table_candidate(
        _spec(),
        set(inspect(store.engine).get_table_names()),
    )

    assert candidate is not None
    assert scan_id in str(candidate["source_reference"])

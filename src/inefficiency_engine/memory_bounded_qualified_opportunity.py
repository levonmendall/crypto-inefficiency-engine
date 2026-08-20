from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import select

from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityExecutability,
    OrderBookSnapshot,
)
from inefficiency_engine.qualified_opportunity import QualifiedOpportunityBridgePublisher


class MemoryBoundedQualifiedOpportunityBridgePublisher(QualifiedOpportunityBridgePublisher):
    """Qualified-opportunity publisher that never reconstructs a full research scan.

    Full discovery remains durably persisted, but canonical bridge projection only
    needs: current spot/perpetual quotes for alpha discovery, the already-bounded L2
    working set, bounded executability rows, and the exact opportunities referenced
    by those executability rows. Provider rows, funding rows, unqualified discovery
    rows, and unrelated opportunities stay on disk.
    """

    def _latest_scan(self) -> ScanSnapshot | None:
        with self.store.engine.connect() as db:
            scan = db.execute(
                select(
                    self.store.scans.c.scan_id,
                    self.store.scans.c.started_at,
                    self.store.scans.c.completed_at,
                    self.store.scans.c.analysis_config_json,
                )
                .order_by(self.store.scans.c.completed_at.desc())
                .limit(1)
            ).mappings().first()
            if scan is None:
                return None

            scan_id = str(scan["scan_id"])
            executability = [
                OpportunityExecutability.model_validate_json(payload)
                for payload in db.execution_options(stream_results=True).execute(
                    select(self.store.executability.c.payload_json)
                    .where(self.store.executability.c.scan_id == scan_id)
                    .order_by(self.store.executability.c.id)
                ).scalars()
            ]
            opportunity_ids = sorted({item.opportunity_id for item in executability})
            opportunities: list[Opportunity] = []
            if opportunity_ids:
                opportunities = [
                    Opportunity.model_validate_json(payload)
                    for payload in db.execution_options(stream_results=True).execute(
                        select(self.store.opportunities.c.payload_json)
                        .where(self.store.opportunities.c.scan_id == scan_id)
                        .where(self.store.opportunities.c.opportunity_id.in_(opportunity_ids))
                        .order_by(self.store.opportunities.c.id)
                    ).scalars()
                ]

            market_quotes: list[MarketQuote] = []
            for payload in db.execution_options(stream_results=True).execute(
                select(self.store.market_quotes.c.payload_json)
                .where(self.store.market_quotes.c.scan_id == scan_id)
                .order_by(self.store.market_quotes.c.id)
            ).scalars():
                quote = MarketQuote.model_validate_json(payload)
                if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                    market_quotes.append(quote)

            order_books: list[OrderBookSnapshot] = []
            for payload in db.execution_options(stream_results=True).execute(
                select(self.store.order_books.c.payload_json)
                .where(self.store.order_books.c.scan_id == scan_id)
                .order_by(self.store.order_books.c.id)
            ).scalars():
                book = OrderBookSnapshot.model_validate_json(payload)
                if book.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                    order_books.append(book)

        analysis_raw = scan["analysis_config_json"] or "{}"
        analysis_config = json.loads(analysis_raw) if isinstance(analysis_raw, str) else {}
        return ScanSnapshot(
            scan_id=scan_id,
            started_at=datetime.fromisoformat(str(scan["started_at"])),
            completed_at=datetime.fromisoformat(str(scan["completed_at"])),
            providers=[],
            funding_quotes=[],
            market_quotes=market_quotes,
            opportunities=opportunities,
            order_books=order_books,
            executability=executability,
            analysis_config=analysis_config,
        )

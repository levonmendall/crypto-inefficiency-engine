from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import select

from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import (
    FundingQuote,
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityExecutability,
    OrderBookSnapshot,
)
from inefficiency_engine.qualified_opportunity import QualifiedOpportunityBridgePublisher
from inefficiency_engine.service import _books_for_opportunity


class MemoryBoundedQualifiedOpportunityBridgePublisher(QualifiedOpportunityBridgePublisher):
    """Project the newest durable full market/L2 scan into canonical candidates.

    Production persists several scan roles into the same append-only ledger. In
    particular, the bounded alpha L2 sampler intentionally writes L2-only scans with
    no market quotes, funding, opportunities, or executability. Treating the newest
    row in ``scans`` as a complete decision snapshot therefore allowed an L2-only
    maintenance write to erase the canonical bridge input every cycle.

    The bridge now prefers the newest permanent-source scan, explicitly skips L2-only
    maintenance rows, and reconstructs only the bounded executability projection
    needed by the canonical portfolio when the source scan has not already persisted
    it. If the permanent owner is absent, a complete generic executable scan remains
    a fail-safe fallback. No provider request is made here: all inputs come from the
    durable source ledger. Economic, cost, statistical, freshness, risk, settlement,
    and paper-only gates remain unchanged.
    """

    _SCAN_LOOKBACK = 200

    @staticmethod
    def _analysis_config(raw: object) -> dict[str, object]:
        if not isinstance(raw, str) or not raw:
            return {}
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _row_has_market_quote(db, store, scan_id: str) -> bool:
        return db.execute(
            select(store.market_quotes.c.id)
            .where(store.market_quotes.c.scan_id == scan_id)
            .limit(1)
        ).scalar_one_or_none() is not None

    @staticmethod
    def _row_has_depth_or_executability(db, store, scan_id: str) -> bool:
        has_book = db.execute(
            select(store.order_books.c.id)
            .where(store.order_books.c.scan_id == scan_id)
            .limit(1)
        ).scalar_one_or_none()
        if has_book is not None:
            return True
        return db.execute(
            select(store.executability.c.id)
            .where(store.executability.c.scan_id == scan_id)
            .limit(1)
        ).scalar_one_or_none() is not None

    def _select_full_scan(self, db):
        rows = list(
            db.execute(
                select(
                    self.store.scans.c.scan_id,
                    self.store.scans.c.started_at,
                    self.store.scans.c.completed_at,
                    self.store.scans.c.analysis_config_json,
                )
                .order_by(self.store.scans.c.completed_at.desc())
                .limit(self._SCAN_LOOKBACK)
            ).mappings()
        )

        parsed = [
            (row, self._analysis_config(row["analysis_config_json"]))
            for row in rows
        ]

        # The permanent source process is the canonical owner of current market/L2
        # truth. Prefer its newest full quote snapshot even when the L2 set is empty:
        # an empty current depth set is a real fail-closed condition and must not be
        # replaced by older depth merely to manufacture an opportunity.
        for row, config in parsed:
            if not bool(config.get("permanent_source_plane")):
                continue
            scan_id = str(row["scan_id"])
            if self._row_has_market_quote(db, self.store, scan_id):
                return row, config

        # Source ownership may be unavailable during fail-safe recovery. In that
        # case accept the newest generic scan only when it contains both quote truth
        # and enough persisted depth/executability to support a real bridge decision.
        for row, config in parsed:
            if bool(config.get("alpha_l2_sampling")):
                continue
            scan_id = str(row["scan_id"])
            if not self._row_has_market_quote(db, self.store, scan_id):
                continue
            if self._row_has_depth_or_executability(db, self.store, scan_id):
                return row, config
        return None, {}

    def _latest_scan(self) -> ScanSnapshot | None:
        with self.store.engine.connect() as db:
            scan, analysis_config = self._select_full_scan(db)
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

            funding_quotes = [
                FundingQuote.model_validate_json(payload)
                for payload in db.execution_options(stream_results=True).execute(
                    select(self.store.funding_quotes.c.payload_json)
                    .where(self.store.funding_quotes.c.scan_id == scan_id)
                    .order_by(self.store.funding_quotes.c.id)
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

        synthesized = False
        if not executability:
            # The permanent source plane is intentionally an acquisition plane, so
            # it persists fresh quotes/funding/L2 without granting portfolio
            # authority. Reconstruct the same deterministic structural opportunity
            # and executability projection here from that already-persisted evidence.
            # No external provider work is performed and every existing hurdle still
            # lives inside ``analyze`` / ``qualify_opportunity``.
            opportunities = self.core.analyze(funding_quotes, market_quotes)
            latency_resolver = self.core.empirical_latency_resolver()
            qualification_time = datetime.now(timezone.utc)
            executability = [
                qualify_opportunity(
                    opportunity,
                    _books_for_opportunity(opportunity, order_books),
                    self.core.settings,
                    notionals_usd=self.core.settings.capital_tiers_usd,
                    now=qualification_time,
                    latency_model_resolver=latency_resolver.resolve,
                )
                for opportunity in opportunities
            ]
            synthesized = True

        analysis_config = {
            **analysis_config,
            "canonical_bridge_source_scan": True,
            "bridge_projection_from_durable_evidence": True,
            "bridge_projection_synthesized_executability": synthesized,
            "bridge_projection_opportunity_count": len(opportunities),
            "bridge_projection_executability_count": len(executability),
            "bridge_projection_provider_requests": 0,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }
        return ScanSnapshot(
            scan_id=scan_id,
            started_at=datetime.fromisoformat(str(scan["started_at"])),
            completed_at=datetime.fromisoformat(str(scan["completed_at"])),
            providers=[],
            funding_quotes=funding_quotes,
            market_quotes=market_quotes,
            opportunities=opportunities,
            order_books=order_books,
            executability=executability,
            analysis_config=analysis_config,
        )

from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperAllocatorService


CANONICAL_PORTFOLIO_ID = "crypto-opportunity-engine-paper-portfolio"
CANONICAL_INITIAL_CAPITAL_USD = 250_000.0
PortfolioEventType = Literal["genesis", "open", "mark", "close", "skip"]
ValuationStatus = Literal["cash_only", "fresh", "partial", "stale", "unavailable"]
PortfolioCycleStatus = Literal["accounting_only", "success", "degraded", "failed"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: BaseModel) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


class CanonicalPortfolioEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    portfolio_id: str = CANONICAL_PORTFOLIO_ID
    event_type: PortfolioEventType
    observed_at: datetime = Field(default_factory=_now)
    position_id: str | None = None
    candidate_id: str | None = None
    family: str | None = None
    strategy: str | None = None
    asset: str | None = None
    venue: str | None = None
    symbol: str | None = None
    market_kind: str | None = None
    exposure_kind: str | None = None
    cash_delta_usd: float = 0.0
    realized_pnl_delta_usd: float = 0.0
    modeled_cost_usd: float = 0.0
    capital_reserved_usd: float = 0.0
    notional_usd: float = 0.0
    entry_reference_price: float | None = Field(default=None, gt=0)
    reference_price: float | None = Field(default=None, gt=0)
    due_at: datetime | None = None
    modeled_roundtrip_cost_return: float | None = Field(default=None, ge=0)
    unrealized_pnl_usd: float = 0.0
    reason: str | None = None
    details: dict[str, object] = Field(default_factory=dict)
    paper_only: bool = True
    live_execution_authority: bool = False


class CanonicalPaperPosition(BaseModel):
    position_id: str
    candidate_id: str
    family: str
    strategy: str
    asset: str
    venue: str
    symbol: str
    market_kind: str
    exposure_kind: str
    opened_at: datetime
    due_at: datetime
    capital_reserved_usd: float = Field(gt=0)
    notional_usd: float = Field(gt=0)
    entry_reference_price: float = Field(gt=0)
    current_reference_price: float = Field(gt=0)
    valuation_observed_at: datetime | None = None
    modeled_roundtrip_cost_return: float = Field(ge=0)
    unrealized_pnl_usd: float
    paper_only: bool = True


class CanonicalPaperPortfolioSnapshot(BaseModel):
    portfolio_id: str = CANONICAL_PORTFOLIO_ID
    observed_at: datetime
    market_evidence_observed_at: datetime | None = None
    valuation_status: ValuationStatus = "unavailable"
    cycle_status: PortfolioCycleStatus = "accounting_only"
    fallback_snapshot: bool = False
    cycle_error_type: str | None = None
    allocation_family_failures: dict[str, str] = Field(default_factory=dict)
    stale_position_count: int = Field(default=0, ge=0)
    initial_capital_usd: float = CANONICAL_INITIAL_CAPITAL_USD
    cash_usd: float
    reserved_capital_usd: float = Field(ge=0)
    unrealized_pnl_usd: float
    realized_pnl_usd: float
    cumulative_modeled_cost_usd: float = Field(ge=0)
    nav_usd: float
    total_return: float
    peak_nav_usd: float
    drawdown_fraction: float = Field(ge=0)
    max_drawdown_fraction: float = Field(ge=0)
    open_position_count: int = Field(ge=0)
    closed_trade_count: int = Field(ge=0)
    skipped_allocation_count: int = Field(ge=0)
    positions: list[CanonicalPaperPosition] = Field(default_factory=list)
    pnl_by_mechanism_usd: dict[str, float] = Field(default_factory=dict)
    pnl_by_strategy_usd: dict[str, float] = Field(default_factory=dict)
    paper_only: bool = True
    live_execution_authority: bool = False


class CanonicalPaperPortfolioCycle(BaseModel):
    cycle_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    opened_position_count: int = Field(ge=0)
    closed_position_count: int = Field(ge=0)
    marked_position_count: int = Field(ge=0)
    stale_position_count: int = Field(default=0, ge=0)
    skipped_allocation_count: int = Field(ge=0)
    nav_usd: float
    cash_usd: float
    valuation_status: ValuationStatus = "unavailable"
    degraded: bool = False
    allocation_error_type: str | None = None
    allocation_family_failures: dict[str, str] = Field(default_factory=dict)
    market_snapshot_id: str | None = None
    paper_only: bool = True


class CanonicalPaperPortfolioLedger:
    """Append-only canonical paper account with hash-chained portfolio events."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.events = Table(
            "canonical_paper_portfolio_events",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_id", String(64), nullable=False, unique=True),
            Column("portfolio_id", Text, nullable=False),
            Column("event_type", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("position_id", Text),
            Column("payload_json", Text, nullable=False),
            Column("previous_lineage_hash", String(64)),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.snapshots = Table(
            "canonical_paper_portfolio_snapshots",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("portfolio_id", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_canonical_portfolio_events_time", self.events.c.portfolio_id, self.events.c.observed_at)
        Index("ix_canonical_portfolio_events_position", self.events.c.position_id)
        Index("ix_canonical_portfolio_snapshots_time", self.snapshots.c.portfolio_id, self.snapshots.c.observed_at)
        metadata.create_all(store.engine)

    def _last_event_hash(self) -> str | None:
        with self.store.engine.connect() as db:
            return db.execute(
                select(self.events.c.lineage_hash)
                .where(self.events.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.events.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()

    def record_event(self, event: CanonicalPortfolioEvent) -> str:
        if event.portfolio_id != CANONICAL_PORTFOLIO_ID:
            raise ValueError("only the canonical paper portfolio is supported")
        raw = _json(event)
        previous = self._last_event_hash()
        lineage = hashlib.sha256(f"{previous or ''}|{raw}".encode()).hexdigest()
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.events.c.event_id).where(self.events.c.event_id == event.event_id)
            ).scalar_one_or_none()
            if exists is None:
                db.execute(insert(self.events), {
                    "event_id": event.event_id,
                    "portfolio_id": event.portfolio_id,
                    "event_type": event.event_type,
                    "observed_at": event.observed_at.isoformat(),
                    "position_id": event.position_id,
                    "payload_json": raw,
                    "previous_lineage_hash": previous,
                    "lineage_hash": lineage,
                })
        return event.event_id

    def events_all(self) -> list[CanonicalPortfolioEvent]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.events.c.payload_json)
                .where(self.events.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.events.c.id)
            ).scalars())
        return [CanonicalPortfolioEvent.model_validate_json(payload) for payload in payloads]

    def ensure_genesis(self, *, observed_at: datetime | None = None) -> CanonicalPortfolioEvent:
        rows = self.events_all()
        if rows:
            first = rows[0]
            if first.event_type != "genesis":
                raise RuntimeError("canonical paper portfolio exists without a genesis event")
            initial = float(first.details.get("initial_capital_usd", 0.0))
            if abs(initial - CANONICAL_INITIAL_CAPITAL_USD) > 1e-9:
                raise RuntimeError("canonical paper portfolio genesis capital does not match $250,000 invariant")
            return first
        event = CanonicalPortfolioEvent(
            event_type="genesis",
            observed_at=observed_at or _now(),
            cash_delta_usd=CANONICAL_INITIAL_CAPITAL_USD,
            reason="canonical paper portfolio genesis",
            details={"initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD},
        )
        self.record_event(event)
        return event

    def record_snapshot(self, snapshot: CanonicalPaperPortfolioSnapshot) -> None:
        raw = _json(snapshot)
        lineage = hashlib.sha256(raw.encode()).hexdigest()
        with self.store.engine.begin() as db:
            db.execute(insert(self.snapshots), {
                "portfolio_id": snapshot.portfolio_id,
                "observed_at": snapshot.observed_at.isoformat(),
                "payload_json": raw,
                "lineage_hash": lineage,
            })

    def latest_snapshot(self) -> CanonicalPaperPortfolioSnapshot | None:
        with self.store.engine.connect() as db:
            payload = db.execute(
                select(self.snapshots.c.payload_json)
                .where(self.snapshots.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.snapshots.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        return CanonicalPaperPortfolioSnapshot.model_validate_json(payload) if payload else None

    def snapshot_history(self, *, limit: int = 100) -> list[CanonicalPaperPortfolioSnapshot]:
        limit = max(1, min(1000, int(limit)))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.snapshots.c.payload_json)
                .where(self.snapshots.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.snapshots.c.id.desc())
                .limit(limit)
            ).scalars())
        return [CanonicalPaperPortfolioSnapshot.model_validate_json(payload) for payload in payloads]

    def trade_history(self, *, limit: int = 100) -> list[CanonicalPortfolioEvent]:
        limit = max(1, min(1000, int(limit)))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.events.c.payload_json)
                .where(self.events.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .where(self.events.c.event_type == "close")
                .order_by(self.events.c.id.desc())
                .limit(limit)
            ).scalars())
        return [CanonicalPortfolioEvent.model_validate_json(payload) for payload in payloads]

    def _historical_peak_and_drawdown(self) -> tuple[float, float]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.snapshots.c.payload_json)
                .where(self.snapshots.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .order_by(self.snapshots.c.id)
            ).scalars())
        rows = [CanonicalPaperPortfolioSnapshot.model_validate_json(payload) for payload in payloads]
        peak = max([CANONICAL_INITIAL_CAPITAL_USD, *[row.peak_nav_usd for row in rows]])
        max_drawdown = max([0.0, *[row.max_drawdown_fraction for row in rows]])
        return peak, max_drawdown

    def current_state(self, *, observed_at: datetime | None = None) -> CanonicalPaperPortfolioSnapshot:
        self.ensure_genesis(observed_at=observed_at)
        events = self.events_all()
        opened: dict[str, CanonicalPortfolioEvent] = {}
        closed: dict[str, CanonicalPortfolioEvent] = {}
        latest_mark: dict[str, CanonicalPortfolioEvent] = {}
        cash = 0.0
        realized = 0.0
        costs = 0.0
        skipped = 0
        pnl_mechanism: dict[str, float] = defaultdict(float)
        pnl_strategy: dict[str, float] = defaultdict(float)

        for event in events:
            cash += event.cash_delta_usd
            realized += event.realized_pnl_delta_usd
            costs += max(0.0, event.modeled_cost_usd)
            if event.event_type == "open" and event.position_id:
                opened[event.position_id] = event
            elif event.event_type == "mark" and event.position_id:
                latest_mark[event.position_id] = event
            elif event.event_type == "close" and event.position_id:
                closed[event.position_id] = event
                if event.family:
                    pnl_mechanism[event.family] += event.realized_pnl_delta_usd
                if event.strategy:
                    pnl_strategy[event.strategy] += event.realized_pnl_delta_usd
            elif event.event_type == "skip":
                skipped += 1

        positions: list[CanonicalPaperPosition] = []
        unrealized = 0.0
        for position_id, event in opened.items():
            if position_id in closed:
                continue
            if not all([event.candidate_id, event.family, event.strategy, event.asset, event.venue, event.symbol, event.market_kind, event.exposure_kind]):
                continue
            if event.entry_reference_price is None or event.due_at is None or event.modeled_roundtrip_cost_return is None:
                continue
            mark = latest_mark.get(position_id, event)
            current = mark.reference_price or event.entry_reference_price
            pnl = mark.unrealized_pnl_usd
            unrealized += pnl
            pnl_mechanism[event.family] += pnl
            pnl_strategy[event.strategy] += pnl
            positions.append(CanonicalPaperPosition(
                position_id=position_id,
                candidate_id=event.candidate_id,
                family=event.family,
                strategy=event.strategy,
                asset=event.asset,
                venue=event.venue,
                symbol=event.symbol,
                market_kind=event.market_kind,
                exposure_kind=event.exposure_kind,
                opened_at=event.observed_at,
                due_at=event.due_at,
                capital_reserved_usd=event.capital_reserved_usd,
                notional_usd=event.notional_usd,
                entry_reference_price=event.entry_reference_price,
                current_reference_price=current,
                valuation_observed_at=mark.observed_at,
                modeled_roundtrip_cost_return=event.modeled_roundtrip_cost_return,
                unrealized_pnl_usd=pnl,
            ))

        reserved = sum(position.capital_reserved_usd for position in positions)
        nav = cash + reserved + unrealized
        historical_peak, historical_max_dd = self._historical_peak_and_drawdown()
        peak = max(historical_peak, nav)
        drawdown = max(0.0, (peak - nav) / peak) if peak > 0 else 0.0
        max_drawdown = max(historical_max_dd, drawdown)
        valuation_status: ValuationStatus = "cash_only" if not positions else "stale"
        return CanonicalPaperPortfolioSnapshot(
            observed_at=observed_at or _now(),
            valuation_status=valuation_status,
            cycle_status="accounting_only",
            stale_position_count=len(positions),
            cash_usd=cash,
            reserved_capital_usd=reserved,
            unrealized_pnl_usd=unrealized,
            realized_pnl_usd=realized,
            cumulative_modeled_cost_usd=costs,
            nav_usd=nav,
            total_return=nav / CANONICAL_INITIAL_CAPITAL_USD - 1.0,
            peak_nav_usd=peak,
            drawdown_fraction=drawdown,
            max_drawdown_fraction=max_drawdown,
            open_position_count=len(positions),
            closed_trade_count=len(closed),
            skipped_allocation_count=skipped,
            positions=sorted(positions, key=lambda item: (item.due_at, item.position_id)),
            pnl_by_mechanism_usd=dict(sorted(pnl_mechanism.items())),
            pnl_by_strategy_usd=dict(sorted(pnl_strategy.items())),
        )


class CanonicalPaperPortfolioService:
    """Compounding paper account driven only by already-qualified allocator decisions.

    The portfolio opens only positions whose forward settlement is currently
    defensible: spot directional-long alpha with an exact venue/symbol entry
    reference and a known point-in-time round-trip cost model. Unsupported or
    degraded opportunity families remain fail-closed without freezing accounting
    for the rest of the portfolio.
    """

    def __init__(
        self,
        core,
        allocator: UnifiedPaperAllocatorService,
        store: EvidenceStore,
    ):
        self.core = core
        self.allocator = allocator
        self.store = store
        self.ledger = CanonicalPaperPortfolioLedger(store)

    @staticmethod
    def _quote_index(snapshot: ScanSnapshot) -> dict[tuple[str, str, str, str], MarketQuote]:
        index: dict[tuple[str, str, str, str], MarketQuote] = {}
        for quote in snapshot.market_quotes:
            key = (quote.venue, quote.asset.upper(), quote.market_kind.value, quote.symbol)
            previous = index.get(key)
            if previous is None or quote.observed_at > previous.observed_at:
                index[key] = quote
        return index

    @staticmethod
    def _support_reason(allocation: UnifiedPaperAllocation) -> tuple[bool, str | None]:
        supported = bool(
            allocation.family == "alpha"
            and allocation.exposure_kind == "directional_long"
            and allocation.instrument_market_kind == MarketKind.SPOT.value
            and len(allocation.venues) == 1
            and allocation.instrument_symbol
            and allocation.entry_reference_price is not None
            and allocation.modeled_roundtrip_cost_return is not None
            and allocation.modeled_holding_hours is not None
        )
        if supported:
            return True, None
        if allocation.family != "alpha":
            return False, "multi-leg structural portfolio settlement is not yet supported"
        if allocation.exposure_kind == "directional_short":
            return False, "perpetual short portfolio settlement requires realized funding accrual"
        return False, "allocation lacks complete spot directional settlement metadata"

    @staticmethod
    def _position_key(position: CanonicalPaperPosition) -> tuple[str, str]:
        return position.venue, position.symbol

    def _mark_event(self, position: CanonicalPaperPosition, quote: MarketQuote, *, observed_at: datetime) -> CanonicalPortfolioEvent:
        gross = quote.mid / position.entry_reference_price - 1.0
        net = gross - position.modeled_roundtrip_cost_return
        pnl = position.notional_usd * net
        return CanonicalPortfolioEvent(
            event_type="mark",
            observed_at=observed_at,
            position_id=position.position_id,
            candidate_id=position.candidate_id,
            family=position.family,
            strategy=position.strategy,
            asset=position.asset,
            venue=position.venue,
            symbol=position.symbol,
            market_kind=position.market_kind,
            exposure_kind=position.exposure_kind,
            entry_reference_price=position.entry_reference_price,
            reference_price=quote.mid,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            unrealized_pnl_usd=pnl,
            details={"gross_return": gross, "net_return_after_modeled_roundtrip_cost": net},
        )

    def _close_event(self, position: CanonicalPaperPosition, quote: MarketQuote, *, observed_at: datetime) -> CanonicalPortfolioEvent:
        gross = quote.mid / position.entry_reference_price - 1.0
        net = gross - position.modeled_roundtrip_cost_return
        pnl = position.notional_usd * net
        cost = position.notional_usd * position.modeled_roundtrip_cost_return
        return CanonicalPortfolioEvent(
            event_type="close",
            observed_at=observed_at,
            position_id=position.position_id,
            candidate_id=position.candidate_id,
            family=position.family,
            strategy=position.strategy,
            asset=position.asset,
            venue=position.venue,
            symbol=position.symbol,
            market_kind=position.market_kind,
            exposure_kind=position.exposure_kind,
            cash_delta_usd=position.capital_reserved_usd + pnl,
            realized_pnl_delta_usd=pnl,
            modeled_cost_usd=cost,
            capital_reserved_usd=position.capital_reserved_usd,
            notional_usd=position.notional_usd,
            entry_reference_price=position.entry_reference_price,
            reference_price=quote.mid,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            reason="paper holding horizon matured",
            details={"gross_return": gross, "net_return_after_modeled_roundtrip_cost": net},
        )

    @staticmethod
    def _quote_for(position: CanonicalPaperPosition, quote_index: dict[tuple[str, str, str, str], MarketQuote]) -> MarketQuote | None:
        return quote_index.get((position.venue, position.asset.upper(), position.market_kind, position.symbol))

    async def run_cycle(self) -> CanonicalPaperPortfolioCycle:
        self.ledger.ensure_genesis()
        snapshot = await self.core.collect_live_executability()
        quote_index = self._quote_index(snapshot)
        state = self.ledger.current_state(observed_at=snapshot.completed_at)
        closed = marked = opened = skipped = stale_positions = 0

        for position in state.positions:
            quote = self._quote_for(position, quote_index)
            if quote is None:
                stale_positions += 1
                continue
            if position.due_at <= snapshot.completed_at:
                self.ledger.record_event(self._close_event(position, quote, observed_at=snapshot.completed_at))
                closed += 1
            else:
                self.ledger.record_event(self._mark_event(position, quote, observed_at=snapshot.completed_at))
                marked += 1

        after_close = self.ledger.current_state(observed_at=snapshot.completed_at)
        cash_available = max(0.0, after_close.cash_usd)
        open_keys = {self._position_key(position) for position in after_close.positions}
        allocation_error_type: str | None = None
        allocation_family_failures: dict[str, str] = {}
        plan_degraded = False

        if cash_available > 0:
            try:
                plan = await self.allocator.allocate(total_capital_usd=cash_available)
            except Exception as exc:
                allocation_error_type = type(exc).__name__
                plan = None

            if plan is not None:
                allocation_family_failures = dict(getattr(plan, "family_failures", {}) or {})
                plan_degraded = bool(getattr(plan, "degraded", False))
                remaining_cash = cash_available
                for allocation in plan.allocations:
                    supported, reason = self._support_reason(allocation)
                    venue = allocation.venues[0] if allocation.venues else None
                    symbol = allocation.instrument_symbol
                    if not supported:
                        self.ledger.record_event(CanonicalPortfolioEvent(
                            event_type="skip",
                            observed_at=plan.observed_at,
                            candidate_id=allocation.candidate_id,
                            family=allocation.family,
                            strategy=allocation.strategy,
                            asset=allocation.asset,
                            venue=venue,
                            symbol=symbol,
                            market_kind=allocation.instrument_market_kind,
                            exposure_kind=allocation.exposure_kind,
                            reason=reason,
                        ))
                        skipped += 1
                        continue
                    assert venue is not None and symbol is not None
                    if (venue, symbol) in open_keys:
                        self.ledger.record_event(CanonicalPortfolioEvent(
                            event_type="skip",
                            observed_at=plan.observed_at,
                            candidate_id=allocation.candidate_id,
                            family=allocation.family,
                            strategy=allocation.strategy,
                            asset=allocation.asset,
                            venue=venue,
                            symbol=symbol,
                            market_kind=allocation.instrument_market_kind,
                            exposure_kind=allocation.exposure_kind,
                            reason="canonical portfolio already has an open position in this venue/symbol",
                        ))
                        skipped += 1
                        continue
                    capital = allocation.capital_required_usd
                    if capital > remaining_cash + 1e-9:
                        self.ledger.record_event(CanonicalPortfolioEvent(
                            event_type="skip",
                            observed_at=plan.observed_at,
                            candidate_id=allocation.candidate_id,
                            family=allocation.family,
                            strategy=allocation.strategy,
                            asset=allocation.asset,
                            venue=venue,
                            symbol=symbol,
                            reason="insufficient canonical paper cash after existing open positions",
                        ))
                        skipped += 1
                        continue
                    assert allocation.entry_reference_price is not None
                    assert allocation.modeled_roundtrip_cost_return is not None
                    assert allocation.modeled_holding_hours is not None
                    position_id = uuid.uuid4().hex
                    due_at = (allocation.source_observed_at or plan.observed_at) + timedelta(hours=allocation.modeled_holding_hours)
                    initial_unrealized = -allocation.notional_usd_per_leg * allocation.modeled_roundtrip_cost_return
                    self.ledger.record_event(CanonicalPortfolioEvent(
                        event_type="open",
                        observed_at=plan.observed_at,
                        position_id=position_id,
                        candidate_id=allocation.candidate_id,
                        family=allocation.family,
                        strategy=allocation.strategy,
                        asset=allocation.asset,
                        venue=venue,
                        symbol=symbol,
                        market_kind=allocation.instrument_market_kind,
                        exposure_kind=allocation.exposure_kind,
                        cash_delta_usd=-capital,
                        capital_reserved_usd=capital,
                        notional_usd=allocation.notional_usd_per_leg,
                        entry_reference_price=allocation.entry_reference_price,
                        reference_price=allocation.entry_reference_price,
                        due_at=due_at,
                        modeled_roundtrip_cost_return=allocation.modeled_roundtrip_cost_return,
                        unrealized_pnl_usd=initial_unrealized,
                        reason="qualified allocator decision opened in canonical paper portfolio",
                        details={
                            "expected_profit_usd": allocation.expected_profit_usd_per_deployment,
                            "expected_return_on_reserved_capital": allocation.expected_return_on_reserved_capital,
                            "source_return_metric": allocation.source_return_metric,
                            "source_return_value": allocation.source_return_value,
                        },
                    ))
                    remaining_cash -= capital
                    open_keys.add((venue, symbol))
                    opened += 1

        final_state = self.ledger.current_state(observed_at=snapshot.completed_at)
        if final_state.open_position_count == 0:
            valuation_status: ValuationStatus = "cash_only"
        elif stale_positions == 0:
            valuation_status = "fresh"
        elif stale_positions < final_state.open_position_count:
            valuation_status = "partial"
        else:
            valuation_status = "stale"
        degraded = bool(
            stale_positions
            or allocation_error_type
            or allocation_family_failures
            or plan_degraded
        )
        final_state = final_state.model_copy(update={
            "market_evidence_observed_at": snapshot.completed_at,
            "valuation_status": valuation_status,
            "cycle_status": "degraded" if degraded else "success",
            "fallback_snapshot": False,
            "cycle_error_type": allocation_error_type,
            "allocation_family_failures": allocation_family_failures,
            "stale_position_count": stale_positions,
        })
        self.ledger.record_snapshot(final_state)
        return CanonicalPaperPortfolioCycle(
            observed_at=snapshot.completed_at,
            opened_position_count=opened,
            closed_position_count=closed,
            marked_position_count=marked,
            stale_position_count=stale_positions,
            skipped_allocation_count=skipped,
            nav_usd=final_state.nav_usd,
            cash_usd=final_state.cash_usd,
            valuation_status=valuation_status,
            degraded=degraded,
            allocation_error_type=allocation_error_type,
            allocation_family_failures=allocation_family_failures,
            market_snapshot_id=snapshot.scan_id,
        )

    def performance_summary(self) -> dict[str, object]:
        latest = self.ledger.latest_snapshot()
        history = list(reversed(self.ledger.snapshot_history(limit=1000)))
        if latest is None:
            return {
                "available": False,
                "portfolio_id": CANONICAL_PORTFOLIO_ID,
                "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
                "paper_only": True,
            }
        returns: list[float] = []
        for previous, current in zip(history, history[1:]):
            if previous.nav_usd > 0:
                returns.append(current.nav_usd / previous.nav_usd - 1.0)
        return {
            "available": True,
            "portfolio_id": latest.portfolio_id,
            "initial_capital_usd": latest.initial_capital_usd,
            "current_nav_usd": latest.nav_usd,
            "cash_usd": latest.cash_usd,
            "reserved_capital_usd": latest.reserved_capital_usd,
            "realized_pnl_usd": latest.realized_pnl_usd,
            "unrealized_pnl_usd": latest.unrealized_pnl_usd,
            "total_return": latest.total_return,
            "max_drawdown_fraction": latest.max_drawdown_fraction,
            "open_position_count": latest.open_position_count,
            "closed_trade_count": latest.closed_trade_count,
            "mean_snapshot_return": statistics.fmean(returns) if returns else None,
            "positive_snapshot_rate": sum(value > 0 for value in returns) / len(returns) if returns else None,
            "pnl_by_mechanism_usd": latest.pnl_by_mechanism_usd,
            "pnl_by_strategy_usd": latest.pnl_by_strategy_usd,
            "market_evidence_observed_at": latest.market_evidence_observed_at,
            "valuation_status": latest.valuation_status,
            "cycle_status": latest.cycle_status,
            "fallback_snapshot": latest.fallback_snapshot,
            "cycle_error_type": latest.cycle_error_type,
            "allocation_family_failures": latest.allocation_family_failures,
            "stale_position_count": latest.stale_position_count,
            "paper_only": True,
            "live_execution_authority": False,
        }

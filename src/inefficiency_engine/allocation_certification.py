from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.execution import InsufficientDepthError, estimate_market_order_quantity
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, OrderBookSnapshot, TradeSide
from inefficiency_engine.unified_allocation import (
    PaperSettlementLeg,
    UnifiedPaperAllocation,
    UnifiedPaperAllocatorService,
)


SettlementStatus = Literal["settled"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _payload(value: BaseModel) -> tuple[str, str]:
    raw = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return raw, hashlib.sha256(raw.encode()).hexdigest()


class PaperSettlementLegOutcome(BaseModel):
    venue: str
    asset: str
    market_kind: str
    side: Literal["long", "short"]
    symbol: str
    base_quantity: float = Field(gt=0)
    entry_price: float = Field(gt=0)
    exit_price: float = Field(gt=0)
    entry_notional_usd: float = Field(gt=0)
    exit_notional_usd: float = Field(gt=0)
    price_pnl_usd: float
    funding_pnl_usd: float = 0.0
    funding_event_count: int = Field(default=0, ge=0)
    exit_slippage_bps: float = Field(default=0.0, ge=0)
    exit_levels_consumed: int = Field(default=0, ge=0)
    exit_fill_source: str = "visible_l2"


class PaperAllocationTrial(BaseModel):
    trial_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    plan_observed_at: datetime
    candidate_id: str
    family: str
    strategy: str
    asset: str
    venues: list[str]
    exposure_kind: str
    capital_required_usd: float = Field(gt=0)
    notional_usd: float = Field(gt=0)
    predicted_profit_usd: float = Field(ge=0)
    predicted_return_on_reserved_capital: float = Field(ge=0)
    source_observed_at: datetime | None = None
    due_at: datetime | None = None
    instrument_symbol: str | None = None
    instrument_market_kind: str | None = None
    entry_reference_price: float | None = Field(default=None, gt=0)
    modeled_roundtrip_cost_return: float | None = Field(default=None, ge=0)
    settlement_legs: list[PaperSettlementLeg] = Field(default_factory=list)
    modeled_non_slippage_cost_bps: float | None = Field(default=None, ge=0)
    modeled_safety_buffer_bps: float | None = Field(default=None, ge=0)
    capital_multiple: float | None = Field(default=None, gt=0)
    settlement_supported: bool = False
    settlement_method: str | None = None
    settlement_blocker: str | None = None
    cohort_key: str
    recorded_at: datetime = Field(default_factory=_now)
    live_execution_authority: bool = False
    paper_only: bool = True


class PaperAllocationOutcome(BaseModel):
    outcome_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trial_id: str
    candidate_id: str
    family: str
    strategy: str
    asset: str
    matured_at: datetime
    due_at: datetime
    entry_reference_price: float | None = Field(default=None, gt=0)
    exit_reference_price: float | None = Field(default=None, gt=0)
    realized_gross_return: float
    realized_net_return: float
    realized_profit_usd: float
    realized_price_pnl_usd: float | None = None
    realized_funding_pnl_usd: float = 0.0
    modeled_non_slippage_cost_usd: float = 0.0
    leg_outcomes: list[PaperSettlementLegOutcome] = Field(default_factory=list)
    predicted_profit_usd: float = Field(ge=0)
    prediction_error_usd: float
    profit_capture_ratio: float | None = None
    profitable: bool
    settlement_status: SettlementStatus = "settled"
    settlement_method: str
    settlement_evidence_complete: bool = True
    live_execution_authority: bool = False
    paper_only: bool = True


class AllocationCertificationCycle(BaseModel):
    cycle_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    plan_allocation_count: int = Field(ge=0)
    trials_recorded: int = Field(ge=0)
    supported_trials_recorded: int = Field(ge=0)
    unsupported_trials_recorded: int = Field(ge=0)
    outcomes_matured: int = Field(ge=0)
    paper_only: bool = True


class AllocationCertificationLedger:
    """Append-only allocator-decision and forward-outcome evidence."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.trials = Table(
            "allocation_forward_trials",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("candidate_id", Text, nullable=False),
            Column("family", Text, nullable=False),
            Column("strategy", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("cohort_key", Text, nullable=False),
            Column("plan_observed_at", Text, nullable=False),
            Column("due_at", Text),
            Column("settlement_supported", Boolean, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.outcomes_table = Table(
            "allocation_forward_outcomes",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("outcome_id", String(64), nullable=False, unique=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("strategy", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("matured_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_allocation_trial_due", self.trials.c.due_at)
        Index("ix_allocation_trial_cohort", self.trials.c.cohort_key)
        Index("ix_allocation_outcome_strategy_asset", self.outcomes_table.c.strategy, self.outcomes_table.c.asset)
        metadata.create_all(store.engine)

    def _completed_trial_ids(self) -> set[str]:
        with self.store.engine.connect() as db:
            return set(db.execute(select(self.outcomes_table.c.trial_id)).scalars())

    def record_trial(self, trial: PaperAllocationTrial) -> str:
        raw, lineage = _payload(trial)
        with self.store.engine.begin() as db:
            existing = db.execute(
                select(self.trials.c.trial_id).where(self.trials.c.trial_id == trial.trial_id)
            ).scalar_one_or_none()
            if existing is None:
                db.execute(insert(self.trials), {
                    "trial_id": trial.trial_id,
                    "candidate_id": trial.candidate_id,
                    "family": trial.family,
                    "strategy": trial.strategy,
                    "asset": trial.asset,
                    "cohort_key": trial.cohort_key,
                    "plan_observed_at": trial.plan_observed_at.isoformat(),
                    "due_at": trial.due_at.isoformat() if trial.due_at is not None else None,
                    "settlement_supported": trial.settlement_supported,
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })
        return trial.trial_id

    def record_outcome(self, outcome: PaperAllocationOutcome) -> str:
        raw, lineage = _payload(outcome)
        with self.store.engine.begin() as db:
            existing = db.execute(
                select(self.outcomes_table.c.trial_id).where(self.outcomes_table.c.trial_id == outcome.trial_id)
            ).scalar_one_or_none()
            if existing is None:
                db.execute(insert(self.outcomes_table), {
                    "outcome_id": outcome.outcome_id,
                    "trial_id": outcome.trial_id,
                    "strategy": outcome.strategy,
                    "asset": outcome.asset,
                    "matured_at": outcome.matured_at.isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })
        return outcome.outcome_id

    def pending_supported_trials(self, *, now: datetime | None = None) -> list[PaperAllocationTrial]:
        now = now or _now()
        completed = self._completed_trial_ids()
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.trials.c.payload_json)
                .where(self.trials.c.settlement_supported.is_(True))
                .where(self.trials.c.due_at.is_not(None))
                .where(self.trials.c.due_at <= now.isoformat())
                .order_by(self.trials.c.id)
            ).scalars())
        rows = [PaperAllocationTrial.model_validate_json(payload) for payload in payloads]
        return [row for row in rows if row.trial_id not in completed]

    def has_unsettled_supported_cohort(self, cohort_key: str) -> bool:
        completed = self._completed_trial_ids()
        with self.store.engine.connect() as db:
            ids = list(db.execute(
                select(self.trials.c.trial_id)
                .where(self.trials.c.cohort_key == cohort_key)
                .where(self.trials.c.settlement_supported.is_(True))
            ).scalars())
        return any(trial_id not in completed for trial_id in ids)

    def trials_all(self) -> list[PaperAllocationTrial]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(select(self.trials.c.payload_json).order_by(self.trials.c.id)).scalars())
        return [PaperAllocationTrial.model_validate_json(payload) for payload in payloads]

    def outcomes(self) -> list[PaperAllocationOutcome]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.outcomes_table.c.payload_json).order_by(self.outcomes_table.c.id)
            ).scalars())
        return [PaperAllocationOutcome.model_validate_json(payload) for payload in payloads]

    def summary(self) -> dict[str, object]:
        trials = self.trials_all()
        outcomes = self.outcomes()
        supported = [row for row in trials if row.settlement_supported]
        realized_profit = sum(row.realized_profit_usd for row in outcomes)
        predicted_profit = sum(row.predicted_profit_usd for row in outcomes)
        errors = [row.prediction_error_usd for row in outcomes]
        capture = [row.profit_capture_ratio for row in outcomes if row.profit_capture_ratio is not None]
        multileg_trials = [row for row in supported if len(row.settlement_legs) >= 2]
        multileg_outcomes = [row for row in outcomes if len(row.leg_outcomes) >= 2]
        return {
            "trial_count": len(trials),
            "supported_trial_count": len(supported),
            "unsupported_trial_count": len(trials) - len(supported),
            "settled_outcome_count": len(outcomes),
            "pending_supported_count": max(0, len(supported) - len(outcomes)),
            "multi_leg_supported_trial_count": len(multileg_trials),
            "multi_leg_settled_outcome_count": len(multileg_outcomes),
            "settlement_coverage_fraction": len(supported) / len(trials) if trials else None,
            "predicted_profit_usd_settled_trials": predicted_profit,
            "realized_profit_usd_settled_trials": realized_profit,
            "profit_capture_ratio_aggregate": realized_profit / predicted_profit if predicted_profit > 0 else None,
            "median_trial_profit_capture_ratio": statistics.median(capture) if capture else None,
            "mean_prediction_error_usd": statistics.fmean(errors) if errors else None,
            "profitable_trial_rate": (
                sum(row.profitable for row in outcomes) / len(outcomes) if outcomes else None
            ),
            "live_execution_authority": False,
            "paper_only": True,
        }


class AllocationForwardCertificationService:
    """Forward certification of the unified allocator's actual paper decisions.

    Spot directional alpha remains forward-settled from public price evidence.
    Core two-leg CEX allocations now share one canonical settlement path that
    reconstructs both close legs from visible L2, accrues observed funding events,
    subtracts the precommitted non-slippage cost model, and persists exact per-leg
    lineage. Missing required evidence remains pending rather than becoming P&L.
    """

    SETTLEMENT_METHOD = "spot_mid_forward_minus_point_in_time_roundtrip_cost"
    PERP_SHORT_SETTLEMENT_METHOD = "perp_short_mid_forward_plus_observed_funding_minus_point_in_time_roundtrip_cost"
    MULTI_LEG_SETTLEMENT_METHOD = "visible_l2_multileg_close_plus_observed_funding_minus_point_in_time_non_slippage_cost"

    def __init__(self, core, allocator: UnifiedPaperAllocatorService, store: EvidenceStore):
        self.core = core
        self.allocator = allocator
        self.store = store
        self.ledger = AllocationCertificationLedger(store)

    @staticmethod
    def _cohort_key(allocation: UnifiedPaperAllocation) -> str:
        leg_key = ";".join(
            f"{leg.venue}:{leg.market_kind}:{leg.symbol}:{leg.side}" for leg in allocation.settlement_legs
        )
        venue = allocation.venues[0] if allocation.venues else "unknown"
        return "|".join([
            allocation.family,
            allocation.strategy,
            allocation.asset,
            allocation.exposure_kind,
            leg_key or venue,
            allocation.instrument_symbol or "unknown",
        ])

    @classmethod
    def trial_from_allocation(
        cls,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        source_time = allocation.source_observed_at or plan_observed_at
        spot_long = bool(
            allocation.family == "alpha"
            and allocation.exposure_kind == "directional_long"
            and allocation.instrument_market_kind == MarketKind.SPOT.value
            and len(allocation.venues) == 1
            and allocation.instrument_symbol
            and allocation.entry_reference_price is not None
            and allocation.modeled_roundtrip_cost_return is not None
            and allocation.modeled_holding_hours is not None
        )
        perp_short = bool(
            allocation.family == "alpha"
            and allocation.exposure_kind == "directional_short"
            and allocation.instrument_market_kind == MarketKind.PERPETUAL.value
            and len(allocation.venues) == 1
            and allocation.instrument_symbol
            and allocation.entry_reference_price is not None
            and allocation.modeled_roundtrip_cost_return is not None
            and allocation.modeled_holding_hours is not None
        )
        multi_leg = bool(
            allocation.family == "core_cex"
            and allocation.exposure_kind == "market_neutral"
            and allocation.modeled_holding_hours is not None
            and len(allocation.settlement_legs) == 2
            and allocation.modeled_non_slippage_cost_bps is not None
            and allocation.capital_multiple is not None
        )
        supported = spot_long or perp_short or multi_leg
        due_at = source_time + timedelta(hours=allocation.modeled_holding_hours or 0.0) if supported else None
        method = None
        blocker = None
        settlement_legs = list(allocation.settlement_legs)
        if spot_long:
            method = cls.SETTLEMENT_METHOD
        elif perp_short:
            method = cls.PERP_SHORT_SETTLEMENT_METHOD
            settlement_legs = [PaperSettlementLeg(
                venue=allocation.venues[0],
                asset=allocation.asset.upper(),
                market_kind=MarketKind.PERPETUAL.value,
                side="short",
                symbol=allocation.instrument_symbol or allocation.asset,
                base_quantity=allocation.notional_usd_per_leg / float(allocation.entry_reference_price),
                entry_price=float(allocation.entry_reference_price),
                entry_notional_usd=allocation.notional_usd_per_leg,
            )]
        elif multi_leg:
            method = cls.MULTI_LEG_SETTLEMENT_METHOD
        elif allocation.family == "core_cex":
            blocker = "core allocation is missing exact two-leg entry/L2 cost metadata required for canonical settlement"
        elif allocation.family == "alpha":
            blocker = "directional alpha settlement metadata is incomplete or unsupported"
        elif allocation.family == "cex_dex":
            blocker = "CEX-DEX allocation still requires amount-specific realized route and hedge settlement evidence"
        else:
            blocker = "allocation requires an authoritative family settlement contract"
        return PaperAllocationTrial(
            plan_observed_at=plan_observed_at,
            candidate_id=allocation.candidate_id,
            family=allocation.family,
            strategy=allocation.strategy,
            asset=allocation.asset,
            venues=allocation.venues,
            exposure_kind=allocation.exposure_kind,
            capital_required_usd=allocation.capital_required_usd,
            notional_usd=allocation.notional_usd_per_leg,
            predicted_profit_usd=allocation.expected_profit_usd_per_deployment,
            predicted_return_on_reserved_capital=allocation.expected_return_on_reserved_capital,
            source_observed_at=source_time,
            due_at=due_at,
            instrument_symbol=allocation.instrument_symbol,
            instrument_market_kind=allocation.instrument_market_kind,
            entry_reference_price=allocation.entry_reference_price,
            modeled_roundtrip_cost_return=allocation.modeled_roundtrip_cost_return,
            settlement_legs=settlement_legs,
            modeled_non_slippage_cost_bps=allocation.modeled_non_slippage_cost_bps,
            modeled_safety_buffer_bps=allocation.modeled_safety_buffer_bps,
            capital_multiple=allocation.capital_multiple,
            settlement_supported=supported,
            settlement_method=method,
            settlement_blocker=blocker,
            cohort_key=cls._cohort_key(allocation),
        )

    @staticmethod
    def _quote_index(snapshot: ScanSnapshot) -> dict[tuple[str, str, str, str], MarketQuote]:
        return {
            (quote.venue, quote.asset.upper(), quote.market_kind.value, quote.symbol): quote
            for quote in snapshot.market_quotes
        }

    @staticmethod
    def _book_index(snapshot: ScanSnapshot) -> dict[tuple[str, str, str, str], OrderBookSnapshot]:
        return {
            (book.venue, book.asset.upper(), book.market_kind.value, book.symbol): book
            for book in snapshot.order_books
        }

    def _funding_quotes_for_leg(
        self,
        leg: PaperSettlementLeg,
        *,
        source_at: datetime,
        due_at: datetime,
    ) -> list[FundingQuote]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.store.funding_quotes.c.payload_json)
                .where(self.store.funding_quotes.c.venue == leg.venue)
                .where(self.store.funding_quotes.c.asset == leg.asset.upper())
                .where(self.store.funding_quotes.c.observed_at >= source_at.isoformat())
                .where(self.store.funding_quotes.c.observed_at <= due_at.isoformat())
                .order_by(self.store.funding_quotes.c.id)
            ).scalars())
        rows = [FundingQuote.model_validate_json(payload) for payload in payloads]
        return [
            row for row in rows
            if row.venue == leg.venue
            and row.asset.upper() == leg.asset.upper()
            and (not row.symbol or row.symbol == leg.symbol)
        ]

    def _historical_mid(self, leg: PaperSettlementLeg, observed_at: datetime) -> float | None:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.store.market_quotes.c.payload_json)
                .where(self.store.market_quotes.c.venue == leg.venue)
                .where(self.store.market_quotes.c.asset == leg.asset.upper())
                .where(self.store.market_quotes.c.observed_at <= observed_at.isoformat())
                .order_by(self.store.market_quotes.c.id.desc())
                .limit(200)
            ).scalars())
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            if quote.market_kind.value == leg.market_kind and quote.symbol == leg.symbol and quote.mid > 0:
                return quote.mid
        return None

    def _funding_for_leg(
        self,
        leg: PaperSettlementLeg,
        *,
        source_at: datetime,
        due_at: datetime,
    ) -> tuple[float, int] | None:
        if leg.market_kind != MarketKind.PERPETUAL.value:
            return 0.0, 0
        rows = self._funding_quotes_for_leg(leg, source_at=source_at, due_at=due_at)
        if not rows:
            return None
        by_event: dict[datetime, FundingQuote] = {}
        has_scheduling_evidence = False
        for quote in rows:
            event_at = quote.next_funding_time
            if event_at is None:
                continue
            has_scheduling_evidence = True
            if source_at < event_at <= due_at and quote.observed_at <= event_at:
                previous = by_event.get(event_at)
                if previous is None or quote.observed_at > previous.observed_at:
                    by_event[event_at] = quote
        if not has_scheduling_evidence:
            return None
        total = 0.0
        sign = 1.0 if leg.side == "long" else -1.0
        for event_at, quote in sorted(by_event.items()):
            mid = self._historical_mid(leg, event_at)
            if mid is None:
                return None
            event_notional = leg.base_quantity * mid
            total += -sign * event_notional * quote.rate
        return total, len(by_event)

    def _settle_spot_long(
        self,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if trial.entry_reference_price is None or trial.modeled_roundtrip_cost_return is None:
            return None
        if not trial.venues or trial.instrument_symbol is None or trial.instrument_market_kind is None:
            return None
        quote = self._quote_index(snapshot).get((
            trial.venues[0],
            trial.asset.upper(),
            trial.instrument_market_kind,
            trial.instrument_symbol,
        ))
        if quote is None or quote.mid <= 0:
            return None
        gross = quote.mid / trial.entry_reference_price - 1.0
        realized_net = gross - trial.modeled_roundtrip_cost_return
        realized_profit = trial.notional_usd * realized_net
        error = realized_profit - trial.predicted_profit_usd
        capture = realized_profit / trial.predicted_profit_usd if trial.predicted_profit_usd > 0 else None
        return PaperAllocationOutcome(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate_id,
            family=trial.family,
            strategy=trial.strategy,
            asset=trial.asset,
            matured_at=snapshot.completed_at,
            due_at=trial.due_at or snapshot.completed_at,
            entry_reference_price=trial.entry_reference_price,
            exit_reference_price=quote.mid,
            realized_gross_return=gross,
            realized_net_return=realized_net,
            realized_profit_usd=realized_profit,
            realized_price_pnl_usd=trial.notional_usd * gross,
            modeled_non_slippage_cost_usd=trial.notional_usd * trial.modeled_roundtrip_cost_return,
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0,
            settlement_method=trial.settlement_method or self.SETTLEMENT_METHOD,
        )

    def _settle_perp_short(
        self,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if (
            trial.entry_reference_price is None
            or trial.modeled_roundtrip_cost_return is None
            or trial.due_at is None
            or trial.source_observed_at is None
            or not trial.settlement_legs
            or not trial.venues
            or trial.instrument_symbol is None
            or trial.instrument_market_kind is None
        ):
            return None
        quote = self._quote_index(snapshot).get((
            trial.venues[0], trial.asset.upper(), trial.instrument_market_kind, trial.instrument_symbol
        ))
        if quote is None or quote.mid <= 0:
            return None
        funding = self._funding_for_leg(
            trial.settlement_legs[0], source_at=trial.source_observed_at, due_at=trial.due_at
        )
        if funding is None:
            return None
        funding_pnl, funding_events = funding
        gross_price_return = 1.0 - quote.mid / trial.entry_reference_price
        price_pnl = trial.notional_usd * gross_price_return
        realized_profit = price_pnl + funding_pnl - trial.notional_usd * trial.modeled_roundtrip_cost_return
        realized_net = realized_profit / trial.capital_required_usd
        gross = (price_pnl + funding_pnl) / trial.capital_required_usd
        error = realized_profit - trial.predicted_profit_usd
        capture = realized_profit / trial.predicted_profit_usd if trial.predicted_profit_usd > 0 else None
        leg = trial.settlement_legs[0]
        return PaperAllocationOutcome(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate_id,
            family=trial.family,
            strategy=trial.strategy,
            asset=trial.asset,
            matured_at=snapshot.completed_at,
            due_at=trial.due_at,
            entry_reference_price=trial.entry_reference_price,
            exit_reference_price=quote.mid,
            realized_gross_return=gross,
            realized_net_return=realized_net,
            realized_profit_usd=realized_profit,
            realized_price_pnl_usd=price_pnl,
            realized_funding_pnl_usd=funding_pnl,
            modeled_non_slippage_cost_usd=trial.notional_usd * trial.modeled_roundtrip_cost_return,
            leg_outcomes=[PaperSettlementLegOutcome(
                venue=leg.venue,
                asset=leg.asset,
                market_kind=leg.market_kind,
                side=leg.side,
                symbol=leg.symbol,
                base_quantity=leg.base_quantity,
                entry_price=leg.entry_price,
                exit_price=quote.mid,
                entry_notional_usd=leg.entry_notional_usd,
                exit_notional_usd=leg.base_quantity * quote.mid,
                price_pnl_usd=price_pnl,
                funding_pnl_usd=funding_pnl,
                funding_event_count=funding_events,
                exit_fill_source="market_quote_mid",
            )],
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0,
            settlement_method=trial.settlement_method or self.PERP_SHORT_SETTLEMENT_METHOD,
        )

    def _settle_multileg(
        self,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if (
            trial.due_at is None
            or trial.source_observed_at is None
            or len(trial.settlement_legs) != 2
            or trial.modeled_non_slippage_cost_bps is None
        ):
            return None
        books = self._book_index(snapshot)
        funding_rows: list[tuple[float, int]] = []
        for leg in trial.settlement_legs:
            funding = self._funding_for_leg(leg, source_at=trial.source_observed_at, due_at=trial.due_at)
            if funding is None:
                return None
            funding_rows.append(funding)

        leg_outcomes: list[PaperSettlementLegOutcome] = []
        price_pnl = 0.0
        funding_pnl = 0.0
        for leg, (leg_funding_pnl, funding_event_count) in zip(trial.settlement_legs, funding_rows):
            book = books.get((leg.venue, leg.asset.upper(), leg.market_kind, leg.symbol))
            if book is None:
                return None
            close_side = TradeSide.SELL if leg.side == "long" else TradeSide.BUY
            try:
                fill = estimate_market_order_quantity(
                    book, close_side, leg.base_quantity, require_full_fill=True
                )
            except InsufficientDepthError:
                return None
            leg_price_pnl = (
                leg.base_quantity * (fill.average_price - leg.entry_price)
                if leg.side == "long"
                else leg.base_quantity * (leg.entry_price - fill.average_price)
            )
            price_pnl += leg_price_pnl
            funding_pnl += leg_funding_pnl
            leg_outcomes.append(PaperSettlementLegOutcome(
                venue=leg.venue,
                asset=leg.asset,
                market_kind=leg.market_kind,
                side=leg.side,
                symbol=leg.symbol,
                base_quantity=leg.base_quantity,
                entry_price=leg.entry_price,
                exit_price=fill.average_price,
                entry_notional_usd=leg.entry_notional_usd,
                exit_notional_usd=fill.filled_notional_usd,
                price_pnl_usd=leg_price_pnl,
                funding_pnl_usd=leg_funding_pnl,
                funding_event_count=funding_event_count,
                exit_slippage_bps=fill.slippage_bps,
                exit_levels_consumed=fill.levels_consumed,
            ))

        cost_usd = trial.notional_usd * trial.modeled_non_slippage_cost_bps / 10_000.0
        gross_profit = price_pnl + funding_pnl
        realized_profit = gross_profit - cost_usd
        gross_return = gross_profit / trial.capital_required_usd
        realized_net = realized_profit / trial.capital_required_usd
        error = realized_profit - trial.predicted_profit_usd
        capture = realized_profit / trial.predicted_profit_usd if trial.predicted_profit_usd > 0 else None
        return PaperAllocationOutcome(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate_id,
            family=trial.family,
            strategy=trial.strategy,
            asset=trial.asset,
            matured_at=snapshot.completed_at,
            due_at=trial.due_at,
            realized_gross_return=gross_return,
            realized_net_return=realized_net,
            realized_profit_usd=realized_profit,
            realized_price_pnl_usd=price_pnl,
            realized_funding_pnl_usd=funding_pnl,
            modeled_non_slippage_cost_usd=cost_usd,
            leg_outcomes=leg_outcomes,
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0,
            settlement_method=trial.settlement_method or self.MULTI_LEG_SETTLEMENT_METHOD,
        )

    def _settle_trial(
        self,
        trial: PaperAllocationTrial,
        snapshot: ScanSnapshot,
    ) -> PaperAllocationOutcome | None:
        if not trial.settlement_supported or trial.due_at is None:
            return None
        if trial.settlement_method == self.MULTI_LEG_SETTLEMENT_METHOD:
            return self._settle_multileg(trial, snapshot)
        if trial.settlement_method == self.PERP_SHORT_SETTLEMENT_METHOD:
            return self._settle_perp_short(trial, snapshot)
        return self._settle_spot_long(trial, snapshot)

    async def run_cycle(self, *, total_capital_usd: float = 100000.0) -> AllocationCertificationCycle:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        snapshot = await self.core.collect_live_executability()
        matured = 0
        for trial in self.ledger.pending_supported_trials(now=snapshot.completed_at):
            outcome = self._settle_trial(trial, snapshot)
            if outcome is not None:
                self.ledger.record_outcome(outcome)
                matured += 1

        plan = await self.allocator.allocate(total_capital_usd=total_capital_usd)
        recorded = supported = unsupported = 0
        for allocation in plan.allocations:
            trial = self.trial_from_allocation(allocation, plan_observed_at=plan.observed_at)
            if trial.settlement_supported and self.ledger.has_unsettled_supported_cohort(trial.cohort_key):
                continue
            self.ledger.record_trial(trial)
            recorded += 1
            if trial.settlement_supported:
                supported += 1
            else:
                unsupported += 1
        return AllocationCertificationCycle(
            observed_at=plan.observed_at,
            plan_allocation_count=len(plan.allocations),
            trials_recorded=recorded,
            supported_trials_recorded=supported,
            unsupported_trials_recorded=unsupported,
            outcomes_matured=matured,
        )

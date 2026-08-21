from __future__ import annotations

import hashlib
import json
import math
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, Float, Index, Integer, MetaData, String, Table, Text, insert, inspect, select, text

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.incremental_forward_sizing import forward_evidence_allocation_fraction
from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookSnapshot
from inefficiency_engine.research_mechanisms import (
    CapitalLocationResearchService,
    OptionQuoteObservation,
    VolatilityResearchService,
    YieldObservation,
    YieldResearchService,
)
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation
from inefficiency_engine.trade_flow import TradeFlowLedger
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperCandidate


MechanismId = Literal[
    "yield",
    "liquidity_provision",
    "volatility",
    "liquidation_distress",
    "capital_location_settlement",
]

MECHANISM_IDS: tuple[MechanismId, ...] = (
    "yield",
    "liquidity_provision",
    "volatility",
    "liquidation_distress",
    "capital_location_settlement",
)
FULL_FORWARD_TARGET = 30
MIN_FORWARD_START = 3
MIN_HIT_RATE = 0.55
MAX_INCREMENTAL_DRAWDOWN = 0.08


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable(*parts: object) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:48]


def _mean_lower(values: list[float]) -> float | None:
    if not values:
        return None
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean
    stdev = statistics.stdev(values)
    return mean - 1.96 * stdev / math.sqrt(len(values))


def _max_drawdown(values: list[float]) -> float:
    wealth = peak = 1.0
    drawdown = 0.0
    for value in values:
        wealth *= max(0.0, 1.0 + value)
        peak = max(peak, wealth)
        if peak > 0:
            drawdown = max(drawdown, 1.0 - wealth / peak)
    return drawdown


def _json(value: BaseModel) -> tuple[str, str]:
    raw = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return raw, hashlib.sha256(raw.encode()).hexdigest()


class MechanismTrialSpec(BaseModel):
    mechanism_id: MechanismId
    cohort_key: str
    asset: str
    venues: list[str]
    source_observed_at: datetime
    holding_hours: float = Field(gt=0)
    capital_usd: float = Field(gt=0)
    predicted_net_return: float
    settlement_payload: dict[str, object]
    conflict_keys: list[str] = Field(default_factory=list)


class MechanismForwardTrial(BaseModel):
    trial_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mechanism_id: MechanismId
    cohort_key: str
    asset: str
    venues: list[str]
    source_observed_at: datetime
    due_at: datetime
    capital_usd: float = Field(gt=0)
    predicted_net_return: float
    predicted_profit_usd: float
    settlement_payload: dict[str, object]
    conflict_keys: list[str] = Field(default_factory=list)
    recorded_at: datetime = Field(default_factory=_now)
    paper_only: bool = True
    live_execution_authority: bool = False


class MechanismForwardOutcome(BaseModel):
    outcome_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trial_id: str
    mechanism_id: MechanismId
    cohort_key: str
    asset: str
    matured_at: datetime
    due_at: datetime
    predicted_net_return: float
    realized_gross_return: float
    realized_net_return: float
    realized_profit_usd: float
    profitable: bool
    settlement_method: str
    settlement_evidence_complete: bool = True
    detail: dict[str, object] = Field(default_factory=dict)
    paper_only: bool = True
    live_execution_authority: bool = False


class MechanismQualification(BaseModel):
    mechanism_id: MechanismId
    cohort_key: str
    sample_count: int = Field(ge=0)
    positive_count: int = Field(ge=0)
    hit_rate: float | None = None
    mean_net_return: float | None = None
    mean_net_return_ci_lower: float | None = None
    max_drawdown: float | None = None
    allocation_fraction: float = Field(ge=0, le=1)
    incremental_eligible: bool = False
    fully_statistically_qualified: bool = False
    blockers: list[str] = Field(default_factory=list)
    paper_only: bool = True


class MechanismPaperCandidate(BaseModel):
    candidate_id: str
    mechanism_id: MechanismId
    cohort_key: str
    asset: str
    venues: list[str]
    observed_at: datetime
    holding_hours: float
    capital_usd: float
    expected_net_return: float
    expected_profit_usd: float
    evidence_sample_count: int
    evidence_allocation_fraction: float
    settlement_payload: dict[str, object]
    conflict_keys: list[str]
    paper_only: bool = True
    live_execution_authority: bool = False


class MechanismEvidenceCycle(BaseModel):
    observed_at: datetime
    trials_recorded: int = Field(ge=0)
    outcomes_matured: int = Field(ge=0)
    current_specs: int = Field(ge=0)
    promoted_candidates: int = Field(ge=0)
    by_mechanism: dict[str, dict[str, object]] = Field(default_factory=dict)
    paper_only: bool = True


class MechanismSettlementResult(BaseModel):
    matured_at: datetime
    gross_return: float
    net_return: float
    settlement_method: str
    detail: dict[str, object] = Field(default_factory=dict)


class CapitalTransferObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    source: str
    route: str
    venue: str
    asset: str
    observed_at: datetime = Field(default_factory=_now)
    transfer_cost_usd: float = Field(ge=0)
    transfer_latency_seconds: float = Field(ge=0)
    notional_usd: float = Field(gt=0)
    authoritative: bool = True
    point_in_time: bool = True
    paper_only: bool = True


class MechanismExecutionLedger:
    """Append-only research trials, outcomes and qualified mechanism candidates."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.trials = Table(
            "mechanism_forward_trials",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("mechanism_id", Text, nullable=False),
            Column("cohort_key", Text, nullable=False),
            Column("source_observed_at", Text, nullable=False),
            Column("due_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.outcomes_table = Table(
            "mechanism_forward_outcomes",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("outcome_id", String(64), nullable=False, unique=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("mechanism_id", Text, nullable=False),
            Column("cohort_key", Text, nullable=False),
            Column("matured_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.candidates = Table(
            "mechanism_paper_candidates",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("candidate_id", String(128), nullable=False, unique=True),
            Column("mechanism_id", Text, nullable=False),
            Column("cohort_key", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.maker_outcomes = Table(
            "maker_shadow_outcomes",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("outcome_id", String(64), nullable=False, unique=True),
            Column("venue", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("realized_net_return", Float, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        self.transfer_outcomes = Table(
            "capital_transfer_outcomes",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("venue", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index("ix_mechanism_trial_cohort", self.trials.c.cohort_key, self.trials.c.due_at)
        Index("ix_mechanism_outcome_cohort", self.outcomes_table.c.cohort_key, self.outcomes_table.c.matured_at)
        Index("ix_mechanism_candidate_time", self.candidates.c.mechanism_id, self.candidates.c.observed_at)
        Index("ix_maker_shadow_time", self.maker_outcomes.c.venue, self.maker_outcomes.c.asset, self.maker_outcomes.c.observed_at)
        Index("ix_transfer_time", self.transfer_outcomes.c.venue, self.transfer_outcomes.c.asset, self.transfer_outcomes.c.observed_at)
        metadata.create_all(store.engine)

    def record_trial(self, trial: MechanismForwardTrial) -> str:
        raw, lineage = _json(trial)
        with self.store.engine.begin() as db:
            if db.execute(select(self.trials.c.trial_id).where(self.trials.c.trial_id == trial.trial_id)).scalar_one_or_none() is None:
                db.execute(insert(self.trials), {
                    "trial_id": trial.trial_id,
                    "mechanism_id": trial.mechanism_id,
                    "cohort_key": trial.cohort_key,
                    "source_observed_at": trial.source_observed_at.isoformat(),
                    "due_at": trial.due_at.isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })
        return trial.trial_id

    def record_outcome(self, outcome: MechanismForwardOutcome) -> str:
        raw, lineage = _json(outcome)
        with self.store.engine.begin() as db:
            if db.execute(select(self.outcomes_table.c.trial_id).where(self.outcomes_table.c.trial_id == outcome.trial_id)).scalar_one_or_none() is None:
                db.execute(insert(self.outcomes_table), {
                    "outcome_id": outcome.outcome_id,
                    "trial_id": outcome.trial_id,
                    "mechanism_id": outcome.mechanism_id,
                    "cohort_key": outcome.cohort_key,
                    "matured_at": outcome.matured_at.isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })
                if outcome.mechanism_id == "liquidity_provision":
                    db.execute(insert(self.maker_outcomes), {
                        "outcome_id": outcome.outcome_id,
                        "venue": str(outcome.detail.get("venue") or "unknown"),
                        "asset": outcome.asset,
                        "observed_at": outcome.matured_at.isoformat(),
                        "realized_net_return": outcome.realized_net_return,
                        "payload_json": raw,
                    })
        return outcome.outcome_id

    def record_candidate(self, candidate: MechanismPaperCandidate) -> str:
        raw, lineage = _json(candidate)
        with self.store.engine.begin() as db:
            if db.execute(select(self.candidates.c.candidate_id).where(self.candidates.c.candidate_id == candidate.candidate_id)).scalar_one_or_none() is None:
                db.execute(insert(self.candidates), {
                    "candidate_id": candidate.candidate_id,
                    "mechanism_id": candidate.mechanism_id,
                    "cohort_key": candidate.cohort_key,
                    "observed_at": candidate.observed_at.isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })
        return candidate.candidate_id

    def record_transfer(self, row: CapitalTransferObservation) -> str:
        with self.store.engine.begin() as db:
            if db.execute(select(self.transfer_outcomes.c.observation_id).where(self.transfer_outcomes.c.observation_id == row.observation_id)).scalar_one_or_none() is None:
                db.execute(insert(self.transfer_outcomes), {
                    "observation_id": row.observation_id,
                    "venue": row.venue,
                    "asset": row.asset.upper(),
                    "observed_at": row.observed_at.isoformat(),
                    "payload_json": row.model_dump_json(),
                })
        return row.observation_id

    def pending(self, *, now: datetime) -> list[MechanismForwardTrial]:
        with self.store.engine.connect() as db:
            completed = set(db.execute(select(self.outcomes_table.c.trial_id)).scalars())
            payloads = list(db.execute(
                select(self.trials.c.payload_json)
                .where(self.trials.c.due_at <= now.isoformat())
                .order_by(self.trials.c.id)
                .limit(1000)
            ).scalars())
        return [
            trial
            for payload in payloads
            if (trial := MechanismForwardTrial.model_validate_json(payload)).trial_id not in completed
        ]

    def has_open_cohort(self, cohort_key: str) -> bool:
        with self.store.engine.connect() as db:
            ids = list(db.execute(select(self.trials.c.trial_id).where(self.trials.c.cohort_key == cohort_key)).scalars())
            completed = set(db.execute(select(self.outcomes_table.c.trial_id)).scalars())
        return any(trial_id not in completed for trial_id in ids)

    def outcomes(self, *, cohort_key: str | None = None, mechanism_id: str | None = None) -> list[MechanismForwardOutcome]:
        query = select(self.outcomes_table.c.payload_json)
        if cohort_key is not None:
            query = query.where(self.outcomes_table.c.cohort_key == cohort_key)
        if mechanism_id is not None:
            query = query.where(self.outcomes_table.c.mechanism_id == mechanism_id)
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query.order_by(self.outcomes_table.c.id)).scalars())
        return [MechanismForwardOutcome.model_validate_json(payload) for payload in payloads]

    def candidate(self, candidate_id: str) -> MechanismPaperCandidate | None:
        with self.store.engine.connect() as db:
            raw = db.execute(select(self.candidates.c.payload_json).where(self.candidates.c.candidate_id == candidate_id)).scalar_one_or_none()
        return MechanismPaperCandidate.model_validate_json(raw) if raw else None

    def latest_candidates(self, *, max_age_hours: float = 24.0, now: datetime | None = None) -> list[MechanismPaperCandidate]:
        now = now or _now()
        cutoff = now - timedelta(hours=max(0.01, max_age_hours))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.candidates.c.payload_json)
                .where(self.candidates.c.observed_at >= cutoff.isoformat())
                .order_by(self.candidates.c.id.desc())
                .limit(500)
            ).scalars())
        latest: dict[str, MechanismPaperCandidate] = {}
        for payload in payloads:
            row = MechanismPaperCandidate.model_validate_json(payload)
            latest.setdefault(row.cohort_key, row)
        return list(latest.values())

    def latest_transfer(self, *, venue: str, asset: str) -> CapitalTransferObservation | None:
        with self.store.engine.connect() as db:
            raw = db.execute(
                select(self.transfer_outcomes.c.payload_json)
                .where(self.transfer_outcomes.c.venue == venue)
                .where(self.transfer_outcomes.c.asset == asset.upper())
                .order_by(self.transfer_outcomes.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        return CapitalTransferObservation.model_validate_json(raw) if raw else None


class MechanismExecutionService:
    """Forward-test and promote the five previously research-only mechanism lanes."""

    def __init__(self, core, store: EvidenceStore):
        self.core = core
        self.store = store
        self.settings = core.settings
        self.ledger = MechanismExecutionLedger(store)
        self.source_plane = SourceCoveragePlane(store)
        self.yield_service = YieldResearchService(store)
        self.volatility_service = VolatilityResearchService(store)
        self.location_service = CapitalLocationResearchService(store)
        self.trade_flow = TradeFlowLedger(store)

    def qualification(self, cohort_key: str, mechanism_id: MechanismId) -> MechanismQualification:
        outcomes = self.ledger.outcomes(cohort_key=cohort_key)
        values = [row.realized_net_return for row in outcomes]
        positive = sum(value > 0 for value in values)
        hit = positive / len(values) if values else None
        mean = statistics.fmean(values) if values else None
        lower = _mean_lower(values)
        drawdown = _max_drawdown(values) if values else None
        fraction = forward_evidence_allocation_fraction(len(values), full_target=FULL_FORWARD_TARGET)
        blockers: list[str] = []
        if len(values) < MIN_FORWARD_START:
            blockers.append("fewer than three independent forward outcomes")
        if mean is None or mean <= 0:
            blockers.append("mean realized net return is non-positive")
        if hit is None or hit < MIN_HIT_RATE:
            blockers.append("forward hit rate is below 55%")
        if drawdown is None or drawdown > MAX_INCREMENTAL_DRAWDOWN:
            blockers.append("forward drawdown exceeds mechanism paper-risk limit")
        full = bool(
            len(values) >= FULL_FORWARD_TARGET
            and lower is not None
            and lower > 0
            and hit is not None
            and hit >= MIN_HIT_RATE
            and drawdown is not None
            and drawdown <= MAX_INCREMENTAL_DRAWDOWN
        )
        incremental = bool(
            MIN_FORWARD_START <= len(values) < FULL_FORWARD_TARGET
            and not blockers
            and fraction > 0
        )
        if len(values) >= FULL_FORWARD_TARGET and not full:
            blockers.append("full 30-outcome statistical gate is not satisfied")
        return MechanismQualification(
            mechanism_id=mechanism_id,
            cohort_key=cohort_key,
            sample_count=len(values),
            positive_count=positive,
            hit_rate=hit,
            mean_net_return=mean,
            mean_net_return_ci_lower=lower,
            max_drawdown=drawdown,
            allocation_fraction=1.0 if full else fraction if incremental else 0.0,
            incremental_eligible=incremental,
            fully_statistically_qualified=full,
            blockers=blockers,
        )

    def _latest_quote(self, snapshot: ScanSnapshot, *, venue: str, asset: str, market_kind: MarketKind | None = None) -> MarketQuote | None:
        rows = [q for q in snapshot.market_quotes if q.venue == venue and q.asset.upper() == asset.upper()]
        if market_kind is not None:
            rows = [q for q in rows if q.market_kind == market_kind]
        return max(rows, key=lambda q: q.observed_at) if rows else None

    def _yield_specs(self, *, now: datetime, total_capital_usd: float) -> list[MechanismTrialSpec]:
        observations = {row.observation_id: row for row in self.yield_service.observations()}
        specs: list[MechanismTrialSpec] = []
        for candidate in self.yield_service.candidates(now=now):
            observation = observations.get(candidate.observation_id)
            if observation is None or candidate.conservative_net_apy <= 0:
                continue
            holding = max(1.0, min(candidate.holding_hours, 24.0 * 7.0))
            predicted = candidate.conservative_net_apy * holding / 8760.0
            if predicted <= 0:
                continue
            capital = min(total_capital_usd * 0.05, candidate.capacity_usd * 0.10)
            if capital <= 0:
                continue
            cohort = f"yield|{candidate.protocol}|{candidate.asset}|{candidate.kind}"
            specs.append(MechanismTrialSpec(
                mechanism_id="yield",
                cohort_key=cohort,
                asset=candidate.asset,
                venues=[candidate.protocol],
                source_observed_at=observation.observed_at,
                holding_hours=holding,
                capital_usd=capital,
                predicted_net_return=predicted,
                settlement_payload={
                    "observation_id": observation.observation_id,
                    "protocol": observation.protocol,
                    "asset": observation.asset,
                    "kind": observation.kind,
                    "entry_net_apy": candidate.conservative_net_apy,
                    "entry_exit_cost_bps": observation.entry_exit_cost_bps,
                    "capacity_usd": observation.capacity_usd,
                },
                conflict_keys=[f"yield:{candidate.protocol}:{candidate.asset}"],
            ))
        return specs[:12]

    def _maker_specs(self, snapshot: ScanSnapshot, *, total_capital_usd: float) -> list[MechanismTrialSpec]:
        specs: list[MechanismTrialSpec] = []
        quote_index = {(q.venue, q.asset.upper(), q.market_kind.value, q.symbol): q for q in snapshot.market_quotes}
        for book in snapshot.order_books:
            if not book.bids or not book.asks:
                continue
            trades = self.trade_flow.recent(
                asset=book.asset,
                venue=book.venue,
                before=snapshot.completed_at,
                max_age_hours=0.25,
                limit=500,
            )
            if len(trades) < 4:
                continue
            best_bid = max(level.price for level in book.bids)
            best_ask = min(level.price for level in book.asks)
            mid = (best_bid + best_ask) / 2.0
            if mid <= 0 or best_ask <= best_bid:
                continue
            top_bid_size = sum(level.size for level in book.bids if level.price == best_bid)
            top_ask_size = sum(level.size for level in book.asks if level.price == best_ask)
            top_depth = min(top_bid_size * best_bid, top_ask_size * best_ask)
            if top_depth <= 0:
                continue
            flow = sum(item.notional_usd for item in trades)
            fill_probability = min(0.90, flow / max(flow + top_depth, 1.0))
            spread_return = (best_ask - best_bid) / mid
            adverse_buffer = max(0.0001, float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0)
            predicted = fill_probability * spread_return - adverse_buffer
            if predicted <= 0:
                continue
            capital = min(total_capital_usd * 0.02, top_depth * 0.05)
            if capital <= 0:
                continue
            cohort = f"maker|{book.venue}|{book.asset.upper()}|{book.symbol}"
            specs.append(MechanismTrialSpec(
                mechanism_id="liquidity_provision",
                cohort_key=cohort,
                asset=book.asset.upper(),
                venues=[book.venue],
                source_observed_at=book.observed_at,
                holding_hours=0.25,
                capital_usd=capital,
                predicted_net_return=predicted,
                settlement_payload={
                    "venue": book.venue,
                    "asset": book.asset.upper(),
                    "symbol": book.symbol,
                    "market_kind": book.market_kind.value,
                    "bid": best_bid,
                    "ask": best_ask,
                    "mid": mid,
                    "quantity": capital / mid,
                    "estimated_fill_probability": fill_probability,
                },
                conflict_keys=[f"maker:{book.venue}:{book.symbol}"],
            ))
        specs.sort(key=lambda item: item.predicted_net_return, reverse=True)
        return specs[:8]

    def _realized_volatility(self, *, now: datetime) -> dict[str, float]:
        cutoff = now - timedelta(hours=24.0)
        table = self.store.market_quotes
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(table.c.payload_json)
                .where(table.c.observed_at >= cutoff.isoformat())
                .order_by(table.c.id.desc())
                .limit(5000)
            ).scalars())
        grouped: dict[str, list[MarketQuote]] = defaultdict(list)
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            if quote.market_kind == MarketKind.SPOT and quote.asset.upper() in {"BTC", "ETH"}:
                grouped[quote.asset.upper()].append(quote)
        result: dict[str, float] = {}
        seconds_per_year = 365.0 * 24.0 * 3600.0
        for asset, rows in grouped.items():
            ordered = sorted(rows, key=lambda q: q.observed_at)
            rates: list[float] = []
            for previous, current in zip(ordered, ordered[1:]):
                dt = (current.observed_at - previous.observed_at).total_seconds()
                if dt <= 0 or previous.mid <= 0 or current.mid <= 0:
                    continue
                r = math.log(current.mid / previous.mid)
                rates.append((r * r) * seconds_per_year / dt)
            if len(rates) >= 3:
                result[asset] = math.sqrt(max(0.0, statistics.fmean(rates)))
        return result

    def _volatility_specs(self, snapshot: ScanSnapshot, *, total_capital_usd: float) -> list[MechanismTrialSpec]:
        realized = self._realized_volatility(now=snapshot.completed_at)
        candidates = self.volatility_service.candidates(
            realized_volatility_by_underlying=realized,
            hedge_cost_bps=max(2.0, float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0))),
            min_abs_vrp=0.03,
        )
        observations = self.volatility_service.observations()
        specs: list[MechanismTrialSpec] = []
        for candidate in candidates:
            matching = [
                row for row in observations
                if row.underlying == candidate.underlying
                and row.expiry == candidate.expiry
                and abs(abs(row.delta) - 0.50) <= 0.20
            ]
            if not matching:
                continue
            option = min(matching, key=lambda row: abs(abs(row.delta) - 0.50))
            entry_mid = (option.bid + option.ask) / 2.0
            spread = (option.ask - option.bid) / entry_mid
            hedge_cost = candidate.hedge_cost_bps / 10_000.0
            predicted = max(0.0, abs(candidate.volatility_risk_premium) * 0.05 - spread - hedge_cost)
            if predicted <= 0:
                continue
            underlying_quote = next((q for q in snapshot.market_quotes if q.asset.upper() == candidate.underlying and q.market_kind == MarketKind.SPOT), None)
            if underlying_quote is None:
                continue
            holding = max(1.0, min(24.0, (candidate.expiry - snapshot.completed_at).total_seconds() / 3600.0 / 4.0))
            capital = min(total_capital_usd * 0.02, max(100.0, total_capital_usd * 0.005))
            cohort = f"vol|{option.venue}|{candidate.underlying}|{candidate.direction}"
            specs.append(MechanismTrialSpec(
                mechanism_id="volatility",
                cohort_key=cohort,
                asset=candidate.underlying,
                venues=[option.venue],
                source_observed_at=option.observed_at,
                holding_hours=holding,
                capital_usd=capital,
                predicted_net_return=predicted,
                settlement_payload={
                    "venue": option.venue,
                    "underlying": option.underlying,
                    "expiry": option.expiry.isoformat(),
                    "strike": option.strike,
                    "option_type": option.option_type,
                    "entry_mid": entry_mid,
                    "entry_delta": option.delta,
                    "underlying_entry_price": underlying_quote.mid,
                    "direction": candidate.direction,
                    "spread_fraction": spread,
                    "hedge_cost_return": hedge_cost,
                },
                conflict_keys=[f"option:{option.venue}:{option.underlying}:{option.expiry.isoformat()}:{option.strike}:{option.option_type}"],
            ))
        return specs[:6]

    def _liquidation_events(self, *, now: datetime, max_age_hours: float = 0.10) -> list[SourceEventObservation]:
        table = self.source_plane.events.rows
        cutoff = now - timedelta(hours=max_age_hours)
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(table.c.payload_json)
                .where(table.c.lane_id == "liquidation_distress")
                .where(table.c.event_type == "exchange_liquidation")
                .where(table.c.event_at >= cutoff.isoformat())
                .order_by(table.c.id.desc())
                .limit(200)
            ).scalars())
        return [SourceEventObservation.model_validate_json(payload) for payload in payloads]

    def _liquidation_specs(self, snapshot: ScanSnapshot, *, total_capital_usd: float) -> list[MechanismTrialSpec]:
        specs: list[MechanismTrialSpec] = []
        for event in self._liquidation_events(now=snapshot.completed_at):
            raw = event.payload
            try:
                price = float(raw.get("price") or 0.0)
                quantity = float(raw.get("quantity") or raw.get("size") or 0.0)
            except (TypeError, ValueError):
                continue
            side = str(raw.get("side") or "").lower()
            if price <= 0 or quantity <= 0 or side not in {"buy", "sell"}:
                continue
            quote = next((q for q in snapshot.market_quotes if q.venue == "Bybit" and q.asset.upper() == (event.asset or "").upper() and q.market_kind == MarketKind.PERPETUAL), None)
            if quote is None or quote.mid <= 0:
                continue
            direction = "long" if side == "sell" else "short"
            immediate_edge = (quote.mid / price - 1.0) if direction == "long" else (1.0 - quote.mid / price)
            cost = float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0
            predicted = min(0.01, max(0.0, immediate_edge * 0.50)) - cost
            if predicted <= 0:
                continue
            capacity = price * quantity
            capital = min(total_capital_usd * 0.02, capacity * 0.05)
            if capital <= 0:
                continue
            cohort = f"liquidation|Bybit|{(event.asset or '').upper()}|{direction}"
            specs.append(MechanismTrialSpec(
                mechanism_id="liquidation_distress",
                cohort_key=cohort,
                asset=(event.asset or "").upper(),
                venues=["Bybit"],
                source_observed_at=event.event_at,
                holding_hours=1.0,
                capital_usd=capital,
                predicted_net_return=predicted,
                settlement_payload={
                    "event_id": event.event_id,
                    "venue": "Bybit",
                    "asset": (event.asset or "").upper(),
                    "symbol": str(raw.get("symbol") or f"{event.asset}USDT"),
                    "entry_price": price,
                    "direction": direction,
                    "quantity": quantity,
                    "cost_return": cost,
                },
                conflict_keys=[f"liquidation:{event.event_id}"],
            ))
        return specs[:8]

    def _location_specs(self, *, now: datetime, total_capital_usd: float) -> list[MechanismTrialSpec]:
        plan = self.location_service.plan(reserve_capital_usd=max(1000.0, total_capital_usd * 0.20))
        specs: list[MechanismTrialSpec] = []
        for recommendation in plan.recommendations[:8]:
            transfer = self.ledger.latest_transfer(venue=recommendation.venue, asset=recommendation.asset)
            if transfer is None:
                continue
            age = max(0.0, (now - transfer.observed_at).total_seconds() / 3600.0)
            if age > 24.0:
                continue
            capital = min(total_capital_usd * 0.05, recommendation.recommended_reserve_usd)
            if capital <= 0:
                continue
            holding = 24.0
            transfer_cost_return = transfer.transfer_cost_usd / max(capital, 1.0)
            historical_edge = max(0.0, recommendation.mean_positive_net_annualized_return) * holding / 8760.0 * 0.10
            latency_penalty = min(0.01, transfer.transfer_latency_seconds / 86_400.0 * 0.001)
            predicted = historical_edge - transfer_cost_return - latency_penalty
            if predicted <= 0:
                continue
            cohort = f"location|{recommendation.venue}|{recommendation.asset}"
            specs.append(MechanismTrialSpec(
                mechanism_id="capital_location_settlement",
                cohort_key=cohort,
                asset=recommendation.asset,
                venues=[recommendation.venue],
                source_observed_at=now,
                holding_hours=holding,
                capital_usd=capital,
                predicted_net_return=predicted,
                settlement_payload={
                    "venue": recommendation.venue,
                    "asset": recommendation.asset,
                    "historical_opportunity_count": recommendation.opportunity_count,
                    "historical_mean_annualized_return": recommendation.mean_positive_net_annualized_return,
                    "transfer_cost_usd": transfer.transfer_cost_usd,
                    "transfer_latency_seconds": transfer.transfer_latency_seconds,
                },
                conflict_keys=[f"capital-location:{recommendation.venue}:{recommendation.asset}"],
            ))
        return specs

    def discover_specs(self, snapshot: ScanSnapshot, *, total_capital_usd: float) -> list[MechanismTrialSpec]:
        rows = [
            *self._yield_specs(now=snapshot.completed_at, total_capital_usd=total_capital_usd),
            *self._maker_specs(snapshot, total_capital_usd=total_capital_usd),
            *self._volatility_specs(snapshot, total_capital_usd=total_capital_usd),
            *self._liquidation_specs(snapshot, total_capital_usd=total_capital_usd),
            *self._location_specs(now=snapshot.completed_at, total_capital_usd=total_capital_usd),
        ]
        rows.sort(key=lambda row: row.predicted_net_return, reverse=True)
        return rows

    def _next_yield_observation(self, trial: MechanismForwardTrial) -> YieldObservation | None:
        protocol = str(trial.settlement_payload.get("protocol") or "")
        asset = str(trial.settlement_payload.get("asset") or trial.asset).upper()
        rows = [
            row for row in self.yield_service.observations()
            if row.protocol == protocol and row.asset.upper() == asset and row.observed_at >= trial.due_at
        ]
        return min(rows, key=lambda row: row.observed_at) if rows else None

    def _settle_yield(self, trial: MechanismForwardTrial) -> MechanismSettlementResult | None:
        exit_observation = self._next_yield_observation(trial)
        if exit_observation is None:
            return None
        entry_net = float(trial.settlement_payload.get("entry_net_apy") or 0.0)
        annualized_cost = (exit_observation.entry_exit_cost_bps / 10_000.0) * 8760.0 / max(1.0, trial.due_at.timestamp() - trial.source_observed_at.timestamp()) * 3600.0
        risk = (
            exit_observation.credit_or_protocol_risk_haircut_apy
            + exit_observation.slashing_or_liquidation_risk_haircut_apy
            + exit_observation.incentive_decay_haircut_apy
        )
        exit_net = exit_observation.gross_apy - annualized_cost - risk
        hours = (trial.due_at - trial.source_observed_at).total_seconds() / 3600.0
        gross = statistics.fmean([entry_net, exit_net]) * hours / 8760.0
        if exit_observation.capacity_usd < trial.capital_usd:
            net = -max(0.0, float(trial.settlement_payload.get("entry_exit_cost_bps") or 0.0)) / 10_000.0
            exit_ok = False
        else:
            net = gross
            exit_ok = True
        return MechanismSettlementResult(
            matured_at=exit_observation.observed_at,
            gross_return=gross,
            net_return=net,
            settlement_method="realized_yield_accrual_plus_exit_liquidity",
            detail={
                "exit_capacity_usd": exit_observation.capacity_usd,
                "exit_liquidity_sufficient": exit_ok,
                "exit_net_apy": exit_net,
            },
        )

    def _quote_after(self, *, venue: str, asset: str, due_at: datetime, market_kind: str | None = None, symbol: str | None = None) -> MarketQuote | None:
        table = self.store.market_quotes
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(table.c.payload_json)
                .where(table.c.venue == venue)
                .where(table.c.asset == asset.upper())
                .where(table.c.observed_at >= due_at.isoformat())
                .order_by(table.c.id)
                .limit(200)
            ).scalars())
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            if market_kind is not None and quote.market_kind.value != market_kind:
                continue
            if symbol is not None and quote.symbol != symbol:
                continue
            return quote
        return None

    def _trades_between(self, trial: MechanismForwardTrial) -> list[SourceEventObservation]:
        table = self.source_plane.events.rows
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(table.c.payload_json)
                .where(table.c.lane_id == "liquidity_provision")
                .where(table.c.event_type == "public_trade")
                .where(table.c.event_at >= trial.source_observed_at.isoformat())
                .where(table.c.event_at <= trial.due_at.isoformat())
                .order_by(table.c.id)
                .limit(5000)
            ).scalars())
        return [SourceEventObservation.model_validate_json(payload) for payload in payloads]

    def _settle_maker(self, trial: MechanismForwardTrial) -> MechanismSettlementResult | None:
        payload = trial.settlement_payload
        venue = str(payload.get("venue") or "")
        asset = str(payload.get("asset") or trial.asset)
        symbol = str(payload.get("symbol") or "")
        market_kind = str(payload.get("market_kind") or "")
        bid = float(payload.get("bid") or 0.0)
        ask = float(payload.get("ask") or 0.0)
        mid = float(payload.get("mid") or 0.0)
        if not venue or not symbol or bid <= 0 or ask <= bid or mid <= 0:
            return None
        trades = [
            event for event in self._trades_between(trial)
            if (event.asset or "").upper() == asset.upper()
            and str(event.payload.get("venue") or "") == venue
            and str(event.payload.get("symbol") or "") == symbol
        ]
        exit_quote = self._quote_after(
            venue=venue,
            asset=asset,
            due_at=trial.due_at,
            market_kind=market_kind or None,
            symbol=symbol,
        )
        if exit_quote is None:
            return None
        bid_filled = any(
            str(event.payload.get("aggressor_side") or "").lower() == "sell"
            and float(event.payload.get("price") or math.inf) <= bid
            for event in trades
        )
        ask_filled = any(
            str(event.payload.get("aggressor_side") or "").lower() == "buy"
            and float(event.payload.get("price") or 0.0) >= ask
            for event in trades
        )
        if bid_filled and ask_filled:
            gross = (ask - bid) / mid
        elif bid_filled:
            gross = (exit_quote.mid - bid) / mid
        elif ask_filled:
            gross = (ask - exit_quote.mid) / mid
        else:
            gross = 0.0
        fee_buffer = float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0
        net = gross - fee_buffer if (bid_filled or ask_filled) else 0.0
        return MechanismSettlementResult(
            matured_at=exit_quote.observed_at,
            gross_return=gross,
            net_return=net,
            settlement_method="shadow_post_only_fill_plus_inventory_mark",
            detail={
                "venue": venue,
                "trade_count": len(trades),
                "bid_filled": bid_filled,
                "ask_filled": ask_filled,
                "exit_mid": exit_quote.mid,
                "empirical_fill_observed": bid_filled or ask_filled,
            },
        )

    def _next_option(self, trial: MechanismForwardTrial) -> OptionQuoteObservation | None:
        payload = trial.settlement_payload
        expiry = str(payload.get("expiry") or "")
        rows = [
            row for row in self.volatility_service.observations()
            if row.venue == str(payload.get("venue") or "")
            and row.underlying == str(payload.get("underlying") or trial.asset).upper()
            and row.expiry.isoformat() == expiry
            and abs(row.strike - float(payload.get("strike") or 0.0)) < 1e-9
            and row.option_type == str(payload.get("option_type") or "")
            and row.observed_at >= trial.due_at
        ]
        return min(rows, key=lambda row: row.observed_at) if rows else None

    def _settle_volatility(self, trial: MechanismForwardTrial) -> MechanismSettlementResult | None:
        option = self._next_option(trial)
        if option is None:
            return None
        payload = trial.settlement_payload
        entry_mid = float(payload.get("entry_mid") or 0.0)
        if entry_mid <= 0:
            return None
        exit_mid = (option.bid + option.ask) / 2.0
        option_return = exit_mid / entry_mid - 1.0
        direction = str(payload.get("direction") or "")
        directional = option_return if direction == "long_volatility" else -option_return
        underlying_entry = float(payload.get("underlying_entry_price") or 0.0)
        underlying_exit = self._quote_after(
            venue=next((q.venue for q in self.store.load_scan(self.store.latest_scan_id()).market_quotes if q.asset.upper() == trial.asset and q.market_kind == MarketKind.SPOT), "Coinbase") if hasattr(self.store, "latest_scan_id") else "Coinbase",
            asset=trial.asset,
            due_at=trial.due_at,
            market_kind=MarketKind.SPOT.value,
        )
        underlying_move = 0.0
        if underlying_exit is not None and underlying_entry > 0:
            underlying_move = underlying_exit.mid / underlying_entry - 1.0
        hedge_cost = float(payload.get("hedge_cost_return") or 0.0)
        spread = float(payload.get("spread_fraction") or 0.0)
        residual_delta_penalty = abs(underlying_move) * abs(float(payload.get("entry_delta") or 0.0)) * 0.25
        net = directional - hedge_cost - spread - residual_delta_penalty
        return MechanismSettlementResult(
            matured_at=option.observed_at,
            gross_return=directional,
            net_return=net,
            settlement_method="option_mark_forward_with_delta_hedge_cost_and_residual_penalty",
            detail={
                "entry_option_mid": entry_mid,
                "exit_option_mid": exit_mid,
                "option_mark_return": option_return,
                "underlying_return": underlying_move,
                "residual_delta_penalty": residual_delta_penalty,
                "hedge_cost_return": hedge_cost,
            },
        )

    def _settle_liquidation(self, trial: MechanismForwardTrial) -> MechanismSettlementResult | None:
        payload = trial.settlement_payload
        venue = str(payload.get("venue") or "")
        asset = str(payload.get("asset") or trial.asset)
        symbol = str(payload.get("symbol") or "")
        entry = float(payload.get("entry_price") or 0.0)
        direction = str(payload.get("direction") or "")
        if entry <= 0 or direction not in {"long", "short"}:
            return None
        quote = self._quote_after(
            venue=venue,
            asset=asset,
            due_at=trial.due_at,
            market_kind=MarketKind.PERPETUAL.value,
            symbol=symbol or None,
        )
        if quote is None:
            return None
        gross = quote.mid / entry - 1.0 if direction == "long" else 1.0 - quote.mid / entry
        cost = float(payload.get("cost_return") or 0.0)
        net = gross - cost
        return MechanismSettlementResult(
            matured_at=quote.observed_at,
            gross_return=gross,
            net_return=net,
            settlement_method="observed_liquidation_price_to_recovery_mark",
            detail={
                "event_id": payload.get("event_id"),
                "entry_price": entry,
                "recovery_price": quote.mid,
                "direction": direction,
                "capture_assumed": False,
                "paper_backstop_shadow": True,
            },
        )

    def _settle_location(self, trial: MechanismForwardTrial) -> MechanismSettlementResult | None:
        payload = trial.settlement_payload
        venue = str(payload.get("venue") or "")
        asset = str(payload.get("asset") or trial.asset).upper()
        table = self.store.opportunities
        with self.store.engine.connect() as db:
            raws = list(db.execute(
                select(table.c.payload_json)
                .where(table.c.observed_at > trial.source_observed_at.isoformat())
                .where(table.c.observed_at <= trial.due_at.isoformat())
                .order_by(table.c.id)
                .limit(5000)
            ).scalars())
        from inefficiency_engine.models import Opportunity
        opportunities = [Opportunity.model_validate_json(raw) for raw in raws]
        matching = [
            opportunity for opportunity in opportunities
            if opportunity.asset.upper() == asset
            and opportunity.net_annualized_return > 0
            and any(leg.venue == venue for leg in opportunity.legs)
        ]
        # A completed forward window is evidence even when there were zero opportunities.
        latest_market_at = None
        with self.store.engine.connect() as db:
            latest_market_at = db.execute(select(self.store.market_quotes.c.observed_at).order_by(self.store.market_quotes.c.id.desc()).limit(1)).scalar_one_or_none()
        if latest_market_at is None or datetime.fromisoformat(str(latest_market_at)) < trial.due_at:
            return None
        hours = (trial.due_at - trial.source_observed_at).total_seconds() / 3600.0
        opportunity_value = sum(
            max(0.0, item.net_annualized_return) * hours / 8760.0 * 0.10
            for item in matching
        )
        transfer_cost_return = float(payload.get("transfer_cost_usd") or 0.0) / trial.capital_usd
        latency_penalty = min(0.01, float(payload.get("transfer_latency_seconds") or 0.0) / 86_400.0 * 0.001)
        net = opportunity_value - transfer_cost_return - latency_penalty
        return MechanismSettlementResult(
            matured_at=trial.due_at,
            gross_return=opportunity_value,
            net_return=net,
            settlement_method="forward_location_opportunity_incidence_minus_transfer_cost_latency",
            detail={
                "venue": venue,
                "future_positive_opportunity_count": len(matching),
                "transfer_cost_return": transfer_cost_return,
                "latency_penalty": latency_penalty,
            },
        )

    def settle_trial(self, trial: MechanismForwardTrial) -> MechanismSettlementResult | None:
        if trial.mechanism_id == "yield":
            return self._settle_yield(trial)
        if trial.mechanism_id == "liquidity_provision":
            return self._settle_maker(trial)
        if trial.mechanism_id == "volatility":
            return self._settle_volatility(trial)
        if trial.mechanism_id == "liquidation_distress":
            return self._settle_liquidation(trial)
        if trial.mechanism_id == "capital_location_settlement":
            return self._settle_location(trial)
        return None

    def _outcome(self, trial: MechanismForwardTrial, settlement: MechanismSettlementResult) -> MechanismForwardOutcome:
        realized_profit = trial.capital_usd * settlement.net_return
        return MechanismForwardOutcome(
            trial_id=trial.trial_id,
            mechanism_id=trial.mechanism_id,
            cohort_key=trial.cohort_key,
            asset=trial.asset,
            matured_at=settlement.matured_at,
            due_at=trial.due_at,
            predicted_net_return=trial.predicted_net_return,
            realized_gross_return=settlement.gross_return,
            realized_net_return=settlement.net_return,
            realized_profit_usd=realized_profit,
            profitable=realized_profit > 0,
            settlement_method=settlement.settlement_method,
            detail=settlement.detail,
        )

    def _candidate_from_spec(self, spec: MechanismTrialSpec) -> MechanismPaperCandidate | None:
        qualification = self.qualification(spec.cohort_key, spec.mechanism_id)
        fraction = qualification.allocation_fraction
        if fraction <= 0 or not (qualification.incremental_eligible or qualification.fully_statistically_qualified):
            return None
        anchor = qualification.mean_net_return_ci_lower if qualification.fully_statistically_qualified else qualification.mean_net_return
        if anchor is None or anchor <= 0:
            return None
        expected = min(spec.predicted_net_return, anchor if qualification.fully_statistically_qualified else anchor * 0.50)
        if expected <= 0:
            return None
        capital = spec.capital_usd * fraction
        if capital <= 0:
            return None
        candidate_id = f"mechanism:{spec.mechanism_id}:{_stable(spec.cohort_key, spec.source_observed_at.isoformat())}"
        candidate = MechanismPaperCandidate(
            candidate_id=candidate_id,
            mechanism_id=spec.mechanism_id,
            cohort_key=spec.cohort_key,
            asset=spec.asset,
            venues=spec.venues,
            observed_at=spec.source_observed_at,
            holding_hours=spec.holding_hours,
            capital_usd=capital,
            expected_net_return=expected,
            expected_profit_usd=capital * expected,
            evidence_sample_count=qualification.sample_count,
            evidence_allocation_fraction=fraction,
            settlement_payload=spec.settlement_payload,
            conflict_keys=spec.conflict_keys,
        )
        self.ledger.record_candidate(candidate)
        return candidate

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None) -> MechanismEvidenceCycle:
        capital = float(total_capital_usd or getattr(self.settings, "alpha_research_capital_usd", 250_000.0))
        snapshot = await self.core.collect_live_executability()
        matured = 0
        for trial in self.ledger.pending(now=snapshot.completed_at):
            settlement = self.settle_trial(trial)
            if settlement is None:
                continue
            self.ledger.record_outcome(self._outcome(trial, settlement))
            matured += 1

        specs = self.discover_specs(snapshot, total_capital_usd=capital)
        recorded = 0
        promoted = 0
        for spec in specs:
            if not self.ledger.has_open_cohort(spec.cohort_key):
                trial = MechanismForwardTrial(
                    mechanism_id=spec.mechanism_id,
                    cohort_key=spec.cohort_key,
                    asset=spec.asset,
                    venues=spec.venues,
                    source_observed_at=spec.source_observed_at,
                    due_at=spec.source_observed_at + timedelta(hours=spec.holding_hours),
                    capital_usd=spec.capital_usd,
                    predicted_net_return=spec.predicted_net_return,
                    predicted_profit_usd=spec.capital_usd * spec.predicted_net_return,
                    settlement_payload=spec.settlement_payload,
                    conflict_keys=spec.conflict_keys,
                )
                self.ledger.record_trial(trial)
                recorded += 1
            if self._candidate_from_spec(spec) is not None:
                promoted += 1

        by_mechanism: dict[str, dict[str, object]] = {}
        for mechanism_id in MECHANISM_IDS:
            outcomes = self.ledger.outcomes(mechanism_id=mechanism_id)
            cohorts = sorted({row.cohort_key for row in outcomes})
            qualifications = [self.qualification(cohort, mechanism_id) for cohort in cohorts]
            by_mechanism[mechanism_id] = {
                "outcome_count": len(outcomes),
                "cohort_count": len(cohorts),
                "qualified_cohort_count": sum(
                    q.incremental_eligible or q.fully_statistically_qualified for q in qualifications
                ),
                "full_qualified_cohort_count": sum(q.fully_statistically_qualified for q in qualifications),
                "paper_execution_capable": True,
            }
        return MechanismEvidenceCycle(
            observed_at=snapshot.completed_at,
            trials_recorded=recorded,
            outcomes_matured=matured,
            current_specs=len(specs),
            promoted_candidates=promoted,
            by_mechanism=by_mechanism,
        )

    def promoted_candidates(self, *, max_age_hours: float = 24.0) -> list[MechanismPaperCandidate]:
        rows = self.ledger.latest_candidates(max_age_hours=max_age_hours)
        return [
            row for row in rows
            if (q := self.qualification(row.cohort_key, row.mechanism_id)).incremental_eligible
            or q.fully_statistically_qualified
        ]

    def promoted_proxy_candidates(self, *, total_capital_usd: float) -> list[UnifiedPaperCandidate]:
        rows: list[UnifiedPaperCandidate] = []
        for item in self.promoted_candidates(max_age_hours=24.0):
            capital = min(item.capital_usd, total_capital_usd * 0.05)
            if capital <= 0 or item.expected_net_return <= 0:
                continue
            rows.append(UnifiedPaperCandidate(
                candidate_id=item.candidate_id,
                family="alpha",  # internal risk/allocation proxy; rewritten to mechanism in the final plan
                strategy=f"mechanism:{item.mechanism_id}:{_stable(item.cohort_key)}",
                asset=item.asset,
                venues=item.venues,
                capital_required_usd=capital,
                notional_usd_per_leg=capital,
                expected_profit_usd_per_deployment=capital * item.expected_net_return,
                expected_return_on_reserved_capital=item.expected_net_return,
                modeled_holding_hours=item.holding_hours,
                source_return_metric="mechanism_forward_evidence_net_return",
                source_return_value=item.expected_net_return,
                exposure_kind="market_neutral",
                source_observed_at=item.observed_at,
                instrument_symbol=item.asset,
                instrument_market_kind="mechanism",
                entry_reference_price=1.0,
                modeled_roundtrip_cost_return=0.0,
                conflict_keys=item.conflict_keys,
                evidence_id=item.candidate_id,
                opportunity_id=item.mechanism_id,
                allocation_eligible=True,
                executable_eligible=False,
                paper_only=True,
            ))
        return rows

    def mechanism_allocation_from_proxy(self, allocation: UnifiedPaperAllocation) -> UnifiedPaperAllocation:
        mechanism_id = allocation.opportunity_id
        if not mechanism_id or mechanism_id not in MECHANISM_IDS:
            return allocation
        return allocation.model_copy(update={"family": "mechanism"})

    def canonical_trial_from_candidate(self, candidate_id: str, *, plan_observed_at: datetime):
        from inefficiency_engine.allocation_certification import PaperAllocationTrial
        candidate = self.ledger.candidate(candidate_id)
        if candidate is None:
            return None
        source = candidate.observed_at
        due = source + timedelta(hours=candidate.holding_hours)
        return PaperAllocationTrial(
            plan_observed_at=plan_observed_at,
            candidate_id=candidate.candidate_id,
            family="mechanism",
            strategy=f"mechanism:{candidate.mechanism_id}:{_stable(candidate.cohort_key)}",
            asset=candidate.asset,
            venues=candidate.venues,
            exposure_kind="market_neutral",
            capital_required_usd=candidate.capital_usd,
            notional_usd=candidate.capital_usd,
            predicted_profit_usd=candidate.expected_profit_usd,
            predicted_return_on_reserved_capital=candidate.expected_net_return,
            source_observed_at=source,
            due_at=due,
            instrument_symbol=candidate.asset,
            instrument_market_kind="mechanism",
            entry_reference_price=1.0,
            modeled_roundtrip_cost_return=0.0,
            settlement_supported=True,
            settlement_method=f"mechanism:{candidate.mechanism_id}",
            settlement_blocker=None,
            cohort_key=f"mechanism|{candidate.cohort_key}",
        )

    def settle_canonical_candidate(self, candidate_id: str, *, due_at: datetime | None = None) -> MechanismSettlementResult | None:
        candidate = self.ledger.candidate(candidate_id)
        if candidate is None:
            return None
        trial = MechanismForwardTrial(
            trial_id=f"canonical-{_stable(candidate_id, due_at or '')}",
            mechanism_id=candidate.mechanism_id,
            cohort_key=candidate.cohort_key,
            asset=candidate.asset,
            venues=candidate.venues,
            source_observed_at=candidate.observed_at,
            due_at=due_at or candidate.observed_at + timedelta(hours=candidate.holding_hours),
            capital_usd=candidate.capital_usd,
            predicted_net_return=candidate.expected_net_return,
            predicted_profit_usd=candidate.expected_profit_usd,
            settlement_payload=candidate.settlement_payload,
            conflict_keys=candidate.conflict_keys,
        )
        return self.settle_trial(trial)

    def readiness_summary(self) -> dict[str, dict[str, object]]:
        result: dict[str, dict[str, object]] = {}
        current = self.promoted_candidates(max_age_hours=24.0)
        for mechanism_id in MECHANISM_IDS:
            outcomes = self.ledger.outcomes(mechanism_id=mechanism_id)
            cohorts = sorted({row.cohort_key for row in outcomes})
            qualifications = [self.qualification(cohort, mechanism_id) for cohort in cohorts]
            promoted = [row for row in current if row.mechanism_id == mechanism_id]
            result[mechanism_id] = {
                "paper_execution_capable": True,
                "forward_outcome_count": len(outcomes),
                "cohort_count": len(cohorts),
                "incremental_qualified_cohort_count": sum(q.incremental_eligible for q in qualifications),
                "full_qualified_cohort_count": sum(q.fully_statistically_qualified for q in qualifications),
                "current_promoted_candidate_count": len(promoted),
                "currently_qualified": bool(promoted),
            }
        return result

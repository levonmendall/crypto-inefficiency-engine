from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, Opportunity, OrderBookSnapshot, Strategy
from inefficiency_engine.research_mechanisms import CapitalLocationPlan


WorkerResearchState = Literal[
    "healthy_current",
    "waiting_scheduled",
    "late",
    "stalled",
    "failed",
    "unknown",
]


def classify_research_worker_state(
    *,
    now: datetime,
    heartbeat_at: datetime | None,
    heartbeat_state: str | None,
    error_type: str | None,
    last_cycle_at: datetime | None,
    expected_interval_seconds: float,
    stale_after_seconds: float,
) -> WorkerResearchState:
    """Classify cadence separately from process liveness.

    A healthy worker that is simply waiting for its predeclared staggered research
    cycle is not an alert. Once the expected research deadline passes it becomes
    late, and only a stale heartbeat becomes stalled. Explicit worker/research
    errors are failed immediately.
    """

    if error_type or heartbeat_state in {"error", "stopped"}:
        return "failed"
    if heartbeat_at is None:
        return "unknown"
    heartbeat_age = max(0.0, (now - heartbeat_at).total_seconds())
    if heartbeat_age > max(1.0, stale_after_seconds):
        return "stalled"
    if heartbeat_state not in {"starting", "running", "success", "completed"}:
        return "unknown"
    if last_cycle_at is None:
        return "healthy_current"
    due_at = last_cycle_at + timedelta(seconds=max(1.0, expected_interval_seconds))
    if now < due_at:
        return "waiting_scheduled"
    grace = max(30.0, expected_interval_seconds * 0.35)
    if now <= due_at + timedelta(seconds=grace):
        return "late"
    return "late" if heartbeat_age <= max(1.0, stale_after_seconds) else "stalled"


class RejectionFunnelSnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mechanism_id: str
    observed_at: datetime
    raw_candidate_count: int = Field(default=0, ge=0)
    emitted_candidate_count: int = Field(default=0, ge=0)
    best_gross_economics: float | None = None
    best_cost_economics: float | None = None
    best_net_economics: float | None = None
    required_net_economics: float | None = None
    gap_to_hurdle: float | None = None
    economics_unit: Literal["annualized_return", "horizon_return"]
    dominant_rejection_gate: str
    rejection_gate_counts: dict[str, int] = Field(default_factory=dict)
    best_candidate_reference: str | None = None
    paper_only: bool = True


class CapitalLocationForwardTrial(BaseModel):
    trial_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    due_at: datetime
    horizon_hours: float = Field(gt=0)
    reserve_capital_usd: float = Field(gt=0)
    recommendations: list[dict[str, object]]
    paper_only: bool = True


class CapitalLocationForwardOutcome(BaseModel):
    outcome_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trial_id: str
    observed_at: datetime
    due_at: datetime
    matured_at: datetime
    future_positive_opportunity_count: int = Field(default=0, ge=0)
    matched_opportunity_count: int = Field(default=0, ge=0)
    recommended_weighted_option_value: float = 0.0
    equal_weight_option_value: float = 0.0
    incremental_option_value: float = 0.0
    transfer_evidence_complete: bool = False
    decision_grade: bool = False
    paper_only: bool = True


class MakerShadowTrial(BaseModel):
    trial_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    venue: str
    asset: str
    market_kind: str
    symbol: str
    observed_at: datetime
    due_at: datetime
    bid_price: float = Field(gt=0)
    ask_price: float = Field(gt=0)
    mid_price: float = Field(gt=0)
    visible_bid_depth_usd: float = Field(ge=0)
    visible_ask_depth_usd: float = Field(ge=0)
    queue_position_observable: bool = False
    paper_only: bool = True


class MakerShadowOutcome(BaseModel):
    outcome_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trial_id: str
    matured_at: datetime
    bid_crossed_through: bool = False
    ask_crossed_through: bool = False
    queue_fill_confirmed: bool = False
    bid_post_move_bps: float | None = None
    ask_post_move_bps: float | None = None
    queue_position_observable: bool = False
    empirical_for_adverse_selection: bool = False
    decision_grade: bool = False
    paper_only: bool = True


def _payload(value: BaseModel) -> tuple[str, str]:
    raw = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return raw, hashlib.sha256(raw.encode()).hexdigest()


class ResearchClosureLedger:
    """Append-only diagnostics and forward research that cannot authorize allocation."""

    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.rejections = Table(
            "research_rejection_funnel",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_id", String(64), nullable=False, unique=True),
            Column("mechanism_id", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.location_trials = Table(
            "capital_location_forward_trials",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("observed_at", Text, nullable=False),
            Column("due_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.location_outcomes = Table(
            "capital_location_forward_outcomes",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("outcome_id", String(64), nullable=False, unique=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("matured_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.maker_trials = Table(
            "maker_shadow_trials",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("venue", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("market_kind", Text, nullable=False),
            Column("symbol", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("due_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.maker_outcomes = Table(
            "maker_shadow_outcomes",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("outcome_id", String(64), nullable=False, unique=True),
            Column("trial_id", String(64), nullable=False, unique=True),
            Column("matured_at", Text, nullable=False),
            Column("queue_fill_confirmed", Boolean, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_research_rejection_mechanism", self.rejections.c.mechanism_id, self.rejections.c.id)
        Index("ix_location_trial_due", self.location_trials.c.due_at)
        Index("ix_maker_trial_due", self.maker_trials.c.due_at)
        Index("ix_maker_trial_instrument", self.maker_trials.c.venue, self.maker_trials.c.symbol, self.maker_trials.c.id)
        metadata.create_all(store.engine)

    def _insert_once(self, table, unique_column, unique_value: str, values: dict[str, object]) -> None:
        with self.store.engine.begin() as db:
            exists = db.execute(select(unique_column).where(unique_column == unique_value)).scalar_one_or_none()
            if exists is None:
                db.execute(insert(table), values)

    def record_rejection(self, row: RejectionFunnelSnapshot) -> None:
        raw, lineage = _payload(row)
        self._insert_once(self.rejections, self.rejections.c.snapshot_id, row.snapshot_id, {
            "snapshot_id": row.snapshot_id,
            "mechanism_id": row.mechanism_id,
            "observed_at": row.observed_at.isoformat(),
            "payload_json": raw,
            "lineage_hash": lineage,
        })

    def latest_rejection(self, mechanism_id: str) -> RejectionFunnelSnapshot | None:
        with self.store.engine.connect() as db:
            raw = db.execute(
                select(self.rejections.c.payload_json)
                .where(self.rejections.c.mechanism_id == mechanism_id)
                .order_by(self.rejections.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        return RejectionFunnelSnapshot.model_validate_json(raw) if raw else None

    def record_location_trial(self, row: CapitalLocationForwardTrial) -> None:
        raw, lineage = _payload(row)
        self._insert_once(self.location_trials, self.location_trials.c.trial_id, row.trial_id, {
            "trial_id": row.trial_id,
            "observed_at": row.observed_at.isoformat(),
            "due_at": row.due_at.isoformat(),
            "payload_json": raw,
            "lineage_hash": lineage,
        })

    def record_location_outcome(self, row: CapitalLocationForwardOutcome) -> None:
        raw, lineage = _payload(row)
        self._insert_once(self.location_outcomes, self.location_outcomes.c.trial_id, row.trial_id, {
            "outcome_id": row.outcome_id,
            "trial_id": row.trial_id,
            "matured_at": row.matured_at.isoformat(),
            "payload_json": raw,
            "lineage_hash": lineage,
        })

    def location_trials_all(self) -> list[CapitalLocationForwardTrial]:
        with self.store.engine.connect() as db:
            raws = list(db.execute(select(self.location_trials.c.payload_json).order_by(self.location_trials.c.id)).scalars())
        return [CapitalLocationForwardTrial.model_validate_json(raw) for raw in raws]

    def location_outcomes_all(self) -> list[CapitalLocationForwardOutcome]:
        with self.store.engine.connect() as db:
            raws = list(db.execute(select(self.location_outcomes.c.payload_json).order_by(self.location_outcomes.c.id)).scalars())
        return [CapitalLocationForwardOutcome.model_validate_json(raw) for raw in raws]

    def record_maker_trial(self, row: MakerShadowTrial) -> None:
        raw, lineage = _payload(row)
        self._insert_once(self.maker_trials, self.maker_trials.c.trial_id, row.trial_id, {
            "trial_id": row.trial_id,
            "venue": row.venue,
            "asset": row.asset,
            "market_kind": row.market_kind,
            "symbol": row.symbol,
            "observed_at": row.observed_at.isoformat(),
            "due_at": row.due_at.isoformat(),
            "payload_json": raw,
            "lineage_hash": lineage,
        })

    def record_maker_outcome(self, row: MakerShadowOutcome) -> None:
        raw, lineage = _payload(row)
        self._insert_once(self.maker_outcomes, self.maker_outcomes.c.trial_id, row.trial_id, {
            "outcome_id": row.outcome_id,
            "trial_id": row.trial_id,
            "matured_at": row.matured_at.isoformat(),
            "queue_fill_confirmed": row.queue_fill_confirmed,
            "payload_json": raw,
            "lineage_hash": lineage,
        })

    def maker_trials_all(self) -> list[MakerShadowTrial]:
        with self.store.engine.connect() as db:
            raws = list(db.execute(select(self.maker_trials.c.payload_json).order_by(self.maker_trials.c.id)).scalars())
        return [MakerShadowTrial.model_validate_json(raw) for raw in raws]

    def maker_outcomes_all(self) -> list[MakerShadowOutcome]:
        with self.store.engine.connect() as db:
            raws = list(db.execute(select(self.maker_outcomes.c.payload_json).order_by(self.maker_outcomes.c.id)).scalars())
        return [MakerShadowOutcome.model_validate_json(raw) for raw in raws]


class ResearchClosureService:
    """Closes diagnostic/forward-research gaps without relaxing any promotion gate."""

    def __init__(self, store, settings):
        self.store = store
        self.settings = settings
        self.ledger = ResearchClosureLedger(store)

    def _rejection_row(
        self,
        mechanism_id: str,
        *,
        observed_at: datetime,
        candidates: list[tuple[float, float, float, str]],
        emitted_count: int,
        required: float,
        unit: Literal["annualized_return", "horizon_return"],
        no_candidate_gate: str,
    ) -> RejectionFunnelSnapshot:
        # candidates = gross, cost, net, reference
        gates: Counter[str] = Counter()
        for gross, _cost, net, _ref in candidates:
            if gross <= 0:
                gates["gross_edge_not_positive"] += 1
            elif net <= required:
                gates["net_return_hurdle"] += 1
            else:
                gates["detector_emitted"] += 1
        if not candidates:
            gate = no_candidate_gate
        elif emitted_count > 0:
            gate = "detector_emitted"
        else:
            gate = gates.most_common(1)[0][0] if gates else "unknown"
        best = max(candidates, key=lambda item: item[2]) if candidates else None
        return RejectionFunnelSnapshot(
            mechanism_id=mechanism_id,
            observed_at=observed_at,
            raw_candidate_count=len(candidates),
            emitted_candidate_count=max(0, emitted_count),
            best_gross_economics=best[0] if best else None,
            best_cost_economics=best[1] if best else None,
            best_net_economics=best[2] if best else None,
            required_net_economics=required,
            gap_to_hurdle=(best[2] - required) if best else None,
            economics_unit=unit,
            dominant_rejection_gate=gate,
            rejection_gate_counts=dict(gates),
            best_candidate_reference=best[3] if best else None,
        )

    def record_rejection_funnels(
        self,
        *,
        market_quotes: list[MarketQuote],
        funding_quotes: list[FundingQuote],
        opportunities: list[Opportunity],
        order_books: list[OrderBookSnapshot],
        microstructure_emitted_count: int,
        observed_at: datetime,
    ) -> dict[str, RejectionFunnelSnapshot]:
        annual_factor = 24.0 * 365.0
        required_annual = float(self.settings.min_net_annualized_return)
        price_candidates: list[tuple[float, float, float, str]] = []
        spots = [q for q in market_quotes if q.market_kind == MarketKind.SPOT and q.quote_currency and q.bid and q.ask]
        for buy in spots:
            for sell in spots:
                if buy is sell or buy.venue == sell.venue or buy.asset != sell.asset:
                    continue
                if (buy.quote_currency or "").upper() != (sell.quote_currency or "").upper():
                    continue
                holding = max(1e-6, float(self.settings.spot_dislocation_holding_hours))
                gross_hour = ((float(sell.bid) / float(buy.ask)) - 1.0) / holding
                cost_hour = ((float(self.settings.pair_roundtrip_cost_bps) / 10_000.0) / holding) + (
                    float(self.settings.safety_buffer_bps_per_hour) / 10_000.0
                )
                price_candidates.append((
                    gross_hour * annual_factor,
                    cost_hour * annual_factor,
                    (gross_hour - cost_hour) * annual_factor,
                    f"{buy.asset}:{buy.venue}->{sell.venue}",
                ))

        carry_candidates: list[tuple[float, float, float, str]] = []
        funding_by_asset: dict[str, list[FundingQuote]] = defaultdict(list)
        for quote in funding_quotes:
            funding_by_asset[quote.asset].append(quote)
        for asset, rows in funding_by_asset.items():
            for long_quote in rows:
                for short_quote in rows:
                    if long_quote is short_quote or long_quote.venue == short_quote.venue:
                        continue
                    if long_quote.quote_currency and short_quote.quote_currency and long_quote.quote_currency.upper() != short_quote.quote_currency.upper():
                        continue
                    gross_hour = short_quote.hourly_rate - long_quote.hourly_rate
                    holding = max(1e-6, float(self.settings.default_holding_hours))
                    cost_hour = ((float(self.settings.pair_roundtrip_cost_bps) / 10_000.0) / holding) + (
                        float(self.settings.safety_buffer_bps_per_hour) / 10_000.0
                    )
                    carry_candidates.append((
                        gross_hour * annual_factor,
                        cost_hour * annual_factor,
                        (gross_hour - cost_hour) * annual_factor,
                        f"funding:{asset}:{long_quote.venue}->{short_quote.venue}",
                    ))
        market_by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in market_quotes:
            market_by_asset[quote.asset].append(quote)
        for asset, rows in market_by_asset.items():
            spots_for_asset = [q for q in rows if q.market_kind == MarketKind.SPOT]
            derivatives = [q for q in rows if q.market_kind in {MarketKind.PERPETUAL, MarketKind.FUTURE}]
            for spot in spots_for_asset:
                for derivative in derivatives:
                    if spot.quote_currency and derivative.quote_currency and spot.quote_currency.upper() != derivative.quote_currency.upper():
                        continue
                    observed = min(spot.observed_at, derivative.observed_at)
                    if derivative.market_kind == MarketKind.FUTURE:
                        if derivative.expires_at is None or derivative.expires_at <= observed:
                            continue
                        holding = max(1e-6, (derivative.expires_at - observed).total_seconds() / 3600.0)
                    else:
                        holding = max(1e-6, float(self.settings.default_holding_hours))
                    gross_fraction = (derivative.mid / spot.mid) - 1.0
                    gross_hour = gross_fraction / holding
                    cost_hour = ((float(self.settings.pair_roundtrip_cost_bps) / 10_000.0) / holding) + (
                        float(self.settings.safety_buffer_bps_per_hour) / 10_000.0
                    )
                    carry_candidates.append((
                        gross_hour * annual_factor,
                        cost_hour * annual_factor,
                        (gross_hour - cost_hour) * annual_factor,
                        f"basis:{asset}:{spot.venue}->{derivative.venue}:{derivative.market_kind.value}",
                    ))

        micro_candidates: list[tuple[float, float, float, str]] = []
        levels = max(1, int(getattr(self.settings, "alpha_microstructure_depth_levels", 5)))
        min_imbalance = float(getattr(self.settings, "alpha_microstructure_min_abs_imbalance", 0.20))
        return_scale = float(getattr(self.settings, "alpha_microstructure_return_scale", 0.012))
        max_return = float(getattr(self.settings, "alpha_microstructure_max_expected_return", 0.006))
        min_current = float(getattr(self.settings, "alpha_min_current_net_return", 0.0005))
        cost_floor = float(getattr(self.settings, "alpha_research_cost_floor_bps", 25.0)) / 10_000.0
        for book in order_books:
            bids = sorted(book.bids, key=lambda item: item.price, reverse=True)[:levels]
            asks = sorted(book.asks, key=lambda item: item.price)[:levels]
            bid_depth = sum(item.price * item.size for item in bids)
            ask_depth = sum(item.price * item.size for item in asks)
            total = bid_depth + ask_depth
            if total <= 0:
                continue
            imbalance = (bid_depth - ask_depth) / total
            best_bid = max(item.price for item in book.bids)
            best_ask = min(item.price for item in book.asks)
            spread = (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
            gross = min(max_return, abs(imbalance) * return_scale)
            cost = max(cost_floor, spread)
            net = gross - cost
            ref = f"{book.asset}:{book.venue}:{book.symbol}:imbalance={imbalance:.4f}"
            # Keep all books in the funnel, including those below the imbalance gate.
            if abs(imbalance) < min_imbalance:
                net = min(net, min_current - abs(min_imbalance - abs(imbalance)) * return_scale)
            micro_candidates.append((gross, cost, net, ref))

        price_emitted = sum(o.strategy == Strategy.CEX_SPOT_DISLOCATION for o in opportunities)
        carry_emitted = sum(o.strategy in {Strategy.FUNDING_DISPERSION, Strategy.SPOT_PERP_BASIS, Strategy.FUTURES_BASIS} for o in opportunities)
        rows = {
            "price_discrepancy": self._rejection_row(
                "price_discrepancy", observed_at=observed_at, candidates=price_candidates,
                emitted_count=price_emitted, required=required_annual, unit="annualized_return",
                no_candidate_gate="no_comparable_cross_venue_spot_pair",
            ),
            "carry": self._rejection_row(
                "carry", observed_at=observed_at, candidates=carry_candidates,
                emitted_count=carry_emitted, required=required_annual, unit="annualized_return",
                no_candidate_gate="no_comparable_carry_pair",
            ),
            "microstructure": self._rejection_row(
                "microstructure", observed_at=observed_at, candidates=micro_candidates,
                emitted_count=microstructure_emitted_count, required=min_current, unit="horizon_return",
                no_candidate_gate="no_usable_order_book",
            ),
        }
        for row in rows.values():
            self.ledger.record_rejection(row)
        return rows

    def run_capital_location_forward_cycle(
        self,
        plan: CapitalLocationPlan,
        *,
        now: datetime,
        horizon_hours: float = 1.0,
    ) -> dict[str, object]:
        horizon_hours = max(0.25, float(horizon_hours))
        trials = self.ledger.location_trials_all()
        outcomes = self.ledger.location_outcomes_all()
        completed = {row.trial_id for row in outcomes}

        # Mature due cohorts against opportunities that became known strictly after
        # the recommendation. This preserves point-in-time forward evaluation.
        for trial in trials:
            if trial.trial_id in completed or trial.due_at > now:
                continue
            with self.store.engine.connect() as db:
                raws = list(db.execute(
                    select(self.store.opportunities.c.payload_json)
                    .where(self.store.opportunities.c.observed_at > trial.observed_at.isoformat())
                    .where(self.store.opportunities.c.observed_at <= trial.due_at.isoformat())
                    .order_by(self.store.opportunities.c.id)
                ).scalars())
            future = [Opportunity.model_validate_json(raw) for raw in raws]
            positive = [row for row in future if row.net_annualized_return > 0]
            location_values: dict[tuple[str, str], list[float]] = defaultdict(list)
            for opportunity in positive:
                for leg in opportunity.legs:
                    location_values[(leg.venue, opportunity.asset.upper())].append(opportunity.net_annualized_return)
            recs = trial.recommendations
            weighted = 0.0
            equal = 0.0
            matched = 0
            equal_weight = 1.0 / len(recs) if recs else 0.0
            for rec in recs:
                key = (str(rec.get("venue") or ""), str(rec.get("asset") or "").upper())
                values = location_values.get(key, [])
                score = statistics.fmean(values) if values else 0.0
                if values:
                    matched += len(values)
                weighted += float(rec.get("recommended_weight") or 0.0) * score
                equal += equal_weight * score
            self.ledger.record_location_outcome(CapitalLocationForwardOutcome(
                trial_id=trial.trial_id,
                observed_at=trial.observed_at,
                due_at=trial.due_at,
                matured_at=now,
                future_positive_opportunity_count=len(positive),
                matched_opportunity_count=matched,
                recommended_weighted_option_value=weighted,
                equal_weight_option_value=equal,
                incremental_option_value=weighted - equal,
                transfer_evidence_complete=False,
                decision_grade=False,
            ))

        # Only one overlapping cohort at a time, so sample count remains independent.
        refreshed_trials = self.ledger.location_trials_all()
        refreshed_outcomes = self.ledger.location_outcomes_all()
        done = {row.trial_id for row in refreshed_outcomes}
        open_trial = any(row.trial_id not in done and row.due_at > now for row in refreshed_trials)
        if plan.recommendations and not open_trial:
            self.ledger.record_location_trial(CapitalLocationForwardTrial(
                observed_at=now,
                due_at=now + timedelta(hours=horizon_hours),
                horizon_hours=horizon_hours,
                reserve_capital_usd=plan.reserve_capital_usd,
                recommendations=[row.model_dump(mode="json") for row in plan.recommendations],
            ))
            refreshed_trials = self.ledger.location_trials_all()

        refreshed_outcomes = self.ledger.location_outcomes_all()
        increments = [row.incremental_option_value for row in refreshed_outcomes]
        return {
            "trial_count": len(refreshed_trials),
            "outcome_count": len(refreshed_outcomes),
            "mean_incremental_option_value": statistics.fmean(increments) if increments else None,
            "positive_incremental_rate": sum(value > 0 for value in increments) / len(increments) if increments else None,
            "transfer_evidence_complete": False,
            "decision_grade": False,
        }

    def run_maker_shadow_cycle(
        self,
        books: list[OrderBookSnapshot],
        *,
        now: datetime,
        horizon_seconds: float = 60.0,
    ) -> dict[str, object]:
        horizon_seconds = max(5.0, float(horizon_seconds))
        trials = self.ledger.maker_trials_all()
        outcomes = self.ledger.maker_outcomes_all()
        completed = {row.trial_id for row in outcomes}
        current = {(b.venue, b.asset.upper(), b.market_kind.value, b.symbol): b for b in books}

        for trial in trials:
            if trial.trial_id in completed or trial.due_at > now:
                continue
            book = current.get((trial.venue, trial.asset, trial.market_kind, trial.symbol))
            if book is None:
                continue
            best_bid = max(level.price for level in book.bids)
            best_ask = min(level.price for level in book.asks)
            current_mid = (best_bid + best_ask) / 2.0
            bid_crossed = best_ask <= trial.bid_price
            ask_crossed = best_bid >= trial.ask_price
            bid_move = ((current_mid / trial.bid_price) - 1.0) * 10_000.0 if bid_crossed else None
            ask_move = ((trial.ask_price / current_mid) - 1.0) * 10_000.0 if ask_crossed else None
            self.ledger.record_maker_outcome(MakerShadowOutcome(
                trial_id=trial.trial_id,
                matured_at=now,
                bid_crossed_through=bid_crossed,
                ask_crossed_through=ask_crossed,
                queue_fill_confirmed=False,
                bid_post_move_bps=bid_move,
                ask_post_move_bps=ask_move,
                queue_position_observable=False,
                empirical_for_adverse_selection=bid_crossed or ask_crossed,
                decision_grade=False,
            ))

        existing_open = {
            (row.venue, row.asset, row.market_kind, row.symbol)
            for row in self.ledger.maker_trials_all()
            if row.trial_id not in {out.trial_id for out in self.ledger.maker_outcomes_all()} and row.due_at > now
        }
        for book in books:
            key = (book.venue, book.asset.upper(), book.market_kind.value, book.symbol)
            if key in existing_open:
                continue
            best_bid = max(level.price for level in book.bids)
            best_ask = min(level.price for level in book.asks)
            mid = (best_bid + best_ask) / 2.0
            bid_depth = best_bid * sum(level.size for level in book.bids if level.price == best_bid)
            ask_depth = best_ask * sum(level.size for level in book.asks if level.price == best_ask)
            self.ledger.record_maker_trial(MakerShadowTrial(
                venue=book.venue,
                asset=book.asset.upper(),
                market_kind=book.market_kind.value,
                symbol=book.symbol,
                observed_at=now,
                due_at=now + timedelta(seconds=horizon_seconds),
                bid_price=best_bid,
                ask_price=best_ask,
                mid_price=mid,
                visible_bid_depth_usd=bid_depth,
                visible_ask_depth_usd=ask_depth,
                queue_position_observable=False,
            ))

        all_trials = self.ledger.maker_trials_all()
        all_outcomes = self.ledger.maker_outcomes_all()
        return {
            "trial_count": len(all_trials),
            "outcome_count": len(all_outcomes),
            "crossed_through_count": sum(row.bid_crossed_through or row.ask_crossed_through for row in all_outcomes),
            "queue_fill_confirmed_count": sum(row.queue_fill_confirmed for row in all_outcomes),
            "adverse_selection_observation_count": sum(row.empirical_for_adverse_selection for row in all_outcomes),
            "queue_position_observable": False,
            "decision_grade": False,
        }

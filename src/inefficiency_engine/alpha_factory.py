from __future__ import annotations

import hashlib
import json
import math
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal, Protocol

from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, Index, insert, select

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.execution import estimate_market_order
from inefficiency_engine.models import MarketKind, MarketQuote, OpportunityLeg, Side, TradeSide


AlphaDirection = Literal["long", "short", "market_neutral"]
AlphaStage = Literal["research", "paper_qualified"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: BaseModel | dict[str, object]) -> str:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _lineage(value: BaseModel | dict[str, object]) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _wilson_lower(successes: int, total: int, z: float = 1.96) -> float | None:
    if total <= 0:
        return None
    p = successes / total
    denominator = 1.0 + z * z / total
    center = p + z * z / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return max(0.0, (center - margin) / denominator)


def _mean_lower(values: list[float], z: float = 1.96) -> float | None:
    if not values:
        return None
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean
    sd = statistics.stdev(values)
    return mean - z * sd / math.sqrt(len(values))


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, q)) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


class AlphaStrategyManifest(BaseModel):
    strategy_id: str
    family: str
    description: str
    predictive: bool
    horizons_hours: list[float]
    requires_l2_for_promotion: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class AlphaCandidate(BaseModel):
    candidate_id: str
    strategy_id: str
    family: str
    asset: str
    direction: AlphaDirection
    venue: str
    market_kind: MarketKind
    symbol: str
    observed_at: datetime
    horizon_hours: float = Field(gt=0)
    lookback_hours: float = Field(gt=0)
    entry_reference_price: float = Field(gt=0)
    expected_gross_return: float
    estimated_cost_return: float = Field(ge=0)
    expected_net_return: float
    expected_profit_usd: float
    notional_usd: float = Field(gt=0)
    capital_required_usd: float = Field(gt=0)
    confidence_score: float = Field(ge=0, le=1)
    regime: Literal["low_vol", "normal", "high_vol"]
    conflict_keys: list[str] = Field(default_factory=list)
    features: dict[str, float | int | str | bool] = Field(default_factory=dict)
    stage: AlphaStage = "research"
    paper_allocation_eligible: bool = False
    executable_eligible: bool = False
    live_execution_eligible: bool = False
    paper_only: bool = True


class AlphaForwardSignal(BaseModel):
    signal_id: str
    candidate: AlphaCandidate
    due_at: datetime
    recorded_at: datetime = Field(default_factory=_now)
    paper_only: bool = True


class AlphaForwardOutcome(BaseModel):
    signal_id: str
    strategy_id: str
    family: str
    asset: str
    direction: AlphaDirection
    venue: str
    market_kind: MarketKind
    symbol: str
    observed_at: datetime
    due_at: datetime
    matured_at: datetime
    horizon_hours: float
    regime: str
    predicted_net_return: float
    entry_price: float
    exit_price: float
    realized_gross_return: float
    realized_net_return: float
    correct_direction: bool
    paper_only: bool = True


class AlphaQualification(BaseModel):
    strategy_id: str
    family: str
    asset: str
    direction: AlphaDirection
    sample_count: int = Field(ge=0)
    positive_count: int = Field(ge=0)
    hit_rate: float | None = None
    hit_rate_ci_lower: float | None = None
    mean_realized_net_return: float | None = None
    mean_realized_net_return_ci_lower: float | None = None
    p10_realized_net_return: float | None = None
    worst_realized_net_return: float | None = None
    regime_count: int = Field(ge=0)
    regime_means: dict[str, float] = Field(default_factory=dict)
    multiple_testing_penalty_return: float = 0.0
    required_mean_lower_bound: float = 0.0
    statistically_qualified: bool = False
    blockers: list[str] = Field(default_factory=list)
    paper_allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class AlphaEvidenceCycle(BaseModel):
    cycle_id: str
    observed_at: datetime
    candidate_count: int = Field(ge=0)
    signals_recorded: int = Field(ge=0)
    outcomes_matured: int = Field(ge=0)
    paper_only: bool = True


class AlphaStrategy(Protocol):
    manifest: AlphaStrategyManifest

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings: Settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]: ...


class TimeSeriesMomentumStrategy:
    """Conservative directional research signal promoted only by forward evidence.

    The forecast is intentionally simple. The strategy's purpose is to give the
    Alpha Factory a real predictive family to test, not to smuggle a backtest into
    allocation authority. Statistical promotion is entirely forward/out-of-sample.
    """

    manifest = AlphaStrategyManifest(
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        description="Volatility-scaled time-series momentum with conservative forecast shrinkage.",
        predictive=True,
        horizons_hours=[6.0],
    )

    @staticmethod
    def _returns(quotes: list[MarketQuote]) -> list[float]:
        ordered = sorted(quotes, key=lambda item: item.observed_at)
        values: list[float] = []
        for previous, current in zip(ordered, ordered[1:]):
            if previous.mid > 0 and current.mid > 0:
                values.append(math.log(current.mid / previous.mid))
        return values

    @staticmethod
    def _regime(returns: list[float]) -> Literal["low_vol", "normal", "high_vol"]:
        if len(returns) < 2:
            return "normal"
        vol = statistics.pstdev(returns)
        if vol < 0.0015:
            return "low_vol"
        if vol > 0.008:
            return "high_vol"
        return "normal"

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings: Settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        rows: list[AlphaCandidate] = []
        horizon = max(0.25, settings.alpha_momentum_horizon_hours)
        lookback = max(horizon, settings.alpha_momentum_lookback_hours)
        shrinkage = max(0.0, min(1.0, settings.alpha_forecast_shrinkage))
        current_by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                current_by_asset[quote.asset.upper()].append(quote)

        for asset, current_quotes in current_by_asset.items():
            scored: list[tuple[float, AlphaCandidate]] = []
            for quote in current_quotes:
                key = (quote.venue, asset, quote.market_kind)
                series = [item for item in history.get(key, []) if item.observed_at <= quote.observed_at]
                if len(series) < settings.alpha_min_history_points:
                    continue
                ordered = sorted(series, key=lambda item: item.observed_at)
                cutoff = quote.observed_at - timedelta(hours=lookback)
                window = [item for item in ordered if item.observed_at >= cutoff]
                if len(window) < settings.alpha_min_history_points:
                    continue
                first = window[0]
                if first.mid <= 0 or quote.mid <= 0:
                    continue
                trailing_return = quote.mid / first.mid - 1.0
                direction: AlphaDirection = "long" if trailing_return > 0 else "short"
                if abs(trailing_return) < settings.alpha_momentum_min_abs_return:
                    continue
                if direction == "short" and quote.market_kind != MarketKind.PERPETUAL:
                    continue
                if direction == "long" and quote.market_kind == MarketKind.PERPETUAL:
                    # Prefer spot for positive directional exposure when available.
                    if any(item.market_kind == MarketKind.SPOT for item in current_quotes):
                        continue
                realized = self._returns(window)
                regime = self._regime(realized)
                realized_vol = statistics.pstdev(realized) if len(realized) >= 2 else 0.0
                scaled = trailing_return * (horizon / lookback) * shrinkage
                if direction == "short":
                    scaled = abs(scaled)
                cost_return = settings.alpha_research_cost_floor_bps / 10_000.0
                expected_net = scaled - cost_return
                if expected_net <= 0:
                    continue
                notional = min(
                    max(settings.alpha_min_notional_usd, total_capital_usd * settings.alpha_candidate_capital_fraction),
                    total_capital_usd,
                )
                capital_multiple = settings.spot_collateral_fraction if quote.market_kind == MarketKind.SPOT else settings.perp_collateral_fraction
                capital_required = max(1.0, notional * max(0.01, capital_multiple))
                confidence = min(1.0, abs(trailing_return) / max(0.01, 3.0 * max(realized_vol, 1e-6)))
                candidate = AlphaCandidate(
                    candidate_id=f"alpha:{self.manifest.strategy_id}:{asset}:{quote.venue}:{quote.market_kind.value}:{uuid.uuid4().hex[:12]}",
                    strategy_id=self.manifest.strategy_id,
                    family=self.manifest.family,
                    asset=asset,
                    direction=direction,
                    venue=quote.venue,
                    market_kind=quote.market_kind,
                    symbol=quote.symbol,
                    observed_at=quote.observed_at,
                    horizon_hours=horizon,
                    lookback_hours=lookback,
                    entry_reference_price=quote.mid,
                    expected_gross_return=scaled,
                    estimated_cost_return=cost_return,
                    expected_net_return=expected_net,
                    expected_profit_usd=notional * expected_net,
                    notional_usd=notional,
                    capital_required_usd=capital_required,
                    confidence_score=confidence,
                    regime=regime,
                    conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}"],
                    features={
                        "trailing_return": trailing_return,
                        "realized_log_return_vol": realized_vol,
                        "history_points": len(window),
                        "forecast_shrinkage": shrinkage,
                    },
                )
                scored.append((expected_net, candidate))
            if scored:
                scored.sort(key=lambda item: item[0], reverse=True)
                rows.append(scored[0][1])
        return rows


class AlphaStrategyRegistry:
    def __init__(self, strategies: list[AlphaStrategy] | None = None):
        self._strategies = strategies or [TimeSeriesMomentumStrategy()]
        ids = [item.manifest.strategy_id for item in self._strategies]
        if len(ids) != len(set(ids)):
            raise ValueError("alpha strategy ids must be unique")

    @classmethod
    def default(cls) -> "AlphaStrategyRegistry":
        return cls()

    def manifests(self) -> list[AlphaStrategyManifest]:
        return [item.manifest for item in self._strategies]

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings: Settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        rows: list[AlphaCandidate] = []
        for strategy in self._strategies:
            rows.extend(strategy.discover(snapshot, history, settings, total_capital_usd=total_capital_usd))
        rows.sort(key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        return rows


class AlphaEvidenceLedger:
    """Append-only signal/outcome ledger isolated from trading authority."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.events = Table(
            "alpha_forward_events",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_id", String(64), nullable=False, unique=True),
            Column("signal_id", String(128), nullable=False),
            Column("event_type", String(16), nullable=False),
            Column("strategy_id", Text, nullable=False),
            Column("family", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("direction", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("due_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_alpha_signal", self.events.c.signal_id)
        Index("ix_alpha_strategy_asset", self.events.c.strategy_id, self.events.c.asset)
        Index("ix_alpha_due", self.events.c.due_at)
        metadata.create_all(store.engine)

    def record_signal(self, signal: AlphaForwardSignal) -> str:
        payload = _json(signal)
        with self.store.engine.begin() as db:
            db.execute(insert(self.events), {
                "event_id": uuid.uuid4().hex,
                "signal_id": signal.signal_id,
                "event_type": "signal",
                "strategy_id": signal.candidate.strategy_id,
                "family": signal.candidate.family,
                "asset": signal.candidate.asset,
                "direction": signal.candidate.direction,
                "observed_at": signal.candidate.observed_at.isoformat(),
                "due_at": signal.due_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": _lineage(signal),
            })
        return signal.signal_id

    def record_outcome(self, outcome: AlphaForwardOutcome) -> str:
        payload = _json(outcome)
        with self.store.engine.begin() as db:
            db.execute(insert(self.events), {
                "event_id": uuid.uuid4().hex,
                "signal_id": outcome.signal_id,
                "event_type": "outcome",
                "strategy_id": outcome.strategy_id,
                "family": outcome.family,
                "asset": outcome.asset,
                "direction": outcome.direction,
                "observed_at": outcome.matured_at.isoformat(),
                "due_at": outcome.due_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": _lineage(outcome),
            })
        return outcome.signal_id

    def pending_signals(self, *, now: datetime | None = None) -> list[AlphaForwardSignal]:
        now = now or _now()
        with self.store.engine.connect() as db:
            signal_rows = list(db.execute(
                select(self.events.c.signal_id, self.events.c.payload_json)
                .where(self.events.c.event_type == "signal")
                .where(self.events.c.due_at <= now.isoformat())
                .order_by(self.events.c.id)
            ))
            completed = set(db.execute(
                select(self.events.c.signal_id).where(self.events.c.event_type == "outcome")
            ).scalars())
        return [AlphaForwardSignal.model_validate_json(payload) for signal_id, payload in signal_rows if signal_id not in completed]

    def outcomes(
        self,
        *,
        strategy_id: str | None = None,
        asset: str | None = None,
        direction: AlphaDirection | None = None,
    ) -> list[AlphaForwardOutcome]:
        query = select(self.events.c.payload_json).where(self.events.c.event_type == "outcome")
        if strategy_id is not None:
            query = query.where(self.events.c.strategy_id == strategy_id)
        if asset is not None:
            query = query.where(self.events.c.asset == asset)
        if direction is not None:
            query = query.where(self.events.c.direction == direction)
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query.order_by(self.events.c.id)).scalars())
        return [AlphaForwardOutcome.model_validate_json(item) for item in payloads]

    def summary(self) -> dict[str, object]:
        with self.store.engine.connect() as db:
            signals = list(db.execute(select(self.events.c.signal_id).where(self.events.c.event_type == "signal")).scalars())
            outcomes = list(db.execute(select(self.events.c.signal_id).where(self.events.c.event_type == "outcome")).scalars())
        return {
            "signal_count": len(signals),
            "outcome_count": len(outcomes),
            "pending_count": max(0, len(set(signals)) - len(set(outcomes))),
            "paper_only": True,
        }


class AlphaFactoryService:
    def __init__(
        self,
        core,
        store: EvidenceStore,
        registry: AlphaStrategyRegistry | None = None,
    ):
        self.core = core
        self.settings: Settings = core.settings
        self.store = store
        self.registry = registry or AlphaStrategyRegistry.default()
        self.ledger = AlphaEvidenceLedger(store)

    def manifests(self) -> list[AlphaStrategyManifest]:
        return self.registry.manifests()

    def _history(self, *, now: datetime | None = None) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        now = now or _now()
        cutoff = now - timedelta(hours=max(1.0, self.settings.alpha_history_hours))
        query = select(self.store.market_quotes.c.payload_json).where(
            self.store.market_quotes.c.observed_at >= cutoff.isoformat()
        ).order_by(self.store.market_quotes.c.observed_at)
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            grouped[(quote.venue, quote.asset.upper(), quote.market_kind)].append(quote)
        return grouped

    def discover(self, snapshot: ScanSnapshot, *, total_capital_usd: float) -> list[AlphaCandidate]:
        return self.registry.discover(
            snapshot,
            self._history(now=snapshot.completed_at),
            self.settings,
            total_capital_usd=total_capital_usd,
        )

    def qualification(self, candidate: AlphaCandidate) -> AlphaQualification:
        outcomes = self.ledger.outcomes(
            strategy_id=candidate.strategy_id,
            asset=candidate.asset,
            direction=candidate.direction,
        )
        values = [item.realized_net_return for item in outcomes]
        positives = sum(value > 0 for value in values)
        hit_lower = _wilson_lower(positives, len(values))
        mean_lower = _mean_lower(values)
        regime_values: dict[str, list[float]] = defaultdict(list)
        for item in outcomes:
            regime_values[item.regime].append(item.realized_net_return)
        regime_means = {key: statistics.fmean(rows) for key, rows in regime_values.items() if rows}
        strategy_count = max(1, len(self.registry.manifests()))
        penalty = self.settings.alpha_multiple_testing_penalty_return * math.sqrt(math.log(strategy_count + 1.0))
        required = self.settings.alpha_min_forward_mean_return + penalty
        blockers: list[str] = []
        if len(values) < self.settings.alpha_min_forward_samples:
            blockers.append("insufficient independent forward samples")
        if mean_lower is None or mean_lower <= required:
            blockers.append("forward net-return confidence lower bound is below hurdle")
        if hit_lower is None or hit_lower < self.settings.alpha_min_hit_rate_lower_bound:
            blockers.append("forward hit-rate confidence lower bound is below hurdle")
        if len(regime_means) < self.settings.alpha_min_regimes:
            blockers.append("insufficient regime coverage")
        elif any(value <= self.settings.alpha_min_regime_mean_return for value in regime_means.values()):
            blockers.append("one or more observed regimes have non-qualifying mean return")
        qualified = not blockers
        return AlphaQualification(
            strategy_id=candidate.strategy_id,
            family=candidate.family,
            asset=candidate.asset,
            direction=candidate.direction,
            sample_count=len(values),
            positive_count=positives,
            hit_rate=positives / len(values) if values else None,
            hit_rate_ci_lower=hit_lower,
            mean_realized_net_return=statistics.fmean(values) if values else None,
            mean_realized_net_return_ci_lower=mean_lower,
            p10_realized_net_return=_quantile(values, 0.10),
            worst_realized_net_return=min(values) if values else None,
            regime_count=len(regime_means),
            regime_means=regime_means,
            multiple_testing_penalty_return=penalty,
            required_mean_lower_bound=required,
            statistically_qualified=qualified,
            blockers=blockers,
            paper_allocation_authority=qualified,
            live_execution_authority=False,
            paper_only=True,
        )

    async def _current_l2_cost(self, candidate: AlphaCandidate) -> float | None:
        leg = OpportunityLeg(
            venue=candidate.venue,
            asset=candidate.asset,
            market_kind=candidate.market_kind,
            side=Side.LONG if candidate.direction == "long" else Side.SHORT,
            symbol=candidate.symbol,
            reference_price=candidate.entry_reference_price,
        )
        request = self.core.adapter_registry.book_request(leg)
        if request is None:
            return None
        try:
            book = await request.awaitable
            side = TradeSide.BUY if candidate.direction == "long" else TradeSide.SELL
            estimate = estimate_market_order(book, side, candidate.notional_usd)
        except Exception:
            return None
        fee_bps = self._one_way_fee_bps(candidate.venue, candidate.market_kind)
        if fee_bps is None:
            return None
        total_bps = (
            2.0 * fee_bps
            + estimate.slippage_bps * (1.0 + self.settings.exit_slippage_multiplier)
            + self.settings.alpha_execution_risk_floor_bps
        )
        return max(0.0, total_bps / 10_000.0)

    def _one_way_fee_bps(self, venue: str, kind: MarketKind) -> float | None:
        if venue == "Coinbase" and kind == MarketKind.SPOT:
            return self.settings.coinbase_spot_taker_fee_bps
        if venue == "Kraken" and kind == MarketKind.SPOT:
            return self.settings.kraken_spot_taker_fee_bps
        if venue == "Bybit":
            return self.settings.bybit_spot_taker_fee_bps if kind == MarketKind.SPOT else self.settings.bybit_derivatives_taker_fee_bps
        if venue == "OKX":
            return self.settings.okx_spot_taker_fee_bps if kind == MarketKind.SPOT else self.settings.okx_derivatives_taker_fee_bps
        if venue == "HlPerp" and kind == MarketKind.PERPETUAL:
            return self.settings.hyperliquid_perp_taker_fee_bps
        return None

    async def promoted_candidates(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        promoted: list[AlphaCandidate] = []
        for candidate in self.discover(snapshot, total_capital_usd=total_capital_usd):
            qualification = self.qualification(candidate)
            if not qualification.statistically_qualified:
                continue
            current_cost = await self._current_l2_cost(candidate)
            if current_cost is None:
                continue
            net = candidate.expected_gross_return - current_cost
            conservative_forward = qualification.mean_realized_net_return_ci_lower or 0.0
            conservative = min(net, conservative_forward)
            if conservative <= self.settings.alpha_min_current_net_return:
                continue
            candidate.estimated_cost_return = current_cost
            candidate.expected_net_return = conservative
            candidate.expected_profit_usd = candidate.notional_usd * conservative
            candidate.stage = "paper_qualified"
            candidate.paper_allocation_eligible = True
            promoted.append(candidate)
        promoted.sort(key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        return promoted

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None) -> AlphaEvidenceCycle:
        total_capital_usd = total_capital_usd or self.settings.alpha_research_capital_usd
        snapshot = await self.core.collect_live_evidence()
        matured = 0
        current_index = {
            (quote.venue, quote.asset.upper(), quote.market_kind, quote.symbol): quote
            for quote in snapshot.market_quotes
        }
        for signal in self.ledger.pending_signals(now=snapshot.completed_at):
            candidate = signal.candidate
            quote = current_index.get((candidate.venue, candidate.asset, candidate.market_kind, candidate.symbol))
            if quote is None or quote.mid <= 0:
                continue
            raw = quote.mid / candidate.entry_reference_price - 1.0
            directional = raw if candidate.direction == "long" else -raw
            outcome = AlphaForwardOutcome(
                signal_id=signal.signal_id,
                strategy_id=candidate.strategy_id,
                family=candidate.family,
                asset=candidate.asset,
                direction=candidate.direction,
                venue=candidate.venue,
                market_kind=candidate.market_kind,
                symbol=candidate.symbol,
                observed_at=candidate.observed_at,
                due_at=signal.due_at,
                matured_at=snapshot.completed_at,
                horizon_hours=candidate.horizon_hours,
                regime=candidate.regime,
                predicted_net_return=candidate.expected_net_return,
                entry_price=candidate.entry_reference_price,
                exit_price=quote.mid,
                realized_gross_return=directional,
                realized_net_return=directional - candidate.estimated_cost_return,
                correct_direction=directional > 0,
            )
            self.ledger.record_outcome(outcome)
            matured += 1

        candidates = self.discover(snapshot, total_capital_usd=total_capital_usd)
        recorded = 0
        for candidate in candidates:
            signal = AlphaForwardSignal(
                signal_id=candidate.candidate_id,
                candidate=candidate,
                due_at=candidate.observed_at + timedelta(hours=candidate.horizon_hours),
            )
            self.ledger.record_signal(signal)
            recorded += 1
        return AlphaEvidenceCycle(
            cycle_id=uuid.uuid4().hex,
            observed_at=snapshot.completed_at,
            candidate_count=len(candidates),
            signals_recorded=recorded,
            outcomes_matured=matured,
        )

from __future__ import annotations

import hashlib
import json
import math
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field, model_validator
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import Opportunity, OrderBookSnapshot


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _payload(value: BaseModel) -> tuple[str, str]:
    raw = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return raw, hashlib.sha256(raw.encode()).hexdigest()


class MechanismObservationLedger:
    """Append-only provider-neutral evidence for research mechanisms.

    The ledger deliberately does not infer provider authority. Each observation
    carries its own point-in-time/commercial/authority claims, and services filter
    on those fields before using evidence in economics.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.observations = Table(
            "mechanism_research_observations",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("mechanism", Text, nullable=False),
            Column("provider", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_mechanism_research_kind", self.observations.c.mechanism, self.observations.c.observed_at)
        Index("ix_mechanism_research_provider", self.observations.c.provider)
        metadata.create_all(store.engine)

    def record(self, mechanism: str, observation: BaseModel, *, provider: str, observed_at: datetime, observation_id: str) -> str:
        payload, lineage = _payload(observation)
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.observations.c.observation_id).where(self.observations.c.observation_id == observation_id)
            ).scalar_one_or_none()
            if exists is not None:
                return observation_id
            db.execute(insert(self.observations), {
                "observation_id": observation_id,
                "mechanism": mechanism,
                "provider": provider,
                "observed_at": observed_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": lineage,
            })
        return observation_id

    def payloads(self, mechanism: str) -> list[str]:
        query = select(self.observations.c.payload_json).where(
            self.observations.c.mechanism == mechanism
        ).order_by(self.observations.c.observed_at, self.observations.c.id)
        with self.store.engine.connect() as db:
            return list(db.execute(query).scalars())

    def summary(self, mechanism: str) -> dict[str, object]:
        query = select(
            self.observations.c.provider,
            self.observations.c.observed_at,
        ).where(self.observations.c.mechanism == mechanism)
        with self.store.engine.connect() as db:
            rows = list(db.execute(query))
        return {
            "mechanism": mechanism,
            "observation_count": len(rows),
            "providers": sorted({row[0] for row in rows}),
            "latest_observed_at": max((row[1] for row in rows), default=None),
            "paper_only": True,
        }


YieldKind = Literal["staking", "lending", "fixed_yield", "lp_fee", "incentive"]


class YieldObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    provider: str
    protocol: str
    venue_or_chain: str
    asset: str
    kind: YieldKind
    observed_at: datetime = Field(default_factory=_now)
    as_of_at: datetime
    gross_apy: float
    capacity_usd: float = Field(gt=0)
    holding_hours: float = Field(gt=0)
    entry_exit_cost_bps: float = Field(default=0.0, ge=0)
    credit_or_protocol_risk_haircut_apy: float = Field(default=0.0, ge=0)
    slashing_or_liquidation_risk_haircut_apy: float = Field(default=0.0, ge=0)
    incentive_decay_haircut_apy: float = Field(default=0.0, ge=0)
    withdrawal_or_lockup_hours: float = Field(default=0.0, ge=0)
    source_reference: str | None = None
    authoritative: bool = False
    commercial_use_permitted: bool = False
    point_in_time: bool = True
    paper_only: bool = True

    @model_validator(mode="after")
    def validate_observation(self):
        self.asset = self.asset.upper()
        if self.as_of_at > self.observed_at:
            raise ValueError("yield as_of_at cannot be after observed_at")
        return self


class YieldCandidate(BaseModel):
    observation_id: str
    protocol: str
    venue_or_chain: str
    asset: str
    kind: YieldKind
    observed_at: datetime
    capacity_usd: float
    holding_hours: float
    gross_apy: float
    annualized_entry_exit_cost: float
    total_risk_haircut_apy: float
    conservative_net_apy: float
    paper_allocation_eligible: bool = False
    blocker: str
    paper_only: bool = True


class YieldResearchService:
    MECHANISM = "yield"

    def __init__(self, store: EvidenceStore):
        self.ledger = MechanismObservationLedger(store)

    def record(self, observation: YieldObservation) -> str:
        return self.ledger.record(
            self.MECHANISM,
            observation,
            provider=observation.provider,
            observed_at=observation.observed_at,
            observation_id=observation.observation_id,
        )

    def observations(self) -> list[YieldObservation]:
        return [YieldObservation.model_validate_json(item) for item in self.ledger.payloads(self.MECHANISM)]

    def candidates(self, *, now: datetime | None = None, max_age_hours: float = 24.0) -> list[YieldCandidate]:
        now = now or _now()
        rows: list[YieldCandidate] = []
        for observation in self.observations():
            age_hours = max(0.0, (now - observation.as_of_at).total_seconds() / 3600.0)
            if age_hours > max_age_hours:
                continue
            if not (observation.authoritative and observation.commercial_use_permitted and observation.point_in_time):
                continue
            annualized_cost = (observation.entry_exit_cost_bps / 10_000.0) * 8760.0 / observation.holding_hours
            risk = (
                observation.credit_or_protocol_risk_haircut_apy
                + observation.slashing_or_liquidation_risk_haircut_apy
                + observation.incentive_decay_haircut_apy
            )
            net = observation.gross_apy - annualized_cost - risk
            rows.append(YieldCandidate(
                observation_id=observation.observation_id,
                protocol=observation.protocol,
                venue_or_chain=observation.venue_or_chain,
                asset=observation.asset,
                kind=observation.kind,
                observed_at=observation.observed_at,
                capacity_usd=observation.capacity_usd,
                holding_hours=observation.holding_hours,
                gross_apy=observation.gross_apy,
                annualized_entry_exit_cost=annualized_cost,
                total_risk_haircut_apy=risk,
                conservative_net_apy=net,
                paper_allocation_eligible=False,
                blocker="forward realized-yield, exit-liquidity and protocol-loss evidence have not yet passed a statistical promotion gate",
            ))
        rows.sort(key=lambda item: (item.conservative_net_apy, item.capacity_usd), reverse=True)
        return rows

    def summary(self) -> dict[str, object]:
        rows = self.observations()
        candidates = self.candidates()
        return {
            **self.ledger.summary(self.MECHANISM),
            "authoritative_count": sum(row.authoritative for row in rows),
            "commercial_use_permitted_count": sum(row.commercial_use_permitted for row in rows),
            "economic_candidate_count": len(candidates),
            "paper_allocation_count": 0,
        }


class OptionQuoteObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    provider: str
    venue: str
    underlying: str
    expiry: datetime
    strike: float = Field(gt=0)
    option_type: Literal["call", "put"]
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    implied_volatility: float = Field(gt=0)
    delta: float
    gamma: float | None = None
    vega: float | None = None
    observed_at: datetime = Field(default_factory=_now)
    source_reference: str | None = None
    authoritative: bool = False
    commercial_use_permitted: bool = False
    point_in_time: bool = True
    paper_only: bool = True

    @model_validator(mode="after")
    def validate_quote(self):
        self.underlying = self.underlying.upper()
        if self.bid > self.ask:
            raise ValueError("option bid cannot exceed ask")
        if self.expiry <= self.observed_at:
            raise ValueError("option expiry must be after observed_at")
        return self


class VolatilityResearchCandidate(BaseModel):
    underlying: str
    observed_at: datetime
    expiry: datetime
    option_quote_count: int
    median_atm_implied_volatility: float
    realized_volatility: float
    volatility_risk_premium: float
    direction: Literal["long_volatility", "short_volatility"]
    representative_spread_fraction: float
    hedge_cost_bps: float = Field(ge=0)
    paper_allocation_eligible: bool = False
    blockers: list[str]
    paper_only: bool = True


class VolatilityResearchService:
    MECHANISM = "volatility"

    def __init__(self, store: EvidenceStore):
        self.ledger = MechanismObservationLedger(store)

    def record(self, observation: OptionQuoteObservation) -> str:
        return self.ledger.record(
            self.MECHANISM,
            observation,
            provider=observation.provider,
            observed_at=observation.observed_at,
            observation_id=observation.observation_id,
        )

    def observations(self) -> list[OptionQuoteObservation]:
        return [OptionQuoteObservation.model_validate_json(item) for item in self.ledger.payloads(self.MECHANISM)]

    def candidates(
        self,
        *,
        realized_volatility_by_underlying: dict[str, float],
        hedge_cost_bps: float = 0.0,
        min_abs_vrp: float = 0.05,
    ) -> list[VolatilityResearchCandidate]:
        groups: dict[tuple[str, datetime], list[OptionQuoteObservation]] = defaultdict(list)
        for row in self.observations():
            if not (row.authoritative and row.commercial_use_permitted and row.point_in_time):
                continue
            if abs(abs(row.delta) - 0.50) > 0.20:
                continue
            groups[(row.underlying, row.expiry)].append(row)
        candidates: list[VolatilityResearchCandidate] = []
        for (underlying, expiry), rows in groups.items():
            realized = realized_volatility_by_underlying.get(underlying)
            if realized is None or realized <= 0 or len(rows) < 2:
                continue
            iv = statistics.median(row.implied_volatility for row in rows)
            vrp = iv - realized
            if abs(vrp) < min_abs_vrp:
                continue
            spreads = [(row.ask - row.bid) / ((row.ask + row.bid) / 2.0) for row in rows]
            candidates.append(VolatilityResearchCandidate(
                underlying=underlying,
                observed_at=max(row.observed_at for row in rows),
                expiry=expiry,
                option_quote_count=len(rows),
                median_atm_implied_volatility=iv,
                realized_volatility=realized,
                volatility_risk_premium=vrp,
                direction="short_volatility" if vrp > 0 else "long_volatility",
                representative_spread_fraction=statistics.median(spreads),
                hedge_cost_bps=hedge_cost_bps,
                blockers=[
                    "option executable L2/capacity is not yet authoritative",
                    "delta-hedge path and realized hedge costs are not yet forward certified",
                    "vega/gamma and gap risk remain research-only",
                ],
            ))
        candidates.sort(key=lambda item: abs(item.volatility_risk_premium), reverse=True)
        return candidates

    def summary(self) -> dict[str, object]:
        rows = self.observations()
        return {
            **self.ledger.summary(self.MECHANISM),
            "authoritative_count": sum(row.authoritative for row in rows),
            "commercial_use_permitted_count": sum(row.commercial_use_permitted for row in rows),
            "underlyings": sorted({row.underlying for row in rows}),
            "paper_allocation_count": 0,
        }


class MarketMakingSimulation(BaseModel):
    venue: str
    asset: str
    symbol: str
    observed_at: datetime
    spread_bps: float
    visible_top_depth_usd: float
    empirical_fill_probability: float | None = None
    maker_rebate_bps_roundtrip: float = 0.0
    adverse_selection_bps: float | None = None
    inventory_penalty_bps: float = 0.0
    expected_net_bps_per_completed_roundtrip: float | None = None
    economics_complete: bool = False
    decision_grade: bool = False
    blockers: list[str] = Field(default_factory=list)
    paper_allocation_eligible: bool = False
    paper_only: bool = True


class MarketMakingResearchService:
    @staticmethod
    def simulate(
        book: OrderBookSnapshot,
        *,
        empirical_fill_probability: float | None = None,
        maker_rebate_bps_roundtrip: float = 0.0,
        adverse_selection_bps: float | None = None,
        inventory_penalty_bps: float = 0.0,
        queue_model_empirical: bool = False,
    ) -> MarketMakingSimulation:
        best_bid = max(level.price for level in book.bids)
        best_ask = min(level.price for level in book.asks)
        mid = (best_bid + best_ask) / 2.0
        spread_bps = (best_ask - best_bid) / mid * 10_000.0
        visible_top_depth = best_bid * max(item.size for item in book.bids if item.price == best_bid) + best_ask * max(
            item.size for item in book.asks if item.price == best_ask
        )
        blockers: list[str] = []
        if empirical_fill_probability is None or not (0.0 <= empirical_fill_probability <= 1.0):
            blockers.append("empirical maker fill probability is unavailable")
        if not queue_model_empirical:
            blockers.append("queue-position/priority model is not empirical")
        if adverse_selection_bps is None:
            blockers.append("post-fill adverse-selection evidence is unavailable")
        expected = None
        if empirical_fill_probability is not None and adverse_selection_bps is not None:
            gross = spread_bps + maker_rebate_bps_roundtrip
            expected = empirical_fill_probability * (gross - adverse_selection_bps - inventory_penalty_bps)
        economics_complete = empirical_fill_probability is not None and adverse_selection_bps is not None
        return MarketMakingSimulation(
            venue=book.venue,
            asset=book.asset.upper(),
            symbol=book.symbol,
            observed_at=book.observed_at,
            spread_bps=spread_bps,
            visible_top_depth_usd=visible_top_depth,
            empirical_fill_probability=empirical_fill_probability,
            maker_rebate_bps_roundtrip=maker_rebate_bps_roundtrip,
            adverse_selection_bps=adverse_selection_bps,
            inventory_penalty_bps=inventory_penalty_bps,
            expected_net_bps_per_completed_roundtrip=expected,
            economics_complete=economics_complete,
            decision_grade=economics_complete and queue_model_empirical and not blockers,
            blockers=blockers,
            paper_allocation_eligible=False,
        )


DistressKind = Literal["liquidation", "solver_auction", "backstop"]


class DistressOpportunityObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    provider: str
    venue_or_protocol: str
    asset: str
    kind: DistressKind
    observed_at: datetime = Field(default_factory=_now)
    expires_at: datetime
    capacity_usd: float = Field(gt=0)
    gross_reward_usd: float = Field(ge=0)
    execution_cost_usd: float = Field(ge=0)
    worst_case_recovery_loss_usd: float = Field(ge=0)
    capture_probability: float = Field(ge=0, le=1)
    settlement_probability: float = Field(ge=0, le=1)
    source_reference: str | None = None
    authoritative: bool = False
    commercial_use_permitted: bool = False
    point_in_time: bool = True
    paper_only: bool = True

    @model_validator(mode="after")
    def validate_observation(self):
        self.asset = self.asset.upper()
        if self.expires_at <= self.observed_at:
            raise ValueError("distress opportunity must expire after observation")
        return self


class DistressResearchCandidate(BaseModel):
    observation_id: str
    venue_or_protocol: str
    asset: str
    kind: DistressKind
    capacity_usd: float
    expected_capture_and_settlement_probability: float
    conservative_expected_profit_usd: float
    conservative_return_on_capacity: float
    paper_allocation_eligible: bool = False
    blockers: list[str]
    paper_only: bool = True


class DistressResearchService:
    MECHANISM = "liquidation_distress"

    def __init__(self, store: EvidenceStore):
        self.ledger = MechanismObservationLedger(store)

    def record(self, observation: DistressOpportunityObservation) -> str:
        return self.ledger.record(
            self.MECHANISM,
            observation,
            provider=observation.provider,
            observed_at=observation.observed_at,
            observation_id=observation.observation_id,
        )

    def observations(self) -> list[DistressOpportunityObservation]:
        return [DistressOpportunityObservation.model_validate_json(item) for item in self.ledger.payloads(self.MECHANISM)]

    def candidates(self, *, now: datetime | None = None) -> list[DistressResearchCandidate]:
        now = now or _now()
        rows: list[DistressResearchCandidate] = []
        for observation in self.observations():
            if observation.expires_at <= now:
                continue
            if not (observation.authoritative and observation.commercial_use_permitted and observation.point_in_time):
                continue
            success_probability = observation.capture_probability * observation.settlement_probability
            success_profit = max(0.0, observation.gross_reward_usd - observation.execution_cost_usd)
            failure_loss = observation.execution_cost_usd + observation.worst_case_recovery_loss_usd
            expected_profit = success_probability * success_profit - (1.0 - success_probability) * failure_loss
            rows.append(DistressResearchCandidate(
                observation_id=observation.observation_id,
                venue_or_protocol=observation.venue_or_protocol,
                asset=observation.asset,
                kind=observation.kind,
                capacity_usd=observation.capacity_usd,
                expected_capture_and_settlement_probability=success_probability,
                conservative_expected_profit_usd=expected_profit,
                conservative_return_on_capacity=expected_profit / observation.capacity_usd,
                blockers=[
                    "capture probability is provider/model evidence and has not yet passed independent forward calibration",
                    "transaction ordering/auction selection and recovery remain unexecuted research assumptions",
                ],
            ))
        rows.sort(key=lambda item: item.conservative_return_on_capacity, reverse=True)
        return rows

    def summary(self) -> dict[str, object]:
        rows = self.observations()
        return {
            **self.ledger.summary(self.MECHANISM),
            "authoritative_count": sum(row.authoritative for row in rows),
            "commercial_use_permitted_count": sum(row.commercial_use_permitted for row in rows),
            "kinds": sorted({row.kind for row in rows}),
            "paper_allocation_count": 0,
        }


class CapitalLocationScore(BaseModel):
    venue: str
    asset: str
    opportunity_count: int = Field(ge=0)
    mean_positive_net_annualized_return: float
    max_positive_net_annualized_return: float
    raw_score: float = Field(ge=0)
    recommended_weight: float = Field(ge=0, le=1)
    recommended_reserve_usd: float = Field(ge=0)


class CapitalLocationPlan(BaseModel):
    observed_at: datetime = Field(default_factory=_now)
    reserve_capital_usd: float = Field(gt=0)
    historical_opportunity_count: int = Field(ge=0)
    recommendations: list[CapitalLocationScore]
    blockers: list[str] = Field(default_factory=list)
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class CapitalLocationResearchService:
    """Ranks where paper inventory would historically have had the most option value.

    This is intentionally a research policy. It does not move inventory or alter
    the unified allocator. Forward validation of the recommendation itself is a
    separate requirement before capital-location can become decision-grade.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store

    def plan(
        self,
        *,
        reserve_capital_usd: float,
        max_location_fraction: float = 0.35,
    ) -> CapitalLocationPlan:
        if reserve_capital_usd <= 0:
            raise ValueError("reserve_capital_usd must be positive")
        with self.store.engine.connect() as db:
            payloads = list(db.execute(select(self.store.opportunities.c.payload_json).order_by(self.store.opportunities.c.id)).scalars())
        opportunities = [Opportunity.model_validate_json(payload) for payload in payloads]
        positive = [item for item in opportunities if item.net_annualized_return > 0]
        by_location: dict[tuple[str, str], list[float]] = defaultdict(list)
        for opportunity in positive:
            for leg in opportunity.legs:
                by_location[(leg.venue, opportunity.asset.upper())].append(opportunity.net_annualized_return)
        if not by_location:
            return CapitalLocationPlan(
                reserve_capital_usd=reserve_capital_usd,
                historical_opportunity_count=len(positive),
                recommendations=[],
                blockers=["no positive persisted opportunity history is available for location learning"],
            )
        raw: dict[tuple[str, str], float] = {}
        for key, values in by_location.items():
            mean = statistics.fmean(values)
            raw[key] = max(0.0, len(values) * math.log1p(max(0.0, mean)))
        total_score = sum(raw.values())
        if total_score <= 0:
            return CapitalLocationPlan(
                reserve_capital_usd=reserve_capital_usd,
                historical_opportunity_count=len(positive),
                recommendations=[],
                blockers=["persisted opportunity history has no positive capital-location score"],
            )
        preliminary = {key: min(max_location_fraction, value / total_score) for key, value in raw.items()}
        normalization = sum(preliminary.values()) or 1.0
        recommendations: list[CapitalLocationScore] = []
        for key, score in raw.items():
            values = by_location[key]
            weight = preliminary[key] / normalization
            recommendations.append(CapitalLocationScore(
                venue=key[0],
                asset=key[1],
                opportunity_count=len(values),
                mean_positive_net_annualized_return=statistics.fmean(values),
                max_positive_net_annualized_return=max(values),
                raw_score=score,
                recommended_weight=weight,
                recommended_reserve_usd=reserve_capital_usd * weight,
            ))
        recommendations.sort(key=lambda item: item.recommended_weight, reverse=True)
        return CapitalLocationPlan(
            reserve_capital_usd=reserve_capital_usd,
            historical_opportunity_count=len(positive),
            recommendations=recommendations,
            blockers=[
                "recommendation is learned from historical opportunity incidence and is not yet forward-certified",
                "rebalancing costs and transfer/withdrawal latency require venue-specific authoritative evidence before allocation authority",
            ],
        )

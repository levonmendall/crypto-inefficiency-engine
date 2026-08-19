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

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaDirection, AlphaStrategyManifest
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _returns(quotes: list[MarketQuote]) -> list[float]:
    ordered = sorted(quotes, key=lambda item: item.observed_at)
    return [
        math.log(current.mid / previous.mid)
        for previous, current in zip(ordered, ordered[1:])
        if previous.mid > 0 and current.mid > 0
    ]


def _regime(quotes: list[MarketQuote]) -> Literal["low_vol", "normal", "high_vol"]:
    values = _returns(quotes)
    if len(values) < 2:
        return "normal"
    vol = statistics.pstdev(values)
    if vol < 0.0015:
        return "low_vol"
    if vol > 0.008:
        return "high_vol"
    return "normal"


def _capital(settings: Settings, quote: MarketQuote, total_capital_usd: float) -> tuple[float, float]:
    notional = min(
        max(settings.alpha_min_notional_usd, total_capital_usd * settings.alpha_candidate_capital_fraction),
        total_capital_usd,
    )
    capital_multiple = (
        settings.spot_collateral_fraction
        if quote.market_kind == MarketKind.SPOT
        else settings.perp_collateral_fraction
    )
    return notional, max(1.0, notional * max(0.01, capital_multiple))


class MeanReversionStrategy:
    """Robust price-deviation reversal signal with forward-only promotion.

    Discovery uses only point-in-time market history. The robust center and scale
    reduce sensitivity to a single price spike, while promotion remains governed
    by the Alpha Factory's independent forward outcomes and live L2 economics.
    """

    manifest = AlphaStrategyManifest(
        strategy_id="mean_reversion_v1",
        family="directional_reversal",
        description="Robust median/MAD mean-reversion after statistically unusual price displacement.",
        predictive=True,
        horizons_hours=[6.0],
    )

    @staticmethod
    def _robust_z(log_prices: list[float], current_log_price: float) -> tuple[float, float, float]:
        center = statistics.median(log_prices)
        absolute = [abs(value - center) for value in log_prices]
        mad = statistics.median(absolute)
        scale = 1.4826 * mad
        if scale <= 1e-9 and len(log_prices) >= 2:
            scale = statistics.pstdev(log_prices)
        if scale <= 1e-9:
            return 0.0, center, scale
        return (current_log_price - center) / scale, center, scale

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings: Settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        rows: list[AlphaCandidate] = []
        horizon = max(0.25, settings.alpha_reversion_horizon_hours)
        lookback = max(horizon, settings.alpha_reversion_lookback_hours)
        min_z = max(0.5, settings.alpha_reversion_min_robust_z)
        shrinkage = max(0.0, min(1.0, settings.alpha_reversion_forecast_shrinkage))
        current_by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                current_by_asset[quote.asset.upper()].append(quote)

        for asset, current_quotes in current_by_asset.items():
            scored: list[tuple[float, AlphaCandidate]] = []
            for quote in current_quotes:
                key = (quote.venue, asset, quote.market_kind)
                series = [item for item in history.get(key, []) if item.observed_at <= quote.observed_at]
                cutoff = quote.observed_at - timedelta(hours=lookback)
                window = sorted((item for item in series if item.observed_at >= cutoff), key=lambda item: item.observed_at)
                if len(window) < settings.alpha_min_history_points or quote.mid <= 0:
                    continue
                log_prices = [math.log(item.mid) for item in window if item.mid > 0]
                if len(log_prices) < settings.alpha_min_history_points:
                    continue
                robust_z, center, robust_scale = self._robust_z(log_prices, math.log(quote.mid))
                if abs(robust_z) < min_z:
                    continue
                direction: AlphaDirection = "short" if robust_z > 0 else "long"
                if direction == "short" and quote.market_kind != MarketKind.PERPETUAL:
                    continue
                if direction == "long" and quote.market_kind == MarketKind.PERPETUAL:
                    if any(item.market_kind == MarketKind.SPOT for item in current_quotes):
                        continue
                center_price = math.exp(center)
                convergence_return = abs(center_price / quote.mid - 1.0)
                gross = min(
                    settings.alpha_reversion_max_expected_return,
                    convergence_return * shrinkage,
                )
                cost_return = settings.alpha_research_cost_floor_bps / 10_000.0
                net = gross - cost_return
                if net <= 0:
                    continue
                notional, capital_required = _capital(settings, quote, total_capital_usd)
                confidence = min(1.0, abs(robust_z) / max(4.0, min_z * 2.0))
                candidate = AlphaCandidate(
                    candidate_id=(
                        f"alpha:{self.manifest.strategy_id}:{asset}:{quote.venue}:"
                        f"{quote.market_kind.value}:{uuid.uuid4().hex[:12]}"
                    ),
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
                    expected_gross_return=gross,
                    estimated_cost_return=cost_return,
                    expected_net_return=net,
                    expected_profit_usd=notional * net,
                    notional_usd=notional,
                    capital_required_usd=capital_required,
                    confidence_score=confidence,
                    regime=_regime(window),
                    conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}"],
                    features={
                        "robust_z": robust_z,
                        "robust_log_price_scale": robust_scale,
                        "center_price": center_price,
                        "convergence_return": convergence_return,
                        "history_points": len(window),
                        "forecast_shrinkage": shrinkage,
                    },
                )
                scored.append((net, candidate))
            if scored:
                scored.sort(key=lambda item: (item[0], item[1].confidence_score), reverse=True)
                rows.append(scored[0][1])
        return rows


class FundamentalFactorObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    provider: str
    asset: str
    observed_at: datetime = Field(default_factory=_now)
    as_of_at: datetime
    factor_scores: dict[str, float]
    source_reference: str | None = None
    authoritative: bool = False
    commercial_use_permitted: bool = False
    point_in_time: bool = True
    paper_only: bool = True

    @model_validator(mode="after")
    def validate_observation(self):
        self.asset = self.asset.upper()
        if self.as_of_at > self.observed_at:
            raise ValueError("factor as_of_at cannot be after observed_at")
        if not self.factor_scores:
            raise ValueError("at least one factor score is required")
        if any(not math.isfinite(value) or value < -1.0 or value > 1.0 for value in self.factor_scores.values()):
            raise ValueError("factor scores must be finite values in [-1, 1]")
        return self


class FundamentalFactorLedger:
    """Append-only point-in-time fundamental/on-chain evidence.

    Provider adapters may write normalized directional factor scores here. A
    positive score means the provider contract defines the observed factor as
    favorable for forward return; negative means unfavorable. Promotion still
    requires forward market outcomes and current execution economics.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.observations = Table(
            "alpha_fundamental_observations",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("provider", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("as_of_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_alpha_fundamental_asset", self.observations.c.asset, self.observations.c.as_of_at)
        Index("ix_alpha_fundamental_provider", self.observations.c.provider)
        metadata.create_all(store.engine)

    @staticmethod
    def _payload(observation: FundamentalFactorObservation) -> tuple[str, str]:
        payload = json.dumps(observation.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return payload, hashlib.sha256(payload.encode()).hexdigest()

    def record(self, observation: FundamentalFactorObservation) -> str:
        payload, lineage = self._payload(observation)
        with self.store.engine.begin() as db:
            existing = db.execute(
                select(self.observations.c.observation_id).where(
                    self.observations.c.observation_id == observation.observation_id
                )
            ).scalar_one_or_none()
            if existing is not None:
                return observation.observation_id
            db.execute(insert(self.observations), {
                "observation_id": observation.observation_id,
                "provider": observation.provider,
                "asset": observation.asset,
                "observed_at": observation.observed_at.isoformat(),
                "as_of_at": observation.as_of_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": lineage,
            })
        return observation.observation_id

    def latest(
        self,
        asset: str,
        *,
        before: datetime,
        max_age_hours: float,
        require_authoritative: bool = True,
        require_commercial_use: bool = True,
    ) -> FundamentalFactorObservation | None:
        query = select(self.observations.c.payload_json).where(
            self.observations.c.asset == asset.upper(),
            self.observations.c.as_of_at <= before.isoformat(),
        ).order_by(self.observations.c.as_of_at.desc(), self.observations.c.id.desc())
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        for payload in payloads:
            observation = FundamentalFactorObservation.model_validate_json(payload)
            age = max(0.0, (before - observation.as_of_at).total_seconds() / 3600.0)
            if age > max_age_hours:
                continue
            if require_authoritative and not observation.authoritative:
                continue
            if require_commercial_use and not observation.commercial_use_permitted:
                continue
            return observation
        return None

    def summary(self) -> dict[str, object]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(select(self.observations.c.payload_json).order_by(self.observations.c.id)).scalars())
        rows = [FundamentalFactorObservation.model_validate_json(payload) for payload in payloads]
        return {
            "observation_count": len(rows),
            "providers": sorted({row.provider for row in rows}),
            "assets": sorted({row.asset for row in rows}),
            "authoritative_count": sum(row.authoritative for row in rows),
            "commercial_use_permitted_count": sum(row.commercial_use_permitted for row in rows),
            "paper_only": True,
        }


class OnChainFundamentalStrategy:
    """Provider-neutral on-chain/fundamental composite factor research family.

    This family is deliberately fail-closed: only recent observations marked both
    authoritative and commercially usable can produce a research candidate. The
    observation still has no allocation authority until forward alpha promotion.
    """

    manifest = AlphaStrategyManifest(
        strategy_id="onchain_fundamental_composite_v1",
        family="onchain_fundamental",
        description="Point-in-time composite of authoritative normalized on-chain/fundamental factors.",
        predictive=True,
        horizons_hours=[24.0],
    )

    def __init__(self, ledger: FundamentalFactorLedger):
        self.ledger = ledger

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings: Settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        current_by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                current_by_asset[quote.asset.upper()].append(quote)
        rows: list[AlphaCandidate] = []
        for asset, quotes in current_by_asset.items():
            observation = self.ledger.latest(
                asset,
                before=snapshot.completed_at,
                max_age_hours=settings.alpha_factor_max_age_hours,
            )
            if observation is None or len(observation.factor_scores) < settings.alpha_factor_min_count:
                continue
            composite = statistics.fmean(observation.factor_scores.values())
            if abs(composite) < settings.alpha_factor_min_abs_score:
                continue
            direction: AlphaDirection = "long" if composite > 0 else "short"
            eligible_quotes = [
                quote for quote in quotes
                if (direction == "long" and quote.market_kind == MarketKind.SPOT)
                or (direction == "short" and quote.market_kind == MarketKind.PERPETUAL)
            ]
            if not eligible_quotes:
                continue
            quote = sorted(eligible_quotes, key=lambda item: (item.venue, item.symbol))[0]
            gross = min(
                settings.alpha_factor_max_expected_return,
                abs(composite) * settings.alpha_factor_return_scale * settings.alpha_factor_forecast_shrinkage,
            )
            cost_return = settings.alpha_research_cost_floor_bps / 10_000.0
            net = gross - cost_return
            if net <= 0:
                continue
            notional, capital_required = _capital(settings, quote, total_capital_usd)
            price_history = history.get((quote.venue, asset, quote.market_kind), [])
            rows.append(AlphaCandidate(
                candidate_id=(
                    f"alpha:{self.manifest.strategy_id}:{asset}:{quote.venue}:"
                    f"{quote.market_kind.value}:{uuid.uuid4().hex[:12]}"
                ),
                strategy_id=self.manifest.strategy_id,
                family=self.manifest.family,
                asset=asset,
                direction=direction,
                venue=quote.venue,
                market_kind=quote.market_kind,
                symbol=quote.symbol,
                observed_at=quote.observed_at,
                horizon_hours=settings.alpha_factor_horizon_hours,
                lookback_hours=settings.alpha_factor_lookback_hours,
                entry_reference_price=quote.mid,
                expected_gross_return=gross,
                estimated_cost_return=cost_return,
                expected_net_return=net,
                expected_profit_usd=notional * net,
                notional_usd=notional,
                capital_required_usd=capital_required,
                confidence_score=min(1.0, abs(composite)),
                regime=_regime(price_history[-settings.alpha_min_history_points:]),
                conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}"],
                features={
                    "factor_composite_score": composite,
                    "factor_count": len(observation.factor_scores),
                    "factor_provider": observation.provider,
                    "factor_observation_age_hours": max(
                        0.0,
                        (snapshot.completed_at - observation.as_of_at).total_seconds() / 3600.0,
                    ),
                    **{f"factor:{key}": value for key, value in observation.factor_scores.items()},
                },
            ))
        return rows

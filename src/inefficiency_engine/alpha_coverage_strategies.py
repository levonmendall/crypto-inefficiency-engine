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
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookSnapshot


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _capital(settings, quote: MarketQuote, total_capital_usd: float) -> tuple[float, float]:
    notional = min(
        max(settings.alpha_min_notional_usd, total_capital_usd * settings.alpha_candidate_capital_fraction),
        total_capital_usd,
    )
    collateral = settings.spot_collateral_fraction if quote.market_kind == MarketKind.SPOT else settings.perp_collateral_fraction
    return notional, max(1.0, notional * max(0.01, collateral))


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


def _preferred_quote(quotes: list[MarketQuote], *, direction: AlphaDirection) -> MarketQuote | None:
    if direction == "long":
        spot = [item for item in quotes if item.market_kind == MarketKind.SPOT]
        if spot:
            return sorted(spot, key=lambda item: (item.venue, item.symbol))[0]
    if direction == "short":
        perps = [item for item in quotes if item.market_kind == MarketKind.PERPETUAL]
        if perps:
            return sorted(perps, key=lambda item: (item.venue, item.symbol))[0]
    return None


class CrossSectionalRelativeValueStrategy:
    """Cross-sectional relative-strength/reversal research family.

    Each signal is still settled independently by the shared alpha forward ledger.
    The portfolio allocator may combine opposite-direction candidates, but this
    strategy never assumes a market-neutral pair was filled unless both legs are
    independently selected and supported.
    """

    manifest = AlphaStrategyManifest(
        strategy_id="cross_sectional_relative_value_v1",
        family="cross_sectional_relative_value",
        description="Cross-sectional residual return ranking against the contemporaneous crypto universe.",
        predictive=True,
        horizons_hours=[12.0],
    )

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        lookback = max(settings.alpha_cross_sectional_horizon_hours, settings.alpha_cross_sectional_lookback_hours)
        cutoff = snapshot.completed_at - timedelta(hours=lookback)
        current_by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                current_by_asset[quote.asset.upper()].append(quote)

        asset_returns: dict[str, float] = {}
        asset_windows: dict[str, list[MarketQuote]] = {}
        for asset, current_quotes in current_by_asset.items():
            reference = _preferred_quote(current_quotes, direction="long") or _preferred_quote(current_quotes, direction="short")
            if reference is None:
                continue
            series = [
                item for item in history.get((reference.venue, asset, reference.market_kind), [])
                if cutoff <= item.observed_at <= reference.observed_at
            ]
            series = sorted(series, key=lambda item: item.observed_at)
            if len(series) < settings.alpha_min_history_points or series[0].mid <= 0 or reference.mid <= 0:
                continue
            asset_returns[asset] = reference.mid / series[0].mid - 1.0
            asset_windows[asset] = series

        if len(asset_returns) < settings.alpha_cross_sectional_min_assets:
            return []
        values = list(asset_returns.values())
        center = statistics.median(values)
        deviations = [abs(value - center) for value in values]
        mad = statistics.median(deviations)
        scale = 1.4826 * mad
        if scale <= 1e-9 and len(values) >= 2:
            scale = statistics.pstdev(values)
        if scale <= 1e-9:
            return []

        rows: list[AlphaCandidate] = []
        for asset, trailing_return in asset_returns.items():
            residual = trailing_return - center
            z_score = residual / scale
            if abs(z_score) < settings.alpha_cross_sectional_min_abs_z:
                continue
            direction: AlphaDirection = "long" if residual > 0 else "short"
            quote = _preferred_quote(current_by_asset[asset], direction=direction)
            if quote is None:
                continue
            gross = min(
                settings.alpha_cross_sectional_max_expected_return,
                abs(residual) * settings.alpha_cross_sectional_forecast_shrinkage,
            )
            cost_return = settings.alpha_research_cost_floor_bps / 10_000.0
            net = gross - cost_return
            if net <= settings.alpha_min_current_net_return:
                continue
            notional, capital_required = _capital(settings, quote, total_capital_usd)
            rows.append(AlphaCandidate(
                candidate_id=f"alpha:{self.manifest.strategy_id}:{asset}:{quote.venue}:{quote.market_kind.value}:{uuid.uuid4().hex[:12]}",
                strategy_id=self.manifest.strategy_id,
                family=self.manifest.family,
                asset=asset,
                direction=direction,
                venue=quote.venue,
                market_kind=quote.market_kind,
                symbol=quote.symbol,
                observed_at=quote.observed_at,
                horizon_hours=settings.alpha_cross_sectional_horizon_hours,
                lookback_hours=lookback,
                entry_reference_price=quote.mid,
                expected_gross_return=gross,
                estimated_cost_return=cost_return,
                expected_net_return=net,
                expected_profit_usd=notional * net,
                notional_usd=notional,
                capital_required_usd=capital_required,
                confidence_score=min(1.0, abs(z_score) / 4.0),
                regime=_regime(asset_windows[asset]),
                conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}", f"cross-sectional:{asset}"],
                features={
                    "cross_sectional_return": trailing_return,
                    "cross_sectional_center": center,
                    "cross_sectional_residual": residual,
                    "cross_sectional_z": z_score,
                    "cross_sectional_asset_count": len(asset_returns),
                },
            ))
        rows.sort(key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        return rows[: settings.alpha_cross_sectional_max_candidates]


class MicrostructureImbalanceStrategy:
    """Short-horizon L2 imbalance alpha, promoted only after forward evidence.

    Discovery uses visible L2 only. It never assumes maker priority or a fill;
    promoted candidates still pass the shared current-L2 taker economics gate.
    """

    manifest = AlphaStrategyManifest(
        strategy_id="microstructure_imbalance_v1",
        family="microstructure_orderflow",
        description="Top-of-book depth imbalance and spread-conditioned short-horizon directional alpha.",
        predictive=True,
        horizons_hours=[0.25],
    )

    @staticmethod
    def _depth(book: OrderBookSnapshot, levels: int) -> tuple[float, float]:
        bids = sorted(book.bids, key=lambda item: item.price, reverse=True)[:levels]
        asks = sorted(book.asks, key=lambda item: item.price)[:levels]
        return (
            sum(level.price * level.size for level in bids),
            sum(level.price * level.size for level in asks),
        )

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        quote_index = {(q.venue, q.asset.upper(), q.market_kind, q.symbol): q for q in snapshot.market_quotes}
        has_spot = {q.asset.upper() for q in snapshot.market_quotes if q.market_kind == MarketKind.SPOT}
        rows: list[AlphaCandidate] = []
        for book in snapshot.order_books:
            if book.market_kind not in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                continue
            quote = quote_index.get((book.venue, book.asset.upper(), book.market_kind, book.symbol))
            if quote is None or quote.mid <= 0:
                continue
            bid_depth, ask_depth = self._depth(book, settings.alpha_microstructure_depth_levels)
            total_depth = bid_depth + ask_depth
            if total_depth <= 0:
                continue
            imbalance = (bid_depth - ask_depth) / total_depth
            if abs(imbalance) < settings.alpha_microstructure_min_abs_imbalance:
                continue
            direction: AlphaDirection = "long" if imbalance > 0 else "short"
            if direction == "short" and book.market_kind != MarketKind.PERPETUAL:
                continue
            if direction == "long" and book.market_kind == MarketKind.PERPETUAL and book.asset.upper() in has_spot:
                continue
            best_bid = max(level.price for level in book.bids)
            best_ask = min(level.price for level in book.asks)
            spread_return = (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
            gross = min(
                settings.alpha_microstructure_max_expected_return,
                abs(imbalance) * settings.alpha_microstructure_return_scale,
            )
            cost_return = max(settings.alpha_research_cost_floor_bps / 10_000.0, spread_return)
            net = gross - cost_return
            if net <= settings.alpha_min_current_net_return:
                continue
            notional, capital_required = _capital(settings, quote, total_capital_usd)
            hist = history.get((quote.venue, quote.asset.upper(), quote.market_kind), [])
            rows.append(AlphaCandidate(
                candidate_id=f"alpha:{self.manifest.strategy_id}:{quote.asset.upper()}:{quote.venue}:{quote.market_kind.value}:{uuid.uuid4().hex[:12]}",
                strategy_id=self.manifest.strategy_id,
                family=self.manifest.family,
                asset=quote.asset.upper(),
                direction=direction,
                venue=quote.venue,
                market_kind=quote.market_kind,
                symbol=quote.symbol,
                observed_at=quote.observed_at,
                horizon_hours=settings.alpha_microstructure_horizon_hours,
                lookback_hours=settings.alpha_microstructure_lookback_hours,
                entry_reference_price=quote.mid,
                expected_gross_return=gross,
                estimated_cost_return=cost_return,
                expected_net_return=net,
                expected_profit_usd=notional * net,
                notional_usd=notional,
                capital_required_usd=capital_required,
                confidence_score=min(1.0, abs(imbalance)),
                regime=_regime(hist[-settings.alpha_min_history_points:]),
                conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}", f"microstructure:{quote.venue}:{quote.symbol}"],
                features={
                    "l2_imbalance": imbalance,
                    "l2_bid_depth_usd": bid_depth,
                    "l2_ask_depth_usd": ask_depth,
                    "l2_spread_return": spread_return,
                    "l2_depth_levels": settings.alpha_microstructure_depth_levels,
                    "maker_fill_assumed": False,
                },
            ))
        rows.sort(key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        return rows[: settings.alpha_microstructure_max_candidates]


class EventObservation(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    provider: str
    asset: str
    event_type: str
    known_at: datetime
    event_at: datetime
    observed_at: datetime = Field(default_factory=_now)
    surprise_score: float = Field(ge=-1.0, le=1.0)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source_reference: str | None = None
    authoritative: bool = False
    commercial_use_permitted: bool = False
    point_in_time: bool = True
    paper_only: bool = True

    @model_validator(mode="after")
    def validate_times(self):
        self.asset = self.asset.upper()
        if self.known_at > self.observed_at:
            raise ValueError("event known_at cannot be after observed_at")
        return self


class EventLedger:
    """Append-only point-in-time event evidence with explicit source authority."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.events = Table(
            "alpha_event_observations",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_id", String(64), nullable=False, unique=True),
            Column("provider", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("event_type", Text, nullable=False),
            Column("known_at", Text, nullable=False),
            Column("event_at", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_alpha_event_asset_known", self.events.c.asset, self.events.c.known_at)
        Index("ix_alpha_event_type", self.events.c.event_type)
        metadata.create_all(store.engine)

    @staticmethod
    def _payload(event: EventObservation) -> tuple[str, str]:
        payload = json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return payload, hashlib.sha256(payload.encode()).hexdigest()

    def record(self, event: EventObservation) -> str:
        payload, lineage = self._payload(event)
        with self.store.engine.begin() as db:
            exists = db.execute(select(self.events.c.event_id).where(self.events.c.event_id == event.event_id)).scalar_one_or_none()
            if exists is not None:
                return event.event_id
            db.execute(insert(self.events), {
                "event_id": event.event_id,
                "provider": event.provider,
                "asset": event.asset,
                "event_type": event.event_type,
                "known_at": event.known_at.isoformat(),
                "event_at": event.event_at.isoformat(),
                "observed_at": event.observed_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": lineage,
            })
        return event.event_id

    def recent(self, *, asset: str, before: datetime, max_age_hours: float) -> list[EventObservation]:
        cutoff = before - timedelta(hours=max_age_hours)
        query = select(self.events.c.payload_json).where(
            self.events.c.asset == asset.upper(),
            self.events.c.known_at <= before.isoformat(),
            self.events.c.known_at >= cutoff.isoformat(),
        ).order_by(self.events.c.known_at.desc(), self.events.c.id.desc())
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        rows = [EventObservation.model_validate_json(payload) for payload in payloads]
        return [
            row for row in rows
            if row.authoritative and row.commercial_use_permitted and row.point_in_time
        ]

    def summary(self) -> dict[str, object]:
        with self.store.engine.connect() as db:
            payloads = list(db.execute(select(self.events.c.payload_json).order_by(self.events.c.id)).scalars())
        rows = [EventObservation.model_validate_json(payload) for payload in payloads]
        return {
            "observation_count": len(rows),
            "authoritative_count": sum(row.authoritative for row in rows),
            "commercial_use_permitted_count": sum(row.commercial_use_permitted for row in rows),
            "assets": sorted({row.asset for row in rows}),
            "event_types": sorted({row.event_type for row in rows}),
            "providers": sorted({row.provider for row in rows}),
            "paper_only": True,
        }


class EventDrivenStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="event_driven_surprise_v1",
        family="event_driven",
        description="Point-in-time authoritative event surprise mapped to forward return cohorts.",
        predictive=True,
        horizons_hours=[12.0],
    )

    def __init__(self, ledger: EventLedger):
        self.ledger = ledger

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        current_by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                current_by_asset[quote.asset.upper()].append(quote)
        rows: list[AlphaCandidate] = []
        for asset, quotes in current_by_asset.items():
            events = self.ledger.recent(asset=asset, before=snapshot.completed_at, max_age_hours=settings.alpha_event_max_age_hours)
            if not events:
                continue
            event = events[0]
            effective_score = event.surprise_score * event.confidence
            if abs(effective_score) < settings.alpha_event_min_abs_surprise:
                continue
            direction: AlphaDirection = "long" if effective_score > 0 else "short"
            quote = _preferred_quote(quotes, direction=direction)
            if quote is None:
                continue
            gross = min(
                settings.alpha_event_max_expected_return,
                abs(effective_score) * settings.alpha_event_return_scale * settings.alpha_event_forecast_shrinkage,
            )
            cost_return = settings.alpha_research_cost_floor_bps / 10_000.0
            net = gross - cost_return
            if net <= settings.alpha_min_current_net_return:
                continue
            notional, capital_required = _capital(settings, quote, total_capital_usd)
            hist = history.get((quote.venue, asset, quote.market_kind), [])
            rows.append(AlphaCandidate(
                candidate_id=f"alpha:{self.manifest.strategy_id}:{asset}:{event.event_id}:{uuid.uuid4().hex[:8]}",
                strategy_id=self.manifest.strategy_id,
                family=self.manifest.family,
                asset=asset,
                direction=direction,
                venue=quote.venue,
                market_kind=quote.market_kind,
                symbol=quote.symbol,
                observed_at=snapshot.completed_at,
                horizon_hours=settings.alpha_event_horizon_hours,
                lookback_hours=max(0.25, settings.alpha_event_max_age_hours),
                entry_reference_price=quote.mid,
                expected_gross_return=gross,
                estimated_cost_return=cost_return,
                expected_net_return=net,
                expected_profit_usd=notional * net,
                notional_usd=notional,
                capital_required_usd=capital_required,
                confidence_score=min(1.0, abs(effective_score)),
                regime=_regime(hist[-settings.alpha_min_history_points:]),
                conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}", f"event:{event.event_id}"],
                features={
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "event_provider": event.provider,
                    "event_surprise_score": event.surprise_score,
                    "event_confidence": event.confidence,
                    "event_effective_score": effective_score,
                    "event_known_age_hours": max(0.0, (snapshot.completed_at - event.known_at).total_seconds() / 3600.0),
                },
            ))
        rows.sort(key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        return rows

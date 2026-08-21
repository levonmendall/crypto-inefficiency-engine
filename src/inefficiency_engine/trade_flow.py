from __future__ import annotations

import asyncio
import hashlib
import json
import math
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import select
from websockets.asyncio.client import connect as websocket_connect

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaStrategyManifest
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation


BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"
TRADE_FLOW_SOURCE_ID = "public-trade-flow"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_id(*parts: object) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:48]


def _number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _event_time(value: object) -> datetime | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    if number > 10_000_000_000:
        number /= 1000.0
    return datetime.fromtimestamp(number, tz=timezone.utc)


class PublicTrade(BaseModel):
    event_id: str
    source_id: str = TRADE_FLOW_SOURCE_ID
    venue: str
    asset: str
    symbol: str
    event_at: datetime
    observed_at: datetime
    aggressor_side: Literal["buy", "sell"]
    price: float = Field(gt=0)
    size: float = Field(gt=0)
    notional_usd: float = Field(gt=0)
    paper_only: bool = True


class TradeFlowLedger:
    """Read the append-only public trade events already owned by the source plane."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        self.events = SourceCoveragePlane(store).events

    def recent(
        self,
        *,
        asset: str,
        before: datetime,
        max_age_hours: float,
        venue: str | None = None,
        limit: int = 2000,
    ) -> list[PublicTrade]:
        cutoff = before - timedelta(hours=max(0.01, max_age_hours))
        table = self.events.rows
        query = (
            select(table.c.payload_json)
            .where(table.c.lane_id == "microstructure")
            .where(table.c.event_type == "public_trade")
            .where(table.c.event_at >= cutoff.isoformat())
            .where(table.c.event_at <= before.isoformat())
            .order_by(table.c.id.desc())
            .limit(max(1, min(5000, int(limit))))
        )
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        rows: list[PublicTrade] = []
        for payload in payloads:
            event = SourceEventObservation.model_validate_json(payload)
            if (event.asset or "").upper() != asset.upper():
                continue
            raw = event.payload
            raw_venue = str(raw.get("venue") or "")
            if venue is not None and raw_venue != venue:
                continue
            price = _number(raw.get("price"))
            size = _number(raw.get("size"))
            side = str(raw.get("aggressor_side") or "").lower()
            if price is None or size is None or price <= 0 or size <= 0 or side not in {"buy", "sell"}:
                continue
            rows.append(PublicTrade(
                event_id=event.event_id,
                venue=raw_venue or "Bybit",
                asset=(event.asset or "").upper(),
                symbol=str(raw.get("symbol") or event.asset or ""),
                event_at=event.event_at,
                observed_at=event.observed_at,
                aggressor_side=side,  # type: ignore[arg-type]
                price=price,
                size=size,
                notional_usd=price * size,
            ))
        rows.sort(key=lambda item: item.event_at)
        return rows


async def collect_bybit_trade_flow(
    coverage: SourceCoveragePlane,
    *,
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    listen_seconds: float = 1.75,
) -> SourceProbeResult:
    """Capture actual public taker flow without assuming any maker fill."""

    topics = [f"publicTrade.{symbol}" for symbol in symbols]
    observed_at = _now()
    trades: list[PublicTrade] = []
    acknowledged = False
    async with websocket_connect(
        BYBIT_LINEAR_WS,
        open_timeout=5,
        close_timeout=2,
        ping_interval=20,
    ) as websocket:
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))
        deadline = asyncio.get_running_loop().time() + max(0.25, listen_seconds)
        while asyncio.get_running_loop().time() < deadline:
            timeout = max(0.05, deadline - asyncio.get_running_loop().time())
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            except TimeoutError:
                break
            payload: Any = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            if isinstance(payload, dict) and payload.get("success") is True:
                acknowledged = True
            if not isinstance(payload, dict) or not str(payload.get("topic") or "").startswith("publicTrade."):
                continue
            rows = payload.get("data")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("s") or "")
                price = _number(row.get("p"))
                size = _number(row.get("v"))
                event_at = _event_time(row.get("T")) or observed_at
                side_raw = str(row.get("S") or "").lower()
                side = "buy" if side_raw == "buy" else "sell" if side_raw == "sell" else None
                if not symbol or price is None or size is None or price <= 0 or size <= 0 or side is None:
                    continue
                acknowledged = True
                asset = symbol[:-4] if symbol.endswith("USDT") else symbol
                trade = PublicTrade(
                    event_id=_stable_id("bybit-trade", symbol, event_at.isoformat(), side, price, size, row.get("i")),
                    venue="Bybit",
                    asset=asset.upper(),
                    symbol=symbol,
                    event_at=event_at,
                    observed_at=observed_at,
                    aggressor_side=side,
                    price=price,
                    size=size,
                    notional_usd=price * size,
                )
                trades.append(trade)

    if not acknowledged:
        raise ValueError("Bybit public-trade subscription was not acknowledged")

    for trade in trades:
        payload = {
            "venue": trade.venue,
            "symbol": trade.symbol,
            "aggressor_side": trade.aggressor_side,
            "price": trade.price,
            "size": trade.size,
            "notional_usd": trade.notional_usd,
        }
        for lane_id in ("microstructure", "liquidity_provision"):
            coverage.record_event(SourceEventObservation(
                event_id=_stable_id(trade.event_id, lane_id),
                lane_id=lane_id,
                source_id=TRADE_FLOW_SOURCE_ID,
                event_type="public_trade",
                event_at=trade.event_at,
                observed_at=trade.observed_at,
                asset=trade.asset,
                source_reference=BYBIT_LINEAR_WS,
                payload=payload,
            ))

    return SourceProbeResult(
        source_id=TRADE_FLOW_SOURCE_ID,
        item_count=len(trades),
        source_reference=BYBIT_LINEAR_WS,
        evidence_by_lane={
            "microstructure": ["trade_flow"],
            "liquidity_provision": ["trade_flow"],
        },
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
        economic_fields_complete=bool(trades),
        forward_testable_evidence=bool(trades),
        detail={
            "topics": topics,
            "subscription_acknowledged": acknowledged,
            "trade_count": len(trades),
            "maker_fill_assumed": False,
        },
    )


class TradeFlowImbalanceStrategy:
    """Directional taker-flow alpha; maker execution remains a separate lane."""

    manifest = AlphaStrategyManifest(
        strategy_id="public_trade_flow_imbalance_v1",
        family="microstructure_orderflow",
        description="Short-horizon public taker-notional imbalance with shared forward/statistical promotion.",
        predictive=True,
        horizons_hours=[0.25],
    )

    def __init__(self, ledger: TradeFlowLedger):
        self.ledger = ledger

    @staticmethod
    def _regime(quotes: list[MarketQuote]) -> Literal["low_vol", "normal", "high_vol"]:
        ordered = sorted(quotes, key=lambda item: item.observed_at)
        returns = [
            math.log(current.mid / previous.mid)
            for previous, current in zip(ordered, ordered[1:])
            if previous.mid > 0 and current.mid > 0
        ]
        if len(returns) < 2:
            return "normal"
        mean = sum(returns) / len(returns)
        variance = sum((value - mean) ** 2 for value in returns) / len(returns)
        vol = math.sqrt(max(0.0, variance))
        if vol < 0.0015:
            return "low_vol"
        if vol > 0.008:
            return "high_vol"
        return "normal"

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                by_asset[quote.asset.upper()].append(quote)

        rows: list[AlphaCandidate] = []
        max_age = max(0.05, float(getattr(settings, "alpha_microstructure_lookback_hours", 6.0)))
        min_imbalance = max(0.10, float(getattr(settings, "alpha_microstructure_min_abs_imbalance", 0.20)))
        for asset, quotes in by_asset.items():
            trades = self.ledger.recent(asset=asset, before=snapshot.completed_at, max_age_hours=max_age)
            if len(trades) < 4:
                continue
            buy = sum(item.notional_usd for item in trades if item.aggressor_side == "buy")
            sell = sum(item.notional_usd for item in trades if item.aggressor_side == "sell")
            total = buy + sell
            if total <= 0:
                continue
            imbalance = (buy - sell) / total
            if abs(imbalance) < min_imbalance:
                continue
            direction: Literal["long", "short"] = "long" if imbalance > 0 else "short"
            preferred = [q for q in quotes if q.market_kind == (MarketKind.SPOT if direction == "long" else MarketKind.PERPETUAL)]
            if not preferred:
                continue
            quote = max(preferred, key=lambda item: item.observed_at)
            gross = min(
                float(getattr(settings, "alpha_microstructure_max_expected_return", 0.006)),
                abs(imbalance) * float(getattr(settings, "alpha_microstructure_return_scale", 0.012)) * 0.50,
            )
            cost = float(getattr(settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0
            net = gross - cost
            if net <= float(getattr(settings, "alpha_min_current_net_return", 0.0005)):
                continue
            notional = min(
                max(
                    float(getattr(settings, "alpha_min_notional_usd", 100.0)),
                    total_capital_usd * float(getattr(settings, "alpha_candidate_capital_fraction", 0.02)),
                ),
                total_capital_usd,
            )
            capital_multiple = (
                float(getattr(settings, "spot_collateral_fraction", 1.0))
                if quote.market_kind == MarketKind.SPOT
                else float(getattr(settings, "perp_collateral_fraction", 0.25))
            )
            capital = max(1.0, notional * max(0.01, capital_multiple))
            hist = history.get((quote.venue, asset, quote.market_kind), [])
            rows.append(AlphaCandidate(
                candidate_id=f"alpha:{self.manifest.strategy_id}:{asset}:{quote.venue}:{uuid.uuid4().hex[:10]}",
                strategy_id=self.manifest.strategy_id,
                family=self.manifest.family,
                asset=asset,
                direction=direction,
                venue=quote.venue,
                market_kind=quote.market_kind,
                symbol=quote.symbol,
                observed_at=snapshot.completed_at,
                horizon_hours=float(getattr(settings, "alpha_microstructure_horizon_hours", 0.25)),
                lookback_hours=max_age,
                entry_reference_price=quote.mid,
                expected_gross_return=gross,
                estimated_cost_return=cost,
                expected_net_return=net,
                expected_profit_usd=notional * net,
                notional_usd=notional,
                capital_required_usd=capital,
                confidence_score=min(1.0, abs(imbalance)),
                regime=self._regime(hist[-max(3, int(getattr(settings, "alpha_min_history_points", 8))):]),
                conflict_keys=[
                    f"alpha-instrument:{quote.venue}:{quote.symbol}",
                    f"trade-flow:{quote.venue}:{quote.symbol}",
                ],
                features={
                    "trade_flow_imbalance": imbalance,
                    "buy_notional_usd": buy,
                    "sell_notional_usd": sell,
                    "trade_count": len(trades),
                    "maker_fill_assumed": False,
                },
            ))
        rows.sort(key=lambda item: (item.expected_net_return, item.confidence_score), reverse=True)
        return rows[: int(getattr(settings, "alpha_microstructure_max_candidates", 6))]

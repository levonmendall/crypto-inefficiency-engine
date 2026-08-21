from __future__ import annotations

import math
import statistics
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Literal

from inefficiency_engine.alpha_extensions import (
    FundamentalFactorLedger,
    MeanReversionStrategy,
    _capital,
    _regime,
    _returns,
)
from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaDirection, AlphaStrategyManifest
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.trade_flow import TradeFlowLedger


def _setting(settings, name: str, default: float) -> float:
    try:
        return float(getattr(settings, name, default))
    except (TypeError, ValueError):
        return default


def _cost(settings) -> float:
    return max(0.0, _setting(settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0


def _net_hurdle(settings) -> float:
    return max(0.0, _setting(settings, "alpha_min_current_net_return", 0.0005))


def _reversion_horizon(settings) -> float:
    return max(0.25, _setting(settings, "alpha_reversion_horizon_hours", 6.0))


def _max_reversion_return(settings) -> float:
    return max(0.0001, _setting(settings, "alpha_reversion_max_expected_return", 0.03))


def _shrinkage(settings) -> float:
    return max(0.0, min(1.0, _setting(settings, "alpha_reversion_forecast_shrinkage", 0.35)))


def _candidate(
    *,
    manifest: AlphaStrategyManifest,
    quote: MarketQuote,
    direction: AlphaDirection,
    gross: float,
    cost: float,
    confidence: float,
    lookback_hours: float,
    history_window: list[MarketQuote],
    settings,
    total_capital_usd: float,
    features: dict[str, float | int | str | bool],
) -> AlphaCandidate | None:
    net = gross - cost
    if net <= _net_hurdle(settings):
        return None
    notional, capital = _capital(settings, quote, total_capital_usd)
    return AlphaCandidate(
        candidate_id=(
            f"alpha:{manifest.strategy_id}:{quote.asset.upper()}:{quote.venue}:"
            f"{quote.market_kind.value}:{uuid.uuid4().hex[:12]}"
        ),
        strategy_id=manifest.strategy_id,
        family=manifest.family,
        asset=quote.asset.upper(),
        direction=direction,
        venue=quote.venue,
        market_kind=quote.market_kind,
        symbol=quote.symbol,
        observed_at=quote.observed_at,
        horizon_hours=_reversion_horizon(settings),
        lookback_hours=max(0.25, lookback_hours),
        entry_reference_price=quote.mid,
        expected_gross_return=gross,
        estimated_cost_return=cost,
        expected_net_return=net,
        expected_profit_usd=notional * net,
        notional_usd=notional,
        capital_required_usd=capital,
        confidence_score=max(0.0, min(1.0, confidence)),
        regime=_regime(history_window),
        conflict_keys=[f"alpha-instrument:{quote.venue}:{quote.symbol}"],
        features=features,
    )


def _eligible_quote(
    quotes: list[MarketQuote],
    *,
    direction: Literal["long", "short"],
) -> MarketQuote | None:
    preferred_kind = MarketKind.SPOT if direction == "long" else MarketKind.PERPETUAL
    rows = [quote for quote in quotes if quote.market_kind == preferred_kind and quote.mid > 0]
    return max(rows, key=lambda item: item.observed_at) if rows else None


class CrossVenueResidualMeanReversionStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="mean_reversion_cross_venue_residual_v1",
        family="directional_reversal",
        description="Reverse point-in-time venue residuals against the contemporaneous cross-venue median.",
        predictive=True,
        horizons_hours=[3.0],
    )

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        by_asset_kind: dict[tuple[str, MarketKind], list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL} and quote.mid > 0:
                by_asset_kind[(quote.asset.upper(), quote.market_kind)].append(quote)
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        min_residual = max(0.0025, cost * 3.0)
        for (asset, kind), quotes in by_asset_kind.items():
            if len({quote.venue for quote in quotes}) < 2:
                continue
            median = statistics.median(quote.mid for quote in quotes)
            for quote in quotes:
                residual = quote.mid / median - 1.0
                if abs(residual) < min_residual:
                    continue
                direction: Literal["long", "short"] = "short" if residual > 0 else "long"
                if direction == "short" and kind != MarketKind.PERPETUAL:
                    continue
                if direction == "long" and kind != MarketKind.SPOT:
                    continue
                lookback = max(3.0, _setting(settings, "alpha_reversion_lookback_hours", 24.0))
                window = [item for item in history.get((quote.venue, asset, kind), []) if item.observed_at >= quote.observed_at - timedelta(hours=lookback)]
                gross = min(_max_reversion_return(settings), abs(residual) * _shrinkage(settings))
                candidate = _candidate(
                    manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                    cost=cost, confidence=min(1.0, abs(residual) / max(min_residual * 4.0, 1e-9)),
                    lookback_hours=lookback, history_window=window, settings=settings,
                    total_capital_usd=total_capital_usd,
                    features={
                        "cross_venue_median": median,
                        "cross_venue_residual": residual,
                        "independent_venue_count": len({item.venue for item in quotes}),
                        "separate_forward_cohort": True,
                    },
                )
                if candidate is not None:
                    rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]


class MultiHorizonMeanReversionStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="mean_reversion_multi_horizon_v1",
        family="directional_reversal",
        description="Require robust reversal agreement across short, medium, and long lookbacks.",
        predictive=True,
        horizons_hours=[6.0],
    )

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        min_z = max(0.75, _setting(settings, "alpha_reversion_min_robust_z", 1.5))
        min_points = int(max(6, _setting(settings, "alpha_min_history_points", 8)))
        horizons = (6.0, 24.0, 72.0)
        for quote in snapshot.market_quotes:
            if quote.market_kind not in {MarketKind.SPOT, MarketKind.PERPETUAL} or quote.mid <= 0:
                continue
            series = [item for item in history.get((quote.venue, quote.asset.upper(), quote.market_kind), []) if item.observed_at <= quote.observed_at and item.mid > 0]
            zs: list[float] = []
            convergence: list[float] = []
            longest_window: list[MarketQuote] = []
            for hours in horizons:
                window = [item for item in series if item.observed_at >= quote.observed_at - timedelta(hours=hours)]
                if hours == horizons[-1]:
                    longest_window = window
                if len(window) < min_points:
                    continue
                logs = [math.log(item.mid) for item in window]
                z, center, _ = MeanReversionStrategy._robust_z(logs, math.log(quote.mid))
                if abs(z) >= min_z:
                    zs.append(z)
                    convergence.append(abs(math.exp(center) / quote.mid - 1.0))
            if len(zs) < 2 or not all(value > 0 for value in zs) and not all(value < 0 for value in zs):
                continue
            signed = statistics.fmean(zs)
            direction: Literal["long", "short"] = "short" if signed > 0 else "long"
            if direction == "short" and quote.market_kind != MarketKind.PERPETUAL:
                continue
            if direction == "long" and quote.market_kind != MarketKind.SPOT:
                continue
            gross = min(_max_reversion_return(settings), statistics.fmean(convergence) * _shrinkage(settings))
            candidate = _candidate(
                manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                cost=cost, confidence=min(1.0, abs(signed) / max(min_z * 3.0, 1e-9)),
                lookback_hours=max(horizons), history_window=longest_window, settings=settings,
                total_capital_usd=total_capital_usd,
                features={
                    "agreeing_horizon_count": len(zs),
                    "mean_robust_z": signed,
                    "mean_convergence_return": statistics.fmean(convergence),
                    "separate_forward_cohort": True,
                },
            )
            if candidate is not None:
                rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]


class VolatilityConditionedMeanReversionStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="mean_reversion_vol_conditioned_v1",
        family="directional_reversal",
        description="Reverse robust dislocations only in normal/high-volatility exhaustion regimes.",
        predictive=True,
        horizons_hours=[6.0],
    )

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        lookback = max(12.0, _setting(settings, "alpha_reversion_lookback_hours", 24.0))
        min_z = max(1.0, _setting(settings, "alpha_reversion_min_robust_z", 1.5))
        min_points = int(max(6, _setting(settings, "alpha_min_history_points", 8)))
        for quote in snapshot.market_quotes:
            if quote.market_kind not in {MarketKind.SPOT, MarketKind.PERPETUAL} or quote.mid <= 0:
                continue
            window = [item for item in history.get((quote.venue, quote.asset.upper(), quote.market_kind), []) if quote.observed_at - timedelta(hours=lookback) <= item.observed_at <= quote.observed_at and item.mid > 0]
            if len(window) < min_points or _regime(window) == "low_vol":
                continue
            logs = [math.log(item.mid) for item in window]
            z, center, _ = MeanReversionStrategy._robust_z(logs, math.log(quote.mid))
            recent_returns = _returns(window[-min(5, len(window)):])
            recent_move = sum(recent_returns) if recent_returns else 0.0
            if abs(z) < min_z or (z > 0 and recent_move <= 0) or (z < 0 and recent_move >= 0):
                continue
            direction: Literal["long", "short"] = "short" if z > 0 else "long"
            if direction == "short" and quote.market_kind != MarketKind.PERPETUAL:
                continue
            if direction == "long" and quote.market_kind != MarketKind.SPOT:
                continue
            convergence = abs(math.exp(center) / quote.mid - 1.0)
            gross = min(_max_reversion_return(settings), convergence * _shrinkage(settings))
            candidate = _candidate(
                manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                cost=cost, confidence=min(1.0, abs(z) / max(min_z * 3.0, 1e-9)),
                lookback_hours=lookback, history_window=window, settings=settings,
                total_capital_usd=total_capital_usd,
                features={
                    "robust_z": z,
                    "recent_exhaustion_return": recent_move,
                    "conditioned_regime": _regime(window),
                    "separate_forward_cohort": True,
                },
            )
            if candidate is not None:
                rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]


class LiquidityConditionedMeanReversionStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="mean_reversion_liquidity_conditioned_v1",
        family="directional_reversal",
        description="Robust reversal only when current L2 spread and visible depth support conservative execution.",
        predictive=True,
        horizons_hours=[6.0],
    )

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        books = {(book.venue, book.asset.upper(), book.market_kind, book.symbol): book for book in snapshot.order_books}
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        lookback = max(12.0, _setting(settings, "alpha_reversion_lookback_hours", 24.0))
        min_z = max(1.0, _setting(settings, "alpha_reversion_min_robust_z", 1.5))
        min_points = int(max(6, _setting(settings, "alpha_min_history_points", 8)))
        for quote in snapshot.market_quotes:
            if quote.market_kind not in {MarketKind.SPOT, MarketKind.PERPETUAL} or quote.mid <= 0:
                continue
            book = books.get((quote.venue, quote.asset.upper(), quote.market_kind, quote.symbol))
            if book is None or not book.bids or not book.asks:
                continue
            best_bid = max(level.price for level in book.bids)
            best_ask = min(level.price for level in book.asks)
            if best_ask <= best_bid:
                continue
            spread = (best_ask - best_bid) / ((best_ask + best_bid) / 2.0)
            window = [item for item in history.get((quote.venue, quote.asset.upper(), quote.market_kind), []) if quote.observed_at - timedelta(hours=lookback) <= item.observed_at <= quote.observed_at and item.mid > 0]
            if len(window) < min_points:
                continue
            logs = [math.log(item.mid) for item in window]
            z, center, _ = MeanReversionStrategy._robust_z(logs, math.log(quote.mid))
            if abs(z) < min_z:
                continue
            direction: Literal["long", "short"] = "short" if z > 0 else "long"
            if direction == "short" and quote.market_kind != MarketKind.PERPETUAL:
                continue
            if direction == "long" and quote.market_kind != MarketKind.SPOT:
                continue
            notional, _ = _capital(settings, quote, total_capital_usd)
            bid_depth = sum(level.price * level.size for level in book.bids[:10])
            ask_depth = sum(level.price * level.size for level in book.asks[:10])
            executable_depth = min(bid_depth, ask_depth)
            if executable_depth < notional * 3.0 or spread > max(0.003, cost * 4.0):
                continue
            convergence = abs(math.exp(center) / quote.mid - 1.0)
            gross = min(_max_reversion_return(settings), convergence * _shrinkage(settings))
            candidate = _candidate(
                manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                cost=cost + spread * 0.5,
                confidence=min(1.0, abs(z) / max(min_z * 3.0, 1e-9)),
                lookback_hours=lookback, history_window=window, settings=settings,
                total_capital_usd=total_capital_usd,
                features={
                    "robust_z": z,
                    "visible_depth_usd": executable_depth,
                    "depth_to_notional": executable_depth / max(notional, 1.0),
                    "current_spread_fraction": spread,
                    "separate_forward_cohort": True,
                },
            )
            if candidate is not None:
                rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]


class BtcRelativeResidualMeanReversionStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="mean_reversion_btc_relative_v1",
        family="directional_reversal",
        description="Reverse large residual returns after estimating rolling beta to BTC on the same venue/market kind.",
        predictive=True,
        horizons_hours=[6.0],
    )

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        lookback = max(12.0, _setting(settings, "alpha_reversion_lookback_hours", 24.0))
        min_points = int(max(8, _setting(settings, "alpha_min_history_points", 8)))
        min_residual = max(0.01, cost * 6.0)
        for quote in snapshot.market_quotes:
            asset = quote.asset.upper()
            if asset == "BTC" or quote.market_kind not in {MarketKind.SPOT, MarketKind.PERPETUAL} or quote.mid <= 0:
                continue
            asset_window = [item for item in history.get((quote.venue, asset, quote.market_kind), []) if quote.observed_at - timedelta(hours=lookback) <= item.observed_at <= quote.observed_at and item.mid > 0]
            btc_window = [item for item in history.get((quote.venue, "BTC", quote.market_kind), []) if quote.observed_at - timedelta(hours=lookback) <= item.observed_at <= quote.observed_at and item.mid > 0]
            if len(asset_window) < min_points or len(btc_window) < min_points:
                continue
            asset_returns = _returns(asset_window)
            btc_returns = _returns(btc_window)
            n = min(len(asset_returns), len(btc_returns))
            if n < min_points - 1:
                continue
            a = asset_returns[-n:]
            b = btc_returns[-n:]
            mean_a, mean_b = statistics.fmean(a), statistics.fmean(b)
            var_b = statistics.fmean([(value - mean_b) ** 2 for value in b])
            beta = statistics.fmean([(x - mean_a) * (y - mean_b) for x, y in zip(a, b)]) / var_b if var_b > 1e-12 else 1.0
            beta = max(-3.0, min(3.0, beta))
            asset_total = math.log(asset_window[-1].mid / asset_window[0].mid)
            btc_total = math.log(btc_window[-1].mid / btc_window[0].mid)
            residual = asset_total - beta * btc_total
            if abs(residual) < min_residual:
                continue
            direction: Literal["long", "short"] = "short" if residual > 0 else "long"
            if direction == "short" and quote.market_kind != MarketKind.PERPETUAL:
                continue
            if direction == "long" and quote.market_kind != MarketKind.SPOT:
                continue
            gross = min(_max_reversion_return(settings), abs(residual) * _shrinkage(settings))
            candidate = _candidate(
                manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                cost=cost, confidence=min(1.0, abs(residual) / max(min_residual * 4.0, 1e-9)),
                lookback_hours=lookback, history_window=asset_window, settings=settings,
                total_capital_usd=total_capital_usd,
                features={
                    "btc_beta": beta,
                    "asset_log_return": asset_total,
                    "btc_log_return": btc_total,
                    "btc_relative_residual": residual,
                    "separate_forward_cohort": True,
                },
            )
            if candidate is not None:
                rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]


class TradeFlowLeadLagStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="public_trade_flow_lead_lag_v1",
        family="microstructure_orderflow",
        description="Use first-party Bybit taker flow as a lead signal only when another venue still lags the reference price.",
        predictive=True,
        horizons_hours=[0.25],
    )

    def __init__(self, ledger: TradeFlowLedger):
        self.ledger = ledger

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL} and quote.mid > 0:
                by_asset[quote.asset.upper()].append(quote)
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        min_imbalance = max(0.20, _setting(settings, "alpha_microstructure_min_abs_imbalance", 0.20))
        for asset, quotes in by_asset.items():
            trades = self.ledger.recent(asset=asset, venue="Bybit", before=snapshot.completed_at, max_age_hours=0.25, limit=1000)
            if len(trades) < 6:
                continue
            buy = sum(item.notional_usd for item in trades if item.aggressor_side == "buy")
            sell = sum(item.notional_usd for item in trades if item.aggressor_side == "sell")
            total = buy + sell
            if total <= 0:
                continue
            imbalance = (buy - sell) / total
            if abs(imbalance) < min_imbalance:
                continue
            reference_rows = [q for q in quotes if q.venue == "Bybit" and q.market_kind == MarketKind.PERPETUAL]
            if not reference_rows:
                continue
            reference = max(reference_rows, key=lambda item: item.observed_at)
            direction: Literal["long", "short"] = "long" if imbalance > 0 else "short"
            targets = [
                q for q in quotes
                if q.venue != "Bybit"
                and q.market_kind == (MarketKind.SPOT if direction == "long" else MarketKind.PERPETUAL)
            ]
            for quote in targets:
                residual = reference.mid / quote.mid - 1.0
                if direction == "long" and residual <= max(0.0015, cost * 2.0):
                    continue
                if direction == "short" and residual >= -max(0.0015, cost * 2.0):
                    continue
                gross = min(
                    _setting(settings, "alpha_microstructure_max_expected_return", 0.006),
                    abs(residual) * 0.50 + abs(imbalance) * _setting(settings, "alpha_microstructure_return_scale", 0.012) * 0.15,
                )
                window = history.get((quote.venue, asset, quote.market_kind), [])
                candidate = _candidate(
                    manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                    cost=cost, confidence=min(1.0, abs(imbalance)), lookback_hours=0.25,
                    history_window=window, settings=settings, total_capital_usd=total_capital_usd,
                    features={
                        "trade_count": len(trades),
                        "taker_notional_usd": total,
                        "flow_imbalance": imbalance,
                        "bybit_reference_mid": reference.mid,
                        "lagging_venue_residual": residual,
                        "maker_fill_assumed": False,
                    },
                )
                if candidate is not None:
                    rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]


class OnChainFactorBreadthStrategy:
    manifest = AlphaStrategyManifest(
        strategy_id="onchain_factor_breadth_v1",
        family="onchain_fundamental",
        description="Require broad sign agreement across authoritative on-chain/fundamental factors before forward testing.",
        predictive=True,
        horizons_hours=[24.0],
    )

    def __init__(self, ledger: FundamentalFactorLedger):
        self.ledger = ledger

    def discover(self, snapshot, history, settings, *, total_capital_usd: float):
        by_asset: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                by_asset[quote.asset.upper()].append(quote)
        rows: list[AlphaCandidate] = []
        cost = _cost(settings)
        min_count = int(max(3, _setting(settings, "alpha_factor_min_count", 2)))
        for asset, quotes in by_asset.items():
            observation = self.ledger.latest(
                asset,
                before=snapshot.completed_at,
                max_age_hours=_setting(settings, "alpha_factor_max_age_hours", 24.0),
            )
            if observation is None or len(observation.factor_scores) < min_count:
                continue
            values = list(observation.factor_scores.values())
            positive = sum(value > 0 for value in values)
            negative = sum(value < 0 for value in values)
            dominant = max(positive, negative)
            breadth = dominant / len(values)
            if breadth < 0.75:
                continue
            composite = statistics.fmean(values)
            min_abs = _setting(settings, "alpha_factor_min_abs_score", 0.15)
            if abs(composite) < min_abs:
                continue
            direction: Literal["long", "short"] = "long" if composite > 0 else "short"
            quote = _eligible_quote(quotes, direction=direction)
            if quote is None:
                continue
            gross = min(
                _setting(settings, "alpha_factor_max_expected_return", 0.03),
                abs(composite)
                * breadth
                * _setting(settings, "alpha_factor_return_scale", 0.04)
                * _setting(settings, "alpha_factor_forecast_shrinkage", 0.35),
            )
            window = history.get((quote.venue, asset, quote.market_kind), [])
            candidate = _candidate(
                manifest=self.manifest, quote=quote, direction=direction, gross=gross,
                cost=cost, confidence=min(1.0, abs(composite) * breadth),
                lookback_hours=_setting(settings, "alpha_factor_max_age_hours", 24.0),
                history_window=window, settings=settings, total_capital_usd=total_capital_usd,
                features={
                    "factor_count": len(values),
                    "factor_breadth": breadth,
                    "factor_composite": composite,
                    "dominant_sign_count": dominant,
                    "separate_forward_cohort": True,
                },
            )
            if candidate is not None:
                rows.append(candidate)
        rows.sort(key=lambda item: item.expected_net_return, reverse=True)
        return rows[:6]

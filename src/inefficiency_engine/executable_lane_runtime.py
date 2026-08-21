from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from inefficiency_engine.allocation_certification import PaperAllocationOutcome, PaperAllocationTrial
from inefficiency_engine.canonical_paper_portfolio import CanonicalPortfolioEvent
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexAwareAllocationForwardCertificationService,
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService,
    CexDexUniversalOperationallyResilientPaperPortfolioService,
)
from inefficiency_engine.mechanism_execution import (
    MECHANISM_IDS,
    MechanismExecutionService,
    MechanismSettlementResult,
    MechanismTrialSpec,
)
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.qualified_opportunity import allocate_prequalified_candidates
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperAllocationPlan


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _number(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ExecutableMechanismExecutionService(MechanismExecutionService):
    """Conservative production settlement for the five mechanism lanes.

    All enhancements remain paper-only and forward-qualified. They make the shadow
    economics harder to pass: maker fills require queue-through volume for new
    trials, options include dynamic hedge/Greek costs, and distress entries begin at
    the first observable quote rather than the original forced-liquidation print.
    """

    def _settle_yield(self, trial):
        exit_observation = self._next_yield_observation(trial)
        if exit_observation is None:
            return None
        entry_net = float(trial.settlement_payload.get("entry_net_apy") or 0.0)
        holding_hours = max(
            1.0,
            (trial.due_at - trial.source_observed_at).total_seconds() / 3600.0,
        )
        annualized_cost = (
            exit_observation.entry_exit_cost_bps / 10_000.0
        ) * 8760.0 / holding_hours
        risk = (
            exit_observation.credit_or_protocol_risk_haircut_apy
            + exit_observation.slashing_or_liquidation_risk_haircut_apy
            + exit_observation.incentive_decay_haircut_apy
        )
        exit_net = exit_observation.gross_apy - annualized_cost - risk
        gross = statistics.fmean([entry_net, exit_net]) * holding_hours / 8760.0
        if exit_observation.capacity_usd < trial.capital_usd:
            net = -max(
                0.0,
                float(trial.settlement_payload.get("entry_exit_cost_bps") or 0.0),
            ) / 10_000.0
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
                "holding_hours": holding_hours,
            },
        )

    def _maker_specs(self, snapshot, *, total_capital_usd: float):
        base = super()._maker_specs(snapshot, total_capital_usd=total_capital_usd)
        books = {
            (book.venue, book.asset.upper(), book.symbol): book
            for book in snapshot.order_books
            if book.bids and book.asks
        }
        rows: list[MechanismTrialSpec] = []
        cost_floor = max(
            0.0001,
            float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0,
        )
        for spec in base:
            payload = dict(spec.settlement_payload)
            key = (
                str(payload.get("venue") or ""),
                str(payload.get("asset") or spec.asset).upper(),
                str(payload.get("symbol") or ""),
            )
            book = books.get(key)
            bid = _number(payload.get("bid"))
            ask = _number(payload.get("ask"))
            mid = _number(payload.get("mid"))
            if book is None or bid <= 0 or ask <= bid or mid <= 0:
                continue
            bid_queue_usd = sum(level.size * level.price for level in book.bids if level.price == bid)
            ask_queue_usd = sum(level.size * level.price for level in book.asks if level.price == ask)
            trades = self.trade_flow.recent(
                asset=spec.asset,
                venue=key[0],
                before=snapshot.completed_at,
                max_age_hours=0.25,
                limit=500,
            )
            flow_notional = sum(item.notional_usd for item in trades)
            order_notional = max(1.0, spec.capital_usd)
            queue_burden = bid_queue_usd + ask_queue_usd + 2.0 * order_notional
            queue_fill_probability = min(0.80, flow_notional / max(flow_notional + queue_burden, 1.0))
            spread_return = (ask - bid) / mid
            predicted = queue_fill_probability * spread_return - cost_floor
            if predicted <= 0:
                continue
            payload.update({
                "queue_model_version": "visible_top_queue_v1",
                "bid_queue_ahead_usd": bid_queue_usd,
                "ask_queue_ahead_usd": ask_queue_usd,
                "order_notional_usd": order_notional,
                "recent_trade_flow_notional_usd": flow_notional,
                "estimated_fill_probability": queue_fill_probability,
                "crossed_without_fill_tracked": True,
                "markout_horizons_seconds": [1, 5, 30],
            })
            rows.append(spec.model_copy(update={
                "predicted_net_return": predicted,
                "settlement_payload": payload,
            }))
        rows.sort(key=lambda item: item.predicted_net_return, reverse=True)
        return rows[:8]

    def _maker_markout(
        self,
        *,
        venue: str,
        asset: str,
        symbol: str,
        market_kind: str,
        fill_at: datetime,
        fill_price: float,
        direction: int,
        horizon_seconds: float,
        due_at: datetime,
    ) -> float | None:
        quote = self._quote_after(
            venue=venue,
            asset=asset,
            due_at=fill_at + timedelta(seconds=horizon_seconds),
            market_kind=market_kind or None,
            symbol=symbol,
        )
        if quote is None or quote.observed_at > due_at or fill_price <= 0:
            return None
        return direction * (quote.mid / fill_price - 1.0)

    def _settle_maker(self, trial):
        payload = trial.settlement_payload
        venue = str(payload.get("venue") or "")
        asset = str(payload.get("asset") or trial.asset)
        symbol = str(payload.get("symbol") or "")
        market_kind = str(payload.get("market_kind") or "")
        bid = _number(payload.get("bid"))
        ask = _number(payload.get("ask"))
        mid = _number(payload.get("mid"))
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

        queue_model = str(payload.get("queue_model_version") or "")
        bid_queue = max(0.0, _number(payload.get("bid_queue_ahead_usd")))
        ask_queue = max(0.0, _number(payload.get("ask_queue_ahead_usd")))
        order_notional = max(0.0, _number(payload.get("order_notional_usd")))
        bid_required = bid_queue + order_notional
        ask_required = ask_queue + order_notional
        bid_through = 0.0
        ask_through = 0.0
        bid_touch = False
        ask_touch = False
        bid_fill_at: datetime | None = None
        ask_fill_at: datetime | None = None
        for event in trades:
            side = str(event.payload.get("aggressor_side") or "").lower()
            price = _number(event.payload.get("price"))
            size = _number(event.payload.get("size") or event.payload.get("quantity"))
            notional = max(0.0, price * size)
            if side == "sell" and price > 0 and price <= bid:
                bid_touch = True
                bid_through += notional
                if queue_model and bid_fill_at is None and bid_through >= bid_required:
                    bid_fill_at = event.event_at
            if side == "buy" and price >= ask:
                ask_touch = True
                ask_through += notional
                if queue_model and ask_fill_at is None and ask_through >= ask_required:
                    ask_fill_at = event.event_at

        if queue_model:
            bid_filled = bid_fill_at is not None
            ask_filled = ask_fill_at is not None
        else:
            # Preserve settlement compatibility for already-open legacy trials. All
            # newly generated trials use the conservative visible-queue model above.
            bid_filled = bid_touch
            ask_filled = ask_touch
            bid_fill_at = next((e.event_at for e in trades if str(e.payload.get("aggressor_side") or "").lower() == "sell" and _number(e.payload.get("price"), math.inf) <= bid), None)
            ask_fill_at = next((e.event_at for e in trades if str(e.payload.get("aggressor_side") or "").lower() == "buy" and _number(e.payload.get("price")) >= ask), None)

        if bid_filled and ask_filled:
            gross = (ask - bid) / mid
        elif bid_filled:
            gross = (exit_quote.mid - bid) / mid
        elif ask_filled:
            gross = (ask - exit_quote.mid) / mid
        else:
            gross = 0.0

        markouts: dict[str, float | None] = {}
        adverse_penalty = 0.0
        for horizon in (1, 5, 30):
            if bid_filled and bid_fill_at is not None:
                value = self._maker_markout(
                    venue=venue, asset=asset, symbol=symbol, market_kind=market_kind,
                    fill_at=bid_fill_at, fill_price=bid, direction=1,
                    horizon_seconds=float(horizon), due_at=trial.due_at,
                )
                markouts[f"bid_{horizon}s"] = value
                if horizon == 5 and value is not None:
                    adverse_penalty += max(0.0, -value) * 0.5
            if ask_filled and ask_fill_at is not None:
                value = self._maker_markout(
                    venue=venue, asset=asset, symbol=symbol, market_kind=market_kind,
                    fill_at=ask_fill_at, fill_price=ask, direction=-1,
                    horizon_seconds=float(horizon), due_at=trial.due_at,
                )
                markouts[f"ask_{horizon}s"] = value
                if horizon == 5 and value is not None:
                    adverse_penalty += max(0.0, -value) * 0.5

        fee_buffer = float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0)) / 10_000.0
        net = gross - fee_buffer - adverse_penalty if (bid_filled or ask_filled) else 0.0
        return MechanismSettlementResult(
            matured_at=exit_quote.observed_at,
            gross_return=gross,
            net_return=net,
            settlement_method="shadow_queue_through_fill_plus_markouts_and_inventory_mark",
            detail={
                "venue": venue,
                "trade_count": len(trades),
                "bid_filled": bid_filled,
                "ask_filled": ask_filled,
                "bid_touched": bid_touch,
                "ask_touched": ask_touch,
                "bid_crossed_without_fill": bid_touch and not bid_filled,
                "ask_crossed_without_fill": ask_touch and not ask_filled,
                "bid_trade_through_notional_usd": bid_through,
                "ask_trade_through_notional_usd": ask_through,
                "bid_required_notional_usd": bid_required,
                "ask_required_notional_usd": ask_required,
                "queue_model_version": queue_model or "legacy_existing_trial",
                "exit_mid": exit_quote.mid,
                "empirical_fill_observed": bid_filled or ask_filled,
                "markouts": markouts,
                "adverse_selection_penalty": adverse_penalty,
            },
        )

    def _any_spot_quote_after(self, *, asset: str, due_at: datetime) -> MarketQuote | None:
        table = self.store.market_quotes
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(table.c.payload_json)
                    .where(table.c.asset == asset.upper())
                    .where(table.c.observed_at >= due_at.isoformat())
                    .order_by(table.c.id)
                    .limit(500)
                ).scalars()
            )
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            if quote.market_kind == MarketKind.SPOT and quote.mid > 0:
                return quote
        return None

    def _volatility_specs(self, snapshot, *, total_capital_usd: float):
        base = super()._volatility_specs(snapshot, total_capital_usd=total_capital_usd)
        observations = self.volatility_service.observations()
        rows: list[MechanismTrialSpec] = []
        for spec in base:
            payload = dict(spec.settlement_payload)
            matching = [
                row for row in observations
                if row.venue == str(payload.get("venue") or "")
                and row.underlying == str(payload.get("underlying") or spec.asset).upper()
                and row.expiry.isoformat() == str(payload.get("expiry") or "")
                and abs(row.strike - _number(payload.get("strike"))) < 1e-9
                and row.option_type == str(payload.get("option_type") or "")
            ]
            if matching:
                option = max(matching, key=lambda row: row.observed_at)
                payload.update({
                    "structure_kind": "atm_volatility_risk_premium",
                    "entry_iv": option.implied_volatility,
                    "entry_gamma": option.gamma,
                    "entry_vega": option.vega,
                })
            rows.append(spec.model_copy(update={"settlement_payload": payload}))

        # Add bounded skew and term-structure relative-value cohorts. They remain
        # subject to the same 3->30 forward promotion gate as every mechanism cohort.
        current = [row for row in observations if row.observed_at >= snapshot.completed_at - timedelta(minutes=15)]
        grouped: dict[tuple[str, str], list[object]] = {}
        for option in current:
            grouped.setdefault((option.venue, option.underlying), []).append(option)
        hedge_cost = max(2.0, float(getattr(self.settings, "alpha_research_cost_floor_bps", 10.0))) / 10_000.0
        for (venue, underlying), surface in grouped.items():
            spot = next((q for q in snapshot.market_quotes if q.asset.upper() == underlying and q.market_kind == MarketKind.SPOT), None)
            if spot is None or spot.mid <= 0:
                continue
            expiries = sorted({row.expiry for row in surface})
            # Skew: compare bounded near-ATM call/put IV within each of the nearest expiries.
            for expiry in expiries[:2]:
                expiry_rows = [row for row in surface if row.expiry == expiry]
                calls = [row for row in expiry_rows if row.option_type == "call"]
                puts = [row for row in expiry_rows if row.option_type == "put"]
                if not calls or not puts:
                    continue
                call = min(calls, key=lambda row: abs(abs(row.delta) - 0.40))
                put = min(puts, key=lambda row: abs(abs(row.delta) - 0.40))
                iv_gap = put.implied_volatility - call.implied_volatility
                if abs(iv_gap) < 0.03:
                    continue
                high, low = (put, call) if iv_gap > 0 else (call, put)
                legs = []
                spread_cost = 0.0
                for option, sign in ((high, -1), (low, 1)):
                    mid = (option.bid + option.ask) / 2.0
                    if mid <= 0:
                        legs = []
                        break
                    spread = (option.ask - option.bid) / mid
                    spread_cost += spread * 0.5
                    legs.append({
                        "venue": option.venue,
                        "underlying": option.underlying,
                        "expiry": option.expiry.isoformat(),
                        "strike": option.strike,
                        "option_type": option.option_type,
                        "entry_mid": mid,
                        "entry_iv": option.implied_volatility,
                        "entry_delta": option.delta,
                        "entry_gamma": option.gamma,
                        "entry_vega": option.vega,
                        "direction_sign": sign,
                    })
                predicted = abs(iv_gap) * 0.02 - spread_cost - hedge_cost
                if len(legs) != 2 or predicted <= 0:
                    continue
                holding = max(1.0, min(12.0, (expiry - snapshot.completed_at).total_seconds() / 3600.0 / 4.0))
                rows.append(MechanismTrialSpec(
                    mechanism_id="volatility",
                    cohort_key=f"vol|{venue}|{underlying}|skew_relative_value",
                    asset=underlying,
                    venues=[venue],
                    source_observed_at=max(call.observed_at, put.observed_at),
                    holding_hours=holding,
                    capital_usd=max(100.0, total_capital_usd * 0.01),
                    predicted_net_return=predicted,
                    settlement_payload={
                        "structure_kind": "skew_relative_value",
                        "direction": "relative_value",
                        "legs": legs,
                        "underlying_entry_price": spot.mid,
                        "spread_fraction": spread_cost,
                        "hedge_cost_return": hedge_cost,
                    },
                    conflict_keys=[f"option-surface:{venue}:{underlying}:{expiry.isoformat()}:skew"],
                ))

            # Term structure: same option side, nearest versus next expiry.
            if len(expiries) >= 2:
                near, far = expiries[:2]
                for option_type in ("call", "put"):
                    near_rows = [row for row in surface if row.expiry == near and row.option_type == option_type]
                    far_rows = [row for row in surface if row.expiry == far and row.option_type == option_type]
                    if not near_rows or not far_rows:
                        continue
                    near_option = min(near_rows, key=lambda row: abs(abs(row.delta) - 0.50))
                    far_option = min(far_rows, key=lambda row: abs(abs(row.delta) - 0.50))
                    iv_gap = far_option.implied_volatility - near_option.implied_volatility
                    if abs(iv_gap) < 0.03:
                        continue
                    high, low = (far_option, near_option) if iv_gap > 0 else (near_option, far_option)
                    legs = []
                    spread_cost = 0.0
                    for option, sign in ((high, -1), (low, 1)):
                        mid = (option.bid + option.ask) / 2.0
                        if mid <= 0:
                            legs = []
                            break
                        spread = (option.ask - option.bid) / mid
                        spread_cost += spread * 0.5
                        legs.append({
                            "venue": option.venue,
                            "underlying": option.underlying,
                            "expiry": option.expiry.isoformat(),
                            "strike": option.strike,
                            "option_type": option.option_type,
                            "entry_mid": mid,
                            "entry_iv": option.implied_volatility,
                            "entry_delta": option.delta,
                            "entry_gamma": option.gamma,
                            "entry_vega": option.vega,
                            "direction_sign": sign,
                        })
                    predicted = abs(iv_gap) * 0.02 - spread_cost - hedge_cost
                    if len(legs) != 2 or predicted <= 0:
                        continue
                    holding = max(1.0, min(12.0, (near - snapshot.completed_at).total_seconds() / 3600.0 / 4.0))
                    rows.append(MechanismTrialSpec(
                        mechanism_id="volatility",
                        cohort_key=f"vol|{venue}|{underlying}|term_relative_value|{option_type}",
                        asset=underlying,
                        venues=[venue],
                        source_observed_at=max(near_option.observed_at, far_option.observed_at),
                        holding_hours=holding,
                        capital_usd=max(100.0, total_capital_usd * 0.01),
                        predicted_net_return=predicted,
                        settlement_payload={
                            "structure_kind": "term_structure_relative_value",
                            "direction": "relative_value",
                            "legs": legs,
                            "underlying_entry_price": spot.mid,
                            "spread_fraction": spread_cost,
                            "hedge_cost_return": hedge_cost,
                        },
                        conflict_keys=[f"option-surface:{venue}:{underlying}:{option_type}:term"],
                    ))
        rows.sort(key=lambda item: item.predicted_net_return, reverse=True)
        return rows[:10]

    def _next_surface_leg(self, leg: dict[str, object], due_at: datetime):
        rows = [
            row for row in self.volatility_service.observations()
            if row.venue == str(leg.get("venue") or "")
            and row.underlying == str(leg.get("underlying") or "").upper()
            and row.expiry.isoformat() == str(leg.get("expiry") or "")
            and abs(row.strike - _number(leg.get("strike"))) < 1e-9
            and row.option_type == str(leg.get("option_type") or "")
            and row.observed_at >= due_at
        ]
        return min(rows, key=lambda row: row.observed_at) if rows else None

    def _settle_volatility(self, trial):
        payload = trial.settlement_payload
        structure_kind = str(payload.get("structure_kind") or "atm_volatility_risk_premium")
        underlying_entry = _number(payload.get("underlying_entry_price"))
        underlying_exit = self._any_spot_quote_after(asset=trial.asset, due_at=trial.due_at)
        if underlying_exit is None or underlying_entry <= 0:
            return None
        underlying_move = underlying_exit.mid / underlying_entry - 1.0
        base_hedge_cost = max(0.0, _number(payload.get("hedge_cost_return")))
        spread = max(0.0, _number(payload.get("spread_fraction")))

        if structure_kind in {"skew_relative_value", "term_structure_relative_value"}:
            raw_legs = payload.get("legs")
            if not isinstance(raw_legs, list) or len(raw_legs) != 2:
                return None
            gross_legs: list[float] = []
            entry_net_delta = 0.0
            exit_net_delta = 0.0
            entry_net_gamma = 0.0
            exit_ivs: list[float] = []
            matured = underlying_exit.observed_at
            leg_details: list[dict[str, object]] = []
            for raw_leg in raw_legs:
                if not isinstance(raw_leg, dict):
                    return None
                exit_option = self._next_surface_leg(raw_leg, trial.due_at)
                entry_mid = _number(raw_leg.get("entry_mid"))
                sign = 1 if _number(raw_leg.get("direction_sign")) > 0 else -1
                if exit_option is None or entry_mid <= 0:
                    return None
                exit_mid = (exit_option.bid + exit_option.ask) / 2.0
                option_return = exit_mid / entry_mid - 1.0
                gross_legs.append(sign * option_return)
                entry_delta = _number(raw_leg.get("entry_delta"))
                entry_gamma = _number(raw_leg.get("entry_gamma"))
                entry_net_delta += sign * entry_delta
                exit_net_delta += sign * exit_option.delta
                entry_net_gamma += sign * entry_gamma
                exit_ivs.append(exit_option.implied_volatility)
                matured = max(matured, exit_option.observed_at)
                leg_details.append({
                    "option_type": raw_leg.get("option_type"),
                    "expiry": raw_leg.get("expiry"),
                    "strike": raw_leg.get("strike"),
                    "direction_sign": sign,
                    "entry_mid": entry_mid,
                    "exit_mid": exit_mid,
                    "entry_iv": raw_leg.get("entry_iv"),
                    "exit_iv": exit_option.implied_volatility,
                    "entry_delta": entry_delta,
                    "exit_delta": exit_option.delta,
                })
            gross = statistics.fmean(gross_legs)
            delta_turnover = abs(exit_net_delta - entry_net_delta)
            hedge_cost = base_hedge_cost * (1.0 + delta_turnover)
            residual_delta_penalty = abs(underlying_move) * abs((entry_net_delta + exit_net_delta) / 2.0) * 0.25
            gamma_gap_penalty = min(0.02, abs(entry_net_gamma) * underlying_move * underlying_move * 0.5)
            net = gross - spread - hedge_cost - residual_delta_penalty - gamma_gap_penalty
            return MechanismSettlementResult(
                matured_at=matured,
                gross_return=gross,
                net_return=net,
                settlement_method="option_surface_relative_value_with_dynamic_delta_hedge_and_greek_penalties",
                detail={
                    "structure_kind": structure_kind,
                    "legs": leg_details,
                    "underlying_return": underlying_move,
                    "underlying_exit_venue": underlying_exit.venue,
                    "entry_net_delta": entry_net_delta,
                    "exit_net_delta": exit_net_delta,
                    "delta_turnover": delta_turnover,
                    "hedge_cost_return": hedge_cost,
                    "spread_fraction": spread,
                    "residual_delta_penalty": residual_delta_penalty,
                    "gamma_gap_penalty": gamma_gap_penalty,
                    "exit_iv_mean": statistics.fmean(exit_ivs) if exit_ivs else None,
                },
            )

        option = self._next_option(trial)
        if option is None:
            return None
        entry_mid = _number(payload.get("entry_mid"))
        if entry_mid <= 0:
            return None
        exit_mid = (option.bid + option.ask) / 2.0
        option_return = exit_mid / entry_mid - 1.0
        direction = str(payload.get("direction") or "")
        if direction not in {"long_volatility", "short_volatility"}:
            return None
        directional = option_return if direction == "long_volatility" else -option_return
        entry_delta = _number(payload.get("entry_delta"))
        exit_delta = option.delta
        delta_turnover = abs(exit_delta - entry_delta)
        hedge_cost = base_hedge_cost * (1.0 + delta_turnover)
        residual_delta_penalty = abs(underlying_move) * abs((entry_delta + exit_delta) / 2.0) * 0.25
        entry_gamma = _number(payload.get("entry_gamma"))
        gamma_gap_penalty = min(0.02, abs(entry_gamma) * underlying_move * underlying_move * 0.5)
        net = directional - hedge_cost - spread - residual_delta_penalty - gamma_gap_penalty
        return MechanismSettlementResult(
            matured_at=max(option.observed_at, underlying_exit.observed_at),
            gross_return=directional,
            net_return=net,
            settlement_method="option_mark_forward_with_dynamic_delta_hedge_and_greek_penalties",
            detail={
                "structure_kind": structure_kind,
                "entry_option_mid": entry_mid,
                "exit_option_mid": exit_mid,
                "option_mark_return": option_return,
                "entry_iv": payload.get("entry_iv"),
                "exit_iv": option.implied_volatility,
                "iv_change": option.implied_volatility - _number(payload.get("entry_iv"), option.implied_volatility),
                "entry_delta": entry_delta,
                "exit_delta": exit_delta,
                "delta_turnover": delta_turnover,
                "entry_gamma": payload.get("entry_gamma"),
                "exit_gamma": option.gamma,
                "entry_vega": payload.get("entry_vega"),
                "exit_vega": option.vega,
                "underlying_return": underlying_move,
                "underlying_exit_venue": underlying_exit.venue,
                "residual_delta_penalty": residual_delta_penalty,
                "gamma_gap_penalty": gamma_gap_penalty,
                "hedge_cost_return": hedge_cost,
                "spread_fraction": spread,
            },
        )

    def _liquidation_specs(self, snapshot, *, total_capital_usd: float):
        base = super()._liquidation_specs(snapshot, total_capital_usd=total_capital_usd)
        events = {event.event_id: event for event in self._liquidation_events(now=snapshot.completed_at)}
        rows: list[MechanismTrialSpec] = []
        for spec in base:
            payload = dict(spec.settlement_payload)
            event = events.get(str(payload.get("event_id") or ""))
            if event is None:
                continue
            latency_seconds = max(0.0, (event.observed_at - event.event_at).total_seconds())
            event_notional = _number(payload.get("entry_price")) * _number(payload.get("quantity"))
            size_factor = min(1.0, event_notional / max(spec.capital_usd * 5.0, 1.0))
            time_factor = math.exp(-latency_seconds / 2.0)
            capture_probability = max(0.02, min(0.75, time_factor * size_factor))
            predicted = spec.predicted_net_return * capture_probability
            if predicted <= 0:
                continue
            payload.update({
                "event_at": event.event_at.isoformat(),
                "observed_at": event.observed_at.isoformat(),
                "observation_latency_seconds": latency_seconds,
                "event_notional_usd": event_notional,
                "capture_probability": capture_probability,
                "entry_reference": "first_reachable_quote_after_observation",
            })
            rows.append(spec.model_copy(update={
                "source_observed_at": event.observed_at,
                "predicted_net_return": predicted,
                "settlement_payload": payload,
            }))
        rows.sort(key=lambda item: item.predicted_net_return, reverse=True)
        return rows[:8]

    def _settle_liquidation(self, trial):
        payload = trial.settlement_payload
        venue = str(payload.get("venue") or "")
        asset = str(payload.get("asset") or trial.asset)
        symbol = str(payload.get("symbol") or "")
        direction = str(payload.get("direction") or "")
        event_price = _number(payload.get("entry_price"))
        observed_raw = str(payload.get("observed_at") or "")
        if event_price <= 0 or direction not in {"long", "short"}:
            return None
        try:
            observed_at = datetime.fromisoformat(observed_raw.replace("Z", "+00:00")) if observed_raw else trial.source_observed_at
        except ValueError:
            observed_at = trial.source_observed_at
        reachable = self._quote_after(
            venue=venue,
            asset=asset,
            due_at=observed_at,
            market_kind=MarketKind.PERPETUAL.value,
            symbol=symbol or None,
        )
        recovery = self._quote_after(
            venue=venue,
            asset=asset,
            due_at=trial.due_at,
            market_kind=MarketKind.PERPETUAL.value,
            symbol=symbol or None,
        )
        if reachable is None or recovery is None or reachable.mid <= 0:
            return None
        gross_recovery = recovery.mid / reachable.mid - 1.0 if direction == "long" else 1.0 - recovery.mid / reachable.mid
        event_to_reachable = reachable.mid / event_price - 1.0 if direction == "long" else 1.0 - reachable.mid / event_price
        capture_probability = max(0.0, min(1.0, _number(payload.get("capture_probability"), 0.0)))
        cost = max(0.0, _number(payload.get("cost_return")))
        gross = gross_recovery * capture_probability
        net = gross - cost * capture_probability
        return MechanismSettlementResult(
            matured_at=recovery.observed_at,
            gross_return=gross,
            net_return=net,
            settlement_method="latency_adjusted_capture_probability_recovery_shadow",
            detail={
                "event_id": payload.get("event_id"),
                "event_price": event_price,
                "first_reachable_price": reachable.mid,
                "recovery_price": recovery.mid,
                "direction": direction,
                "observation_latency_seconds": payload.get("observation_latency_seconds"),
                "capture_probability": capture_probability,
                "event_to_reachable_return": event_to_reachable,
                "raw_recovery_return": gross_recovery,
                "capture_assumed": False,
                "paper_capture_probability_model": True,
            },
        )


class AllLaneQualifiedOpportunityAllocatorService(
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService
):
    """Read both the established bridge and forward-qualified mechanism candidates."""

    def __init__(self, core, cex_dex, alpha_factory):
        super().__init__(core, cex_dex, alpha_factory)
        store = getattr(alpha_factory, "store", None)
        if store is None:
            raise RuntimeError("all-lane allocator requires durable evidence")
        self.mechanisms = ExecutableMechanismExecutionService(core, store)

    def _mechanism_proxy_candidates(self, *, total_capital_usd: float):
        now = _now()
        rows = []
        stale = []
        for item in self.mechanisms.promoted_proxy_candidates(
            total_capital_usd=total_capital_usd
        ):
            horizon_seconds = max(300.0, float(item.modeled_holding_hours or 1.0) * 3600.0)
            max_age = min(86_400.0, horizon_seconds)
            source_at = item.source_observed_at
            age = (now - source_at).total_seconds() if source_at is not None else None
            if age is None or age < 0.0 or age > max_age:
                stale.append(
                    {
                        "candidate_id": item.candidate_id,
                        "family": "mechanism",
                        "reason": "mechanism candidate evidence stale; awaiting fresh forward-qualified observation",
                        "source_observed_at": source_at.isoformat() if source_at else None,
                        "max_age_seconds": max_age,
                    }
                )
                continue
            rows.append(item)
        return rows, stale

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        bridge_candidates, failures, bridge_stale = self._active_candidates_with_diagnostics()
        mechanism_candidates, mechanism_stale = self._mechanism_proxy_candidates(
            total_capital_usd=total_capital_usd
        )
        plan = allocate_prequalified_candidates(
            self.settings,
            candidates=[*bridge_candidates, *mechanism_candidates],
            family_failures=failures,
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )
        converted: list[UnifiedPaperAllocation] = []
        for allocation in plan.allocations:
            if allocation.opportunity_id in MECHANISM_IDS:
                converted.append(allocation.model_copy(update={"family": "mechanism"}))
            else:
                converted.append(allocation)
        return plan.model_copy(
            update={
                "allocations": converted,
                "skipped": [*bridge_stale, *mechanism_stale, *plan.skipped],
            }
        )


class AllLaneAllocationForwardCertificationService(
    CexDexAwareAllocationForwardCertificationService
):
    """Extend canonical forward settlement to every mechanism lane."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.mechanisms = ExecutableMechanismExecutionService(core, store)

    def trial_from_allocation(
        self,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        if allocation.family != "mechanism":
            return super().trial_from_allocation(
                allocation,
                plan_observed_at=plan_observed_at,
            )
        candidate = self.mechanisms.ledger.candidate(allocation.candidate_id)
        if candidate is None:
            base = super().trial_from_allocation(
                allocation.model_copy(update={"family": "alpha"}),
                plan_observed_at=plan_observed_at,
            )
            return base.model_copy(
                update={
                    "family": "mechanism",
                    "settlement_supported": False,
                    "settlement_method": None,
                    "settlement_blocker": "durable mechanism candidate lineage is unavailable",
                }
            )
        due = candidate.observed_at + timedelta(hours=candidate.holding_hours)
        return PaperAllocationTrial(
            plan_observed_at=plan_observed_at,
            candidate_id=allocation.candidate_id,
            family="mechanism",
            strategy=allocation.strategy,
            asset=allocation.asset,
            venues=allocation.venues,
            exposure_kind=allocation.exposure_kind,
            capital_required_usd=allocation.capital_required_usd,
            notional_usd=allocation.notional_usd_per_leg,
            predicted_profit_usd=allocation.expected_profit_usd_per_deployment,
            predicted_return_on_reserved_capital=allocation.expected_return_on_reserved_capital,
            source_observed_at=candidate.observed_at,
            due_at=due,
            instrument_symbol=allocation.instrument_symbol or candidate.asset,
            instrument_market_kind="mechanism",
            entry_reference_price=1.0,
            modeled_roundtrip_cost_return=0.0,
            settlement_supported=True,
            settlement_method=f"mechanism:{candidate.mechanism_id}",
            settlement_blocker=None,
            cohort_key=f"mechanism|{candidate.cohort_key}",
            live_execution_authority=False,
            paper_only=True,
        )

    def _settle_mechanism(
        self,
        trial: PaperAllocationTrial,
    ) -> PaperAllocationOutcome | None:
        result = self.mechanisms.settle_canonical_candidate(
            trial.candidate_id,
            due_at=trial.due_at,
        )
        if result is None:
            return None
        realized_profit = trial.capital_required_usd * result.net_return
        error = realized_profit - trial.predicted_profit_usd
        capture = (
            realized_profit / trial.predicted_profit_usd
            if trial.predicted_profit_usd > 0
            else None
        )
        return PaperAllocationOutcome(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate_id,
            family="mechanism",
            strategy=trial.strategy,
            asset=trial.asset,
            matured_at=result.matured_at,
            due_at=trial.due_at or result.matured_at,
            realized_gross_return=result.gross_return,
            realized_net_return=result.net_return,
            realized_profit_usd=realized_profit,
            predicted_profit_usd=trial.predicted_profit_usd,
            prediction_error_usd=error,
            profit_capture_ratio=capture,
            profitable=realized_profit > 0,
            settlement_method=result.settlement_method,
            settlement_evidence_complete=True,
            live_execution_authority=False,
            paper_only=True,
        )

    def _settle_trial(self, trial, snapshot):
        if str(trial.settlement_method or "").startswith("mechanism:"):
            return self._settle_mechanism(trial)
        return super()._settle_trial(trial, snapshot)


class AllLaneOperationallyResilientPaperPortfolioService(
    CexDexUniversalOperationallyResilientPaperPortfolioService
):
    """Canonical paper account with all thirteen lane settlement contracts enabled."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.settlement = AllLaneAllocationForwardCertificationService(
            core,
            allocator,
            store,
        )

    def _trial_for_allocation(
        self,
        allocation: UnifiedPaperAllocation,
        *,
        plan_observed_at: datetime,
    ) -> PaperAllocationTrial:
        return self.settlement.trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )

    def _support_reason(
        self,
        allocation: UnifiedPaperAllocation,
    ) -> tuple[bool, str | None]:
        trial = self._trial_for_allocation(
            allocation,
            plan_observed_at=allocation.source_observed_at or _now(),
        )
        if trial.settlement_supported:
            return True, None
        return False, trial.settlement_blocker or "allocation lacks a canonical settlement contract"

    def _mark_universal(self, position, trial, snapshot):
        if not str(trial.settlement_method or "").startswith("mechanism:"):
            return super()._mark_universal(position, trial, snapshot)
        return CanonicalPortfolioEvent(
            event_type="mark",
            observed_at=snapshot.completed_at,
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
            reference_price=position.current_reference_price,
            due_at=position.due_at,
            modeled_roundtrip_cost_return=position.modeled_roundtrip_cost_return,
            unrealized_pnl_usd=0.0,
            details={
                "valuation_method": "pending_native_mechanism_forward_settlement",
                "settlement_method": trial.settlement_method,
                "paper_only": True,
                "live_execution_authority": False,
            },
        )
#!/usr/bin/env python3
"""Compatibility runner for the isolated Convex Scout research replay."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("convex_scout_backtest.py")
spec = importlib.util.spec_from_file_location("convex_scout_backtest", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("Unable to load convex_scout_backtest.py")
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def corrected_signals(con, phase_start, phase_end, top_n, confirm_seconds=20):
    query = f"""
    WITH token_launch AS (
        SELECT token, min(ts) AS launch_ts
        FROM trades
        GROUP BY token
    ), qualified AS (
        SELECT t.*, s.rank
        FROM trades t
        JOIN scouts s USING(wallet)
        WHERE s.rank <= {int(top_n)}
          AND t.action='buy'
          AND t.ts >= ? AND t.ts < ?
    ), first_scout AS (
        SELECT q.*, l.launch_ts,
               row_number() OVER (PARTITION BY q.token ORDER BY q.ts, q.slot, q.wallet) AS rn
        FROM qualified q
        JOIN token_launch l USING(token)
        WHERE q.ts <= l.launch_ts + INTERVAL '180 seconds'
    ), first_only AS (
        SELECT * FROM first_scout WHERE rn=1
    ), pair_candidates AS (
        SELECT
            f.token, f.launch_ts,
            f.ts AS first_ts, f.wallet AS first_wallet, f.creator,
            f.slot AS first_slot, f.mark_price AS first_price,
            b.ts AS confirm_ts, b.wallet AS confirm_wallet,
            b.mark_price AS confirm_price, b.reserve_sol,
            row_number() OVER (PARTITION BY f.token ORDER BY b.ts, b.slot, b.wallet) AS confirm_rn
        FROM first_only f
        JOIN qualified b ON b.token=f.token
                            AND b.wallet <> f.wallet
                            AND b.ts > f.ts
                            AND b.ts <= f.ts + INTERVAL '{int(confirm_seconds)} seconds'
        WHERE f.wallet <> coalesce(f.creator, '')
          AND b.wallet <> coalesce(f.creator, '')
    ), pairs AS (
        SELECT * FROM pair_candidates WHERE confirm_rn=1
    ), aggregated AS (
        SELECT
            p.token, p.launch_ts, p.first_ts, p.confirm_ts,
            p.first_wallet, p.confirm_wallet,
            p.first_price, p.confirm_price, p.reserve_sol,
            sum(CASE WHEN t.action='buy' THEN t.sol_amount ELSE 0 END) AS buy_sol_pre,
            sum(CASE WHEN t.action='sell' THEN t.sol_amount ELSE 0 END) AS sell_sol_pre,
            max(CASE WHEN t.action='sell' AND t.wallet=p.creator THEN 1 ELSE 0 END) > 0 AS creator_sold,
            count(DISTINCT CASE WHEN t.action='buy' AND t.slot=p.first_slot THEN t.wallet END) AS same_slot_buyers
        FROM pairs p
        JOIN trades t ON t.token=p.token AND t.ts >= p.launch_ts AND t.ts <= p.confirm_ts
        GROUP BY
            p.token, p.launch_ts, p.first_ts, p.confirm_ts,
            p.first_wallet, p.confirm_wallet, p.first_price, p.confirm_price,
            p.reserve_sol
    )
    SELECT
        row_number() OVER (ORDER BY confirm_ts, token) - 1 AS sid,
        token, launch_ts, first_ts, confirm_ts,
        first_wallet, confirm_wallet, first_price, confirm_price, reserve_sol,
        buy_sol_pre, sell_sol_pre, creator_sold, same_slot_buyers
    FROM aggregated
    ORDER BY confirm_ts, token
    """
    rows = con.execute(query, [phase_start, phase_end]).fetchall()
    result = []
    for row in rows:
        result.append(
            mod.Signal(
                sid=int(row[0]), token=str(row[1]), launch_ts=row[2], first_ts=row[3],
                confirm_ts=row[4], first_wallet=str(row[5]), confirm_wallet=str(row[6]),
                first_price=float(row[7]), confirm_price=float(row[8]), reserve_sol=float(row[9] or 0),
                buy_sol_pre=float(row[10] or 0), sell_sol_pre=float(row[11] or 0),
                creator_sold=bool(row[12]), same_slot_buyers=int(row[13] or 0),
            )
        )
    return result


mod._signals = corrected_signals
raise SystemExit(mod.main())

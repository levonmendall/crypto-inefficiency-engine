#!/usr/bin/env python3
"""Chronological Convex Scout research replay on a public Pump.fun raw day.

Research-only. It does not import or modify the canonical runtime.  It deliberately
uses an earlier training window to select wallets, a validation window to choose
strategy parameters, and an untouched holdout window for the reported result.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import statistics
import urllib.request
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

DATA_URL = (
    "https://github.com/z17620794987-hub/pumpfun-market-lab/releases/download/"
    "dataset-2026-07-31/pump_fun_2026-07-31_full_day_raw.zip"
)


@dataclass(frozen=True)
class Signal:
    sid: int
    token: str
    launch_ts: datetime
    first_ts: datetime
    confirm_ts: datetime
    first_wallet: str
    confirm_wallet: str
    first_price: float
    confirm_price: float
    reserve_sol: float
    buy_sol_pre: float
    sell_sol_pre: float
    creator_sold: bool
    same_slot_buyers: int


@dataclass
class ReplayResult:
    exit_variant: str
    latency_seconds: int
    roundtrip_cost: float
    position_fraction: float
    trades: int
    wins: int
    mean_return: float
    median_return: float
    profit_factor: float
    compounded_return: float
    max_drawdown: float
    top3_removed_return: float
    top1pct_removed_return: float
    skipped_capacity: int


def _download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size > 100_000_000:
        return
    request = urllib.request.Request(url, headers={"User-Agent": "convex-scout-research/1.0"})
    with urllib.request.urlopen(request, timeout=120) as src, destination.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)


def _extract(zip_path: Path, destination: Path) -> list[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    existing = sorted(destination.rglob("*.parquet"))
    if existing:
        return existing
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(destination)
    files = sorted(destination.rglob("*.parquet"))
    if not files:
        raise RuntimeError("No parquet files found in downloaded raw archive")
    return files


def _timestamp_expression(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.execute("DESCRIBE raw").fetchall()
    types = {str(row[0]).lower(): str(row[1]).upper() for row in rows}
    timestamp_type = types.get("timestamp", "")
    if "TIMESTAMP" in timestamp_type or "DATE" in timestamp_type:
        return "CAST(timestamp AS TIMESTAMP)"
    if any(kind in timestamp_type for kind in ("INT", "DOUBLE", "FLOAT", "DECIMAL")):
        maximum = con.execute("SELECT MAX(CAST(timestamp AS DOUBLE)) FROM raw").fetchone()[0]
        if maximum is None:
            raise RuntimeError("timestamp column is empty")
        value = float(maximum)
        if value > 1e15:
            return "to_timestamp(CAST(timestamp AS DOUBLE) / 1000000.0)"
        if value > 1e12:
            return "to_timestamp(CAST(timestamp AS DOUBLE) / 1000.0)"
        return "to_timestamp(CAST(timestamp AS DOUBLE))"
    return "TRY_CAST(timestamp AS TIMESTAMP)"


def _prepare(con: duckdb.DuckDBPyConnection, parquet_root: Path) -> tuple[datetime, datetime]:
    glob = str(parquet_root / "**" / "*.parquet").replace("'", "''")
    con.execute(
        f"CREATE VIEW raw AS SELECT * FROM read_parquet('{glob}', union_by_name=true)"
    )
    ts_expr = _timestamp_expression(con)
    con.execute(
        f"""
        CREATE TABLE trades AS
        SELECT
            CAST(token_mint AS VARCHAR) AS token,
            CAST(user_wallet AS VARCHAR) AS wallet,
            CAST(token_creator AS VARCHAR) AS creator,
            CAST(slot_number AS BIGINT) AS slot,
            {ts_expr} AS ts,
            lower(CAST(action AS VARCHAR)) AS action,
            CAST(token_amount AS DOUBLE) AS token_amount,
            CAST(lamports_amount AS DOUBLE) / 1e9 AS sol_amount,
            coalesce(CAST(fee_lamports AS DOUBLE), 0.0) / 1e9 AS fee_sol,
            CAST(virtual_lamports_reserve AS DOUBLE)
                / nullif(CAST(virtual_token_reserve AS DOUBLE), 0.0) AS mark_price,
            CAST(real_lamports_reserve AS DOUBLE) / 1e9 AS reserve_sol
        FROM raw
        WHERE action IS NOT NULL
          AND user_wallet IS NOT NULL
          AND token_mint IS NOT NULL
          AND {ts_expr} IS NOT NULL
          AND lower(CAST(action AS VARCHAR)) IN ('buy', 'sell')
          AND CAST(token_amount AS DOUBLE) > 0
          AND CAST(lamports_amount AS DOUBLE) >= 0
          AND CAST(virtual_token_reserve AS DOUBLE) > 0
          AND CAST(virtual_lamports_reserve AS DOUBLE) > 0
        """
    )
    con.execute("CREATE INDEX idx_trades_token_ts ON trades(token, ts)")
    con.execute("CREATE INDEX idx_trades_wallet_ts ON trades(wallet, ts)")
    start, end = con.execute("SELECT MIN(ts), MAX(ts) FROM trades").fetchone()
    if start is None or end is None:
        raise RuntimeError("No usable buy/sell trades found")
    return start, end


def _build_wallet_ranking(
    con: duckdb.DuckDBPyConnection, train_start: datetime, train_end: datetime
) -> list[dict[str, Any]]:
    con.execute("DROP TABLE IF EXISTS wallet_token_train")
    con.execute(
        """
        CREATE TEMP TABLE wallet_token_train AS
        WITH aggregate AS (
            SELECT
                wallet,
                token,
                sum(CASE WHEN action='buy' THEN sol_amount + fee_sol ELSE 0 END) AS buy_cost,
                sum(CASE WHEN action='sell' THEN greatest(sol_amount - fee_sol, 0) ELSE 0 END) AS sell_proceeds,
                sum(CASE WHEN action='buy' THEN token_amount ELSE 0 END) AS buy_tokens,
                sum(CASE WHEN action='sell' THEN token_amount ELSE 0 END) AS sell_tokens
            FROM trades
            WHERE ts >= ? AND ts < ?
            GROUP BY wallet, token
        ), closed AS (
            SELECT *, sell_tokens / nullif(buy_tokens, 0) AS sold_ratio
            FROM aggregate
            WHERE buy_cost > 0 AND buy_tokens > 0 AND sell_tokens > 0
        )
        SELECT
            wallet,
            token,
            buy_cost * least(sold_ratio, 1.0) AS cost_basis,
            sell_proceeds,
            sell_proceeds - buy_cost * least(sold_ratio, 1.0) AS pnl,
            (sell_proceeds - buy_cost * least(sold_ratio, 1.0))
                / nullif(buy_cost * least(sold_ratio, 1.0), 0) AS roi
        FROM closed
        WHERE sold_ratio BETWEEN 0.70 AND 1.20
        """,
        [train_start, train_end],
    )
    rows = con.execute(
        """
        WITH by_wallet AS (
            SELECT
                wallet,
                count(*) AS closed_tokens,
                avg(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
                sum(pnl) / nullif(sum(cost_basis), 0) AS realized_roi,
                median(roi) AS median_token_roi,
                sum(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS positive_pnl,
                max(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS max_positive_pnl
            FROM wallet_token_train
            GROUP BY wallet
        )
        SELECT
            wallet, closed_tokens, win_rate, realized_roi, median_token_roi,
            CASE WHEN positive_pnl > 0 THEN max_positive_pnl / positive_pnl ELSE 1 END AS max_winner_share,
            (
                0.30 * least(greatest(realized_roi, -1.0), 3.0) / 3.0
                + 0.30 * win_rate
                + 0.20 * least(greatest(median_token_roi, -1.0), 1.0)
                + 0.20 * least(ln(1 + closed_tokens) / ln(51.0), 1.0)
            ) AS quality_score
        FROM by_wallet
        WHERE closed_tokens >= 5
          AND win_rate >= 0.55
          AND realized_roi > 0
          AND median_token_roi > -0.05
          AND CASE WHEN positive_pnl > 0 THEN max_positive_pnl / positive_pnl ELSE 1 END <= 0.60
        ORDER BY quality_score DESC, closed_tokens DESC
        """
    ).fetchall()
    if len(rows) < 50:
        rows = con.execute(
            """
            WITH by_wallet AS (
                SELECT wallet, count(*) AS closed_tokens,
                       avg(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
                       sum(pnl) / nullif(sum(cost_basis), 0) AS realized_roi,
                       median(roi) AS median_token_roi,
                       sum(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS positive_pnl,
                       max(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS max_positive_pnl
                FROM wallet_token_train GROUP BY wallet
            )
            SELECT wallet, closed_tokens, win_rate, realized_roi, median_token_roi,
                   CASE WHEN positive_pnl > 0 THEN max_positive_pnl / positive_pnl ELSE 1 END,
                   (0.45 * least(greatest(realized_roi, -1.0), 3.0) / 3.0
                    + 0.35 * win_rate
                    + 0.20 * least(ln(1 + closed_tokens) / ln(51.0), 1.0)) AS quality_score
            FROM by_wallet
            WHERE closed_tokens >= 3 AND win_rate >= 0.50 AND realized_roi > 0
            ORDER BY quality_score DESC, closed_tokens DESC
            """
        ).fetchall()
    names = [
        "wallet", "closed_tokens", "win_rate", "realized_roi", "median_token_roi",
        "max_winner_share", "quality_score"
    ]
    return [dict(zip(names, row)) for row in rows]


def _install_scouts(con: duckdb.DuckDBPyConnection, ranking: list[dict[str, Any]]) -> None:
    con.execute("DROP TABLE IF EXISTS scouts")
    con.execute("CREATE TEMP TABLE scouts(wallet VARCHAR, rank INTEGER)")
    con.executemany(
        "INSERT INTO scouts VALUES (?, ?)",
        [(str(item["wallet"]), index + 1) for index, item in enumerate(ranking[:300])],
    )


def _signals(
    con: duckdb.DuckDBPyConnection,
    phase_start: datetime,
    phase_end: datetime,
    top_n: int,
    confirm_seconds: int = 20,
) -> list[Signal]:
    # First qualified scout must touch a newly-active token, then a distinct qualified
    # scout must buy within the confirmation window.  Everything in the risk summary
    # is computed no later than confirmation time.
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
    )
    SELECT
        row_number() OVER (ORDER BY p.confirm_ts, p.token) - 1 AS sid,
        p.token, p.launch_ts, p.first_ts, p.confirm_ts,
        p.first_wallet, p.confirm_wallet,
        p.first_price, p.confirm_price, p.reserve_sol,
        sum(CASE WHEN t.action='buy' THEN t.sol_amount ELSE 0 END) AS buy_sol_pre,
        sum(CASE WHEN t.action='sell' THEN t.sol_amount ELSE 0 END) AS sell_sol_pre,
        max(CASE WHEN t.action='sell' AND t.wallet=p.creator THEN 1 ELSE 0 END) > 0 AS creator_sold,
        count(DISTINCT CASE WHEN t.action='buy' AND t.slot=p.first_slot THEN t.wallet END) AS same_slot_buyers
    FROM pairs p
    JOIN trades t ON t.token=p.token AND t.ts >= p.launch_ts AND t.ts <= p.confirm_ts
    GROUP BY ALL
    ORDER BY p.confirm_ts, p.token
    """
    rows = con.execute(query, [phase_start, phase_end]).fetchall()
    result: list[Signal] = []
    for row in rows:
        result.append(
            Signal(
                sid=int(row[0]), token=str(row[1]), launch_ts=row[2], first_ts=row[3],
                confirm_ts=row[4], first_wallet=str(row[5]), confirm_wallet=str(row[6]),
                first_price=float(row[7]), confirm_price=float(row[8]), reserve_sol=float(row[9] or 0),
                buy_sol_pre=float(row[10] or 0), sell_sol_pre=float(row[11] or 0),
                creator_sold=bool(row[12]), same_slot_buyers=int(row[13] or 0),
            )
        )
    return result


def _paths(
    con: duckdb.DuckDBPyConnection, signals: list[Signal], horizon_seconds: int = 600
) -> dict[int, list[tuple[datetime, float]]]:
    if not signals:
        return {}
    con.execute("DROP TABLE IF EXISTS research_signals")
    con.execute("CREATE TEMP TABLE research_signals(sid INTEGER, token VARCHAR, confirm_ts TIMESTAMP)")
    con.executemany(
        "INSERT INTO research_signals VALUES (?, ?, ?)",
        [(s.sid, s.token, s.confirm_ts) for s in signals],
    )
    rows = con.execute(
        f"""
        SELECT s.sid, t.ts, t.mark_price
        FROM research_signals s
        JOIN trades t ON t.token=s.token
                     AND t.ts >= s.confirm_ts
                     AND t.ts <= s.confirm_ts + INTERVAL '{int(horizon_seconds)} seconds'
        WHERE t.mark_price > 0
        ORDER BY s.sid, t.ts, t.slot
        """
    ).fetchall()
    paths: dict[int, list[tuple[datetime, float]]] = defaultdict(list)
    for sid, ts, price in rows:
        paths[int(sid)].append((ts, float(price)))
    return dict(paths)


def _eligible(signal: Signal, max_drift: float, strict_risk: bool) -> bool:
    if signal.first_price <= 0 or signal.confirm_price <= 0:
        return False
    ratio = signal.confirm_price / signal.first_price
    if ratio < 0.90 or ratio > 1.0 + max_drift:
        return False
    if signal.creator_sold:
        return False
    if not strict_risk:
        return True
    sell_pressure = signal.sell_sol_pre / max(signal.buy_sol_pre, 1e-12)
    return (
        sell_pressure <= 0.15
        and signal.same_slot_buyers < 3
        and signal.reserve_sol >= 0.50
    )


def _trade_return(
    signal: Signal,
    path: list[tuple[datetime, float]],
    latency_seconds: int,
    roundtrip_cost: float,
    variant: str,
) -> tuple[float, datetime] | None:
    target_entry = signal.confirm_ts + timedelta(seconds=latency_seconds)
    entry_index = next((i for i, (ts, _) in enumerate(path) if ts >= target_entry), None)
    if entry_index is None:
        return None
    per_side = roundtrip_cost / 2.0
    entry_ts, raw_entry = path[entry_index]
    entry = raw_entry * (1.0 + per_side)
    if entry <= 0:
        return None
    remaining = 1.0
    proceeds = 0.0
    basis = 1.0
    hit_35 = False
    hit_2x = False
    hit_3x = False
    high = raw_entry
    momentum_hit = False
    end_ts = path[-1][0]

    def sell(fraction: float, raw_price: float) -> None:
        nonlocal remaining, proceeds
        fraction = min(fraction, remaining)
        if fraction <= 0:
            return
        fill = raw_price * (1.0 - per_side)
        proceeds += fraction * fill / entry
        remaining -= fraction

    timeout_seconds = 300 if variant == "fast" else 600
    for ts, price in path[entry_index:]:
        elapsed = (ts - entry_ts).total_seconds()
        if elapsed < 0:
            continue
        multiple = price / entry
        high = max(high, price)
        if multiple >= 1.10:
            momentum_hit = True

        if multiple <= 0.85:
            sell(remaining, price)
            return proceeds - basis, ts

        # Stagnation exit: the edge is expected to reveal itself quickly.
        if elapsed >= 90 and not momentum_hit:
            sell(remaining, price)
            return proceeds - basis, ts

        if variant == "fast":
            if multiple >= 1.35:
                sell(remaining, price)
                return proceeds - basis, ts
        elif variant == "hybrid":
            if not hit_35 and multiple >= 1.35:
                sell(0.50, price)
                hit_35 = True
            if not hit_2x and multiple >= 2.0:
                sell(0.25, price)
                hit_2x = True
            if hit_2x and remaining > 0 and price <= high * 0.75:
                sell(remaining, price)
                return proceeds - basis, ts
        elif variant == "ladder":
            if not hit_35 and multiple >= 1.35:
                sell(0.20, price)
                hit_35 = True
            if not hit_2x and multiple >= 2.0:
                sell(0.30, price)
                hit_2x = True
            if not hit_3x and multiple >= 3.0:
                sell(0.20, price)
                hit_3x = True
            if hit_3x and remaining > 0 and price <= high * 0.75:
                sell(remaining, price)
                return proceeds - basis, ts
        else:
            raise ValueError(f"unknown variant {variant}")

        if elapsed >= timeout_seconds:
            sell(remaining, price)
            return proceeds - basis, ts

    if remaining > 0:
        sell(remaining, path[-1][1])
    return proceeds - basis, end_ts


def _portfolio_metrics(
    signals: list[Signal],
    paths: dict[int, list[tuple[datetime, float]]],
    max_drift: float,
    strict_risk: bool,
    exit_variant: str,
    latency_seconds: int,
    roundtrip_cost: float,
    position_fraction: float = 0.005,
    max_concurrent: int = 5,
) -> ReplayResult:
    returns: list[float] = []
    active_until: list[datetime] = []
    skipped_capacity = 0
    equity = 1.0
    peak_equity = 1.0
    max_dd = 0.0
    equity_returns: list[float] = []

    for signal in sorted(signals, key=lambda s: s.confirm_ts):
        if not _eligible(signal, max_drift=max_drift, strict_risk=strict_risk):
            continue
        path = paths.get(signal.sid, [])
        active_until = [end for end in active_until if end > signal.confirm_ts]
        if len(active_until) >= max_concurrent:
            skipped_capacity += 1
            continue
        outcome = _trade_return(
            signal, path, latency_seconds=latency_seconds,
            roundtrip_cost=roundtrip_cost, variant=exit_variant,
        )
        if outcome is None:
            continue
        ret, exit_ts = outcome
        # A token can gap far beyond the nominal stop; preserve the realized mark.
        ret = max(ret, -1.0)
        returns.append(ret)
        active_until.append(exit_ts)
        equity *= 1.0 + position_fraction * ret
        equity_returns.append(ret)
        peak_equity = max(peak_equity, equity)
        if peak_equity > 0:
            max_dd = max(max_dd, (peak_equity - equity) / peak_equity)

    wins = sum(1 for r in returns if r > 0)
    gains = sum(r for r in returns if r > 0)
    losses = -sum(r for r in returns if r < 0)
    profit_factor = gains / losses if losses > 0 else (math.inf if gains > 0 else 0.0)

    def compounded_without(excluded_indices: set[int]) -> float:
        value = 1.0
        for i, r in enumerate(returns):
            if i not in excluded_indices:
                value *= 1.0 + position_fraction * r
        return value - 1.0

    ordered = sorted(range(len(returns)), key=lambda i: returns[i], reverse=True)
    top3_removed = compounded_without(set(ordered[:3]))
    remove_count = max(1, math.ceil(len(returns) * 0.01)) if returns else 0
    top1_removed = compounded_without(set(ordered[:remove_count]))

    return ReplayResult(
        exit_variant=exit_variant,
        latency_seconds=latency_seconds,
        roundtrip_cost=roundtrip_cost,
        position_fraction=position_fraction,
        trades=len(returns),
        wins=wins,
        mean_return=statistics.fmean(returns) if returns else 0.0,
        median_return=statistics.median(returns) if returns else 0.0,
        profit_factor=profit_factor,
        compounded_return=equity - 1.0,
        max_drawdown=max_dd,
        top3_removed_return=top3_removed,
        top1pct_removed_return=top1_removed,
        skipped_capacity=skipped_capacity,
    )


def _serialize(result: ReplayResult) -> dict[str, Any]:
    data = dict(result.__dict__)
    if math.isinf(float(data["profit_factor"])):
        data["profit_factor"] = "inf"
    return data


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", default="/tmp/convex-scout-research")
    parser.add_argument("--output", default="convex-scout-backtest.json")
    args = parser.parse_args()
    work = Path(args.work_dir)
    archive = work / "pump_fun_2026-07-31_full_day_raw.zip"
    extracted = work / "raw"
    _download(DATA_URL, archive)
    parquet_files = _extract(archive, extracted)

    con = duckdb.connect(str(work / "research.duckdb"))
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='5GB'")
    start, end = _prepare(con, extracted)
    span = end - start
    train_end = start + span * 0.50
    validation_end = start + span * 0.75
    ranking = _build_wallet_ranking(con, start, train_end)
    if len(ranking) < 10:
        raise RuntimeError(f"Only {len(ranking)} wallets passed training filters")
    _install_scouts(con, ranking)

    report: dict[str, Any] = {
        "source": DATA_URL,
        "parquet_files": len(parquet_files),
        "raw_time_start": start.isoformat(),
        "raw_time_end": end.isoformat(),
        "train_end": train_end.isoformat(),
        "validation_end": validation_end.isoformat(),
        "wallets_passing_training_filter": len(ranking),
        "top_wallets": ranking[:20],
        "methodology": {
            "split": "first 50% train wallets; next 25% tune; final 25% untouched holdout",
            "entity_independence": "distinct addresses only; funding-graph entity resolution unavailable in this raw feed",
            "position_fraction": 0.005,
            "max_concurrent": 5,
            "stop": -0.15,
            "stagnation": "exit after 90s if +10% momentum never appears",
            "costs": "round-trip execution stress split evenly across entry and exit",
        },
    }

    # Validation chooses only structural parameters at a conservative 5s / 5% stress.
    validation_cache: dict[int, tuple[list[Signal], dict[int, list[tuple[datetime, float]]]]] = {}
    validation_candidates: list[dict[str, Any]] = []
    for top_n in (50, 100, 200):
        top_n = min(top_n, len(ranking))
        if top_n in validation_cache:
            signals, paths = validation_cache[top_n]
        else:
            signals = _signals(con, train_end, validation_end, top_n=top_n)
            paths = _paths(con, signals)
            validation_cache[top_n] = (signals, paths)
        for max_drift in (0.10, 0.15):
            for strict_risk in (False, True):
                for variant in ("fast", "hybrid", "ladder"):
                    result = _portfolio_metrics(
                        signals, paths, max_drift=max_drift, strict_risk=strict_risk,
                        exit_variant=variant, latency_seconds=5, roundtrip_cost=0.05,
                    )
                    # Require at least 10 observations before a configuration may win.
                    score = -999.0
                    if result.trades >= 10:
                        score = math.log(max(1e-12, 1.0 + result.compounded_return)) - 2.0 * result.max_drawdown
                    validation_candidates.append({
                        "top_n": top_n,
                        "max_drift": max_drift,
                        "strict_risk": strict_risk,
                        "score": score,
                        "result": _serialize(result),
                    })
    validation_candidates.sort(key=lambda x: x["score"], reverse=True)
    chosen = validation_candidates[0]
    report["validation_top10"] = validation_candidates[:10]
    report["chosen_parameters"] = {
        key: chosen[key] for key in ("top_n", "max_drift", "strict_risk")
    } | {"exit_variant": chosen["result"]["exit_variant"]}

    # Completely untouched final quarter.
    n = int(chosen["top_n"])
    holdout_signals = _signals(con, validation_end, end + timedelta(microseconds=1), top_n=n)
    holdout_paths = _paths(con, holdout_signals)
    report["holdout_raw_signals"] = len(holdout_signals)
    stress: list[dict[str, Any]] = []
    for latency in (2, 5, 10):
        for cost in (0.03, 0.05, 0.08):
            outcome = _portfolio_metrics(
                holdout_signals,
                holdout_paths,
                max_drift=float(chosen["max_drift"]),
                strict_risk=bool(chosen["strict_risk"]),
                exit_variant=str(chosen["result"]["exit_variant"]),
                latency_seconds=latency,
                roundtrip_cost=cost,
            )
            stress.append(_serialize(outcome))
    report["holdout_stress_matrix"] = stress

    # Additional baseline: same chosen cohort with no strict risk gate at 5s/5%.
    baseline = _portfolio_metrics(
        holdout_signals, holdout_paths,
        max_drift=float(chosen["max_drift"]), strict_risk=False,
        exit_variant=str(chosen["result"]["exit_variant"]),
        latency_seconds=5, roundtrip_cost=0.05,
    )
    report["holdout_no_strict_risk_baseline"] = _serialize(baseline)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print("CONVEX_SCOUT_BACKTEST_START")
    print(json.dumps(report, indent=2, default=str))
    print("CONVEX_SCOUT_BACKTEST_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

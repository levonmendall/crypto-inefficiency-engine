from __future__ import annotations

import asyncio
import json
import os
import signal
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import Column, MetaData, String, Table, Text, inspect, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_probation import (
    HISTORICAL_BACKFILL_DAYS,
    HISTORICAL_CANDLE_SECONDS,
    HISTORICAL_REPLAY_MIN_SAMPLES,
    HISTORICAL_REPLAY_STEP_HOURS,
    CycleHistoricalResearch,
)
from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store


CYCLE_HISTORY_WORKER_ID = "cycle-history-backfill-maintenance"
CYCLE_HISTORY_STATUS_TABLE = "cycle_historical_backfill_status"
CYCLE_HISTORY_MAINTENANCE_SECONDS = max(
    300.0,
    float(os.getenv("CIE_CYCLE_HISTORY_MAINTENANCE_SECONDS", "1800")),
)
CANONICAL_RESEARCH_CAPITAL_USD = 250_000.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_day_start(value: datetime) -> datetime:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    aware = aware.astimezone(timezone.utc)
    return aware.replace(hour=0, minute=0, second=0, microsecond=0)


def _status_table(metadata: MetaData) -> Table:
    return Table(
        CYCLE_HISTORY_STATUS_TABLE,
        metadata,
        Column("asset", String(32), primary_key=True),
        Column("observed_at", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )


class CycleHistoryStatusLedger:
    """Durable current status for the additive historical research accelerator.

    This table is operational telemetry only. It does not participate in alpha
    qualification, forward-outcome counts, allocator authority, or live execution.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store
        self.metadata = MetaData()
        self.rows = _status_table(self.metadata)
        self.metadata.create_all(store.engine)

    def upsert(self, payload: dict[str, Any]) -> None:
        asset = str(payload.get("asset") or "").upper()
        if not asset:
            raise ValueError("cycle-history status requires an asset")
        row = {
            "asset": asset,
            "observed_at": str(payload.get("observed_at") or _now().isoformat()),
            "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        }
        backend = self.store.engine.url.get_backend_name()
        if backend == "postgresql":
            statement = pg_insert(self.rows).values(row).on_conflict_do_update(
                index_elements=[self.rows.c.asset],
                set_={
                    "observed_at": row["observed_at"],
                    "payload_json": row["payload_json"],
                },
            )
            with self.store.engine.begin() as db:
                db.execute(statement)
            return
        if backend == "sqlite":
            statement = sqlite_insert(self.rows).values(row).on_conflict_do_update(
                index_elements=[self.rows.c.asset],
                set_={
                    "observed_at": row["observed_at"],
                    "payload_json": row["payload_json"],
                },
            )
            with self.store.engine.begin() as db:
                db.execute(statement)
            return

        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.asset).where(self.rows.c.asset == asset)
            ).scalar_one_or_none()
            if exists is None:
                db.execute(insert(self.rows), row)
            else:
                db.execute(
                    update(self.rows)
                    .where(self.rows.c.asset == asset)
                    .values(observed_at=row["observed_at"], payload_json=row["payload_json"])
                )


def _decode_rows(raws: Iterable[object]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in raws:
        try:
            payload = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    rows.sort(key=lambda item: str(item.get("asset") or ""))
    return rows


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_expected = sum(int(row.get("expected_quote_count") or 0) for row in rows)
    total_quotes = sum(int(row.get("quote_count") or 0) for row in rows)
    complete_count = sum(bool(row.get("complete")) for row in rows)
    replay_ready_count = sum(bool(row.get("walk_forward_ready")) for row in rows)
    replay_qualified_count = sum(bool(row.get("historical_replay_long_qualified")) for row in rows)
    return {
        "available": bool(rows),
        "maintenance_worker_id": CYCLE_HISTORY_WORKER_ID,
        "asset_count": len(rows),
        "complete_asset_count": complete_count,
        "walk_forward_ready_asset_count": replay_ready_count,
        "historical_replay_qualified_asset_count": replay_qualified_count,
        "all_complete": bool(rows) and complete_count == len(rows),
        "total_quote_count": total_quotes,
        "expected_quote_count": total_expected,
        "overall_coverage_fraction": (
            min(1.0, total_quotes / total_expected) if total_expected > 0 else 0.0
        ),
        "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
        "historical_counts_as_forward": False,
        "full_forward_promotion_gate_unchanged": True,
        "live_execution_authority": False,
        "assets": rows,
    }


def read_cycle_history_status(store: EvidenceStore) -> dict[str, Any]:
    """Read status without creating schema; safe for the production read plane."""
    if CYCLE_HISTORY_STATUS_TABLE not in set(inspect(store.engine).get_table_names()):
        return {
            "available": False,
            "maintenance_worker_id": CYCLE_HISTORY_WORKER_ID,
            "asset_count": 0,
            "complete_asset_count": 0,
            "walk_forward_ready_asset_count": 0,
            "historical_replay_qualified_asset_count": 0,
            "all_complete": False,
            "total_quote_count": 0,
            "expected_quote_count": 0,
            "overall_coverage_fraction": 0.0,
            "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
            "historical_counts_as_forward": False,
            "full_forward_promotion_gate_unchanged": True,
            "live_execution_authority": False,
            "assets": [],
        }
    metadata = MetaData()
    table = _status_table(metadata)
    with store.engine.connect() as db:
        raws = list(db.execute(select(table.c.payload_json).order_by(table.c.asset)).scalars())
    return _summary(_decode_rows(raws))


def _error_map(errors: Iterable[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in errors:
        text = str(raw)
        asset, separator, error = text.partition(":")
        if separator and asset:
            result[asset.upper()] = error or "HistoricalBackfillError"
    return result


def _asset_status(
    *,
    research: CycleHistoricalResearch,
    asset: str,
    settings: Settings,
    observed_at: datetime,
    last_error_type: str | None,
    fetched_this_attempt: bool,
    stored_quote_count_this_attempt: int,
    replay,
    replay_error_type: str | None,
) -> dict[str, Any]:
    count, earliest, latest = research._coverage(asset)  # noqa: SLF001 - same bounded research subsystem
    expected = research.backfill_days * (86400 // HISTORICAL_CANDLE_SECONDS)
    end = _utc_day_start(observed_at)
    start = end - timedelta(days=research.backfill_days)
    complete = bool(
        count >= int(expected * 0.90)
        and earliest is not None
        and latest is not None
        and earliest <= start + timedelta(days=3)
        and latest >= end - timedelta(days=3)
    )
    coverage = min(1.0, count / expected) if expected > 0 else 0.0
    required_span_hours = (
        CycleAwareMultiHorizonTrendStrategy.required_history_hours(settings)
        + HISTORICAL_REPLAY_MIN_SAMPLES * HISTORICAL_REPLAY_STEP_HOURS
    )
    observed_span_hours = (
        max(0.0, (latest - earliest).total_seconds() / 3600.0)
        if earliest is not None and latest is not None
        else 0.0
    )
    walk_forward_ready = bool(complete and observed_span_hours >= required_span_hours)
    next_retry_at = None if complete else (
        observed_at + timedelta(seconds=CYCLE_HISTORY_MAINTENANCE_SECONDS)
    ).isoformat()
    replay_payload = {
        "sample_count": int(getattr(replay, "sample_count", 0) or 0),
        "hit_rate": getattr(replay, "hit_rate", None),
        "mean_realized_net_return": getattr(replay, "mean_realized_net_return", None),
        "regime_count": int(getattr(replay, "regime_count", 0) or 0),
        "qualified": bool(getattr(replay, "qualified_for_probationary_support", False)),
    }
    return {
        "asset": asset.upper(),
        "observed_at": observed_at.isoformat(),
        "status": "complete" if complete else "retrying",
        "complete": complete,
        "quote_count": count,
        "expected_quote_count": expected,
        "coverage_fraction": coverage,
        "earliest_observed_at": earliest.isoformat() if earliest is not None else None,
        "latest_observed_at": latest.isoformat() if latest is not None else None,
        "target_start_at": start.isoformat(),
        "target_end_at": end.isoformat(),
        "observed_span_hours": observed_span_hours,
        "required_walk_forward_span_hours": required_span_hours,
        "walk_forward_ready": walk_forward_ready,
        "fetched_this_attempt": fetched_this_attempt,
        "stored_quote_count_this_attempt": stored_quote_count_this_attempt,
        "last_error_type": last_error_type,
        "replay_error_type": replay_error_type,
        "historical_replay_long_sample_count": replay_payload["sample_count"],
        "historical_replay_long_hit_rate": replay_payload["hit_rate"],
        "historical_replay_long_mean_net_return": replay_payload["mean_realized_net_return"],
        "historical_replay_long_regime_count": replay_payload["regime_count"],
        "historical_replay_long_qualified": replay_payload["qualified"],
        "next_retry_at": next_retry_at,
        "historical_counts_as_forward": False,
        "live_execution_authority": False,
    }


async def maintain_cycle_history_once(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    assets: Iterable[str] | None = None,
    now: datetime | None = None,
    research: CycleHistoricalResearch | None = None,
) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    observed_at = now or _now()
    requested = tuple(
        sorted(
            {
                str(asset).upper()
                for asset in (assets or CoinbaseSpotAdapter().assets)
                if str(asset).strip()
            }
        )
    )
    research = research or CycleHistoricalResearch(store)
    ledger = CycleHistoryStatusLedger(store)

    report = None
    global_error_type: str | None = None
    try:
        report = await research.ensure_backfilled(requested, now=observed_at)
    except Exception as exc:  # additive accelerator must fail-contained
        global_error_type = type(exc).__name__

    replay = {}
    replay_error_type: str | None = None
    try:
        replay = research.replay_summaries(
            CycleAwareMultiHorizonTrendStrategy(),
            settings,
            total_capital_usd=CANONICAL_RESEARCH_CAPITAL_USD,
            now=observed_at,
        )
    except Exception as exc:  # replay observability must not suppress retry
        replay_error_type = type(exc).__name__
        replay = {}

    errors = _error_map(getattr(report, "errors", ()) if report is not None else ())
    fetched_assets = set(getattr(report, "fetched_assets", ()) if report is not None else ())
    stored_this_attempt = int(getattr(report, "stored_quote_count", 0) if report is not None else 0)

    for asset in requested:
        payload = _asset_status(
            research=research,
            asset=asset,
            settings=settings,
            observed_at=observed_at,
            last_error_type=errors.get(asset) or global_error_type,
            fetched_this_attempt=asset in fetched_assets,
            stored_quote_count_this_attempt=stored_this_attempt if asset in fetched_assets else 0,
            replay=replay.get((asset, "long")),
            replay_error_type=replay_error_type,
        )
        ledger.upsert(payload)

    summary = read_cycle_history_status(store)
    state = "success" if summary.get("all_complete") else "degraded"
    store.record_worker_heartbeat(
        worker_id=CYCLE_HISTORY_WORKER_ID,
        state=state,
        error_type=global_error_type,
        detail={
            "asset_count": summary.get("asset_count", 0),
            "complete_asset_count": summary.get("complete_asset_count", 0),
            "walk_forward_ready_asset_count": summary.get("walk_forward_ready_asset_count", 0),
            "historical_replay_qualified_asset_count": summary.get(
                "historical_replay_qualified_asset_count", 0
            ),
            "overall_coverage_fraction": summary.get("overall_coverage_fraction", 0.0),
            "historical_counts_as_forward": False,
        },
        observed_at=observed_at,
    )
    return summary


async def maintenance_loop() -> None:
    store = build_evidence_store()
    if store is None:
        raise RuntimeError("cycle-history maintenance requires durable evidence persistence")
    settings = Settings.from_env()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    store.record_worker_heartbeat(
        worker_id=CYCLE_HISTORY_WORKER_ID,
        state="starting",
        detail={
            "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
            "historical_counts_as_forward": False,
        },
    )

    while not stop.is_set():
        try:
            await maintain_cycle_history_once(store, settings=settings)
        except Exception as exc:
            store.record_worker_heartbeat(
                worker_id=CYCLE_HISTORY_WORKER_ID,
                state="error",
                error_type=type(exc).__name__,
                detail={
                    "retrying": True,
                    "retry_interval_seconds": CYCLE_HISTORY_MAINTENANCE_SECONDS,
                    "historical_counts_as_forward": False,
                },
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=CYCLE_HISTORY_MAINTENANCE_SECONDS)
        except TimeoutError:
            continue

    store.record_worker_heartbeat(
        worker_id=CYCLE_HISTORY_WORKER_ID,
        state="stopped",
        detail={"historical_counts_as_forward": False},
    )


def main() -> int:
    asyncio.run(maintenance_loop())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

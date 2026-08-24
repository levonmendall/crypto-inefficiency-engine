from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any

from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, and_, delete, insert, or_, select

from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.durable_control_cache import (
    durable_control_cache_namespace,
    load_control_cache_checkpoint,
    save_control_cache_checkpoint,
)
from inefficiency_engine.models import MarketKind, MarketQuote


_METADATA = MetaData()
CONTROL_CYCLE_HISTORY_ROWS = Table(
    "control_cycle_history_rows",
    _METADATA,
    Column("namespace", String(191), primary_key=True),
    Column("source_id", Integer, primary_key=True),
    Column("venue", Text, nullable=False),
    Column("asset", Text, nullable=False),
    Column("market_kind", Text, nullable=False),
    Column("day", String(10), nullable=False),
    Column("observed_at", Text, nullable=False),
    Column("payload_json", Text, nullable=False),
)
Index(
    "ix_control_cycle_history_bucket",
    CONTROL_CYCLE_HISTORY_ROWS.c.namespace,
    CONTROL_CYCLE_HISTORY_ROWS.c.venue,
    CONTROL_CYCLE_HISTORY_ROWS.c.asset,
    CONTROL_CYCLE_HISTORY_ROWS.c.day,
    CONTROL_CYCLE_HISTORY_ROWS.c.source_id,
)
Index(
    "ix_control_cycle_history_observed",
    CONTROL_CYCLE_HISTORY_ROWS.c.namespace,
    CONTROL_CYCLE_HISTORY_ROWS.c.observed_at,
)

_CACHE_KEY = "cycle-history-live-compact-v3"
_CACHE_VERSION = 3
_DEFAULT_BUCKET_QUERY_BUDGET = 32
_DEFAULT_BOOTSTRAP_TIME_BUDGET_SECONDS = 8.0
_RETENTION_SAFETY_HOURS = 24.0


def ensure_durable_control_cycle_history_schema(store: Any) -> None:
    """Create the compact control-only cycle-history table."""

    _METADATA.create_all(store.engine, tables=[CONTROL_CYCLE_HISTORY_ROWS])


def _bucket_query_budget() -> int:
    raw = os.getenv(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET",
        str(_DEFAULT_BUCKET_QUERY_BUDGET),
    )
    try:
        value = int(raw)
    except ValueError:
        value = _DEFAULT_BUCKET_QUERY_BUDGET
    return max(1, min(256, value))


def _bootstrap_time_budget_seconds() -> float:
    raw = os.getenv(
        "CIE_CONTROL_CYCLE_HISTORY_TIME_BUDGET_SECONDS",
        str(_DEFAULT_BOOTSTRAP_TIME_BUDGET_SECONDS),
    )
    try:
        value = float(raw)
    except ValueError:
        value = _DEFAULT_BOOTSTRAP_TIME_BUDGET_SECONDS
    return max(1.0, min(15.0, value))


def _pair_token(venue: str, asset: str) -> str:
    return json.dumps([venue, asset.upper()], separators=(",", ":"))


def _current_keys(factory: Any, snapshot: Any) -> set[tuple[str, str, MarketKind]]:
    return {
        (str(venue), str(asset).upper(), market_kind)
        for venue, asset, market_kind in factory._current_keys(snapshot)
        if market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}
    }


def _current_pairs(factory: Any, snapshot: Any) -> set[tuple[str, str]]:
    return {(venue, asset) for venue, asset, _kind in _current_keys(factory, snapshot)}


def _config(factory: Any) -> tuple[int, float]:
    settings = factory._expanded_settings
    return (
        int(CycleAwareMultiHorizonTrendStrategy.rows_per_day(settings)),
        float(CycleAwareMultiHorizonTrendStrategy.required_history_hours(settings)),
    )


def _fresh_checkpoint(factory: Any) -> dict[str, Any]:
    rows_per_day, required_history_hours = _config(factory)
    return {
        "version": _CACHE_VERSION,
        "rows_per_day": rows_per_day,
        "required_history_hours": required_history_hours,
        "pair_completed_through": {},
        "boundary_days": {},
        "next_pair_index": 0,
        "last_bucket_queries": 0,
    }


def _load_checkpoint(factory: Any) -> tuple[dict[str, Any], bool]:
    checkpoint = load_control_cache_checkpoint(factory.store, cache_key=_CACHE_KEY)
    rows_per_day, required_history_hours = _config(factory)
    valid = bool(
        isinstance(checkpoint, dict)
        and int(checkpoint.get("version") or 0) == _CACHE_VERSION
        and int(checkpoint.get("rows_per_day") or 0) == rows_per_day
        and float(checkpoint.get("required_history_hours") or 0.0)
        == required_history_hours
    )
    return (dict(checkpoint) if valid else _fresh_checkpoint(factory), valid)


def _reset_namespace_rows(store: Any, namespace: str) -> None:
    with store.engine.begin() as db:
        db.execute(
            delete(CONTROL_CYCLE_HISTORY_ROWS).where(
                CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace
            )
        )


def _day_start(day: date) -> datetime:
    return datetime.combine(day, datetime_time.min, tzinfo=timezone.utc)


def _replace_bucket(
    *,
    factory: Any,
    namespace: str,
    venue: str,
    asset: str,
    day: date,
    start: datetime,
    end: datetime,
    limit: int | None,
) -> int:
    """Replace one venue/asset/day cache bucket from an indexed bounded time seek."""

    table = factory.store.market_quotes
    selected: list[dict[str, object]] = []
    if end > start:
        query = (
            select(
                table.c.id,
                table.c.venue,
                table.c.asset,
                table.c.observed_at,
                table.c.payload_json,
            )
            .where(table.c.venue == venue)
            .where(table.c.asset == asset)
            .where(table.c.observed_at >= start.isoformat())
            .where(table.c.observed_at < end.isoformat())
            .order_by(table.c.id.desc())
        )
        if limit is not None:
            query = query.limit(limit)
        with factory.store.engine.connect() as db:
            rows = list(db.execute(query))
        for source_id, row_venue, row_asset, observed_at, payload_json in rows:
            quote = MarketQuote.model_validate_json(payload_json)
            selected.append(
                {
                    "namespace": namespace,
                    "source_id": int(source_id),
                    "venue": str(row_venue),
                    "asset": str(row_asset).upper(),
                    "market_kind": quote.market_kind.value,
                    "day": day.isoformat(),
                    "observed_at": str(observed_at),
                    "payload_json": str(payload_json),
                }
            )

    with factory.store.engine.begin() as db:
        db.execute(
            delete(CONTROL_CYCLE_HISTORY_ROWS)
            .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
            .where(CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue)
            .where(CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset)
            .where(CONTROL_CYCLE_HISTORY_ROWS.c.day == day.isoformat())
        )
        if selected:
            db.execute(insert(CONTROL_CYCLE_HISTORY_ROWS), selected)
    return len(selected)


def _parse_day(raw: object, fallback: date) -> date:
    try:
        return date.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return fallback


def advance_durable_control_cycle_history_cache(
    factory: Any,
    snapshot: Any,
    *,
    stop_at_monotonic: float | None = None,
) -> dict[str, object]:
    """Advance the exact legacy daily projection in a resumable bounded slice.

    Each completed venue/asset/day bucket is checkpointed before another bucket starts.
    A fresh disposable control interpreter therefore resumes after the last durable
    bucket instead of replaying a large batch after an external deadline. Work is also
    capped by both a small query budget and a short wall-clock budget. Partial history
    remains invisible to alpha discovery until every current pair is exact.
    """

    started = time.monotonic()
    time_budget_seconds = _bootstrap_time_budget_seconds()
    local_stop = started + time_budget_seconds
    if stop_at_monotonic is not None:
        local_stop = min(local_stop, float(stop_at_monotonic))

    namespace = durable_control_cache_namespace()
    if namespace is None:
        return {
            "complete": False,
            "error_type": "DurableControlCacheNamespaceUnavailable",
            "paper_only": True,
        }

    ensure_durable_control_cycle_history_schema(factory.store)
    checkpoint, valid = _load_checkpoint(factory)
    checkpoint_writes = 0
    persisted = bool(valid)
    if not valid:
        _reset_namespace_rows(factory.store, namespace)
        persisted = save_control_cache_checkpoint(
            factory.store,
            cache_key=_CACHE_KEY,
            payload=checkpoint,
            complete=False,
        )
        checkpoint_writes += int(bool(persisted))
        if not persisted:
            return {
                "complete": False,
                "error_type": "CycleHistoryCheckpointPersistFailed",
                "mode": "resumable_exact_daily_bucket_cache",
                "durable_checkpoint_persisted": False,
                "paper_only": True,
            }

    current_pairs = sorted(_current_pairs(factory, snapshot))
    rows_per_day, required_history_hours = _config(factory)
    long_cutoff = snapshot.completed_at - timedelta(hours=required_history_hours)
    recent_cutoff = snapshot.completed_at - timedelta(
        hours=factory._effective_history_hours()
    )
    if recent_cutoff <= long_cutoff or not current_pairs:
        checkpoint.update(
            {
                "version": _CACHE_VERSION,
                "rows_per_day": rows_per_day,
                "required_history_hours": required_history_hours,
                "last_bucket_queries": 0,
            }
        )
        persisted = save_control_cache_checkpoint(
            factory.store,
            cache_key=_CACHE_KEY,
            payload=checkpoint,
            complete=True,
        )
        checkpoint_writes += int(bool(persisted))
        return {
            "complete": bool(persisted),
            "mode": "resumable_exact_daily_bucket_cache",
            "current_pair_count": len(current_pairs),
            "bucket_queries": 0,
            "checkpoint_writes": checkpoint_writes,
            "rows_per_day": rows_per_day,
            "durable_checkpoint_persisted": bool(persisted),
            "time_budget_seconds": time_budget_seconds,
            "stopped_for_time_budget": False,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        }

    first_day = long_cutoff.date()
    boundary_day = recent_cutoff.date()
    stable_end_day = boundary_day - timedelta(days=1)
    completed = {
        str(key): str(value)
        for key, value in dict(checkpoint.get("pair_completed_through") or {}).items()
    }
    boundary_days = {
        str(key): str(value)
        for key, value in dict(checkpoint.get("boundary_days") or {}).items()
    }
    query_budget = _bucket_query_budget()
    pair_index = int(checkpoint.get("next_pair_index") or 0) % len(current_pairs)
    bucket_queries = 0
    stable_rows_retained = 0
    boundary_rows_retained = 0
    idle_visits = 0
    stopped_for_time_budget = False
    checkpoint_persist_failed = False

    def persist_progress(*, complete: bool = False) -> bool:
        nonlocal checkpoint_writes
        checkpoint.update(
            {
                "version": _CACHE_VERSION,
                "rows_per_day": rows_per_day,
                "required_history_hours": required_history_hours,
                "pair_completed_through": completed,
                "boundary_days": boundary_days,
                "next_pair_index": pair_index,
                "last_bucket_queries": bucket_queries,
            }
        )
        ok = save_control_cache_checkpoint(
            factory.store,
            cache_key=_CACHE_KEY,
            payload=checkpoint,
            complete=complete,
        )
        checkpoint_writes += int(bool(ok))
        return bool(ok)

    while bucket_queries < query_budget and idle_visits < len(current_pairs):
        if time.monotonic() >= local_stop:
            stopped_for_time_budget = True
            break

        venue, asset = current_pairs[pair_index]
        token = _pair_token(venue, asset)
        did_work = False

        # Refresh the moving cutoff day first. This keeps every current pair's recent
        # boundary available early while the older 180-day history catches up.
        if boundary_days.get(token) != boundary_day.isoformat():
            boundary_start = max(long_cutoff, _day_start(boundary_day))
            boundary_rows_retained += _replace_bucket(
                factory=factory,
                namespace=namespace,
                venue=venue,
                asset=asset,
                day=boundary_day,
                start=boundary_start,
                end=_day_start(boundary_day) + timedelta(days=1),
                limit=None,
            )
            boundary_days[token] = boundary_day.isoformat()
            bucket_queries += 1
            did_work = True
        else:
            fallback_completed = first_day - timedelta(days=1)
            completed_day = _parse_day(completed.get(token), fallback_completed)
            if completed_day < fallback_completed:
                completed_day = fallback_completed
            next_day = max(first_day, completed_day + timedelta(days=1))
            if next_day <= stable_end_day:
                day_start = _day_start(next_day)
                stable_rows_retained += _replace_bucket(
                    factory=factory,
                    namespace=namespace,
                    venue=venue,
                    asset=asset,
                    day=next_day,
                    start=max(long_cutoff, day_start),
                    end=day_start + timedelta(days=1),
                    limit=rows_per_day,
                )
                completed[token] = next_day.isoformat()
                bucket_queries += 1
                did_work = True

        pair_index = (pair_index + 1) % len(current_pairs)
        checkpoint["next_pair_index"] = pair_index

        if did_work:
            idle_visits = 0
            # Persist after every completed bucket. If the supervisor kills a later
            # bucket, at most that in-flight idempotent replacement is replayed.
            persisted = persist_progress(complete=False)
            if not persisted:
                checkpoint_persist_failed = True
                break
        else:
            idle_visits += 1

    current_tokens = [_pair_token(venue, asset) for venue, asset in current_pairs]
    stable_complete = all(
        _parse_day(completed.get(token), first_day - timedelta(days=1))
        >= stable_end_day
        for token in current_tokens
    )
    boundary_complete = all(
        boundary_days.get(token) == boundary_day.isoformat()
        for token in current_tokens
    )
    complete = bool(
        stable_complete
        and boundary_complete
        and not checkpoint_persist_failed
    )

    # Cleanup is non-authoritative and can wait until the exact cache is complete;
    # keeping it off the rebuild hot path preserves deadline headroom.
    if complete and time.monotonic() < local_stop:
        retention_floor = long_cutoff - timedelta(hours=_RETENTION_SAFETY_HOURS)
        with factory.store.engine.begin() as db:
            db.execute(
                delete(CONTROL_CYCLE_HISTORY_ROWS)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
                .where(
                    CONTROL_CYCLE_HISTORY_ROWS.c.observed_at
                    < retention_floor.isoformat()
                )
            )

    if complete:
        persisted = persist_progress(complete=True)
        complete = bool(persisted)

    if time.monotonic() >= local_stop and not complete:
        stopped_for_time_budget = True

    incomplete_pairs = sum(
        (
            _parse_day(completed.get(token), first_day - timedelta(days=1))
            < stable_end_day
        )
        or boundary_days.get(token) != boundary_day.isoformat()
        for token in current_tokens
    )
    result: dict[str, object] = {
        "complete": complete,
        "mode": "resumable_exact_daily_bucket_cache",
        "query_budget": query_budget,
        "bucket_queries": bucket_queries,
        "checkpoint_writes": checkpoint_writes,
        "stable_rows_retained": stable_rows_retained,
        "boundary_rows_retained": boundary_rows_retained,
        "current_pair_count": len(current_pairs),
        "cached_pair_count": len(completed),
        "incomplete_pair_count": incomplete_pairs,
        "next_pair_index": pair_index,
        "rows_per_day": rows_per_day,
        "required_history_hours": required_history_hours,
        "long_cutoff": long_cutoff.isoformat(),
        "recent_cutoff": recent_cutoff.isoformat(),
        "boundary_day": boundary_day.isoformat(),
        "elapsed_seconds": max(0.0, time.monotonic() - started),
        "time_budget_seconds": time_budget_seconds,
        "stopped_for_time_budget": stopped_for_time_budget,
        "durable_checkpoint_persisted": bool(persisted),
        "legacy_window_query_avoided": True,
        "filter_before_daily_rank_preserved": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    if checkpoint_persist_failed:
        result["error_type"] = "CycleHistoryCheckpointPersistFailed"
    return result


def load_durable_control_cycle_history(
    factory: Any,
    snapshot: Any,
) -> dict[tuple[str, str, MarketKind], list[MarketQuote]] | None:
    """Return exact compact live history only after all current pairs are ready."""

    namespace = durable_control_cache_namespace()
    if namespace is None:
        return None
    ensure_durable_control_cycle_history_schema(factory.store)
    checkpoint, valid = _load_checkpoint(factory)
    if not valid:
        return None

    current_keys = _current_keys(factory, snapshot)
    current_pairs = sorted({(venue, asset) for venue, asset, _kind in current_keys})
    if not current_pairs:
        return {}

    rows_per_day, required_history_hours = _config(factory)
    long_cutoff = snapshot.completed_at - timedelta(hours=required_history_hours)
    recent_cutoff = snapshot.completed_at - timedelta(
        hours=factory._effective_history_hours()
    )
    first_day = long_cutoff.date()
    boundary_day = recent_cutoff.date()
    stable_end_day = boundary_day - timedelta(days=1)
    completed = {
        str(key): str(value)
        for key, value in dict(checkpoint.get("pair_completed_through") or {}).items()
    }
    boundary_days = {
        str(key): str(value)
        for key, value in dict(checkpoint.get("boundary_days") or {}).items()
    }
    for venue, asset in current_pairs:
        token = _pair_token(venue, asset)
        if _parse_day(completed.get(token), first_day - timedelta(days=1)) < stable_end_day:
            return None
        if boundary_days.get(token) != boundary_day.isoformat():
            return None

    pair_filters = [
        and_(
            CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue,
            CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset,
        )
        for venue, asset in current_pairs
    ]
    query = (
        select(
            CONTROL_CYCLE_HISTORY_ROWS.c.source_id,
            CONTROL_CYCLE_HISTORY_ROWS.c.venue,
            CONTROL_CYCLE_HISTORY_ROWS.c.asset,
            CONTROL_CYCLE_HISTORY_ROWS.c.day,
            CONTROL_CYCLE_HISTORY_ROWS.c.payload_json,
        )
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at >= long_cutoff.isoformat())
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at < recent_cutoff.isoformat())
        .where(or_(*pair_filters))
    )
    ranked: dict[tuple[str, str, str], list[tuple[int, str]]] = defaultdict(list)
    with factory.store.engine.connect() as db:
        for source_id, venue, asset, day, payload_json in db.execute(query):
            ranked[(str(venue), str(asset).upper(), str(day))].append(
                (int(source_id), str(payload_json))
            )

    grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
    for rows in ranked.values():
        for _source_id, payload in sorted(rows, reverse=True)[:rows_per_day]:
            quote = MarketQuote.model_validate_json(payload)
            key = (quote.venue, quote.asset.upper(), quote.market_kind)
            if key in current_keys:
                grouped[key].append(quote)
    for values in grouped.values():
        values.sort(key=lambda item: item.observed_at)
    return grouped

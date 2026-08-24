from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import timedelta
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

_CACHE_KEY = "cycle-history-live-compact-v1"
_CACHE_VERSION = 1
_DEFAULT_BATCH_ROWS = 2000
_RETENTION_SAFETY_HOURS = 24.0


def ensure_durable_control_cycle_history_schema(store: Any) -> None:
    """Create the compact control-only cycle-history table during serial bootstrap."""

    _METADATA.create_all(store.engine, tables=[CONTROL_CYCLE_HISTORY_ROWS])


def _batch_rows() -> int:
    raw = os.getenv(
        "CIE_CONTROL_CYCLE_HISTORY_BATCH_ROWS",
        str(_DEFAULT_BATCH_ROWS),
    )
    try:
        value = int(raw)
    except ValueError:
        value = _DEFAULT_BATCH_ROWS
    return max(1, min(20000, value))


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
    rows_per_day = int(CycleAwareMultiHorizonTrendStrategy.rows_per_day(settings))
    required_history_hours = float(
        CycleAwareMultiHorizonTrendStrategy.required_history_hours(settings)
    )
    return rows_per_day, required_history_hours


def _fresh_checkpoint(factory: Any) -> dict[str, Any]:
    rows_per_day, required_history_hours = _config(factory)
    return {
        "version": _CACHE_VERSION,
        "rows_per_day": rows_per_day,
        "required_history_hours": required_history_hours,
        "pair_cursors": {},
        "pair_complete": {},
        "last_batch_rows": {},
    }


def _load_checkpoint(factory: Any) -> tuple[dict[str, Any], bool]:
    checkpoint = load_control_cache_checkpoint(
        factory.store,
        cache_key=_CACHE_KEY,
    )
    expected_rows_per_day, expected_history_hours = _config(factory)
    valid = bool(
        isinstance(checkpoint, dict)
        and int(checkpoint.get("version") or 0) == _CACHE_VERSION
        and int(checkpoint.get("rows_per_day") or 0) == expected_rows_per_day
        and float(checkpoint.get("required_history_hours") or 0.0)
        == expected_history_hours
    )
    return (dict(checkpoint) if valid else _fresh_checkpoint(factory), valid)


def _reset_namespace_rows(store: Any, namespace: str) -> None:
    with store.engine.begin() as db:
        db.execute(
            delete(CONTROL_CYCLE_HISTORY_ROWS).where(
                CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace
            )
        )


def _compact_batch(
    *,
    store: Any,
    namespace: str,
    rows: list[tuple[int, str, str, str, str]],
    rows_per_day: int,
    retention_floor_iso: str,
) -> int:
    """Merge one bounded source batch into exact latest-N-per-day buckets.

    The legacy long-history projection ranks ``market_quotes`` by descending source
    row id inside each venue/asset/day bucket, then retains ``rows_per_day`` rows.
    Processing append-only ids in bounded batches and retaining those same latest ids
    is algebraically equivalent to the legacy window query once the cursor is caught
    up, but never asks PostgreSQL to rank the entire append-only history at once.
    """

    incoming_by_pair: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for source_id, venue, asset, observed_at, payload_json in rows:
        quote = MarketQuote.model_validate_json(payload_json)
        incoming_by_pair[(str(venue), str(asset).upper())].append(
            {
                "namespace": namespace,
                "source_id": int(source_id),
                "venue": str(venue),
                "asset": str(asset).upper(),
                "market_kind": quote.market_kind.value,
                "day": quote.observed_at.date().isoformat(),
                "observed_at": str(observed_at),
                "payload_json": str(payload_json),
            }
        )

    if not incoming_by_pair:
        with store.engine.begin() as db:
            db.execute(
                delete(CONTROL_CYCLE_HISTORY_ROWS)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at < retention_floor_iso)
            )
        return 0

    retained_count = 0
    with store.engine.begin() as db:
        db.execute(
            delete(CONTROL_CYCLE_HISTORY_ROWS)
            .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
            .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at < retention_floor_iso)
        )

        for (venue, asset), incoming in incoming_by_pair.items():
            touched_days = sorted({str(row["day"]) for row in incoming})
            existing = list(
                db.execute(
                    select(CONTROL_CYCLE_HISTORY_ROWS)
                    .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
                    .where(CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue)
                    .where(CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset)
                    .where(CONTROL_CYCLE_HISTORY_ROWS.c.day.in_(touched_days))
                ).mappings()
            )
            by_day: dict[str, dict[int, dict[str, object]]] = defaultdict(dict)
            for row in existing:
                by_day[str(row["day"])][int(row["source_id"])] = dict(row)
            for row in incoming:
                by_day[str(row["day"])][int(row["source_id"])] = row

            replacement: list[dict[str, object]] = []
            for day, values in by_day.items():
                latest = sorted(
                    values.values(),
                    key=lambda item: int(item["source_id"]),
                    reverse=True,
                )[:rows_per_day]
                replacement.extend(latest)
                retained_count += len(latest)

            db.execute(
                delete(CONTROL_CYCLE_HISTORY_ROWS)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset)
                .where(CONTROL_CYCLE_HISTORY_ROWS.c.day.in_(touched_days))
            )
            if replacement:
                db.execute(insert(CONTROL_CYCLE_HISTORY_ROWS), replacement)

    return retained_count


def advance_durable_control_cycle_history_cache(
    factory: Any,
    snapshot: Any,
) -> dict[str, object]:
    """Advance exact compact live cycle history in a bounded durable preflight batch.

    Each current venue/asset pair gets a bounded primary-key slice. Partial history is
    checkpointed but never returned to alpha discovery. Canonical qualification remains
    fail-closed until every current pair has caught up. Once complete, later source
    appends are consumed incrementally without rebuilding the long-history window.
    """

    namespace = durable_control_cache_namespace()
    if namespace is None:
        return {
            "complete": False,
            "error_type": "DurableControlCacheNamespaceUnavailable",
            "batch_rows": _batch_rows(),
            "paper_only": True,
        }

    ensure_durable_control_cycle_history_schema(factory.store)
    checkpoint, valid = _load_checkpoint(factory)
    if not valid:
        _reset_namespace_rows(factory.store, namespace)

    current_pairs = sorted(_current_pairs(factory, snapshot))
    if not current_pairs:
        checkpoint["pair_cursors"] = dict(checkpoint.get("pair_cursors") or {})
        checkpoint["pair_complete"] = dict(checkpoint.get("pair_complete") or {})
        persisted = save_control_cache_checkpoint(
            factory.store,
            cache_key=_CACHE_KEY,
            payload=checkpoint,
            complete=True,
        )
        return {
            "complete": bool(persisted),
            "batch_rows": _batch_rows(),
            "current_pair_count": 0,
            "cached_pair_count": len(checkpoint["pair_cursors"]),
            "last_batch_rows": 0,
            "durable_checkpoint_persisted": bool(persisted),
            "paper_only": True,
        }

    rows_per_day, required_history_hours = _config(factory)
    retention_floor = snapshot.completed_at - timedelta(
        hours=required_history_hours + _RETENTION_SAFETY_HOURS
    )
    retention_floor_iso = retention_floor.isoformat()
    total_batch = _batch_rows()
    per_pair_limit = max(1, total_batch // len(current_pairs))
    cursors = {
        str(key): int(value)
        for key, value in dict(checkpoint.get("pair_cursors") or {}).items()
    }
    pair_complete = {
        str(key): bool(value)
        for key, value in dict(checkpoint.get("pair_complete") or {}).items()
    }
    last_batch_rows = {
        str(key): int(value)
        for key, value in dict(checkpoint.get("last_batch_rows") or {}).items()
    }

    table = factory.store.market_quotes
    source_rows: list[tuple[int, str, str, str, str]] = []
    with factory.store.engine.connect() as db:
        for venue, asset in current_pairs:
            token = _pair_token(venue, asset)
            cursor = int(cursors.get(token, 0))
            query = (
                select(
                    table.c.id,
                    table.c.venue,
                    table.c.asset,
                    table.c.observed_at,
                    table.c.payload_json,
                )
                .where(table.c.id > cursor)
                .where(table.c.venue == venue)
                .where(table.c.asset == asset)
                .where(table.c.observed_at >= retention_floor_iso)
                .order_by(table.c.id)
                .limit(per_pair_limit)
            )
            batch = list(db.execute(query))
            if batch:
                source_rows.extend(
                    (
                        int(row[0]),
                        str(row[1]),
                        str(row[2]),
                        str(row[3]),
                        str(row[4]),
                    )
                    for row in batch
                )
                cursors[token] = int(batch[-1][0])
            pair_complete[token] = len(batch) < per_pair_limit
            last_batch_rows[token] = len(batch)

    _compact_batch(
        store=factory.store,
        namespace=namespace,
        rows=source_rows,
        rows_per_day=rows_per_day,
        retention_floor_iso=retention_floor_iso,
    )

    checkpoint.update(
        {
            "version": _CACHE_VERSION,
            "rows_per_day": rows_per_day,
            "required_history_hours": required_history_hours,
            "pair_cursors": cursors,
            "pair_complete": pair_complete,
            "last_batch_rows": last_batch_rows,
        }
    )
    current_tokens = [_pair_token(venue, asset) for venue, asset in current_pairs]
    complete = all(bool(pair_complete.get(token)) for token in current_tokens)
    persisted = save_control_cache_checkpoint(
        factory.store,
        cache_key=_CACHE_KEY,
        payload=checkpoint,
        complete=complete,
    )
    complete = bool(complete and persisted)

    return {
        "complete": complete,
        "mode": "bounded_exact_daily_compaction_then_incremental_tail",
        "batch_rows": total_batch,
        "per_pair_batch_rows": per_pair_limit,
        "current_pair_count": len(current_pairs),
        "cached_pair_count": len(cursors),
        "incomplete_pair_count": sum(
            not bool(pair_complete.get(token)) for token in current_tokens
        ),
        "last_batch_rows": sum(int(last_batch_rows.get(token, 0)) for token in current_tokens),
        "rows_per_day": rows_per_day,
        "required_history_hours": required_history_hours,
        "retention_safety_hours": _RETENTION_SAFETY_HOURS,
        "durable_checkpoint_persisted": bool(persisted),
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def load_durable_control_cycle_history(
    factory: Any,
    snapshot: Any,
) -> dict[tuple[str, str, MarketKind], list[MarketQuote]] | None:
    """Return the exact compact live projection only after the durable cache is ready."""

    namespace = durable_control_cache_namespace()
    if namespace is None:
        return None
    ensure_durable_control_cycle_history_schema(factory.store)
    checkpoint, valid = _load_checkpoint(factory)
    if not valid:
        return None

    current_keys = _current_keys(factory, snapshot)
    current_pairs = sorted({(venue, asset) for venue, asset, _kind in current_keys})
    pair_complete = {
        str(key): bool(value)
        for key, value in dict(checkpoint.get("pair_complete") or {}).items()
    }
    if any(
        not bool(pair_complete.get(_pair_token(venue, asset)))
        for venue, asset in current_pairs
    ):
        return None

    long_cutoff = snapshot.completed_at - timedelta(
        hours=CycleAwareMultiHorizonTrendStrategy.required_history_hours(
            factory._expanded_settings
        )
    )
    recent_cutoff = snapshot.completed_at - timedelta(
        hours=factory._effective_history_hours()
    )
    if recent_cutoff <= long_cutoff or not current_pairs:
        return {}

    pair_filters = [
        and_(
            CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue,
            CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset,
        )
        for venue, asset in current_pairs
    ]
    query = (
        select(CONTROL_CYCLE_HISTORY_ROWS.c.payload_json)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at >= long_cutoff.isoformat())
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at < recent_cutoff.isoformat())
        .where(or_(*pair_filters))
        .order_by(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at)
    )
    grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
    with factory.store.engine.connect() as db:
        payloads = db.execute(query).scalars()
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            key = (quote.venue, quote.asset.upper(), quote.market_kind)
            if key in current_keys:
                grouped[key].append(quote)
    return grouped

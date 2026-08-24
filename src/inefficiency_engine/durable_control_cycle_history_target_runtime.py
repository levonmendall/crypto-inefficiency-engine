from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, delete, or_, select

from inefficiency_engine.durable_control_cache import save_control_cache_checkpoint
from inefficiency_engine.durable_control_cycle_history import (
    CONTROL_CYCLE_HISTORY_ROWS,
    _CACHE_KEY,
    _RETENTION_SAFETY_HOURS,
    _bootstrap_time_budget_seconds,
    _bucket_query_budget,
    _config,
    _current_keys,
    _day_start,
    _load_checkpoint,
    _pair_token,
    _parse_day,
    _replace_bucket,
    _reset_namespace_rows,
    ensure_durable_control_cycle_history_schema,
)
from inefficiency_engine.durable_control_cache import durable_control_cache_namespace
from inefficiency_engine.models import MarketKind, MarketQuote


_RUNTIME_MODE = "frozen_boundary_double_buffer_v1"
_BOUNDARY_SLOT_A = "cycle-history-boundary-a"
_BOUNDARY_SLOT_B = "cycle-history-boundary-b"


def _slot_namespace(base: str, slot: str) -> str:
    return f"{base}:{slot}"


def _serialize_keys(keys: set[tuple[str, str, MarketKind]]) -> list[list[str]]:
    return [
        [venue, asset, market_kind.value]
        for venue, asset, market_kind in sorted(
            keys,
            key=lambda item: (item[0], item[1], item[2].value),
        )
    ]


def _deserialize_keys(raw: object) -> set[tuple[str, str, MarketKind]]:
    rows: set[tuple[str, str, MarketKind]] = set()
    if not isinstance(raw, list):
        return rows
    for value in raw:
        if not isinstance(value, list) or len(value) != 3:
            continue
        try:
            rows.add((str(value[0]), str(value[1]).upper(), MarketKind(str(value[2]))))
        except ValueError:
            continue
    return rows


def _target_completed_at(target: dict[str, object]) -> datetime | None:
    try:
        return datetime.fromisoformat(str(target.get("completed_at")))
    except (TypeError, ValueError):
        return None


def _target_from_snapshot(snapshot: Any, *, boundary_namespace: str) -> dict[str, object]:
    keys = _current_keys_from_snapshot(snapshot)
    return {
        "scan_id": str(snapshot.scan_id),
        "completed_at": snapshot.completed_at.isoformat(),
        "keys": _serialize_keys(keys),
        "boundary_namespace": boundary_namespace,
        "boundary_cutoffs": {},
    }


def _current_keys_from_snapshot(snapshot: Any) -> set[tuple[str, str, MarketKind]]:
    return {
        (str(quote.venue), str(quote.asset).upper(), quote.market_kind)
        for quote in snapshot.market_quotes
        if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}
    }


def _inactive_boundary_namespace(
    base_namespace: str,
    active_target: dict[str, object] | None,
) -> str:
    slot_a = _slot_namespace(base_namespace, _BOUNDARY_SLOT_A)
    slot_b = _slot_namespace(base_namespace, _BOUNDARY_SLOT_B)
    active_namespace = (
        str(active_target.get("boundary_namespace"))
        if isinstance(active_target, dict) and active_target.get("boundary_namespace")
        else None
    )
    return slot_b if active_namespace == slot_a else slot_a


def _target_cutoffs(factory: Any, completed_at: datetime) -> tuple[datetime, datetime]:
    _rows_per_day, required_history_hours = _config(factory)
    long_cutoff = completed_at - timedelta(hours=required_history_hours)
    recent_cutoff = completed_at - timedelta(hours=factory._effective_history_hours())
    return long_cutoff, recent_cutoff


def _checkpoint_target(checkpoint: dict[str, Any], name: str) -> dict[str, object] | None:
    value = checkpoint.get(name)
    return dict(value) if isinstance(value, dict) else None


def _target_matches_snapshot(target: dict[str, object], snapshot: Any) -> bool:
    target_at = _target_completed_at(target)
    if target_at is None:
        return False
    return bool(
        str(target.get("scan_id") or "") == str(snapshot.scan_id)
        and target_at == snapshot.completed_at
        and _deserialize_keys(target.get("keys")) == _current_keys_from_snapshot(snapshot)
    )


def _start_working_target(
    *,
    factory: Any,
    checkpoint: dict[str, Any],
    snapshot: Any,
    base_namespace: str,
) -> tuple[dict[str, object], bool]:
    active_target = _checkpoint_target(checkpoint, "active_target")
    boundary_namespace = _inactive_boundary_namespace(base_namespace, active_target)
    _reset_namespace_rows(factory.store, boundary_namespace)
    working_target = _target_from_snapshot(
        snapshot,
        boundary_namespace=boundary_namespace,
    )
    checkpoint["target_runtime_mode"] = _RUNTIME_MODE
    checkpoint["working_target"] = working_target
    checkpoint["next_pair_index"] = 0
    persisted = save_control_cache_checkpoint(
        factory.store,
        cache_key=_CACHE_KEY,
        payload=checkpoint,
        complete=active_target is not None,
    )
    return working_target, bool(persisted)


def advance_durable_control_cycle_history_cache(
    factory: Any,
    snapshot: Any,
    *,
    stop_at_monotonic: float | None = None,
) -> dict[str, object]:
    """Advance an exact cycle-history target without letting the target move mid-build.

    Stable daily buckets remain in the legacy durable namespace so progress produced by
    the prior v3 implementation is retained. The exact moving-boundary bucket is built
    in one of two durable boundary slots. A target scan/cutoff is frozen until every
    required pair is exact, then that slot is atomically promoted in the checkpoint.
    While the inactive slot rolls toward a newer target, the prior certified target
    remains readable and canonical control can continue to consume that exact point in
    time. Partial working state never becomes allocation-authoritative.
    """

    started = time.monotonic()
    time_budget_seconds = _bootstrap_time_budget_seconds()
    local_stop = started + time_budget_seconds
    if stop_at_monotonic is not None:
        local_stop = min(local_stop, float(stop_at_monotonic))

    base_namespace = durable_control_cache_namespace()
    if base_namespace is None:
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
        _reset_namespace_rows(factory.store, base_namespace)
        _reset_namespace_rows(
            factory.store,
            _slot_namespace(base_namespace, _BOUNDARY_SLOT_A),
        )
        _reset_namespace_rows(
            factory.store,
            _slot_namespace(base_namespace, _BOUNDARY_SLOT_B),
        )
        checkpoint["target_runtime_mode"] = _RUNTIME_MODE
        checkpoint["active_target"] = None
        checkpoint["working_target"] = None
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
                "durable_checkpoint_persisted": False,
                "paper_only": True,
            }

    checkpoint["target_runtime_mode"] = _RUNTIME_MODE
    active_target = _checkpoint_target(checkpoint, "active_target")
    working_target = _checkpoint_target(checkpoint, "working_target")

    # Existing v3 checkpoints have no target metadata. Preserve their stable-day
    # progress in the base namespace, but move all exact boundary work to a dedicated
    # slot and freeze the first post-deploy source snapshot as the working target.
    if working_target is None and (
        active_target is None or not _target_matches_snapshot(active_target, snapshot)
    ):
        working_target, persisted = _start_working_target(
            factory=factory,
            checkpoint=checkpoint,
            snapshot=snapshot,
            base_namespace=base_namespace,
        )
        checkpoint_writes += int(bool(persisted))
        if not persisted:
            return {
                "complete": active_target is not None,
                "error_type": "CycleHistoryCheckpointPersistFailed",
                "serving_scan_id": (
                    str(active_target.get("scan_id")) if active_target is not None else None
                ),
                "durable_checkpoint_persisted": False,
                "paper_only": True,
            }

    if working_target is None:
        # The incoming snapshot is the already-certified target.
        return {
            "complete": active_target is not None,
            "working_complete": True,
            "rolling_refresh_in_progress": False,
            "serving_scan_id": (
                str(active_target.get("scan_id")) if active_target is not None else None
            ),
            "serving_target_completed_at": (
                str(active_target.get("completed_at")) if active_target is not None else None
            ),
            "target_frozen_across_executors": True,
            "double_buffered_boundary": True,
            "durable_checkpoint_persisted": bool(persisted),
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

    target_at = _target_completed_at(working_target)
    if target_at is None:
        return {
            "complete": active_target is not None,
            "error_type": "CycleHistoryWorkingTargetInvalid",
            "serving_scan_id": (
                str(active_target.get("scan_id")) if active_target is not None else None
            ),
            "paper_only": True,
        }

    target_keys = _deserialize_keys(working_target.get("keys"))
    current_pairs = sorted({(venue, asset) for venue, asset, _kind in target_keys})
    rows_per_day, required_history_hours = _config(factory)
    long_cutoff, recent_cutoff = _target_cutoffs(factory, target_at)
    first_day = long_cutoff.date()
    boundary_day = recent_cutoff.date()
    boundary_cutoff = recent_cutoff.isoformat()
    stable_end_day = boundary_day - timedelta(days=1)
    completed = {
        str(key): str(value)
        for key, value in dict(checkpoint.get("pair_completed_through") or {}).items()
    }
    boundary_cutoffs = {
        str(key): str(value)
        for key, value in dict(working_target.get("boundary_cutoffs") or {}).items()
    }
    query_budget = _bucket_query_budget()
    pair_index = (
        int(checkpoint.get("next_pair_index") or 0) % len(current_pairs)
        if current_pairs
        else 0
    )
    bucket_queries = 0
    stable_rows_retained = 0
    boundary_rows_retained = 0
    idle_visits = 0
    stopped_for_time_budget = False
    checkpoint_persist_failed = False
    promoted_working_target = False
    boundary_namespace = str(working_target.get("boundary_namespace") or "")

    def persist_progress(*, complete: bool | None = None) -> bool:
        nonlocal checkpoint_writes
        working_target["boundary_cutoffs"] = boundary_cutoffs
        checkpoint.update(
            {
                "target_runtime_mode": _RUNTIME_MODE,
                "pair_completed_through": completed,
                "next_pair_index": pair_index,
                "last_bucket_queries": bucket_queries,
                "working_target": working_target,
            }
        )
        ok = save_control_cache_checkpoint(
            factory.store,
            cache_key=_CACHE_KEY,
            payload=checkpoint,
            complete=(active_target is not None) if complete is None else bool(complete),
        )
        checkpoint_writes += int(bool(ok))
        return bool(ok)

    if recent_cutoff <= long_cutoff or not current_pairs:
        working_complete = True
    else:
        while bucket_queries < query_budget and idle_visits < len(current_pairs):
            if time.monotonic() >= local_stop:
                stopped_for_time_budget = True
                break

            venue, asset = current_pairs[pair_index]
            token = _pair_token(venue, asset)
            did_work = False
            fallback_completed = first_day - timedelta(days=1)
            completed_day = _parse_day(completed.get(token), fallback_completed)
            if completed_day < fallback_completed:
                completed_day = fallback_completed
            next_day = max(first_day, completed_day + timedelta(days=1))

            # Stable history is independent of the exact intraday moving cutoff. Do it
            # first so an advancing source snapshot cannot consume every bounded visit
            # merely refreshing the boundary and starve the 180-day backfill forever.
            if next_day <= stable_end_day:
                day_start = _day_start(next_day)
                stable_rows_retained += _replace_bucket(
                    factory=factory,
                    namespace=base_namespace,
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
            elif boundary_cutoffs.get(token) != boundary_cutoff:
                boundary_start = max(long_cutoff, _day_start(boundary_day))
                boundary_rows_retained += _replace_bucket(
                    factory=factory,
                    namespace=boundary_namespace,
                    venue=venue,
                    asset=asset,
                    day=boundary_day,
                    start=boundary_start,
                    end=recent_cutoff,
                    limit=rows_per_day,
                )
                boundary_cutoffs[token] = boundary_cutoff
                bucket_queries += 1
                did_work = True

            pair_index = (pair_index + 1) % len(current_pairs)
            checkpoint["next_pair_index"] = pair_index
            if did_work:
                idle_visits = 0
                persisted = persist_progress()
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
            boundary_cutoffs.get(token) == boundary_cutoff
            for token in current_tokens
        )
        working_complete = bool(
            stable_complete and boundary_complete and not checkpoint_persist_failed
        )

    if working_complete:
        previous_active = active_target
        promoted_target = {
            "scan_id": str(working_target.get("scan_id") or ""),
            "completed_at": str(working_target.get("completed_at") or ""),
            "keys": list(working_target.get("keys") or []),
            "boundary_namespace": boundary_namespace,
        }
        checkpoint.update(
            {
                "target_runtime_mode": _RUNTIME_MODE,
                "active_target": promoted_target,
                "working_target": None,
                "pair_completed_through": completed,
                "next_pair_index": 0,
                "last_bucket_queries": bucket_queries,
            }
        )
        persisted = save_control_cache_checkpoint(
            factory.store,
            cache_key=_CACHE_KEY,
            payload=checkpoint,
            complete=True,
        )
        checkpoint_writes += int(bool(persisted))
        if persisted:
            active_target = promoted_target
            working_target = None
            promoted_working_target = True
            if time.monotonic() < local_stop:
                retention_floor = long_cutoff - timedelta(hours=_RETENTION_SAFETY_HOURS)
                with factory.store.engine.begin() as db:
                    db.execute(
                        delete(CONTROL_CYCLE_HISTORY_ROWS)
                        .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == base_namespace)
                        .where(
                            CONTROL_CYCLE_HISTORY_ROWS.c.observed_at
                            < retention_floor.isoformat()
                        )
                    )
        else:
            active_target = previous_active
            checkpoint_persist_failed = True

    if time.monotonic() >= local_stop and working_target is not None:
        stopped_for_time_budget = True

    active_target = _checkpoint_target(checkpoint, "active_target") if persisted else active_target
    live_working_target = _checkpoint_target(checkpoint, "working_target") if persisted else working_target
    serving_scan_id = (
        str(active_target.get("scan_id")) if active_target is not None else None
    )
    serving_target_completed_at = (
        str(active_target.get("completed_at")) if active_target is not None else None
    )
    working_tokens = [_pair_token(venue, asset) for venue, asset in current_pairs]
    incomplete_pairs = sum(
        (
            _parse_day(completed.get(token), first_day - timedelta(days=1))
            < stable_end_day
        )
        or boundary_cutoffs.get(token) != boundary_cutoff
        for token in working_tokens
    ) if live_working_target is not None else 0

    result: dict[str, object] = {
        "complete": active_target is not None,
        "working_complete": live_working_target is None,
        "rolling_refresh_in_progress": live_working_target is not None,
        "promoted_working_target": promoted_working_target,
        "serving_scan_id": serving_scan_id,
        "serving_target_completed_at": serving_target_completed_at,
        "working_target_scan_id": (
            str(live_working_target.get("scan_id"))
            if live_working_target is not None
            else None
        ),
        "working_target_completed_at": (
            str(live_working_target.get("completed_at"))
            if live_working_target is not None
            else None
        ),
        "mode": "resumable_exact_stable_base_with_frozen_double_buffer_boundary",
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
        "boundary_cutoff": boundary_cutoff,
        "elapsed_seconds": max(0.0, time.monotonic() - started),
        "time_budget_seconds": time_budget_seconds,
        "stopped_for_time_budget": stopped_for_time_budget,
        "durable_checkpoint_persisted": bool(persisted),
        "legacy_stable_progress_preserved": True,
        "stable_history_prioritized_before_boundary": True,
        "target_frozen_across_executors": True,
        "double_buffered_boundary": True,
        "partial_working_target_authoritative": False,
        "filter_before_daily_rank_preserved": True,
        "boundary_raw_day_materialization_avoided": True,
        "bucket_payload_fetch_bounded": True,
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
    """Load only the certified compact history for the exact active target scan."""

    base_namespace = durable_control_cache_namespace()
    if base_namespace is None:
        return None
    ensure_durable_control_cycle_history_schema(factory.store)
    checkpoint, valid = _load_checkpoint(factory)
    if not valid:
        return None
    active_target = _checkpoint_target(checkpoint, "active_target")
    if active_target is None or not _target_matches_snapshot(active_target, snapshot):
        return None

    current_keys = _current_keys(factory, snapshot)
    current_pairs = sorted({(venue, asset) for venue, asset, _kind in current_keys})
    if not current_pairs:
        return {}

    target_at = _target_completed_at(active_target)
    if target_at is None:
        return None
    long_cutoff, recent_cutoff = _target_cutoffs(factory, target_at)
    first_day = long_cutoff.date()
    boundary_day = recent_cutoff.date()
    stable_end_day = boundary_day - timedelta(days=1)
    completed = {
        str(key): str(value)
        for key, value in dict(checkpoint.get("pair_completed_through") or {}).items()
    }
    for venue, asset in current_pairs:
        token = _pair_token(venue, asset)
        if _parse_day(completed.get(token), first_day - timedelta(days=1)) < stable_end_day:
            return None

    pair_filters = [
        and_(
            CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue,
            CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset,
        )
        for venue, asset in current_pairs
    ]
    boundary_start = max(long_cutoff, _day_start(boundary_day))
    columns = (
        CONTROL_CYCLE_HISTORY_ROWS.c.source_id,
        CONTROL_CYCLE_HISTORY_ROWS.c.venue,
        CONTROL_CYCLE_HISTORY_ROWS.c.asset,
        CONTROL_CYCLE_HISTORY_ROWS.c.day,
        CONTROL_CYCLE_HISTORY_ROWS.c.payload_json,
    )
    stable_query = (
        select(*columns)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == base_namespace)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at >= long_cutoff.isoformat())
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at < boundary_start.isoformat())
        .where(or_(*pair_filters))
    )
    boundary_namespace = str(active_target.get("boundary_namespace") or "")
    boundary_query = (
        select(*columns)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.namespace == boundary_namespace)
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at >= boundary_start.isoformat())
        .where(CONTROL_CYCLE_HISTORY_ROWS.c.observed_at < recent_cutoff.isoformat())
        .where(or_(*pair_filters))
    )

    rows = []
    with factory.store.engine.connect() as db:
        if boundary_start > long_cutoff:
            rows.extend(db.execute(stable_query))
        if recent_cutoff > boundary_start:
            rows.extend(db.execute(boundary_query))

    grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
    for _source_id, _venue, _asset, _day, payload_json in rows:
        quote = MarketQuote.model_validate_json(payload_json)
        key = (quote.venue, quote.asset.upper(), quote.market_kind)
        if key in current_keys:
            grouped.setdefault(key, []).append(quote)
    for values in grouped.values():
        values.sort(key=lambda item: item.observed_at)
    return grouped


def resolve_durable_control_cycle_history_snapshot(
    store: Any,
    current_snapshot: Any,
    progress: dict[str, object],
):
    """Return the exact source snapshot certified by the active history generation."""

    if not bool(progress.get("complete")):
        return None
    serving_scan_id = str(progress.get("serving_scan_id") or "")
    if not serving_scan_id:
        return None
    if serving_scan_id == str(current_snapshot.scan_id):
        return current_snapshot
    try:
        snapshot = store.load_scan(serving_scan_id)
    except (KeyError, RuntimeError, ValueError):
        return None
    expected_at = str(progress.get("serving_target_completed_at") or "")
    if expected_at and snapshot.completed_at.isoformat() != expected_at:
        return None
    return snapshot

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import MetaData, Table, and_, func, inspect, select

from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


SOURCE_EVENT_TABLE = "source_event_observations"
MECHANISM_RESEARCH_TABLE = "mechanism_research_observations"
FUNDAMENTAL_TABLE = "alpha_fundamental_observations"
OPTION_CAPACITY_TABLE = "option_capacity_observations"
PROVIDER_STATUS_TABLE = "provider_statuses"
PROVIDER_ADMISSION_TABLE = "provider_gap_admissions"


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def _source_spec(source_id: str) -> dict[str, object] | None:
    for spec in SOURCES:
        if str(spec.get("id") or "") == source_id:
            return spec
    return None


def _empty() -> dict[str, dict[str, object]]:
    return {
        lane_id: {
            "source_count": 0,
            "source_earliest": None,
            "source_latest": None,
            "source_ids": set(),
            "evidence_classes": set(),
            "source_ledgers": set(),
        }
        for lane_id in LANES
    }


def _merge_observation(
    result: dict[str, dict[str, object]],
    *,
    lane_id: str,
    source_id: str,
    classes: set[str],
    count: int,
    earliest: datetime | None,
    latest: datetime | None,
    ledger: str,
) -> None:
    if lane_id not in result or count <= 0 or earliest is None or latest is None:
        return
    state = result[lane_id]
    state["source_count"] = int(state.get("source_count") or 0) + int(count)
    current_earliest = state.get("source_earliest")
    current_latest = state.get("source_latest")
    state["source_earliest"] = earliest if current_earliest is None or earliest < current_earliest else current_earliest
    state["source_latest"] = latest if current_latest is None or latest > current_latest else current_latest
    if source_id:
        state["source_ids"].add(source_id)
    state["evidence_classes"].update(classes)
    state["source_ledgers"].add(ledger)


def _merge_provider_history(
    result: dict[str, dict[str, object]],
    *,
    provider: str,
    count: int,
    earliest: datetime | None,
    latest: datetime | None,
    ledger: str,
) -> None:
    """Map trusted provider attempts through the same catalog prefixes as live coverage."""
    if count <= 0 or earliest is None or latest is None:
        return
    for spec in SOURCES:
        if spec.get("authoritative") is False:
            continue
        prefixes = [str(value) for value in list(spec.get("provider") or [])]
        if not prefixes or not any(provider.startswith(prefix) for prefix in prefixes):
            continue
        source_id = str(spec.get("id") or "")
        classes = {str(item) for item in list(spec.get("classes") or [])}
        for lane_id in list(spec.get("lanes") or []):
            lane_id = str(lane_id)
            required = {str(item) for item in list(LANES.get(lane_id, {}).get("required") or [])}
            _merge_observation(
                result,
                lane_id=lane_id,
                source_id=source_id,
                classes=classes & required,
                count=count,
                earliest=earliest,
                latest=latest,
                ledger=ledger,
            )


def _aggregate(
    store,
    table: Table,
    *,
    start_text: str,
    boundary_text: str,
    extra_clause=None,
) -> tuple[int, datetime | None, datetime | None]:
    observed = table.c.observed_at
    clause = and_(observed >= start_text, observed < boundary_text)
    if extra_clause is not None:
        clause = and_(clause, extra_clause)
    query = select(func.count(), func.min(observed), func.max(observed)).where(clause)
    with store.engine.connect() as db:
        row = db.execute(query).one()
    count = int(row[0] or 0)
    if count <= 0:
        return 0, None, None
    return count, _parse_time(row[1]), _parse_time(row[2])


def _aggregate_by_value(
    store,
    table: Table,
    *,
    column_name: str,
    start_text: str,
    boundary_text: str,
) -> dict[str, tuple[int, datetime | None, datetime | None]]:
    """Aggregate a full historical table window once, grouped by catalog key."""
    observed = table.c.observed_at
    group_column = table.c[column_name]
    query = (
        select(
            group_column,
            func.count(),
            func.min(observed),
            func.max(observed),
        )
        .where(observed >= start_text)
        .where(observed < boundary_text)
        .group_by(group_column)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query))
    result: dict[str, tuple[int, datetime | None, datetime | None]] = {}
    for value, count_raw, earliest_raw, latest_raw in rows:
        count = int(count_raw or 0)
        if count <= 0:
            continue
        result[str(value)] = (
            count,
            _parse_time(earliest_raw),
            _parse_time(latest_raw),
        )
    return result


def _recover_catalog_table_sources(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    """Recover source classes with one range scan per durable table/grouping key."""
    reflected: dict[str, Table] = {}
    aggregate_cache: dict[
        tuple[str, str | None],
        tuple[int, datetime | None, datetime | None]
        | dict[str, tuple[int, datetime | None, datetime | None]],
    ] = {}

    for spec in SOURCES:
        if spec.get("authoritative") is False:
            continue
        probe = spec.get("table")
        if not isinstance(probe, tuple) or len(probe) != 3:
            continue
        table_name_raw, column_name_raw, value = probe
        table_name = str(table_name_raw)
        if table_name not in available:
            continue

        table = reflected.get(table_name)
        if table is None:
            table = Table(table_name, MetaData(), autoload_with=store.engine)
            reflected[table_name] = table
        if "observed_at" not in table.c:
            continue

        column_name = None if column_name_raw is None else str(column_name_raw)
        if column_name is not None and column_name not in table.c:
            continue
        cache_key = (table_name, column_name)
        if cache_key not in aggregate_cache:
            if column_name is None:
                aggregate_cache[cache_key] = _aggregate(
                    store,
                    table,
                    start_text=start_text,
                    boundary_text=boundary_text,
                )
            else:
                aggregate_cache[cache_key] = _aggregate_by_value(
                    store,
                    table,
                    column_name=column_name,
                    start_text=start_text,
                    boundary_text=boundary_text,
                )

        cached = aggregate_cache[cache_key]
        if column_name is None:
            count, earliest, latest = cached
        else:
            if not isinstance(cached, dict):
                continue
            count, earliest, latest = cached.get(str(value), (0, None, None))
        if not count:
            continue

        source_id = str(spec.get("id") or "")
        classes = {str(item) for item in list(spec.get("classes") or [])}
        for lane_id in list(spec.get("lanes") or []):
            lane_id = str(lane_id)
            required = {str(item) for item in list(LANES.get(lane_id, {}).get("required") or [])}
            _merge_observation(
                result,
                lane_id=lane_id,
                source_id=source_id,
                classes=classes & required,
                count=count,
                earliest=earliest,
                latest=latest,
                ledger=table_name,
            )


def _recover_provider_statuses(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    """Recover successful public-provider attempts accepted by the live coverage plane."""
    if PROVIDER_STATUS_TABLE not in available:
        return
    table = Table(PROVIDER_STATUS_TABLE, MetaData(), autoload_with=store.engine)
    required_columns = {"provider", "ok", "observed_at"}
    if not required_columns <= set(table.c.keys()):
        return
    query = (
        select(
            table.c.provider,
            func.count(),
            func.min(table.c.observed_at),
            func.max(table.c.observed_at),
        )
        .where(table.c.observed_at >= start_text)
        .where(table.c.observed_at < boundary_text)
        .where(table.c.ok.is_(True))
        .group_by(table.c.provider)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query))
    for provider, count, earliest_raw, latest_raw in rows:
        _merge_provider_history(
            result,
            provider=str(provider or ""),
            count=int(count or 0),
            earliest=_parse_time(earliest_raw),
            latest=_parse_time(latest_raw),
            ledger=PROVIDER_STATUS_TABLE,
        )


def _recover_provider_admissions(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    """Recover admitted provider-gap observations without loading the ledger into memory."""
    if PROVIDER_ADMISSION_TABLE not in available:
        return
    table = Table(PROVIDER_ADMISSION_TABLE, MetaData(), autoload_with=store.engine)
    required_columns = {"provider", "observed_at", "payload_json"}
    if not required_columns <= set(table.c.keys()):
        return
    query = (
        select(table.c.provider, table.c.observed_at, table.c.payload_json)
        .where(table.c.observed_at >= start_text)
        .where(table.c.observed_at < boundary_text)
        .order_by(table.c.id)
    )
    aggregates: dict[str, dict[str, object]] = {}
    with store.engine.connect() as db:
        for provider_raw, observed_raw, payload_raw in db.execution_options(stream_results=True).execute(query):
            try:
                payload = json.loads(str(payload_raw))
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            if not bool(payload.get("healthy")):
                continue
            if payload.get("authoritative") is False:
                continue
            if payload.get("commercial_use_permitted") is False:
                continue
            if payload.get("point_in_time") is False:
                continue
            observed_at = _parse_time(payload.get("observed_at") or observed_raw)
            if observed_at is None:
                continue
            provider = str(payload.get("provider") or provider_raw or "")
            if not provider:
                continue
            state = aggregates.setdefault(
                provider,
                {"count": 0, "earliest": observed_at, "latest": observed_at},
            )
            state["count"] = int(state["count"]) + 1
            if observed_at < state["earliest"]:
                state["earliest"] = observed_at
            if observed_at > state["latest"]:
                state["latest"] = observed_at
    for provider, state in aggregates.items():
        _merge_provider_history(
            result,
            provider=provider,
            count=int(state["count"]),
            earliest=state["earliest"],
            latest=state["latest"],
            ledger=PROVIDER_ADMISSION_TABLE,
        )


def _recover_source_events(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    if SOURCE_EVENT_TABLE not in available:
        return
    table = Table(SOURCE_EVENT_TABLE, MetaData(), autoload_with=store.engine)
    query = (
        select(
            table.c.lane_id,
            table.c.source_id,
            func.count(),
            func.min(table.c.observed_at),
            func.max(table.c.observed_at),
        )
        .where(table.c.observed_at >= start_text)
        .where(table.c.observed_at < boundary_text)
        .group_by(table.c.lane_id, table.c.source_id)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query))
    for lane_id, source_id, count, earliest_raw, latest_raw in rows:
        lane_id = str(lane_id or "")
        source_id = str(source_id or "")
        spec = _source_spec(source_id)
        if lane_id not in LANES or spec is None or spec.get("authoritative") is False:
            continue
        earliest = _parse_time(earliest_raw)
        latest = _parse_time(latest_raw)
        required = {str(item) for item in list(LANES[lane_id].get("required") or [])}
        classes = {str(item) for item in list(spec.get("classes") or [])} & required
        _merge_observation(
            result,
            lane_id=lane_id,
            source_id=source_id,
            classes=classes,
            count=int(count or 0),
            earliest=earliest,
            latest=latest,
            ledger=SOURCE_EVENT_TABLE,
        )


def _recover_mechanism_research(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    if MECHANISM_RESEARCH_TABLE not in available:
        return
    table = Table(MECHANISM_RESEARCH_TABLE, MetaData(), autoload_with=store.engine)
    query = (
        select(
            table.c.mechanism,
            table.c.provider,
            func.count(),
            func.min(table.c.observed_at),
            func.max(table.c.observed_at),
        )
        .where(table.c.observed_at >= start_text)
        .where(table.c.observed_at < boundary_text)
        .group_by(table.c.mechanism, table.c.provider)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query))
    for mechanism, provider, count, earliest_raw, latest_raw in rows:
        mechanism = str(mechanism or "")
        provider = str(provider or "").lower()
        mappings: list[tuple[str, str, set[str]]] = []
        if mechanism == "yield":
            if "morpho" in provider:
                mappings.append(("yield", "morpho-markets", {"yield_rate", "capacity", "exit_liquidity"}))
            elif "lido" in provider:
                mappings.append(("yield", "lido-yield", {"yield_rate"}))
        elif mechanism == "volatility":
            if "deribit" in provider:
                mappings.append(("volatility", "deribit-options", {"option_quotes", "option_greeks", "option_depth"}))
            elif "okx" in provider:
                mappings.append(("volatility", "okx-options", {"option_quotes", "option_greeks", "option_depth"}))
            elif "bybit" in provider:
                mappings.append(("volatility", "bybit-options", {"option_quotes", "option_greeks"}))
        if not mappings:
            continue
        earliest = _parse_time(earliest_raw)
        latest = _parse_time(latest_raw)
        for lane_id, source_id, classes in mappings:
            _merge_observation(
                result,
                lane_id=lane_id,
                source_id=source_id,
                classes=classes,
                count=int(count or 0),
                earliest=earliest,
                latest=latest,
                ledger=MECHANISM_RESEARCH_TABLE,
            )


def _recover_option_capacity(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    if OPTION_CAPACITY_TABLE not in available:
        return
    table = Table(OPTION_CAPACITY_TABLE, MetaData(), autoload_with=store.engine)
    count, earliest, latest = _aggregate(store, table, start_text=start_text, boundary_text=boundary_text)
    _merge_observation(
        result,
        lane_id="volatility",
        source_id="deribit-option-capacity",
        classes={"option_capacity"},
        count=count,
        earliest=earliest,
        latest=latest,
        ledger=OPTION_CAPACITY_TABLE,
    )


def _recover_fundamental(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    if FUNDAMENTAL_TABLE not in available:
        return
    table = Table(FUNDAMENTAL_TABLE, MetaData(), autoload_with=store.engine)
    query = (
        select(
            table.c.provider,
            func.count(),
            func.min(table.c.observed_at),
            func.max(table.c.observed_at),
        )
        .where(table.c.observed_at >= start_text)
        .where(table.c.observed_at < boundary_text)
        .group_by(table.c.provider)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query))
    for provider, count, earliest_raw, latest_raw in rows:
        provider = str(provider or "").lower()
        classes: set[str] = set()
        source_ids: list[str] = []
        if provider.startswith("ethereum-mainnet:"):
            classes.add("chain_activity")
            source_ids.append("ethereum-publicnode")
        elif provider.startswith("ethereum+morpho:"):
            classes.update({"chain_activity", "protocol_fundamentals"})
            source_ids.extend(["ethereum-publicnode", "morpho-markets"])
        if not classes:
            continue
        earliest = _parse_time(earliest_raw)
        latest = _parse_time(latest_raw)
        for source_id in source_ids:
            source_classes = _source_spec(source_id)
            allowed = {str(item) for item in list((source_classes or {}).get("classes") or [])}
            _merge_observation(
                result,
                lane_id="fundamental_onchain",
                source_id=source_id,
                classes=classes & allowed,
                count=int(count or 0),
                earliest=earliest,
                latest=latest,
                ledger=FUNDAMENTAL_TABLE,
            )


def recover_raw_lane_history(store, *, start: datetime, boundary: datetime) -> dict[str, dict[str, object]]:
    """Recover historical source evidence from append-only raw ledgers.

    This is a diagnostic reconstruction only. It never creates candidate identities,
    forward samples, qualification, allocation, or execution authority. Large catalog
    tables are scanned once per grouping key; provider-admission rows are streamed and
    reduced to provider-level edge/count summaries.
    """
    result = _empty()
    try:
        available = set(inspect(store.engine).get_table_names())
    except Exception:
        return result
    start_text = _utc(start).isoformat()
    boundary_text = _utc(boundary).isoformat()
    _recover_catalog_table_sources(store, available, result, start_text, boundary_text)
    _recover_provider_statuses(store, available, result, start_text, boundary_text)
    _recover_provider_admissions(store, available, result, start_text, boundary_text)
    _recover_source_events(store, available, result, start_text, boundary_text)
    _recover_mechanism_research(store, available, result, start_text, boundary_text)
    _recover_option_capacity(store, available, result, start_text, boundary_text)
    _recover_fundamental(store, available, result, start_text, boundary_text)
    return result


__all__ = ["recover_raw_lane_history"]

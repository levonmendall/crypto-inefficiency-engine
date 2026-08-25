from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import MetaData, Table, and_, func, inspect, select

from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


SOURCE_EVENT_TABLE = "source_event_observations"
MECHANISM_RESEARCH_TABLE = "mechanism_research_observations"
FUNDAMENTAL_TABLE = "alpha_fundamental_observations"
OPTION_CAPACITY_TABLE = "option_capacity_observations"


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


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

    def parsed(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return _utc(value)
        if value in (None, ""):
            return None
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        return _utc(result)

    return count, parsed(row[1]), parsed(row[2])


def _recover_catalog_table_sources(store, available: set[str], result, start_text: str, boundary_text: str) -> None:
    """Recover source classes from canonical raw tables using bounded SQL aggregates."""
    reflected: dict[str, Table] = {}
    for spec in SOURCES:
        if spec.get("authoritative") is False:
            continue
        probe = spec.get("table")
        if not isinstance(probe, tuple) or len(probe) != 3:
            continue
        table_name, column_name, value = probe
        table_name = str(table_name)
        if table_name not in available:
            continue
        table = reflected.get(table_name)
        if table is None:
            table = Table(table_name, MetaData(), autoload_with=store.engine)
            reflected[table_name] = table
        if "observed_at" not in table.c:
            continue
        clause = None
        if column_name is not None:
            column_name = str(column_name)
            if column_name not in table.c:
                continue
            clause = table.c[column_name] == value
        count, earliest, latest = _aggregate(
            store,
            table,
            start_text=start_text,
            boundary_text=boundary_text,
            extra_clause=clause,
        )
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
        earliest = datetime.fromisoformat(str(earliest_raw).replace("Z", "+00:00"))
        latest = datetime.fromisoformat(str(latest_raw).replace("Z", "+00:00"))
        required = {str(item) for item in list(LANES[lane_id].get("required") or [])}
        classes = {str(item) for item in list(spec.get("classes") or [])} & required
        _merge_observation(
            result,
            lane_id=lane_id,
            source_id=source_id,
            classes=classes,
            count=int(count or 0),
            earliest=_utc(earliest),
            latest=_utc(latest),
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
        earliest = _utc(datetime.fromisoformat(str(earliest_raw).replace("Z", "+00:00")))
        latest = _utc(datetime.fromisoformat(str(latest_raw).replace("Z", "+00:00")))
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
        earliest = _utc(datetime.fromisoformat(str(earliest_raw).replace("Z", "+00:00")))
        latest = _utc(datetime.fromisoformat(str(latest_raw).replace("Z", "+00:00")))
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
    forward samples, qualification, allocation, or execution authority. SQL aggregate
    reads keep the operation bounded even when raw market tables are large.
    """
    result = _empty()
    try:
        available = set(inspect(store.engine).get_table_names())
    except Exception:
        return result
    start_text = _utc(start).isoformat()
    boundary_text = _utc(boundary).isoformat()
    _recover_catalog_table_sources(store, available, result, start_text, boundary_text)
    _recover_source_events(store, available, result, start_text, boundary_text)
    _recover_mechanism_research(store, available, result, start_text, boundary_text)
    _recover_option_capacity(store, available, result, start_text, boundary_text)
    _recover_fundamental(store, available, result, start_text, boundary_text)
    return result


__all__ = ["recover_raw_lane_history"]

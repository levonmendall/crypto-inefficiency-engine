from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import httpx
from sqlalchemy import Column, MetaData, String, Table, Text, inspect, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.evidence import EvidenceStore, build_evidence_store


TOP_VOLUME_ASSET_COUNT = 40
VOLUME_UNIVERSE_TABLE = "liquid_volume_universe_snapshots"
VOLUME_UNIVERSE_REFRESH_SECONDS = max(
    900.0,
    float(os.getenv("CIE_VOLUME_UNIVERSE_REFRESH_SECONDS", "3600")),
)
BYBIT_URL = "https://api.bybit.com"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"

# Stable-value instruments have their own dedicated mechanisms. Letting them
# consume directional/cross-venue top-volume slots would crowd out risk assets.
STABLE_VALUE_ASSETS = frozenset(
    {
        "USDT",
        "USDC",
        "USDE",
        "FDUSD",
        "DAI",
        "TUSD",
        "USDD",
        "USD1",
        "PYUSD",
        "FRAX",
        "LUSD",
        "USDS",
        "GUSD",
        "EURC",
    }
)
_ASSET_RE = re.compile(r"^[A-Z0-9]{2,15}$")
_MULTIPLIER_RE = re.compile(r"^(?:1000000|10000|1000)([A-Z][A-Z0-9]{1,14})$")


# Used only before a first durable volume snapshot exists or during a total
# ranking-provider outage. Normal production operation replaces this list with
# the rolling top 40 by 24-hour traded notional.
BOOTSTRAP_LIQUID_ASSETS: tuple[str, ...] = (
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "TRX", "AVAX", "LINK",
    "SUI", "BCH", "LTC", "XLM", "DOT", "HYPE", "PEPE", "UNI", "AAVE", "NEAR",
    "ATOM", "ETC", "FIL", "ICP", "APT", "ARB", "OP", "INJ", "SEI", "WIF",
    "BONK", "SHIB", "TAO", "RENDER", "ENA", "ONDO", "CRO", "POL", "ALGO", "FET",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _normalize_asset(raw: object, *, hyperliquid: bool = False) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if hyperliquid and text.startswith("k") and len(text) > 2:
        text = text[1:]
    asset = text.upper()
    multiplier = _MULTIPLIER_RE.fullmatch(asset)
    if multiplier:
        asset = multiplier.group(1)
    if not _ASSET_RE.fullmatch(asset) or asset in STABLE_VALUE_ASSETS:
        return None
    return asset


def _asset_from_bybit_symbol(symbol: object) -> str | None:
    text = str(symbol or "").upper()
    for quote in ("USDT", "USDC"):
        if text.endswith(quote) and len(text) > len(quote):
            return _normalize_asset(text[: -len(quote)])
    return None


def parse_bybit_turnover(payload: object, *, source: str) -> list[dict[str, object]]:
    if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
        raise ValueError("Bybit ticker response did not return retCode=0")
    result = payload.get("result")
    rows = result.get("list") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        raise ValueError("Bybit ticker response must contain result.list")
    observations: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = _asset_from_bybit_symbol(row.get("symbol"))
        try:
            notional = float(row.get("turnover24h") or 0.0)
        except (TypeError, ValueError):
            continue
        if asset and notional > 0:
            observations.append({"asset": asset, "notional_usd": notional, "source": source})
    return observations


def parse_hyperliquid_turnover(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("Hyperliquid metaAndAssetCtxs must be [meta, contexts]")
    meta, contexts = payload
    universe = meta.get("universe") if isinstance(meta, dict) else None
    if not isinstance(universe, list) or not isinstance(contexts, list):
        raise ValueError("Hyperliquid volume response is incomplete")
    observations: list[dict[str, object]] = []
    for instrument, context in zip(universe, contexts):
        if not isinstance(instrument, dict) or not isinstance(context, dict):
            continue
        asset = _normalize_asset(instrument.get("name"), hyperliquid=True)
        try:
            notional = float(context.get("dayNtlVlm") or 0.0)
        except (TypeError, ValueError):
            continue
        if asset and notional > 0:
            observations.append(
                {"asset": asset, "notional_usd": notional, "source": "hyperliquid:dayNtlVlm"}
            )
    return observations


def rank_volume_observations(
    observations: Iterable[dict[str, object]],
    *,
    limit: int = TOP_VOLUME_ASSET_COUNT,
) -> list[dict[str, object]]:
    totals: dict[str, float] = defaultdict(float)
    sources: dict[str, set[str]] = defaultdict(set)
    for row in observations:
        asset = _normalize_asset(row.get("asset"))
        try:
            notional = float(row.get("notional_usd") or 0.0)
        except (TypeError, ValueError):
            continue
        if asset is None or notional <= 0:
            continue
        totals[asset] += notional
        source = str(row.get("source") or "unknown")
        sources[asset].add(source)
    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[: max(1, int(limit))]
    return [
        {
            "rank": index,
            "asset": asset,
            "aggregate_24h_notional_usd": notional,
            "sources": sorted(sources[asset]),
        }
        for index, (asset, notional) in enumerate(ordered, start=1)
    ]


def _table(metadata: MetaData) -> Table:
    return Table(
        VOLUME_UNIVERSE_TABLE,
        metadata,
        Column("lineage_hash", String(64), primary_key=True),
        Column("observed_at", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )


def _snapshot_assets(payload: dict[str, Any] | None) -> tuple[str, ...]:
    if not isinstance(payload, dict):
        return ()
    rows = payload.get("assets")
    if not isinstance(rows, list):
        return ()
    result: list[str] = []
    for row in rows:
        asset = _normalize_asset(row.get("asset") if isinstance(row, dict) else row)
        if asset and asset not in result:
            result.append(asset)
    return tuple(result[:TOP_VOLUME_ASSET_COUNT])


class VolumeUniverseLedger:
    """Append-only record of the liquidity universe used by research collectors."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        self.metadata = MetaData()
        self.snapshots = _table(self.metadata)
        self.metadata.create_all(store.engine)

    def record(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        lineage = hashlib.sha256(encoded.encode()).hexdigest()
        row = {
            "lineage_hash": lineage,
            "observed_at": str(payload["observed_at"]),
            "payload_json": encoded,
        }
        backend = self.store.engine.url.get_backend_name()
        if backend == "postgresql":
            statement = pg_insert(self.snapshots).values(row).on_conflict_do_nothing(
                index_elements=[self.snapshots.c.lineage_hash]
            )
        elif backend == "sqlite":
            statement = sqlite_insert(self.snapshots).values(row).on_conflict_do_nothing(
                index_elements=[self.snapshots.c.lineage_hash]
            )
        else:
            statement = insert(self.snapshots).values(row)
        with self.store.engine.begin() as db:
            db.execute(statement)

    def latest(self) -> dict[str, Any] | None:
        with self.store.engine.connect() as db:
            raw = db.execute(
                select(self.snapshots.c.payload_json)
                .order_by(self.snapshots.c.observed_at.desc())
                .limit(1)
            ).scalar_one_or_none()
        if not raw:
            return None
        try:
            payload = json.loads(str(raw))
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None


def read_latest_volume_universe(store: EvidenceStore | None = None) -> dict[str, Any] | None:
    owned = False
    if store is None:
        store = build_evidence_store()
        owned = store is not None
    if store is None or VOLUME_UNIVERSE_TABLE not in set(inspect(store.engine).get_table_names()):
        return None
    metadata = MetaData()
    table = _table(metadata)
    with store.engine.connect() as db:
        raw = db.execute(
            select(table.c.payload_json).order_by(table.c.observed_at.desc()).limit(1)
        ).scalar_one_or_none()
    if not raw:
        return None
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def persisted_volume_assets() -> tuple[str, ...]:
    try:
        return _snapshot_assets(read_latest_volume_universe())
    except Exception:
        return ()


async def _collect_bybit(client: httpx.AsyncClient) -> tuple[list[dict[str, object]], dict[str, object]]:
    observations: list[dict[str, object]] = []
    health: dict[str, object] = {}
    for category in ("spot", "linear"):
        key = f"bybit:{category}"
        try:
            response = await client.get(f"{BYBIT_URL}/v5/market/tickers", params={"category": category})
            response.raise_for_status()
            rows = parse_bybit_turnover(response.json(), source=f"bybit:{category}:turnover24h")
            observations.extend(rows)
            health[key] = {"ok": bool(rows), "observation_count": len(rows), "error_type": None}
        except Exception as exc:
            health[key] = {"ok": False, "observation_count": 0, "error_type": type(exc).__name__}
    return observations, health


async def _collect_hyperliquid(client: httpx.AsyncClient) -> tuple[list[dict[str, object]], dict[str, object]]:
    try:
        response = await client.post(HYPERLIQUID_INFO_URL, json={"type": "metaAndAssetCtxs"})
        response.raise_for_status()
        rows = parse_hyperliquid_turnover(response.json())
        return rows, {
            "hyperliquid:perpetual": {"ok": bool(rows), "observation_count": len(rows), "error_type": None}
        }
    except Exception as exc:
        return [], {
            "hyperliquid:perpetual": {
                "ok": False,
                "observation_count": 0,
                "error_type": type(exc).__name__,
            }
        }


async def collect_top_volume_snapshot(
    *,
    now: datetime | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    observed_at = _aware(now or _now())
    owns_client = client is None
    client = client or httpx.AsyncClient(
        timeout=10.0,
        headers={"User-Agent": "crypto-inefficiency-engine/volume-universe", "Cache-Control": "no-cache"},
    )
    try:
        bybit_result, hyper_result = await asyncio.gather(
            _collect_bybit(client),
            _collect_hyperliquid(client),
        )
    finally:
        if owns_client:
            await client.aclose()
    observations = [*bybit_result[0], *hyper_result[0]]
    source_health = {**bybit_result[1], **hyper_result[1]}
    ranked = rank_volume_observations(observations, limit=TOP_VOLUME_ASSET_COUNT)
    if len(ranked) < TOP_VOLUME_ASSET_COUNT:
        raise RuntimeError(
            f"volume universe requires {TOP_VOLUME_ASSET_COUNT} eligible assets; observed {len(ranked)}"
        )
    return {
        "observed_at": observed_at.isoformat(),
        "method": "aggregate_24h_traded_notional",
        "asset_count": TOP_VOLUME_ASSET_COUNT,
        "stable_value_assets_excluded": True,
        "source_health": source_health,
        "assets": ranked,
        "paper_only": True,
        "allocation_authority": False,
    }


async def resolve_top_volume_assets(
    store: EvidenceStore | None,
    *,
    now: datetime | None = None,
    force_refresh: bool = False,
    collector=collect_top_volume_snapshot,
) -> tuple[str, ...]:
    """Return the rolling top-40 universe with durable last-known-good fallback."""
    observed_at = _aware(now or _now())
    latest = read_latest_volume_universe(store) if store is not None else None
    latest_assets = _snapshot_assets(latest)
    latest_at: datetime | None = None
    if latest and latest.get("observed_at"):
        try:
            latest_at = datetime.fromisoformat(str(latest["observed_at"]))
            latest_at = _aware(latest_at)
        except ValueError:
            latest_at = None
    if (
        not force_refresh
        and len(latest_assets) == TOP_VOLUME_ASSET_COUNT
        and latest_at is not None
        and observed_at - latest_at <= timedelta(seconds=VOLUME_UNIVERSE_REFRESH_SECONDS)
    ):
        return latest_assets

    try:
        snapshot = await collector(now=observed_at)
        assets = _snapshot_assets(snapshot)
        if len(assets) != TOP_VOLUME_ASSET_COUNT:
            raise RuntimeError("volume selector did not produce exactly 40 assets")
        if store is not None:
            VolumeUniverseLedger(store).record(snapshot)
        return assets
    except Exception:
        if len(latest_assets) == TOP_VOLUME_ASSET_COUNT:
            return latest_assets
        return BOOTSTRAP_LIQUID_ASSETS

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


# Start with the 25 most-liquid eligible assets and prove this cohort is reliable
# end-to-end before expanding the production research universe again.
TOP_VOLUME_ASSET_COUNT = 25
VOLUME_UNIVERSE_TABLE = "liquid_volume_universe_snapshots"
# Membership is a market-state input, not slow research. Keep a bounded cache to
# protect CoinGecko while leaving multiple lightweight refresh opportunities before
# the read plane considers a snapshot stale.
VOLUME_UNIVERSE_REFRESH_SECONDS = max(
    300.0,
    float(os.getenv("CIE_VOLUME_UNIVERSE_REFRESH_SECONDS", "900")),
)
COINGECKO_PUBLIC_API_URL = "https://api.coingecko.com/api/v3"
COINGECKO_PRO_API_URL = "https://pro-api.coingecko.com/api/v3"
# Version both the classification method and cohort size. This deliberately makes
# persisted 40-asset v2 snapshots ineligible as current top-25 state after rollout.
STRICT_VOLUME_METHOD = "marketwide_24h_trading_volume_usd_dynamic_stable_top25_v3"
STRICT_VOLUME_SOURCE = "coingecko:coins_markets:total_volume"
STRICT_STABLE_VALUE_SOURCE = "coingecko:coins_markets:category=stablecoins"

# Defense-in-depth only. This set is no longer the authority for stable-value
# classification. New snapshots additionally require CoinGecko's live stablecoin
# category to succeed and exclude those CoinGecko asset IDs before ranking.
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
        "USDG",
        "RLUSD",
        "U",
    }
)
_ASSET_RE = re.compile(r"^[A-Z][A-Z0-9]{1,19}$")
_MULTIPLIER_RE = re.compile(r"^(?:1000000|10000|1000)([A-Z][A-Z0-9]{1,18})$")


class VolumeUniverseUnavailableError(RuntimeError):
    """Raised when no validated market-wide top-volume ranking is available."""


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
    """Compatibility parser retained for provider diagnostics, not universe ranking."""
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
    """Compatibility parser retained for provider diagnostics, not universe ranking."""
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
    """Aggregate direct-venue observations by underlying for diagnostics/tests."""
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
        sources[asset].add(str(row.get("source") or "unknown"))
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


def parse_coingecko_markets(payload: object) -> list[dict[str, object]]:
    """Parse market-wide 24h volume rows from CoinGecko /coins/markets.

    CoinGecko's ``total_volume`` is the sole ranking metric. Duplicate symbols
    are retained here and later resolved by the highest reported volume rather
    than summed because different CoinGecko IDs can share a ticker.
    """
    if not isinstance(payload, list):
        raise ValueError("CoinGecko coins/markets response must be a list")
    rows: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        asset = _normalize_asset(item.get("symbol"))
        try:
            volume = float(item.get("total_volume") or 0.0)
        except (TypeError, ValueError):
            continue
        coin_id = str(item.get("id") or "").strip()
        if asset and volume > 0 and coin_id:
            rows.append(
                {
                    "asset": asset,
                    "notional_usd": volume,
                    "source": STRICT_VOLUME_SOURCE,
                    "source_asset_id": coin_id,
                }
            )
    return rows


def parse_coingecko_stablecoin_ids(payload: object) -> frozenset[str]:
    """Return CoinGecko IDs dynamically classified in its stablecoin category."""
    if not isinstance(payload, list):
        raise ValueError("CoinGecko stablecoin category response must be a list")
    ids = {
        str(item.get("id") or "").strip()
        for item in payload
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    if not ids:
        raise ValueError("CoinGecko stablecoin category returned no classified assets")
    return frozenset(ids)


def rank_marketwide_volume(
    observations: Iterable[dict[str, object]],
    *,
    limit: int = TOP_VOLUME_ASSET_COUNT,
) -> list[dict[str, object]]:
    """Rank eligible assets strictly by market-wide 24h USD trading volume."""
    best_by_asset: dict[str, dict[str, object]] = {}
    for row in observations:
        asset = _normalize_asset(row.get("asset"))
        try:
            volume = float(row.get("notional_usd") or 0.0)
        except (TypeError, ValueError):
            continue
        if asset is None or volume <= 0:
            continue
        current = best_by_asset.get(asset)
        current_volume = float(current.get("notional_usd") or 0.0) if current else -1.0
        if volume > current_volume:
            best_by_asset[asset] = {
                "asset": asset,
                "notional_usd": volume,
                "source_asset_id": str(row.get("source_asset_id") or ""),
            }
    ordered = sorted(
        best_by_asset.values(),
        key=lambda item: (-float(item["notional_usd"]), str(item["asset"])),
    )[: max(1, int(limit))]
    return [
        {
            "rank": index,
            "asset": str(row["asset"]),
            "reported_24h_volume_usd": float(row["notional_usd"]),
            "aggregate_24h_notional_usd": float(row["notional_usd"]),
            "sources": [STRICT_VOLUME_SOURCE],
            "source_asset_id": str(row.get("source_asset_id") or ""),
        }
        for index, row in enumerate(ordered, start=1)
    ]


def _table(metadata: MetaData) -> Table:
    return Table(
        VOLUME_UNIVERSE_TABLE,
        metadata,
        Column("lineage_hash", String(64), primary_key=True),
        Column("observed_at", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )


def _row_volume(row: dict[str, Any]) -> float:
    raw = row.get("reported_24h_volume_usd", row.get("aggregate_24h_notional_usd"))
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def validated_volume_assets(payload: dict[str, Any] | None) -> tuple[str, ...]:
    """Accept only a strict, descending, dynamically classified market-wide snapshot."""
    if not isinstance(payload, dict):
        return ()
    if payload.get("method") != STRICT_VOLUME_METHOD:
        return ()
    if payload.get("ranking_metric") != "reported_24h_trading_volume_usd":
        return ()
    if payload.get("ranking_source") != STRICT_VOLUME_SOURCE:
        return ()
    try:
        target_count = int(payload.get("universe_target_count") or 0)
    except (TypeError, ValueError):
        return ()
    if target_count != TOP_VOLUME_ASSET_COUNT:
        return ()
    rows = payload.get("assets")
    if not isinstance(rows, list) or len(rows) != TOP_VOLUME_ASSET_COUNT:
        return ()

    result: list[str] = []
    previous_volume: float | None = None
    for expected_rank, row in enumerate(rows, start=1):
        if not isinstance(row, dict) or int(row.get("rank") or 0) != expected_rank:
            return ()
        asset = _normalize_asset(row.get("asset"))
        volume = _row_volume(row)
        if asset is None or asset in result or volume <= 0:
            return ()
        if previous_volume is not None and volume > previous_volume:
            return ()
        previous_volume = volume
        result.append(asset)
    return tuple(result)


class VolumeUniverseLedger:
    """Append-only record of the exact market-wide volume universe used by research."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        self.metadata = MetaData()
        self.snapshots = _table(self.metadata)
        self.metadata.create_all(store.engine)

    def record(self, payload: dict[str, Any]) -> None:
        if len(validated_volume_assets(payload)) != TOP_VOLUME_ASSET_COUNT:
            raise ValueError(
                f"refusing to persist a non-authoritative top-{TOP_VOLUME_ASSET_COUNT} volume snapshot"
            )
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
        return read_latest_volume_universe(self.store)


def read_latest_volume_universe(store: EvidenceStore | None = None) -> dict[str, Any] | None:
    if store is None:
        store = build_evidence_store()
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
        return validated_volume_assets(read_latest_volume_universe())
    except Exception:
        return ()


def _coingecko_client_config() -> tuple[str, dict[str, str]]:
    pro_key = os.getenv("CIE_COINGECKO_API_KEY", "").strip()
    demo_key = os.getenv("CIE_COINGECKO_DEMO_API_KEY", "").strip()
    if pro_key:
        return COINGECKO_PRO_API_URL, {"x-cg-pro-api-key": pro_key}
    if demo_key:
        return COINGECKO_PUBLIC_API_URL, {"x-cg-demo-api-key": demo_key}
    return COINGECKO_PUBLIC_API_URL, {}


async def collect_top_volume_snapshot(
    *,
    now: datetime | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Collect the configured top-volume cohort after live stable-value classification.

    Volume remains the only ordering metric. Eligibility is determined first by
    requiring CoinGecko's live ``stablecoins`` category and excluding matching
    CoinGecko asset IDs. Both feeds must succeed before a new snapshot is accepted.
    """
    observed_at = _aware(now or _now())
    base_url, auth_headers = _coingecko_client_config()
    owns_client = client is None
    client = client or httpx.AsyncClient(
        timeout=15.0,
        headers={
            "User-Agent": "crypto-inefficiency-engine/volume-universe",
            "Cache-Control": "no-cache",
            **auth_headers,
        },
    )

    market_params = {
        "vs_currency": "usd",
        "order": "volume_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
        "include_rehypothecated": "false",
    }
    stable_params = {
        **market_params,
        "category": "stablecoins",
    }

    async def fetch(params: dict[str, object]):
        response = await client.get(f"{base_url}/coins/markets", params=params)
        response.raise_for_status()
        return response.json()

    try:
        market_payload, stable_payload = await asyncio.gather(
            fetch(market_params),
            fetch(stable_params),
        )
        observations = parse_coingecko_markets(market_payload)
        stablecoin_ids = parse_coingecko_stablecoin_ids(stable_payload)
    finally:
        if owns_client:
            await client.aclose()

    eligible_observations = [
        row
        for row in observations
        if str(row.get("source_asset_id") or "") not in stablecoin_ids
    ]
    ranked = rank_marketwide_volume(eligible_observations, limit=TOP_VOLUME_ASSET_COUNT)
    if len(ranked) != TOP_VOLUME_ASSET_COUNT:
        raise VolumeUniverseUnavailableError(
            f"market-wide volume source produced {len(ranked)} eligible assets, "
            f"expected {TOP_VOLUME_ASSET_COUNT}"
        )

    dynamically_excluded = sum(
        1
        for row in observations
        if str(row.get("source_asset_id") or "") in stablecoin_ids
    )
    payload: dict[str, Any] = {
        "observed_at": observed_at.isoformat(),
        "method": STRICT_VOLUME_METHOD,
        "ranking_metric": "reported_24h_trading_volume_usd",
        "ranking_source": STRICT_VOLUME_SOURCE,
        "ranking_scope": "marketwide",
        "volume_is_defining_metric": True,
        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
        "asset_count": TOP_VOLUME_ASSET_COUNT,
        "stable_value_assets_excluded": True,
        "dynamic_stable_value_classification": True,
        "stable_value_classification_source": STRICT_STABLE_VALUE_SOURCE,
        "stable_value_classified_id_count": len(stablecoin_ids),
        "stable_value_observations_excluded": dynamically_excluded,
        "eligibility_note": (
            "CoinGecko stablecoin-category assets and non-canonical ticker symbols "
            "are excluded before ranking; 24h total_volume alone orders eligible assets"
        ),
        "source_health": {
            "coingecko:coins_markets": {
                "ok": True,
                "observation_count": len(observations),
                "error_type": None,
            },
            "coingecko:coins_markets:stablecoins": {
                "ok": True,
                "classified_asset_count": len(stablecoin_ids),
                "error_type": None,
            },
        },
        "assets": ranked,
        "paper_only": True,
        "allocation_authority": False,
    }
    if len(validated_volume_assets(payload)) != TOP_VOLUME_ASSET_COUNT:
        raise VolumeUniverseUnavailableError("market-wide volume snapshot failed strict ordering validation")
    return payload


async def resolve_top_volume_assets(
    store: EvidenceStore | None,
    *,
    now: datetime | None = None,
    force_refresh: bool = False,
    collector=collect_top_volume_snapshot,
) -> tuple[str, ...]:
    """Return only a validated market-wide top-volume universe.

    A prior snapshot from this exact dynamic-classification/cohort version may be
    used as last-known-good during a transient source outage. Older cohort versions
    and static coin lists are never accepted. With no validated snapshot, failure
    is fail-closed.
    """
    observed_at = _aware(now or _now())
    latest = read_latest_volume_universe(store) if store is not None else None
    latest_assets = validated_volume_assets(latest)
    latest_at: datetime | None = None
    if latest and latest.get("observed_at"):
        try:
            latest_at = _aware(datetime.fromisoformat(str(latest["observed_at"])))
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
        assets = validated_volume_assets(snapshot)
        if len(assets) != TOP_VOLUME_ASSET_COUNT:
            raise VolumeUniverseUnavailableError(
                f"collector did not return a validated top-{TOP_VOLUME_ASSET_COUNT} volume ranking"
            )
        if store is not None:
            VolumeUniverseLedger(store).record(snapshot)
        return assets
    except Exception as exc:
        if len(latest_assets) == TOP_VOLUME_ASSET_COUNT:
            return latest_assets
        raise VolumeUniverseUnavailableError(
            f"no validated market-wide top-{TOP_VOLUME_ASSET_COUNT} volume ranking is available"
        ) from exc

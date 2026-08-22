from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

AAVE_V3_ETHEREUM_POOL = "0x87870Bca3F3d6335C3F4ce8392D69350B4fA4E2"
AAVE_LIQUIDATION_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
_BYBIT_OPTION = re.compile(r"^(?P<asset>[A-Z0-9]+)-(?P<expiry>[0-9]{1,2}[A-Z]{3}[0-9]{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$")
_OKX_OPTION = re.compile(r"^(?P<asset>[A-Z0-9]+)-USD-(?P<expiry>[0-9]{6})-(?P<strike>[0-9.]+)-(?P<type>[CP])$")


def stable_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


def number(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def utc_ms(value: object | None) -> datetime | None:
    parsed = number(value)
    if parsed is None or parsed <= 0:
        return None
    return datetime.fromtimestamp(parsed / 1000.0, tz=timezone.utc)


def parse_bybit_liquidation_message(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, dict) or not str(payload.get("topic") or "").startswith("allLiquidation."):
        return []
    data = payload.get("data")
    rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    result: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol, side = str(row.get("s") or "").upper(), str(row.get("S") or "")
        event_at, quantity, price = utc_ms(row.get("T")), number(row.get("v")), number(row.get("p"))
        if not symbol or event_at is None or quantity is None or quantity < 0 or price is None or price <= 0:
            continue
        result.append({"symbol":symbol,"side":side,"quantity":quantity,"price":price,"event_at":event_at})
    return result


def _address(value: object | None) -> str | None:
    raw = str(value or "")
    return "0x" + raw[-40:] if raw.startswith("0x") and len(raw) >= 42 else None


def parse_aave_liquidation_log(row: object) -> dict[str, object] | None:
    if not isinstance(row, dict):
        return None
    topics = row.get("topics")
    if not isinstance(topics, list) or len(topics) < 4 or str(topics[0]).lower() != AAVE_LIQUIDATION_TOPIC.lower():
        return None
    collateral, debt, user = _address(topics[1]), _address(topics[2]), _address(topics[3])
    raw = str(row.get("data") or "")
    body = raw[2:] if raw.startswith("0x") else ""
    words = [body[index:index+64] for index in range(0,len(body),64)] if body and len(body)%64 == 0 else []
    if collateral is None or debt is None or user is None or len(words) < 4:
        return None
    return {"collateral_asset":collateral,"debt_asset":debt,"user":user,"debt_to_cover_raw":int(words[0],16),"liquidated_collateral_raw":int(words[1],16),"liquidator":"0x"+words[2][-40:],"receive_a_token":bool(int(words[3],16)),"transaction_hash":str(row.get("transactionHash") or ""),"log_index":str(row.get("logIndex") or ""),"block_number":str(row.get("blockNumber") or "")}


def parse_snapshot_proposals(payload: object) -> list[dict[str, object]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    proposals = data.get("proposals") if isinstance(data, dict) else None
    if not isinstance(proposals, list):
        raise ValueError("Snapshot response did not contain proposals")
    result: list[dict[str, object]] = []
    for row in proposals:
        if not isinstance(row, dict) or not row.get("id"):
            continue
        space = row.get("space") if isinstance(row.get("space"), dict) else {}
        created, start = number(row.get("created")), number(row.get("start"))
        event_at = datetime.fromtimestamp(start or created or datetime.now(timezone.utc).timestamp(), tz=timezone.utc)
        result.append({"id":str(row["id"]),"title":str(row.get("title") or ""),"state":str(row.get("state") or ""),"space_id":str(space.get("id") or ""),"space_symbol":str(space.get("symbol") or "").upper(),"event_at":event_at})
    return result


def parse_morpho_markets(payload: object) -> list[dict[str, object]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    markets = data.get("markets") if isinstance(data, dict) else None
    items = markets.get("items") if isinstance(markets, dict) else None
    if not isinstance(items, list):
        raise ValueError("Morpho response did not contain markets.items")
    result: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        asset = item.get("loanAsset") if isinstance(item.get("loanAsset"), dict) else {}
        state = item.get("state") if isinstance(item.get("state"), dict) else {}
        apy, supply, liquidity = number(state.get("supplyApy")), number(state.get("supplyAssetsUsd")), number(state.get("liquidityAssetsUsd"))
        symbol = str(asset.get("symbol") or "").upper()
        key = str(item.get("marketId") or item.get("uniqueKey") or item.get("id") or "")
        if key and symbol and apy is not None and supply and supply > 0 and liquidity and liquidity > 0:
            result.append({"market_id":key,"asset":symbol,"supply_apy":apy,"supply_usd":supply,"liquidity_usd":liquidity})
    return result


def parse_bybit_option_symbol(symbol: str) -> tuple[str, datetime, float, str] | None:
    match = _BYBIT_OPTION.match(symbol)
    if match is None:
        return None
    expiry = datetime.strptime(match.group("expiry"), "%d%b%y").replace(hour=8,tzinfo=timezone.utc)
    return match.group("asset"), expiry, float(match.group("strike")), "call" if match.group("type") == "C" else "put"


def parse_okx_option_symbol(symbol: str) -> tuple[str, datetime, float, str] | None:
    match = _OKX_OPTION.match(symbol)
    if match is None:
        return None
    expiry = datetime.strptime(match.group("expiry"), "%y%m%d").replace(hour=8,tzinfo=timezone.utc)
    return match.group("asset"), expiry, float(match.group("strike")), "call" if match.group("type") == "C" else "put"

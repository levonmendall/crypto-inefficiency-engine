from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import (
    number,
    parse_bybit_option_symbol,
    parse_okx_option_symbol,
    stable_id,
    utc_ms,
)
from inefficiency_engine.provider_gap_resilience import BYBIT_BASE_URLS
from inefficiency_engine.research_mechanisms import OptionQuoteObservation

OKX_BASE_URL = "https://www.okx.com"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _bybit_result(payload: object) -> dict[str, Any]:
    if not isinstance(payload,dict) or int(payload.get("retCode",-1)) != 0 or not isinstance(payload.get("result"),dict):
        raise ValueError("Bybit option response is invalid")
    return payload["result"]


def _pick_options(
    rows: list[tuple[dict[str,Any], tuple[str,datetime,float,str], float]],
    limit_per_side: int = 2,
    expiry_count: int = 2,
) -> list[tuple[dict[str,Any], tuple[str,datetime,float,str], float]]:
    """Keep a bounded ATM surface across the nearest two expiries.

    The previous collector retained only one expiry, which made term-structure
    research impossible. Two expiries preserve a useful near/next surface while
    keeping memory and downstream book fanout tightly bounded.
    """
    if not rows:
        return []
    expiries = sorted({row[1][1] for row in rows})[:max(1, int(expiry_count))]
    selected: list[tuple[dict[str,Any], tuple[str,datetime,float,str], float]] = []
    for expiry in expiries:
        expiry_rows = [row for row in rows if row[1][1] == expiry]
        for option_type in ("call","put"):
            side = [row for row in expiry_rows if row[1][3] == option_type]
            side.sort(key=lambda row: abs(row[1][2]/row[2]-1.0))
            selected.extend(side[:limit_per_side])
    return selected


async def collect_bybit_options(volatility_service) -> SourceProbeResult:
    source = f"{BYBIT_BASE_URLS[0]}/v5/market/tickers"
    observations: list[OptionQuoteObservation] = []
    async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control":"no-cache"}) as client:
        for asset in ("BTC","ETH"):
            response = await client.get(source, params={"category":"option","baseCoin":asset})
            response.raise_for_status()
            result = _bybit_result(response.json())
            parsed_rows: list[tuple[dict[str,Any], tuple[str,datetime,float,str], float]] = []
            for raw in result.get("list") or []:
                if not isinstance(raw,dict):
                    continue
                parsed = parse_bybit_option_symbol(str(raw.get("symbol") or ""))
                underlying = number(raw.get("underlyingPrice"))
                if parsed and parsed[1] > _now() and underlying and underlying > 0:
                    parsed_rows.append((raw,parsed,underlying))
            for raw, parsed, _ in _pick_options(parsed_rows):
                underlying_asset, expiry, strike, option_type = parsed
                bid, ask = number(raw.get("bid1Price")), number(raw.get("ask1Price"))
                delta, iv = number(raw.get("delta")), number(raw.get("markIv"))
                if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask or delta is None or iv is None or iv <= 0:
                    continue
                observed_at = _now()
                observation = OptionQuoteObservation(
                    observation_id=stable_id("bybit-options",raw.get("symbol"),int(observed_at.timestamp()*1000)),
                    provider="bybit-v5:option-ticker", venue="Bybit", underlying=underlying_asset,
                    expiry=expiry, strike=strike, option_type=option_type, bid=bid, ask=ask,
                    implied_volatility=iv/100.0 if iv>3 else iv, delta=delta,
                    gamma=number(raw.get("gamma")), vega=number(raw.get("vega")), observed_at=observed_at,
                    source_reference=source, authoritative=True, commercial_use_permitted=True,
                    point_in_time=True, paper_only=True,
                )
                volatility_service.record(observation)
                observations.append(observation)
    if not observations:
        raise ValueError("Bybit returned no bounded executable option quotes with Greeks")
    return SourceProbeResult(
        source_id="bybit-options", item_count=len(observations), source_reference=source,
        evidence_by_lane={"volatility":["option_quotes","option_greeks"]}, economic_fields_complete=True,
        detail={
            "underlyings":sorted({row.underlying for row in observations}),
            "expiry_count":len({row.expiry for row in observations}),
            "bounded_term_structure":True,
        },
    )


async def collect_okx_options(volatility_service) -> SourceProbeResult:
    summary_url, book_url = f"{OKX_BASE_URL}/api/v5/public/opt-summary", f"{OKX_BASE_URL}/api/v5/market/books"
    observations: list[OptionQuoteObservation] = []
    async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control":"no-cache"}) as client:
        for asset in ("BTC","ETH"):
            response = await client.get(summary_url, params={"instFamily":f"{asset}-USD"})
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload,dict) or str(payload.get("code")) != "0" or not isinstance(payload.get("data"),list):
                raise ValueError("OKX option summary response is invalid")
            parsed_rows: list[tuple[dict[str,Any], tuple[str,datetime,float,str], float]] = []
            for raw in payload["data"]:
                if not isinstance(raw,dict):
                    continue
                parsed = parse_okx_option_symbol(str(raw.get("instId") or ""))
                forward = number(raw.get("fwdPx"))
                if parsed and parsed[1] > _now() and forward and forward > 0:
                    parsed_rows.append((raw,parsed,forward))
            for raw, parsed, _ in _pick_options(parsed_rows):
                instrument = str(raw.get("instId") or "")
                book_response = await client.get(book_url, params={"instId":instrument,"sz":5})
                book_response.raise_for_status()
                book_payload = book_response.json()
                book_rows = book_payload.get("data") if isinstance(book_payload,dict) and str(book_payload.get("code")) == "0" else None
                book = book_rows[0] if isinstance(book_rows,list) and book_rows and isinstance(book_rows[0],dict) else None
                bids, asks = (book.get("bids"),book.get("asks")) if isinstance(book,dict) else (None,None)
                bid = number(bids[0][0]) if isinstance(bids,list) and bids and isinstance(bids[0],list) else None
                ask = number(asks[0][0]) if isinstance(asks,list) and asks and isinstance(asks[0],list) else None
                delta = number(raw.get("deltaBS") or raw.get("deltaPA") or raw.get("delta"))
                iv = number(raw.get("markVol") or raw.get("bidVol") or raw.get("askVol"))
                if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask or delta is None or iv is None or iv <= 0:
                    continue
                observed_at = utc_ms(raw.get("ts")) or _now()
                underlying_asset, expiry, strike, option_type = parsed
                observation = OptionQuoteObservation(
                    observation_id=stable_id("okx-options",instrument,int(observed_at.timestamp()*1000)),
                    provider="okx-v5:option-summary-books", venue="OKX", underlying=underlying_asset,
                    expiry=expiry, strike=strike, option_type=option_type, bid=bid, ask=ask,
                    implied_volatility=iv/100.0 if iv>3 else iv, delta=delta,
                    gamma=number(raw.get("gammaBS") or raw.get("gamma")),
                    vega=number(raw.get("vegaBS") or raw.get("vega")), observed_at=observed_at,
                    source_reference=f"{summary_url}|{book_url}", authoritative=True,
                    commercial_use_permitted=True, point_in_time=True, paper_only=True,
                )
                volatility_service.record(observation)
                observations.append(observation)
    if not observations:
        raise ValueError("OKX returned no bounded executable option books with Greeks")
    return SourceProbeResult(
        source_id="okx-options", item_count=len(observations), source_reference=summary_url,
        evidence_by_lane={"volatility":["option_quotes","option_greeks","option_depth"]},
        economic_fields_complete=True,
        detail={
            "underlyings":sorted({row.underlying for row in observations}),
            "book_depth":5,
            "expiry_count":len({row.expiry for row in observations}),
            "bounded_term_structure":True,
        },
    )
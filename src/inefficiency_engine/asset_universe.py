from __future__ import annotations

import os
import re


# Broad enough to create meaningful cross-venue/cycle opportunity coverage while
# remaining bounded for public REST collectors and the one-time historical backfill.
# Hyperliquid is intentionally not constrained by this list; its public universe is
# ingested dynamically. This core is for bounded CEX spot/perp surfaces.
DEFAULT_LIQUID_RESEARCH_ASSETS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "LINK",
    "LTC",
    "BCH",
    "DOT",
    "UNI",
    "AAVE",
    "NEAR",
    "ATOM",
    "SUI",
)

MAX_LIQUID_RESEARCH_ASSETS = 40
_ASSET_RE = re.compile(r"^[A-Z0-9]{2,15}$")


def configured_liquid_research_assets(raw: str | None = None) -> tuple[str, ...]:
    """Return the bounded CEX research universe.

    CIE_LIQUID_RESEARCH_ASSETS may override the default with a comma-separated
    list. Order is preserved and duplicates are removed. The cap prevents a bad
    deployment setting from turning bounded public collectors into an unbounded
    fanout. Eligibility for allocation remains governed later by evidence,
    economics, liquidity, cost, risk and execution controls.
    """

    source = os.getenv("CIE_LIQUID_RESEARCH_ASSETS") if raw is None else raw
    if source is None or not source.strip():
        return DEFAULT_LIQUID_RESEARCH_ASSETS

    values: list[str] = []
    seen: set[str] = set()
    for item in source.split(","):
        asset = item.strip().upper()
        if not asset:
            continue
        if not _ASSET_RE.fullmatch(asset):
            raise ValueError(f"invalid liquid research asset: {asset!r}")
        if asset not in seen:
            values.append(asset)
            seen.add(asset)
    if not values:
        raise ValueError("CIE_LIQUID_RESEARCH_ASSETS must contain at least one asset")
    if len(values) > MAX_LIQUID_RESEARCH_ASSETS:
        raise ValueError(
            f"CIE_LIQUID_RESEARCH_ASSETS cannot exceed {MAX_LIQUID_RESEARCH_ASSETS} assets"
        )
    return tuple(values)

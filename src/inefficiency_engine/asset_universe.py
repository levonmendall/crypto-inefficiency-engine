from __future__ import annotations

import os
import re


MAX_LIQUID_RESEARCH_ASSETS = 40
_ASSET_RE = re.compile(r"^[A-Z0-9]{2,15}$")

# Emergency bootstrap only. Normal production operation reads the durable rolling
# top-40 volume snapshot produced from public market turnover data.
DEFAULT_LIQUID_RESEARCH_ASSETS: tuple[str, ...] = (
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "TRX", "AVAX", "LINK",
    "SUI", "BCH", "LTC", "XLM", "DOT", "HYPE", "PEPE", "UNI", "AAVE", "NEAR",
    "ATOM", "ETC", "FIL", "ICP", "APT", "ARB", "OP", "INJ", "SEI", "WIF",
    "BONK", "SHIB", "TAO", "RENDER", "ENA", "ONDO", "CRO", "POL", "ALGO", "FET",
)


def _validated_assets(source: str) -> tuple[str, ...]:
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


def configured_liquid_research_assets(raw: str | None = None) -> tuple[str, ...]:
    """Return the bounded CEX research universe.

    Production defaults to the latest durable top-40 24-hour volume ranking. The
    explicit CIE_LIQUID_RESEARCH_ASSETS setting remains an emergency/manual
    override. Before the first durable ranking exists, a 40-asset bootstrap keeps
    collectors operational until the live selector succeeds.
    """

    source = os.getenv("CIE_LIQUID_RESEARCH_ASSETS") if raw is None else raw
    if source is not None and source.strip():
        return _validated_assets(source)

    if raw is None:
        try:
            from inefficiency_engine.volume_universe import persisted_volume_assets

            dynamic = persisted_volume_assets()
            if len(dynamic) == MAX_LIQUID_RESEARCH_ASSETS:
                return dynamic
        except Exception:
            pass

    return DEFAULT_LIQUID_RESEARCH_ASSETS

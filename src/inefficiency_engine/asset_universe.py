from __future__ import annotations

import re


# Keep this compatibility helper aligned with the authoritative dynamic production
# cohort in volume_universe.py. It is intentionally duplicated here to avoid an
# import cycle during adapter construction.
MAX_LIQUID_RESEARCH_ASSETS = 25
_ASSET_RE = re.compile(r"^[A-Z][A-Z0-9]{1,19}$")

# Constructor-only seed. It is deliberately NOT a 25-asset universe and must
# never be presented as one. DynamicVolumePublicAdapterRegistry refreshes managed
# adapters from a validated market-wide volume snapshot before live collection.
DEFAULT_LIQUID_RESEARCH_ASSETS: tuple[str, ...] = ("BTC",)


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
        raise ValueError("asset list must contain at least one asset")
    if len(values) > MAX_LIQUID_RESEARCH_ASSETS:
        raise ValueError(
            f"asset list cannot exceed {MAX_LIQUID_RESEARCH_ASSETS} assets"
        )
    return tuple(values)


def configured_liquid_research_assets(raw: str | None = None) -> tuple[str, ...]:
    """Return the current bounded CEX research universe.

    Production ignores static/environment overrides: the only production 25-asset
    universe is the latest validated market-wide 24-hour volume ranking. ``raw``
    remains available only as an explicit call-site/test constructor input. Before
    the first validated ranking exists, return a one-asset constructor seed; the
    dynamic registry must refresh successfully before any live CEX collection.
    """

    if raw is not None:
        return _validated_assets(raw)

    try:
        from inefficiency_engine.volume_universe import persisted_volume_assets

        dynamic = persisted_volume_assets()
        if len(dynamic) == MAX_LIQUID_RESEARCH_ASSETS:
            return dynamic
    except Exception:
        pass

    return DEFAULT_LIQUID_RESEARCH_ASSETS

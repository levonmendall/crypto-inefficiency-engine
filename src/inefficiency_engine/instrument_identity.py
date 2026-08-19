from __future__ import annotations

from inefficiency_engine.models import MarketKind


def normalized_contract_key(market_kind: MarketKind, contract_key: str | None = None) -> str:
    if contract_key:
        return contract_key
    if market_kind == MarketKind.SPOT:
        return "spot"
    if market_kind == MarketKind.PERPETUAL:
        return "continuous"
    return "unspecified"


def book_identity(
    venue: str,
    asset: str,
    market_kind: MarketKind,
    contract_key: str | None = None,
) -> tuple[str, str, str, str]:
    return (
        venue,
        asset.upper(),
        market_kind.value,
        normalized_contract_key(market_kind, contract_key),
    )

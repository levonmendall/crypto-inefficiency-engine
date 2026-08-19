from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field

from inefficiency_engine.instrument_identity import normalized_contract_key
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote


class GraphRelationship(str, Enum):
    LISTS = "lists"
    REPRESENTS = "represents"
    ECONOMIC_EQUIVALENCE = "economic_equivalence"


class CanonicalAsset(BaseModel):
    asset_id: str
    symbol: str


class CanonicalVenue(BaseModel):
    venue_id: str
    name: str


class CanonicalInstrument(BaseModel):
    instrument_id: str
    asset_id: str
    venue_id: str
    venue: str
    asset: str
    market_kind: MarketKind
    contract_key: str
    quote_currency: str | None = None
    expires_at: datetime | None = None
    provider_symbols: dict[str, str] = Field(default_factory=dict)
    sources: list[str] = Field(default_factory=list)
    observed_at: datetime | None = None
    latest_bid: float | None = None
    latest_ask: float | None = None
    latest_mid: float | None = None
    latest_funding_rate: float | None = None
    funding_interval_hours: float | None = None


class MarketGraphEdge(BaseModel):
    edge_id: str
    source_id: str
    target_id: str
    relationship: GraphRelationship
    metadata: dict[str, object] = Field(default_factory=dict)


class MarketGraphSnapshot(BaseModel):
    graph_version: str = "v0.9.1"
    observed_at: datetime
    assets: list[CanonicalAsset] = Field(default_factory=list)
    venues: list[CanonicalVenue] = Field(default_factory=list)
    instruments: list[CanonicalInstrument] = Field(default_factory=list)
    edges: list[MarketGraphEdge] = Field(default_factory=list)
    price_observation_count: int = 0
    funding_observation_count: int = 0

    def instrument_id_for(
        self,
        venue: str,
        asset: str,
        market_kind: MarketKind,
        contract_key: str | None = None,
    ) -> str | None:
        expected = canonical_instrument_id(venue, asset, market_kind, contract_key=contract_key)
        return expected if any(item.instrument_id == expected for item in self.instruments) else None

    def summary(self) -> dict[str, object]:
        return {
            "graph_version": self.graph_version,
            "asset_count": len(self.assets),
            "venue_count": len(self.venues),
            "instrument_count": len(self.instruments),
            "edge_count": len(self.edges),
            "price_observation_count": self.price_observation_count,
            "funding_observation_count": self.funding_observation_count,
            "market_kinds": sorted({item.market_kind.value for item in self.instruments}),
        }


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return normalized or "unknown"


def canonical_asset_id(asset: str) -> str:
    return f"crypto:asset:{asset.strip().upper()}"


def canonical_venue_id(venue: str) -> str:
    return f"crypto:venue:{_slug(venue)}"


def canonical_instrument_id(
    venue: str,
    asset: str,
    market_kind: MarketKind,
    *,
    contract_key: str | None = None,
) -> str:
    key = normalized_contract_key(market_kind, contract_key)
    return (
        f"crypto:instrument:{_slug(venue)}:{market_kind.value}:"
        f"{asset.strip().upper()}:{_slug(key)}"
    )


def _edge_id(source_id: str, target_id: str, relationship: GraphRelationship) -> str:
    return f"{relationship.value}:{source_id}->{target_id}"


def build_market_graph(
    funding_quotes: list[FundingQuote],
    market_quotes: list[MarketQuote],
) -> MarketGraphSnapshot:
    assets: dict[str, CanonicalAsset] = {}
    venues: dict[str, CanonicalVenue] = {}
    instruments: dict[str, CanonicalInstrument] = {}
    edges: dict[str, MarketGraphEdge] = {}
    observed_times: list[datetime] = []

    def ensure_instrument(
        *,
        venue: str,
        asset: str,
        market_kind: MarketKind,
        observed_at: datetime,
        contract_key: str | None = None,
        quote_currency: str | None = None,
        expires_at: datetime | None = None,
    ) -> CanonicalInstrument:
        asset_id = canonical_asset_id(asset)
        venue_id = canonical_venue_id(venue)
        key = normalized_contract_key(market_kind, contract_key)
        instrument_id = canonical_instrument_id(venue, asset, market_kind, contract_key=key)
        assets.setdefault(asset_id, CanonicalAsset(asset_id=asset_id, symbol=asset.upper()))
        venues.setdefault(venue_id, CanonicalVenue(venue_id=venue_id, name=venue))
        instrument = instruments.get(instrument_id)
        if instrument is None:
            instrument = CanonicalInstrument(
                instrument_id=instrument_id,
                asset_id=asset_id,
                venue_id=venue_id,
                venue=venue,
                asset=asset.upper(),
                market_kind=market_kind,
                contract_key=key,
                quote_currency=quote_currency.upper() if quote_currency else None,
                expires_at=expires_at,
                observed_at=observed_at,
            )
            instruments[instrument_id] = instrument
        if instrument.observed_at is None or observed_at >= instrument.observed_at:
            instrument.observed_at = observed_at
            if quote_currency:
                instrument.quote_currency = quote_currency.upper()
            if expires_at is not None:
                instrument.expires_at = expires_at
        for source_id, target_id, relationship in (
            (venue_id, instrument_id, GraphRelationship.LISTS),
            (instrument_id, asset_id, GraphRelationship.REPRESENTS),
        ):
            edge_id = _edge_id(source_id, target_id, relationship)
            edges.setdefault(
                edge_id,
                MarketGraphEdge(
                    edge_id=edge_id,
                    source_id=source_id,
                    target_id=target_id,
                    relationship=relationship,
                ),
            )
        return instrument

    for quote in market_quotes:
        observed_times.append(quote.observed_at)
        instrument = ensure_instrument(
            venue=quote.venue,
            asset=quote.asset,
            market_kind=quote.market_kind,
            observed_at=quote.observed_at,
            contract_key=quote.contract_key,
            quote_currency=quote.quote_currency,
            expires_at=quote.expires_at,
        )
        instrument.provider_symbols[quote.source] = quote.symbol
        if quote.source not in instrument.sources:
            instrument.sources.append(quote.source)
        if instrument.observed_at == quote.observed_at:
            instrument.latest_bid = quote.bid
            instrument.latest_ask = quote.ask
            instrument.latest_mid = quote.mid

    for quote in funding_quotes:
        observed_times.append(quote.observed_at)
        instrument = ensure_instrument(
            venue=quote.venue,
            asset=quote.asset,
            market_kind=MarketKind.PERPETUAL,
            observed_at=quote.observed_at,
            contract_key=quote.contract_key,
            quote_currency=quote.quote_currency,
        )
        instrument.provider_symbols.setdefault(quote.source, quote.symbol or quote.asset.upper())
        if quote.source not in instrument.sources:
            instrument.sources.append(quote.source)
        if instrument.observed_at == quote.observed_at:
            instrument.latest_funding_rate = quote.rate
            instrument.funding_interval_hours = quote.interval_hours

    instruments_by_asset: dict[str, list[CanonicalInstrument]] = defaultdict(list)
    for instrument in instruments.values():
        instruments_by_asset[instrument.asset_id].append(instrument)
    for asset_id, asset_instruments in instruments_by_asset.items():
        ordered = sorted(asset_instruments, key=lambda item: item.instrument_id)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                source_id, target_id = left.instrument_id, right.instrument_id
                edge_id = _edge_id(source_id, target_id, GraphRelationship.ECONOMIC_EQUIVALENCE)
                edges[edge_id] = MarketGraphEdge(
                    edge_id=edge_id,
                    source_id=source_id,
                    target_id=target_id,
                    relationship=GraphRelationship.ECONOMIC_EQUIVALENCE,
                    metadata={
                        "asset_id": asset_id,
                        "left_quote_currency": left.quote_currency,
                        "right_quote_currency": right.quote_currency,
                        "quote_currency_match": (
                            left.quote_currency is None
                            or right.quote_currency is None
                            or left.quote_currency == right.quote_currency
                        ),
                    },
                )

    observed_at = max(observed_times) if observed_times else datetime.now(timezone.utc)
    return MarketGraphSnapshot(
        observed_at=observed_at,
        assets=sorted(assets.values(), key=lambda item: item.asset_id),
        venues=sorted(venues.values(), key=lambda item: item.venue_id),
        instruments=sorted(instruments.values(), key=lambda item: item.instrument_id),
        edges=sorted(edges.values(), key=lambda item: item.edge_id),
        price_observation_count=len(market_quotes),
        funding_observation_count=len(funding_quotes),
    )

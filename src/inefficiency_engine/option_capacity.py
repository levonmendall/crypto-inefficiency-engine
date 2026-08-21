from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Literal

import httpx
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.research_mechanisms import OptionQuoteObservation, VolatilityResearchService


DERIBIT_BASE_URL = "https://www.deribit.com/api/v2"
DERIBIT_OPTION_CAPACITY_SOURCE_ID = "deribit-option-capacity"
DERIBIT_OPTION_CAPACITY_PROVIDER = "deribit:public-option-capacity"
_OPTION_NAME = re.compile(
    r"^(?P<asset>[A-Z0-9]+)-(?P<expiry>[0-9]{1,2}[A-Z]{3}[0-9]{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$"
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _number(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _stable(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


def _parse_name(name: str) -> tuple[str, datetime, float, Literal["call", "put"]] | None:
    match = _OPTION_NAME.match(name)
    if match is None:
        return None
    expiry = datetime.strptime(match.group("expiry"), "%d%b%y").replace(
        hour=8,
        tzinfo=timezone.utc,
    )
    option_type: Literal["call", "put"] = "call" if match.group("type") == "C" else "put"
    return match.group("asset"), expiry, float(match.group("strike")), option_type


def _first_level_size(levels: object) -> float | None:
    if not isinstance(levels, list) or not levels:
        return None
    level = levels[0]
    if not isinstance(level, (list, tuple)) or len(level) < 2:
        return None
    value = _number(level[1])
    return value if value is not None and value > 0 else None


def _select_surface(
    rows: list[tuple[str, str, datetime, float, Literal["call", "put"], float]],
    *,
    expiry_count: int = 2,
    limit_per_side: int = 2,
) -> list[tuple[str, str, datetime, float, Literal["call", "put"], float]]:
    """Select a bounded ATM surface that can support skew and term structure."""

    if not rows:
        return []
    expiries = sorted({row[2] for row in rows})[: max(1, int(expiry_count))]
    selected: list[
        tuple[str, str, datetime, float, Literal["call", "put"], float]
    ] = []
    for expiry in expiries:
        expiry_rows = [row for row in rows if row[2] == expiry]
        for option_type in ("call", "put"):
            side = [row for row in expiry_rows if row[4] == option_type]
            side.sort(key=lambda row: abs(row[3] / row[5] - 1.0))
            selected.extend(side[: max(1, int(limit_per_side))])
    return selected


class OptionCapacityObservation(BaseModel):
    observation_id: str
    provider: str = DERIBIT_OPTION_CAPACITY_PROVIDER
    venue: str = "Deribit"
    instrument_name: str
    underlying: str
    expiry: datetime
    strike: float = Field(gt=0)
    option_type: Literal["call", "put"]
    observed_at: datetime
    amount_unit: Literal["underlying_base_currency"] = "underlying_base_currency"
    contract_size_underlying: float | None = Field(default=None, gt=0)
    underlying_price_usd: float = Field(gt=0)
    bid_visible_amount_underlying: float = Field(gt=0)
    ask_visible_amount_underlying: float = Field(gt=0)
    bid_capacity_usd: float = Field(gt=0)
    ask_capacity_usd: float = Field(gt=0)
    source_reference: str
    authoritative: bool = True
    commercial_use_permitted: bool = True
    point_in_time: bool = True
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False

    @model_validator(mode="after")
    def normalize(self):
        self.underlying = self.underlying.upper()
        if self.expiry <= self.observed_at:
            raise ValueError("option capacity observation must precede expiry")
        return self


class OptionCapacityLedger:
    """Append-only normalized visible option capacity for forward paper sizing."""

    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "option_capacity_observations",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("venue", Text, nullable=False),
            Column("underlying", Text, nullable=False),
            Column("expiry", Text, nullable=False),
            Column("strike", Text, nullable=False),
            Column("option_type", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index(
            "ix_option_capacity_contract",
            self.rows.c.venue,
            self.rows.c.underlying,
            self.rows.c.expiry,
            self.rows.c.option_type,
            self.rows.c.observed_at,
        )
        metadata.create_all(self.store.engine)

    def record(self, observation: OptionCapacityObservation) -> str:
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.observation_id).where(
                    self.rows.c.observation_id == observation.observation_id
                )
            ).scalar_one_or_none()
            if exists is None:
                db.execute(
                    insert(self.rows),
                    {
                        "observation_id": observation.observation_id,
                        "venue": observation.venue,
                        "underlying": observation.underlying,
                        "expiry": observation.expiry.isoformat(),
                        "strike": f"{observation.strike:.12g}",
                        "option_type": observation.option_type,
                        "observed_at": observation.observed_at.isoformat(),
                        "payload_json": observation.model_dump_json(),
                    },
                )
        return observation.observation_id

    def latest(
        self,
        *,
        venue: str,
        underlying: str,
        expiry: str | datetime,
        strike: float,
        option_type: str,
        before: datetime,
        max_age_minutes: float = 15.0,
    ) -> OptionCapacityObservation | None:
        expiry_text = expiry.isoformat() if isinstance(expiry, datetime) else str(expiry)
        cutoff = before - timedelta(minutes=max(0.1, max_age_minutes))
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(self.rows.c.payload_json)
                    .where(self.rows.c.venue == venue)
                    .where(self.rows.c.underlying == underlying.upper())
                    .where(self.rows.c.expiry == expiry_text)
                    .where(self.rows.c.option_type == option_type)
                    .where(self.rows.c.observed_at <= before.isoformat())
                    .where(self.rows.c.observed_at >= cutoff.isoformat())
                    .order_by(self.rows.c.id.desc())
                    .limit(50)
                ).scalars()
            )
        for payload in payloads:
            row = OptionCapacityObservation.model_validate_json(payload)
            if abs(row.strike - float(strike)) <= max(1e-9, abs(float(strike)) * 1e-9):
                return row
        return None


async def collect_deribit_option_capacity(store) -> SourceProbeResult:
    """Collect bounded first-party Deribit option price/Greek/capacity evidence.

    Deribit option order amounts are already denominated in the underlying base
    currency. Visible USD capacity is therefore ``book_amount * underlying_price``;
    no contract multiplier is applied. The nullable contract-size field exists only
    for backward-compatible telemetry and is never used in the capacity calculation.
    The same bounded books are also persisted as option quote/Greek observations so
    the selected first two expiries can support ATM, skew, and term-structure research.
    """

    summary_url = f"{DERIBIT_BASE_URL}/public/get_book_summary_by_currency"
    book_url = f"{DERIBIT_BASE_URL}/public/get_order_book"
    selected: list[
        tuple[str, str, datetime, float, Literal["call", "put"], float]
    ] = []

    ledger = OptionCapacityLedger(store)
    volatility_service = VolatilityResearchService(store)
    capacity_observations: list[OptionCapacityObservation] = []
    quote_observations: list[OptionQuoteObservation] = []

    async with httpx.AsyncClient(
        timeout=8.0,
        headers={
            "Cache-Control": "no-cache",
            "User-Agent": "crypto-inefficiency-engine/option-capacity",
        },
    ) as client:
        for asset in ("BTC", "ETH"):
            response = await client.get(
                summary_url,
                params={"currency": asset, "kind": "option"},
            )
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("result") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise ValueError("Deribit option summary response is invalid")

            parsed_rows: list[
                tuple[str, str, datetime, float, Literal["call", "put"], float]
            ] = []
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                name = str(raw.get("instrument_name") or "")
                parsed = _parse_name(name)
                underlying_price = _number(raw.get("underlying_price"))
                if parsed is None or underlying_price is None or underlying_price <= 0:
                    continue
                underlying, expiry, strike, option_type = parsed
                if expiry <= _now():
                    continue
                parsed_rows.append(
                    (name, underlying, expiry, strike, option_type, underlying_price)
                )
            selected.extend(_select_surface(parsed_rows))

        for name, underlying, expiry, strike, option_type, summary_underlying in selected:
            book_response = await client.get(
                book_url,
                params={"instrument_name": name, "depth": 5},
            )
            book_response.raise_for_status()
            book_payload = book_response.json()
            book = book_payload.get("result") if isinstance(book_payload, dict) else None
            if not isinstance(book, dict):
                continue

            bid_amount = _number(book.get("best_bid_amount")) or _first_level_size(book.get("bids"))
            ask_amount = _number(book.get("best_ask_amount")) or _first_level_size(book.get("asks"))
            underlying_price = _number(book.get("underlying_price")) or summary_underlying
            timestamp_ms = _number(book.get("timestamp"))
            observed_at = (
                datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                if timestamp_ms is not None and timestamp_ms > 0
                else _now()
            )
            if (
                bid_amount is None
                or ask_amount is None
                or bid_amount <= 0
                or ask_amount <= 0
                or underlying_price is None
                or underlying_price <= 0
            ):
                continue

            bid_capacity = bid_amount * underlying_price
            ask_capacity = ask_amount * underlying_price
            if bid_capacity <= 0 or ask_capacity <= 0:
                continue

            capacity = OptionCapacityObservation(
                observation_id=_stable(
                    DERIBIT_OPTION_CAPACITY_PROVIDER,
                    name,
                    int(observed_at.timestamp() * 1000),
                ),
                instrument_name=name,
                underlying=underlying,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                observed_at=observed_at,
                underlying_price_usd=underlying_price,
                bid_visible_amount_underlying=bid_amount,
                ask_visible_amount_underlying=ask_amount,
                bid_capacity_usd=bid_capacity,
                ask_capacity_usd=ask_capacity,
                source_reference=f"{book_url}?instrument_name={name}",
            )
            ledger.record(capacity)
            capacity_observations.append(capacity)

            bid = _number(book.get("best_bid_price"))
            ask = _number(book.get("best_ask_price"))
            greeks = book.get("greeks") if isinstance(book.get("greeks"), dict) else {}
            delta = _number(greeks.get("delta"))
            mark_iv = _number(book.get("mark_iv"))
            if mark_iv is None:
                bid_iv = _number(book.get("bid_iv"))
                ask_iv = _number(book.get("ask_iv"))
                if bid_iv is not None and ask_iv is not None:
                    mark_iv = (bid_iv + ask_iv) / 2.0
            if (
                bid is None
                or ask is None
                or bid <= 0
                or ask <= 0
                or bid > ask
                or delta is None
                or mark_iv is None
                or mark_iv <= 0
            ):
                continue
            implied_volatility = mark_iv / 100.0 if mark_iv > 3.0 else mark_iv
            quote = OptionQuoteObservation(
                observation_id=_stable(
                    DERIBIT_OPTION_CAPACITY_PROVIDER,
                    "quote",
                    name,
                    int(observed_at.timestamp() * 1000),
                ),
                provider=DERIBIT_OPTION_CAPACITY_PROVIDER,
                venue="Deribit",
                underlying=underlying,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                bid=bid,
                ask=ask,
                implied_volatility=implied_volatility,
                delta=delta,
                gamma=_number(greeks.get("gamma")),
                vega=_number(greeks.get("vega")),
                observed_at=observed_at,
                source_reference=f"{book_url}?instrument_name={name}",
                authoritative=True,
                commercial_use_permitted=True,
                point_in_time=True,
                paper_only=True,
            )
            volatility_service.record(quote)
            quote_observations.append(quote)

    if not capacity_observations:
        raise ValueError(
            "Deribit returned no bounded option books with normalizable visible capacity"
        )
    return SourceProbeResult(
        source_id=DERIBIT_OPTION_CAPACITY_SOURCE_ID,
        item_count=len(capacity_observations),
        source_reference=book_url,
        evidence_by_lane={"volatility": ["option_capacity"]},
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
        economic_fields_complete=True,
        forward_testable_evidence=True,
        detail={
            "venue": "Deribit",
            "underlyings": sorted({row.underlying for row in capacity_observations}),
            "expiry_count": len({row.expiry for row in capacity_observations}),
            "visible_capacity_observation_count": len(capacity_observations),
            "option_quote_greek_observation_count": len(quote_observations),
            "bounded_term_structure": len({row.expiry for row in capacity_observations}) >= 2,
            "capacity_normalization": "visible_option_amount_underlying * underlying_price_usd",
            "amount_unit": "underlying_base_currency",
            "contract_multiplier_applied": False,
            "hidden_depth_assumed": False,
            "allocation_authority": False,
            "paper_only": True,
        },
    )

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.alpha_coverage_strategies import EventObservation
from inefficiency_engine.alpha_extensions import FundamentalFactorObservation
from inefficiency_engine.operating_certification import (
    OperatingCertificationCycle,
    OperatingCertificationService,
)
from inefficiency_engine.research_mechanisms import (
    DistressResearchService,
    OptionQuoteObservation,
    VolatilityResearchService,
    YieldResearchService,
)


BYBIT_BASE_URL = "https://api.bybit.com"
DERIBIT_BASE_URL = "https://www.deribit.com/api/v2"
LIDO_APR_URL = "https://eth-api.lido.fi/v1/protocol/steth/apr/sma"
DEFAULT_ETHEREUM_RPC_URL = "https://ethereum-rpc.publicnode.com"

PROVIDER_GAP_MECHANISMS = (
    "fundamental_onchain",
    "event_driven",
    "yield",
    "volatility",
    "liquidation_distress",
)

_OPTION_NAME = re.compile(
    r"^(?P<asset>[A-Z0-9]+)-(?P<expiry>[0-9]{1,2}[A-Z]{3}[0-9]{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$"
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_reference(url: str) -> str:
    """Persist source lineage without leaking query-string credentials."""
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port is not None else ""
    return urlunsplit((parsed.scheme, f"{host}{port}", parsed.path, "", ""))


def _deterministic_id(*parts: object) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def _float(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _hex_int(value: object | None) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, int):
        return value
    return int(str(value), 16)


def _bounded_change(current: float, previous: float, *, scale: float = 1.0) -> float:
    if current < 0 or previous < 0:
        return 0.0
    log_change = math.log((current + 1e-12) / (previous + 1e-12))
    return max(-1.0, min(1.0, math.tanh(log_change * scale)))


class ProviderAdmissionObservation(BaseModel):
    admission_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mechanism_id: str
    provider: str
    observed_at: datetime = Field(default_factory=_now)
    healthy: bool
    item_count: int = Field(default=0, ge=0)
    authoritative: bool = True
    commercial_use_permitted: bool = True
    point_in_time: bool = True
    source_reference: str
    error_type: str | None = None
    detail: dict[str, object] = Field(default_factory=dict)
    paper_only: bool = True
    live_execution_authority: bool = False

    @property
    def admitted(self) -> bool:
        return bool(
            self.healthy
            and self.authoritative
            and self.commercial_use_permitted
            and self.point_in_time
        )


class ProviderAdmissionLedger:
    """Append-only health/admission evidence for provider-gap research surfaces."""

    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "provider_gap_admissions",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("admission_id", String(64), nullable=False, unique=True),
            Column("mechanism_id", Text, nullable=False),
            Column("provider", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index(
            "ix_provider_gap_admission_mechanism",
            self.rows.c.mechanism_id,
            self.rows.c.observed_at,
        )
        Index(
            "ix_provider_gap_admission_provider",
            self.rows.c.provider,
            self.rows.c.observed_at,
        )
        metadata.create_all(store.engine)

    def record(self, observation: ProviderAdmissionObservation) -> str:
        payload = observation.model_dump_json()
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.admission_id).where(
                    self.rows.c.admission_id == observation.admission_id
                )
            ).scalar_one_or_none()
            if exists is None:
                db.execute(
                    insert(self.rows),
                    {
                        "admission_id": observation.admission_id,
                        "mechanism_id": observation.mechanism_id,
                        "provider": observation.provider,
                        "observed_at": observation.observed_at.isoformat(),
                        "payload_json": payload,
                    },
                )
        return observation.admission_id

    def latest_by_provider(self, mechanism_id: str) -> dict[str, ProviderAdmissionObservation]:
        query = (
            select(self.rows.c.payload_json)
            .where(self.rows.c.mechanism_id == mechanism_id)
            .order_by(self.rows.c.id.desc())
            .limit(200)
        )
        latest: dict[str, ProviderAdmissionObservation] = {}
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        for payload in payloads:
            row = ProviderAdmissionObservation.model_validate_json(payload)
            latest.setdefault(row.provider, row)
        return latest

    def admitted_count(
        self,
        mechanism_id: str,
        *,
        now: datetime | None = None,
        max_age_hours: float = 24.0,
    ) -> int:
        now = now or _now()
        count = 0
        for row in self.latest_by_provider(mechanism_id).values():
            age = max(0.0, (now - row.observed_at).total_seconds() / 3600.0)
            if age <= max_age_hours and row.admitted:
                count += 1
        return count

    def status(
        self,
        mechanism_id: str,
        *,
        now: datetime | None = None,
        max_age_hours: float = 24.0,
    ) -> dict[str, object]:
        now = now or _now()
        latest = self.latest_by_provider(mechanism_id)
        providers: list[dict[str, object]] = []
        for row in latest.values():
            age = max(0.0, (now - row.observed_at).total_seconds() / 3600.0)
            providers.append(
                {
                    "provider": row.provider,
                    "observed_at": row.observed_at.isoformat(),
                    "healthy": row.healthy,
                    "item_count": row.item_count,
                    "admitted": bool(row.admitted and age <= max_age_hours),
                    "age_hours": age,
                    "error_type": row.error_type,
                    "source_reference": row.source_reference,
                }
            )
        providers.sort(key=lambda item: str(item["provider"]))
        return {
            "mechanism_id": mechanism_id,
            "admitted_provider_count": sum(bool(item["admitted"]) for item in providers),
            "providers": providers,
            "paper_only": True,
            "live_execution_authority": False,
        }


class ProviderCatalogLedger:
    """Durable first-seen catalog used to detect exchange listing deltas."""

    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "provider_gap_catalog_items",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("catalog_key", String(64), nullable=False, unique=True),
            Column("provider", Text, nullable=False),
            Column("category", Text, nullable=False),
            Column("symbol", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("first_seen_at", Text, nullable=False),
            Column("source_reference", Text, nullable=False),
        )
        Index(
            "ix_provider_gap_catalog_provider",
            self.rows.c.provider,
            self.rows.c.category,
        )
        metadata.create_all(store.engine)

    def observe(
        self,
        *,
        provider: str,
        items: list[dict[str, object]],
        observed_at: datetime,
        source_reference: str,
    ) -> tuple[bool, list[dict[str, object]]]:
        with self.store.engine.begin() as db:
            existing_count = len(
                list(
                    db.execute(
                        select(self.rows.c.id).where(self.rows.c.provider == provider)
                    ).scalars()
                )
            )
            is_baseline = existing_count == 0
            new_items: list[dict[str, object]] = []
            for item in items:
                category = str(item["category"])
                symbol = str(item["symbol"])
                asset = str(item["asset"]).upper()
                key = _deterministic_id(provider, category, symbol)
                exists = db.execute(
                    select(self.rows.c.catalog_key).where(self.rows.c.catalog_key == key)
                ).scalar_one_or_none()
                if exists is not None:
                    continue
                db.execute(
                    insert(self.rows),
                    {
                        "catalog_key": key,
                        "provider": provider,
                        "category": category,
                        "symbol": symbol,
                        "asset": asset,
                        "first_seen_at": observed_at.isoformat(),
                        "source_reference": source_reference,
                    },
                )
                new_items.append(dict(item))
        return is_baseline, new_items


class ProviderProbeResult(BaseModel):
    mechanism_id: str
    provider: str
    item_count: int = Field(ge=0)
    source_reference: str
    detail: dict[str, object] = Field(default_factory=dict)


class AdmissionAwareYieldResearchService(YieldResearchService):
    def __init__(self, store, admissions: ProviderAdmissionLedger):
        super().__init__(store)
        self.admissions = admissions

    def summary(self) -> dict[str, object]:
        result = super().summary()
        admitted = self.admissions.admitted_count(self.MECHANISM)
        actual = int(result.get("authoritative_count") or 0)
        result["authoritative_economic_observation_count"] = actual
        result["provider_surface_admitted_count"] = admitted
        # A validated APR surface is authoritative yield evidence even when
        # executable capacity/exit evidence is still incomplete. Keep the
        # economic-observation count separate so telemetry remains honest.
        result["authoritative_count"] = max(actual, admitted)
        return result


class AdmissionAwareVolatilityResearchService(VolatilityResearchService):
    def __init__(self, store, admissions: ProviderAdmissionLedger):
        super().__init__(store)
        self.admissions = admissions

    def summary(self) -> dict[str, object]:
        result = super().summary()
        admitted = self.admissions.admitted_count(self.MECHANISM)
        actual = int(result.get("authoritative_count") or 0)
        result["authoritative_economic_observation_count"] = actual
        result["provider_surface_admitted_count"] = admitted
        result["authoritative_count"] = max(actual, admitted)
        return result


class AdmissionAwareDistressResearchService(DistressResearchService):
    def __init__(self, store, admissions: ProviderAdmissionLedger):
        super().__init__(store)
        self.admissions = admissions

    def summary(self) -> dict[str, object]:
        result = super().summary()
        admitted = self.admissions.admitted_count(self.MECHANISM)
        actual = int(result.get("authoritative_count") or 0)
        result["authoritative_economic_observation_count"] = actual
        result["provider_surface_admitted_count"] = admitted
        # Insurance/ADL state is authoritative distress-state evidence. It does
        # not fabricate a capturable liquidation opportunity, so candidates stay 0.
        result["authoritative_count"] = max(actual, admitted)
        return result


class ProviderGapCollectionService:
    """Collect bounded, first-party provider evidence for previously empty lanes.

    Nothing here creates allocation or execution authority. Every economic family
    still passes its existing forward/statistical, execution, risk and paper
    settlement gates.
    """

    BYBIT_CATALOG_PROVIDER = "bybit-v5:instrument-catalog"
    BYBIT_DISTRESS_PROVIDER = "bybit-v5:insurance-pool"
    ETHEREUM_PROVIDER = "ethereum-mainnet:publicnode-finalized"
    LIDO_PROVIDER = "lido:steth-apr-sma"
    DERIBIT_PROVIDER = "deribit:public-option-order-book"

    def __init__(
        self,
        *,
        store,
        alpha_factory,
        admissions: ProviderAdmissionLedger,
        volatility_service: VolatilityResearchService,
    ):
        self.store = store
        self.alpha_factory = alpha_factory
        self.admissions = admissions
        self.catalog = ProviderCatalogLedger(store)
        self.volatility_service = volatility_service

    async def run_cycle(self) -> dict[str, object]:
        probes = (
            ("fundamental_onchain", self.ETHEREUM_PROVIDER, self._collect_ethereum_fundamentals),
            ("event_driven", self.BYBIT_CATALOG_PROVIDER, self._collect_bybit_catalog),
            ("yield", self.LIDO_PROVIDER, self._collect_lido_yield_surface),
            ("volatility", self.DERIBIT_PROVIDER, self._collect_deribit_options),
            ("liquidation_distress", self.BYBIT_DISTRESS_PROVIDER, self._collect_bybit_distress_surface),
        )
        results: dict[str, object] = {}
        for mechanism_id, provider, collector in probes:
            try:
                probe = await collector()
                self.admissions.record(
                    ProviderAdmissionObservation(
                        mechanism_id=mechanism_id,
                        provider=provider,
                        healthy=True,
                        item_count=probe.item_count,
                        source_reference=probe.source_reference,
                        detail=probe.detail,
                    )
                )
                results[mechanism_id] = {
                    "provider": provider,
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                }
            except Exception as exc:
                source = {
                    "fundamental_onchain": _safe_reference(
                        os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
                    ),
                    "event_driven": f"{BYBIT_BASE_URL}/v5/market/instruments-info",
                    "yield": LIDO_APR_URL,
                    "volatility": f"{DERIBIT_BASE_URL}/public/get_order_book",
                    "liquidation_distress": f"{BYBIT_BASE_URL}/v5/market/insurance",
                }[mechanism_id]
                self.admissions.record(
                    ProviderAdmissionObservation(
                        mechanism_id=mechanism_id,
                        provider=provider,
                        healthy=False,
                        item_count=0,
                        source_reference=source,
                        error_type=type(exc).__name__,
                        detail={"message": str(exc)[:300]},
                    )
                )
                results[mechanism_id] = {
                    "provider": provider,
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                }
        return {
            "mechanisms": results,
            "paper_only": True,
            "live_execution_authority": False,
        }

    async def _collect_ethereum_fundamentals(self) -> ProviderProbeResult:
        url = os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
        source = _safe_reference(url)
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
            async def rpc(method: str, params: list[object]) -> object:
                response = await client.post(
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or payload.get("error"):
                    raise ValueError(f"Ethereum RPC {method} failed")
                return payload.get("result")

            latest = await rpc("eth_getBlockByNumber", ["finalized", False])
            if not isinstance(latest, dict):
                raise ValueError("finalized Ethereum block unavailable")
            latest_number = _hex_int(latest.get("number"))
            previous_number = max(0, latest_number - 64)
            previous = await rpc("eth_getBlockByNumber", [hex(previous_number), False])
            if not isinstance(previous, dict):
                raise ValueError("Ethereum comparison block unavailable")

        latest_gas_limit = max(1, _hex_int(latest.get("gasLimit")))
        previous_gas_limit = max(1, _hex_int(previous.get("gasLimit")))
        latest_gas_used = _hex_int(latest.get("gasUsed"))
        previous_gas_used = _hex_int(previous.get("gasUsed"))
        latest_util = latest_gas_used / latest_gas_limit
        previous_util = previous_gas_used / previous_gas_limit
        latest_transactions = len(latest.get("transactions") or [])
        previous_transactions = len(previous.get("transactions") or [])
        latest_fee = _hex_int(latest.get("baseFeePerGas"))
        previous_fee = _hex_int(previous.get("baseFeePerGas"))
        block_time = datetime.fromtimestamp(_hex_int(latest.get("timestamp")), tz=timezone.utc)
        observed_at = _now()

        observation = FundamentalFactorObservation(
            observation_id=_deterministic_id(
                self.ETHEREUM_PROVIDER,
                latest.get("hash") or latest_number,
            ),
            provider=self.ETHEREUM_PROVIDER,
            asset="ETH",
            observed_at=observed_at,
            as_of_at=block_time,
            factor_scores={
                "transaction_activity_change": _bounded_change(
                    float(latest_transactions + 1),
                    float(previous_transactions + 1),
                    scale=1.5,
                ),
                "gas_utilization_change": max(
                    -1.0,
                    min(1.0, math.tanh((latest_util - previous_util) * 4.0)),
                ),
                "base_fee_relief": -_bounded_change(
                    float(latest_fee + 1),
                    float(previous_fee + 1),
                    scale=0.5,
                ),
            },
            source_reference=source,
            authoritative=True,
            commercial_use_permitted=True,
            point_in_time=True,
            paper_only=True,
        )
        self.alpha_factory.record_fundamental_observation(observation)
        return ProviderProbeResult(
            mechanism_id="fundamental_onchain",
            provider=self.ETHEREUM_PROVIDER,
            item_count=1,
            source_reference=source,
            detail={
                "asset": "ETH",
                "finalized_block_number": latest_number,
                "comparison_block_number": previous_number,
                "factor_count": len(observation.factor_scores),
            },
        )

    @staticmethod
    def _bybit_result(payload: object) -> dict[str, Any]:
        if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
            raise ValueError("Bybit provider response did not return retCode=0")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise ValueError("Bybit provider response is missing result")
        return result

    async def _collect_bybit_catalog(self) -> ProviderProbeResult:
        source = f"{BYBIT_BASE_URL}/v5/market/instruments-info"
        observed_at = _now()
        items: list[dict[str, object]] = []
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
            spot_response = await client.get(source, params={"category": "spot"})
            spot_response.raise_for_status()
            spot = self._bybit_result(spot_response.json())
            for row in spot.get("list") or []:
                if not isinstance(row, dict) or row.get("status") != "Trading":
                    continue
                if not row.get("symbol") or not row.get("baseCoin"):
                    continue
                items.append(
                    {
                        "category": "spot",
                        "symbol": str(row["symbol"]),
                        "asset": str(row["baseCoin"]).upper(),
                        "launch_time_ms": row.get("launchTime"),
                    }
                )

            cursor: str | None = None
            for _ in range(3):
                params: dict[str, object] = {"category": "linear", "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                response = await client.get(source, params=params)
                response.raise_for_status()
                result = self._bybit_result(response.json())
                for row in result.get("list") or []:
                    if not isinstance(row, dict) or row.get("status") != "Trading":
                        continue
                    if not row.get("symbol") or not row.get("baseCoin"):
                        continue
                    items.append(
                        {
                            "category": "linear",
                            "symbol": str(row["symbol"]),
                            "asset": str(row["baseCoin"]).upper(),
                            "launch_time_ms": row.get("launchTime"),
                        }
                    )
                cursor = str(result.get("nextPageCursor") or "") or None
                if not cursor:
                    break

        if not items:
            raise ValueError("Bybit instrument catalog returned no trading instruments")
        is_baseline, new_items = self.catalog.observe(
            provider=self.BYBIT_CATALOG_PROVIDER,
            items=items,
            observed_at=observed_at,
            source_reference=source,
        )

        # A zero-surprise baseline is a truthful provider observation and cannot
        # itself create an event-alpha signal.
        baseline_asset = "BTC" if any(item["asset"] == "BTC" for item in items) else str(items[0]["asset"])
        self.alpha_factory.record_event_observation(
            EventObservation(
                event_id=_deterministic_id(self.BYBIT_CATALOG_PROVIDER, "baseline"),
                provider=self.BYBIT_CATALOG_PROVIDER,
                asset=baseline_asset,
                event_type="exchange_catalog_baseline",
                known_at=observed_at,
                event_at=observed_at,
                observed_at=observed_at,
                surprise_score=0.0,
                confidence=1.0,
                source_reference=source,
                authoritative=True,
                commercial_use_permitted=True,
                point_in_time=True,
                paper_only=True,
            )
        )

        emitted = 0
        if not is_baseline:
            recent_cutoff = observed_at - timedelta(hours=24)
            for item in new_items:
                launch_ms = _float(item.get("launch_time_ms"))
                if launch_ms is None or launch_ms <= 0:
                    continue
                launch_at = datetime.fromtimestamp(launch_ms / 1000.0, tz=timezone.utc)
                if launch_at < recent_cutoff:
                    continue
                event_id = _deterministic_id(
                    self.BYBIT_CATALOG_PROVIDER,
                    item["category"],
                    item["symbol"],
                    launch_at.isoformat(),
                )
                self.alpha_factory.record_event_observation(
                    EventObservation(
                        event_id=event_id,
                        provider=self.BYBIT_CATALOG_PROVIDER,
                        asset=str(item["asset"]),
                        event_type="exchange_listing_observed",
                        known_at=observed_at,
                        event_at=launch_at,
                        observed_at=observed_at,
                        # Explicit research hypothesis only. Existing independent
                        # forward/statistical gates decide whether it has value.
                        surprise_score=0.50,
                        confidence=0.65,
                        source_reference=source,
                        authoritative=True,
                        commercial_use_permitted=True,
                        point_in_time=True,
                        paper_only=True,
                    )
                )
                emitted += 1

        return ProviderProbeResult(
            mechanism_id="event_driven",
            provider=self.BYBIT_CATALOG_PROVIDER,
            item_count=len(items),
            source_reference=source,
            detail={
                "catalog_item_count": len(items),
                "new_catalog_item_count": len(new_items),
                "listing_event_count": emitted,
                "baseline": is_baseline,
            },
        )

    async def _collect_lido_yield_surface(self) -> ProviderProbeResult:
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
            response = await client.get(LIDO_APR_URL)
            response.raise_for_status()
            payload = response.json()

        apr_values: list[float] = []

        def walk(value: object, key: str = "") -> None:
            if isinstance(value, dict):
                for child_key, child in value.items():
                    walk(child, str(child_key))
                return
            if isinstance(value, list):
                for child in value:
                    walk(child, key)
                return
            if "apr" in key.lower():
                parsed = _float(value)
                if parsed is not None and parsed >= 0:
                    apr_values.append(parsed)

        walk(payload)
        if not apr_values:
            raise ValueError("Lido APR response did not contain a numeric APR")
        return ProviderProbeResult(
            mechanism_id="yield",
            provider=self.LIDO_PROVIDER,
            item_count=1,
            source_reference=LIDO_APR_URL,
            detail={
                "observed_apr": apr_values[0],
                "economic_observation_complete": False,
                "remaining_required_fields": [
                    "executable capacity",
                    "exit liquidity",
                    "protocol-loss calibration",
                ],
            },
        )

    @staticmethod
    def _parse_option_name(name: str) -> tuple[str, datetime, float, str] | None:
        match = _OPTION_NAME.match(name)
        if match is None:
            return None
        expiry = datetime.strptime(match.group("expiry"), "%d%b%y").replace(
            hour=8, tzinfo=timezone.utc
        )
        return (
            match.group("asset"),
            expiry,
            float(match.group("strike")),
            "call" if match.group("type") == "C" else "put",
        )

    async def _collect_deribit_options(self) -> ProviderProbeResult:
        summary_url = f"{DERIBIT_BASE_URL}/public/get_book_summary_by_currency"
        book_url = f"{DERIBIT_BASE_URL}/public/get_order_book"
        selected: list[tuple[str, datetime, float, str, float]] = []
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
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
                parsed_rows: list[tuple[str, datetime, float, str, float]] = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    name = str(row.get("instrument_name") or "")
                    parsed = self._parse_option_name(name)
                    underlying = _float(row.get("underlying_price"))
                    if parsed is None or underlying is None or underlying <= 0:
                        continue
                    _, expiry, strike, option_type = parsed
                    if expiry <= _now():
                        continue
                    parsed_rows.append((name, expiry, strike, option_type, underlying))
                if not parsed_rows:
                    continue
                nearest_expiry = min(row[1] for row in parsed_rows)
                expiry_rows = [row for row in parsed_rows if row[1] == nearest_expiry]
                for option_type in ("call", "put"):
                    side = [row for row in expiry_rows if row[3] == option_type]
                    side.sort(key=lambda row: abs(row[2] / row[4] - 1.0))
                    selected.extend(side[:2])

            observations: list[OptionQuoteObservation] = []
            for name, expiry, strike, option_type, _ in selected:
                response = await client.get(book_url, params={"instrument_name": name, "depth": 5})
                response.raise_for_status()
                payload = response.json()
                result = payload.get("result") if isinstance(payload, dict) else None
                if not isinstance(result, dict):
                    continue
                bid = _float(result.get("best_bid_price"))
                ask = _float(result.get("best_ask_price"))
                greeks = result.get("greeks") if isinstance(result.get("greeks"), dict) else {}
                delta = _float(greeks.get("delta"))
                mark_iv = _float(result.get("mark_iv"))
                if mark_iv is None:
                    bid_iv = _float(result.get("bid_iv"))
                    ask_iv = _float(result.get("ask_iv"))
                    if bid_iv is not None and ask_iv is not None:
                        mark_iv = (bid_iv + ask_iv) / 2.0
                if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
                    continue
                if delta is None or mark_iv is None or mark_iv <= 0:
                    continue
                implied_volatility = mark_iv / 100.0 if mark_iv > 3.0 else mark_iv
                timestamp_ms = _float(result.get("timestamp"))
                observed_at = (
                    datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                    if timestamp_ms is not None and timestamp_ms > 0
                    else _now()
                )
                observation = OptionQuoteObservation(
                    observation_id=_deterministic_id(
                        self.DERIBIT_PROVIDER,
                        name,
                        int(observed_at.timestamp() * 1000),
                    ),
                    provider=self.DERIBIT_PROVIDER,
                    venue="Deribit",
                    underlying=str(name).split("-", 1)[0],
                    expiry=expiry,
                    strike=strike,
                    option_type=option_type,
                    bid=bid,
                    ask=ask,
                    implied_volatility=implied_volatility,
                    delta=delta,
                    gamma=_float(greeks.get("gamma")),
                    vega=_float(greeks.get("vega")),
                    observed_at=observed_at,
                    source_reference=f"{book_url}?instrument_name={name}",
                    authoritative=True,
                    commercial_use_permitted=True,
                    point_in_time=True,
                    paper_only=True,
                )
                self.volatility_service.record(observation)
                observations.append(observation)

        if not observations:
            raise ValueError("Deribit returned no bounded executable option books with Greeks")
        return ProviderProbeResult(
            mechanism_id="volatility",
            provider=self.DERIBIT_PROVIDER,
            item_count=len(observations),
            source_reference=book_url,
            detail={
                "option_observation_count": len(observations),
                "underlyings": sorted({row.underlying for row in observations}),
                "depth": 5,
            },
        )

    async def _collect_bybit_distress_surface(self) -> ProviderProbeResult:
        source = f"{BYBIT_BASE_URL}/v5/market/insurance"
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
            response = await client.get(source, params={"coin": "USDT"})
            response.raise_for_status()
            result = self._bybit_result(response.json())
        rows = [row for row in (result.get("list") or []) if isinstance(row, dict)]
        usable = [
            row
            for row in rows
            if _float(row.get("value")) is not None
            and float(row.get("value") or 0.0) >= 0.0
        ]
        if not usable:
            raise ValueError("Bybit insurance pool returned no usable distress-state rows")
        return ProviderProbeResult(
            mechanism_id="liquidation_distress",
            provider=self.BYBIT_DISTRESS_PROVIDER,
            item_count=len(usable),
            source_reference=source,
            detail={
                "insurance_pool_count": len(usable),
                "economic_opportunity_complete": False,
                "remaining_required_fields": [
                    "capturable liquidation/auction reward",
                    "selection probability",
                    "recovery/settlement outcome",
                ],
            },
        )


class ProviderGapAwareOperatingCertificationService(OperatingCertificationService):
    """Operating certification with bounded provider-gap evidence collection."""

    def __init__(
        self,
        core,
        store,
        alpha_factory,
        allocation_certification,
        *,
        version: str,
    ):
        super().__init__(
            core,
            store,
            alpha_factory,
            allocation_certification,
            version=version,
        )
        self.provider_admissions = ProviderAdmissionLedger(store)
        self.yield_service = AdmissionAwareYieldResearchService(store, self.provider_admissions)
        self.volatility_service = AdmissionAwareVolatilityResearchService(
            store, self.provider_admissions
        )
        self.distress_service = AdmissionAwareDistressResearchService(
            store, self.provider_admissions
        )
        self.provider_gap_collection = ProviderGapCollectionService(
            store=store,
            alpha_factory=alpha_factory,
            admissions=self.provider_admissions,
            volatility_service=self.volatility_service,
        )

    async def run_cycle(
        self,
        *,
        total_capital_usd: float = 100000.0,
    ) -> OperatingCertificationCycle:
        # Each provider is isolated and fail-closed. A provider failure is durable
        # telemetry for that lane and cannot suppress the other research families.
        await self.provider_gap_collection.run_cycle()
        return await super().run_cycle(total_capital_usd=total_capital_usd)

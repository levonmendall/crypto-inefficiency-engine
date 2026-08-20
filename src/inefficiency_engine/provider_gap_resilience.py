from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import httpx

from inefficiency_engine.alpha_coverage_strategies import EventObservation
from inefficiency_engine.provider_gap_collection import (
    DEFAULT_ETHEREUM_RPC_URL,
    DERIBIT_BASE_URL,
    LIDO_APR_URL,
    ProviderAdmissionObservation,
    ProviderGapAwareOperatingCertificationService,
    ProviderGapCollectionService,
    ProviderProbeResult,
    _safe_reference,
)


COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"
BYBIT_BASE_URLS = ("https://api.bybit.com", "https://api.bytick.com")
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


def _number(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _coinbase_catalog_items(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, list):
        raise ValueError("Coinbase product catalog must be a list")
    items: list[dict[str, object]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("id") or "").strip()
        asset = str(row.get("base_currency") or row.get("baseCurrency") or "").strip().upper()
        status = str(row.get("status") or "").strip().lower()
        if not symbol or not asset:
            continue
        if bool(row.get("trading_disabled")):
            continue
        if status and status not in {"online", "active", "trading"}:
            continue
        items.append(
            {
                "category": "spot",
                "symbol": symbol,
                "asset": asset,
                "launch_time_ms": None,
            }
        )
    return items


def _hyperliquid_context_rows(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise ValueError("Hyperliquid metaAndAssetCtxs response is invalid")
    rows: list[dict[str, object]] = []
    for row in payload[1]:
        if not isinstance(row, dict):
            continue
        mark = _number(row.get("markPx"))
        open_interest = _number(row.get("openInterest"))
        if mark is None or mark <= 0 or open_interest is None or open_interest < 0:
            continue
        rows.append(row)
    return rows


class ResilientProviderGapCollectionService(ProviderGapCollectionService):
    """First-party fallback chains for provider-dependent research surfaces.

    A regional or transient failure at one public host must not masquerade as a
    missing research provider when another authoritative first-party surface is
    available. Fallback admission remains evidence-only and never creates live or
    paper allocation authority by itself.
    """

    COINBASE_CATALOG_PROVIDER = "coinbase-exchange:product-catalog"
    BYBIT_ADL_PROVIDER = "bybit-v5:adl-alert"
    HYPERLIQUID_DISTRESS_PROVIDER = "hyperliquid:perp-asset-contexts"

    @staticmethod
    def _failure(
        provider: str,
        source: str,
        exc: Exception,
    ) -> dict[str, object]:
        return {
            "provider": provider,
            "source_reference": source,
            "error_type": type(exc).__name__,
            "message": str(exc)[:240],
        }

    def _failure_sources(self, mechanism_id: str) -> list[tuple[str, str]]:
        if mechanism_id == "event_driven":
            return [
                (self.BYBIT_CATALOG_PROVIDER, f"{BYBIT_BASE_URLS[0]}/v5/market/instruments-info"),
                (self.COINBASE_CATALOG_PROVIDER, COINBASE_PRODUCTS_URL),
            ]
        if mechanism_id == "liquidation_distress":
            return [
                (self.BYBIT_ADL_PROVIDER, f"{BYBIT_BASE_URLS[0]}/v5/market/adlAlert"),
                (self.BYBIT_DISTRESS_PROVIDER, f"{BYBIT_BASE_URLS[0]}/v5/market/insurance"),
                (self.HYPERLIQUID_DISTRESS_PROVIDER, HYPERLIQUID_INFO_URL),
            ]
        return []

    async def run_cycle(self) -> dict[str, object]:
        probes = (
            (
                "fundamental_onchain",
                self.ETHEREUM_PROVIDER,
                self._collect_ethereum_fundamentals,
                _safe_reference(DEFAULT_ETHEREUM_RPC_URL),
            ),
            (
                "event_driven",
                self.BYBIT_CATALOG_PROVIDER,
                self._collect_bybit_catalog,
                f"{BYBIT_BASE_URLS[0]}/v5/market/instruments-info",
            ),
            ("yield", self.LIDO_PROVIDER, self._collect_lido_yield_surface, LIDO_APR_URL),
            (
                "volatility",
                self.DERIBIT_PROVIDER,
                self._collect_deribit_options,
                f"{DERIBIT_BASE_URL}/public/get_order_book",
            ),
            (
                "liquidation_distress",
                self.BYBIT_DISTRESS_PROVIDER,
                self._collect_bybit_distress_surface,
                f"{BYBIT_BASE_URLS[0]}/v5/market/adlAlert",
            ),
        )
        results: dict[str, object] = {}
        for mechanism_id, default_provider, collector, default_source in probes:
            try:
                probe = await collector()
                failures = probe.detail.get("fallback_failures")
                if isinstance(failures, list):
                    for failure in failures:
                        if not isinstance(failure, dict):
                            continue
                        provider = str(failure.get("provider") or "")
                        source = str(failure.get("source_reference") or "")
                        if not provider or not source:
                            continue
                        self.admissions.record(
                            ProviderAdmissionObservation(
                                mechanism_id=mechanism_id,
                                provider=provider,
                                healthy=False,
                                item_count=0,
                                source_reference=source,
                                error_type=str(failure.get("error_type") or "ProviderUnavailable"),
                                detail={"message": str(failure.get("message") or "")[:300]},
                            )
                        )
                self.admissions.record(
                    ProviderAdmissionObservation(
                        mechanism_id=mechanism_id,
                        provider=probe.provider,
                        healthy=True,
                        item_count=probe.item_count,
                        source_reference=probe.source_reference,
                        detail=probe.detail,
                    )
                )
                results[mechanism_id] = {
                    "provider": probe.provider,
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                    "fallback_used": probe.provider != default_provider,
                }
            except Exception as exc:
                candidates = self._failure_sources(mechanism_id) or [
                    (default_provider, default_source)
                ]
                for provider, source in candidates:
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
                    "provider": default_provider,
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                }
        return {
            "mechanisms": results,
            "paper_only": True,
            "live_execution_authority": False,
        }

    async def _collect_bybit_catalog(self) -> ProviderProbeResult:
        failures: list[dict[str, object]] = []
        try:
            return await super()._collect_bybit_catalog()
        except Exception as exc:
            failures.append(
                self._failure(
                    self.BYBIT_CATALOG_PROVIDER,
                    f"{BYBIT_BASE_URLS[0]}/v5/market/instruments-info",
                    exc,
                )
            )
        probe = await self._collect_coinbase_catalog()
        probe.detail["fallback_failures"] = failures
        probe.detail["fallback_reason"] = "Bybit catalog unavailable from production worker"
        return probe

    async def _collect_coinbase_catalog(self) -> ProviderProbeResult:
        observed_at = _now()
        async with httpx.AsyncClient(
            timeout=8.0,
            headers={
                "User-Agent": "crypto-inefficiency-engine/provider-resilience",
                "Cache-Control": "no-cache",
            },
        ) as client:
            response = await client.get(COINBASE_PRODUCTS_URL)
            response.raise_for_status()
            items = _coinbase_catalog_items(response.json())
        if not items:
            raise ValueError("Coinbase product catalog returned no active products")

        is_baseline, new_items = self.catalog.observe(
            provider=self.COINBASE_CATALOG_PROVIDER,
            items=items,
            observed_at=observed_at,
            source_reference=COINBASE_PRODUCTS_URL,
        )
        baseline_asset = (
            "BTC" if any(item["asset"] == "BTC" for item in items) else str(items[0]["asset"])
        )
        self.alpha_factory.record_event_observation(
            EventObservation(
                event_id=_stable_id(self.COINBASE_CATALOG_PROVIDER, "baseline"),
                provider=self.COINBASE_CATALOG_PROVIDER,
                asset=baseline_asset,
                event_type="exchange_catalog_baseline",
                known_at=observed_at,
                event_at=observed_at,
                observed_at=observed_at,
                surprise_score=0.0,
                confidence=1.0,
                source_reference=COINBASE_PRODUCTS_URL,
                authoritative=True,
                commercial_use_permitted=True,
                point_in_time=True,
                paper_only=True,
            )
        )

        emitted = 0
        emitted_assets: set[str] = set()
        if not is_baseline:
            for item in new_items:
                asset = str(item["asset"]).upper()
                if asset in emitted_assets:
                    continue
                emitted_assets.add(asset)
                self.alpha_factory.record_event_observation(
                    EventObservation(
                        event_id=_stable_id(
                            self.COINBASE_CATALOG_PROVIDER,
                            item["symbol"],
                            observed_at.isoformat(),
                        ),
                        provider=self.COINBASE_CATALOG_PROVIDER,
                        asset=asset,
                        event_type="exchange_listing_observed",
                        known_at=observed_at,
                        event_at=observed_at,
                        observed_at=observed_at,
                        surprise_score=0.50,
                        confidence=0.65,
                        source_reference=COINBASE_PRODUCTS_URL,
                        authoritative=True,
                        commercial_use_permitted=True,
                        point_in_time=True,
                        paper_only=True,
                    )
                )
                emitted += 1

        return ProviderProbeResult(
            mechanism_id="event_driven",
            provider=self.COINBASE_CATALOG_PROVIDER,
            item_count=len(items),
            source_reference=COINBASE_PRODUCTS_URL,
            detail={
                "catalog_item_count": len(items),
                "new_catalog_item_count": len(new_items),
                "listing_event_count": emitted,
                "baseline": is_baseline,
                "first_party_fallback": True,
            },
        )

    async def _collect_bybit_adl_surface(self, base_url: str) -> ProviderProbeResult:
        source = f"{base_url}/v5/market/adlAlert"
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
            response = await client.get(source)
            response.raise_for_status()
            result = self._bybit_result(response.json())
        rows = [row for row in (result.get("list") or []) if isinstance(row, dict)]
        usable = [
            row
            for row in rows
            if row.get("symbol")
            and _number(row.get("balance")) is not None
            and _number(row.get("pnlRatio")) is not None
        ]
        if not usable:
            raise ValueError("Bybit ADL alert returned no usable distress rows")
        breaches = 0
        for row in usable:
            pnl = _number(row.get("pnlRatio"))
            threshold = _number(row.get("insurancePnlRatio"))
            if pnl is not None and threshold is not None and pnl < threshold:
                breaches += 1
        return ProviderProbeResult(
            mechanism_id="liquidation_distress",
            provider=self.BYBIT_ADL_PROVIDER,
            item_count=len(usable),
            source_reference=source,
            detail={
                "adl_state_count": len(usable),
                "pnl_threshold_breach_count": breaches,
                "economic_opportunity_complete": False,
                "forward_testable_state": False,
                "remaining_required_fields": [
                    "capturable liquidation flow",
                    "selection/capture probability",
                    "recovery and settlement outcome",
                ],
            },
        )

    async def _collect_bybit_insurance_surface(self, base_url: str) -> ProviderProbeResult:
        source = f"{base_url}/v5/market/insurance"
        async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control": "no-cache"}) as client:
            response = await client.get(source, params={"coin": "USDT"})
            response.raise_for_status()
            result = self._bybit_result(response.json())
        usable = [
            row
            for row in (result.get("list") or [])
            if isinstance(row, dict)
            and _number(row.get("value")) is not None
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
                "forward_testable_state": False,
                "remaining_required_fields": [
                    "capturable liquidation flow",
                    "selection/capture probability",
                    "recovery and settlement outcome",
                ],
            },
        )

    async def _collect_hyperliquid_distress_surface(self) -> ProviderProbeResult:
        async with httpx.AsyncClient(
            timeout=8.0,
            headers={
                "User-Agent": "crypto-inefficiency-engine/provider-resilience",
                "Cache-Control": "no-cache",
            },
        ) as client:
            response = await client.post(HYPERLIQUID_INFO_URL, json={"type": "metaAndAssetCtxs"})
            response.raise_for_status()
            rows = _hyperliquid_context_rows(response.json())
        if not rows:
            raise ValueError("Hyperliquid returned no usable perpetual asset contexts")
        return ProviderProbeResult(
            mechanism_id="liquidation_distress",
            provider=self.HYPERLIQUID_DISTRESS_PROVIDER,
            item_count=len(rows),
            source_reference=HYPERLIQUID_INFO_URL,
            detail={
                "perpetual_context_count": len(rows),
                "market_wide_liquidation_risk_surface": True,
                "economic_opportunity_complete": False,
                "forward_testable_state": False,
                "remaining_required_fields": [
                    "specific capturable liquidation event",
                    "selection/capture probability",
                    "recovery and settlement outcome",
                ],
            },
        )

    async def _collect_bybit_distress_surface(self) -> ProviderProbeResult:
        failures: list[dict[str, object]] = []
        for base_url in BYBIT_BASE_URLS:
            try:
                probe = await self._collect_bybit_adl_surface(base_url)
                probe.detail["fallback_failures"] = failures
                return probe
            except Exception as exc:
                failures.append(
                    self._failure(
                        self.BYBIT_ADL_PROVIDER,
                        f"{base_url}/v5/market/adlAlert",
                        exc,
                    )
                )
        for base_url in BYBIT_BASE_URLS:
            try:
                probe = await self._collect_bybit_insurance_surface(base_url)
                probe.detail["fallback_failures"] = failures
                return probe
            except Exception as exc:
                failures.append(
                    self._failure(
                        self.BYBIT_DISTRESS_PROVIDER,
                        f"{base_url}/v5/market/insurance",
                        exc,
                    )
                )
        try:
            probe = await self._collect_hyperliquid_distress_surface()
            probe.detail["fallback_failures"] = failures
            probe.detail["fallback_reason"] = "Bybit distress surfaces unavailable from production worker"
            return probe
        except Exception as exc:
            failures.append(
                self._failure(
                    self.HYPERLIQUID_DISTRESS_PROVIDER,
                    HYPERLIQUID_INFO_URL,
                    exc,
                )
            )
            summary = "; ".join(
                f"{row['provider']}:{row['error_type']}" for row in failures
            )
            raise RuntimeError(f"all liquidation/distress provider fallbacks failed: {summary}") from exc


class ResilientProviderGapAwareOperatingCertificationService(
    ProviderGapAwareOperatingCertificationService
):
    """Operating certification using resilient first-party provider admission."""

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
        self.provider_gap_collection = ResilientProviderGapCollectionService(
            store=store,
            alpha_factory=alpha_factory,
            admissions=self.provider_admissions,
            volatility_service=self.volatility_service,
        )

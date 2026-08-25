from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from inefficiency_engine import priority_source_collection as priority_sources
from inefficiency_engine import priority_source_options as option_sources
from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2
from inefficiency_engine import source_lane_repair_runtime as lane_repair
from inefficiency_engine.coinbase_trade_flow import DEFAULT_TRADE_PRODUCTS
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.source_market_cadence import FastExecutableMarketCollector


# Public-provider failures seen in production are transport failures. Retry only those
# errors, with a fresh awaitable each time; application/data-shape failures remain
# fail-closed and are not retried here.
_TRANSIENT_PROVIDER_ERRORS = {
    "TimeoutError",
    "ConnectTimeout",
    "ReadTimeout",
    "WriteTimeout",
    "PoolTimeout",
    "ConnectError",
    "ReadError",
    "WriteError",
    "RemoteProtocolError",
}
PROVIDER_SURFACE_ATTEMPTS = 2
PROVIDER_RETRY_DELAY_SECONDS = 0.15

_INSTALL_MARKER = "_production_source_flap_repair_installed"
_INFLIGHT: dict[tuple[object, ...], asyncio.Task[SourceProbeResult]] = {}

_ORIGINAL_AAVE_COLLECTOR: Callable[..., Awaitable[SourceProbeResult]] | None = None
_ORIGINAL_TRADE_FLOW_COLLECTOR: Callable[..., Awaitable[SourceProbeResult]] | None = None
_ORIGINAL_OKX_OPTIONS_COLLECTOR: Callable[..., Awaitable[SourceProbeResult]] | None = None


def _store_key(value: object) -> int:
    store = getattr(value, "store", None)
    if store is None:
        store = getattr(value, "_store", None)
    return id(store if store is not None else value)


async def _singleflight_probe(
    key: tuple[object, ...],
    factory: Callable[[], Awaitable[SourceProbeResult]],
) -> SourceProbeResult:
    """Join an identical in-flight source acquisition instead of duplicating it."""

    loop_key = id(asyncio.get_running_loop())
    full_key = (loop_key, *key)
    task = _INFLIGHT.get(full_key)
    joined = bool(task is not None and not task.done())
    if not joined:
        task = asyncio.create_task(factory())
        _INFLIGHT[full_key] = task
    assert task is not None
    try:
        probe = await asyncio.shield(task)
        result = probe.model_copy(deep=True)
        result.detail["source_transport_singleflight"] = True
        result.detail["singleflight_joined"] = joined
        return result
    finally:
        if _INFLIGHT.get(full_key) is task and task.done():
            _INFLIGHT.pop(full_key, None)


async def collect_aave_liquidations_singleflight(coverage) -> SourceProbeResult:
    collector = _ORIGINAL_AAVE_COLLECTOR
    if collector is None:
        raise RuntimeError("Aave single-flight repair is not installed")
    return await _singleflight_probe(
        ("aave-liquidations", _store_key(coverage)),
        lambda: collector(coverage),
    )


async def collect_coinbase_trade_flow_singleflight(
    coverage,
    *,
    products: tuple[str, ...] = DEFAULT_TRADE_PRODUCTS,
    limit: int = 100,
) -> SourceProbeResult:
    collector = _ORIGINAL_TRADE_FLOW_COLLECTOR
    if collector is None:
        raise RuntimeError("trade-flow single-flight repair is not installed")
    normalized_products = tuple(str(value) for value in products)
    normalized_limit = max(1, int(limit))
    return await _singleflight_probe(
        (
            "public-trade-flow",
            _store_key(coverage),
            normalized_products,
            normalized_limit,
        ),
        lambda: collector(
            coverage,
            products=normalized_products,
            limit=normalized_limit,
        ),
    )


async def collect_okx_options_singleflight(volatility_service) -> SourceProbeResult:
    collector = _ORIGINAL_OKX_OPTIONS_COLLECTOR
    if collector is None:
        raise RuntimeError("OKX option single-flight repair is not installed")
    return await _singleflight_probe(
        ("okx-options", _store_key(volatility_service)),
        lambda: collector(volatility_service),
    )


async def _capture_surface_with_retries(
    registry,
    provider: str,
    awaitable_factory: Callable[[], Awaitable[Any]],
):
    """Retry one bounded public surface with a fresh transport coroutine."""

    last_result = None
    for attempt in range(1, PROVIDER_SURFACE_ATTEMPTS + 1):
        result = await registry._capture_list(provider, awaitable_factory())
        last_result = result
        _, status = result
        error_type = str(getattr(status, "error_type", "") or "")
        if bool(getattr(status, "ok", False)) or error_type not in _TRANSIENT_PROVIDER_ERRORS:
            return result
        if attempt < PROVIDER_SURFACE_ATTEMPTS:
            await asyncio.sleep(PROVIDER_RETRY_DELAY_SECONDS * attempt)
    assert last_result is not None
    return last_result


async def _managed_market_with_transport_retry(
    self: FastExecutableMarketCollector,
    provider: str,
    adapter,
    assets: tuple[str, ...],
):
    original_assets = tuple(getattr(adapter, "assets", assets) or assets)
    try:
        setattr(adapter, "assets", assets)
        return await _capture_surface_with_retries(
            self.registry,
            provider,
            adapter.market_quotes,
        )
    finally:
        setattr(adapter, "assets", original_assets)


async def _okx_with_transport_retry(
    self: FastExecutableMarketCollector,
    assets: tuple[str, ...],
):
    adapter = self.registry.okx
    original_assets = tuple(getattr(adapter, "assets", assets) or assets)
    try:
        adapter.assets = assets
        market_result, funding_result = await asyncio.gather(
            _capture_surface_with_retries(
                self.registry,
                "okx-v5:market:ticker",
                adapter.market_quotes,
            ),
            _capture_surface_with_retries(
                self.registry,
                "okx-v5:public:funding-rate",
                adapter.funding_quotes,
            ),
        )
        return market_result, funding_result
    finally:
        adapter.assets = original_assets


def install_source_flap_repair() -> None:
    """Eliminate duplicate source bursts and retry proven hot-path transport misses.

    The repair changes only transport ownership and idempotency. Evidence-class TTLs,
    provider identities, source breadth, qualification thresholds, allocation authority,
    and paper-only safeguards are unchanged.
    """

    global _ORIGINAL_AAVE_COLLECTOR
    global _ORIGINAL_TRADE_FLOW_COLLECTOR
    global _ORIGINAL_OKX_OPTIONS_COLLECTOR

    if bool(getattr(FastExecutableMarketCollector, _INSTALL_MARKER, False)):
        return

    # Capture the already-installed resilient collectors. This function is invoked
    # after the existing remaining-source transport and refresh-truth repairs.
    _ORIGINAL_AAVE_COLLECTOR = recovery_v2.collect_aave_liquidations_resilient_v2
    _ORIGINAL_TRADE_FLOW_COLLECTOR = recovery_v2.collect_coinbase_trade_flow_resilient
    _ORIGINAL_OKX_OPTIONS_COLLECTOR = option_sources.collect_okx_options

    # The critical freshness loop and normal priority tail previously had independent
    # bindings for these same first-party sources. Route every owner through one
    # in-flight acquisition per source/store/event-loop.
    recovery_v2.collect_aave_liquidations_resilient_v2 = collect_aave_liquidations_singleflight
    lane_repair.collect_aave_liquidations_resilient = collect_aave_liquidations_singleflight
    priority_sources.collect_aave_liquidations = collect_aave_liquidations_singleflight

    recovery_v2.collect_coinbase_trade_flow_resilient = collect_coinbase_trade_flow_singleflight
    lane_repair.collect_coinbase_trade_flow = collect_coinbase_trade_flow_singleflight
    priority_sources.collect_coinbase_trade_flow = collect_coinbase_trade_flow_singleflight

    option_sources.collect_okx_options = collect_okx_options_singleflight
    recovery_v2.collect_okx_options = collect_okx_options_singleflight
    priority_sources.collect_okx_options = collect_okx_options_singleflight

    # The executable hot path remains hard-deadlined. Give Coinbase market quotes and
    # OKX market/funding one fresh bounded retry before persisting a failed provider
    # attempt. This addresses the observed ConnectTimeout/TimeoutError flapping without
    # extending source-validity windows or masking sustained provider failure.
    FastExecutableMarketCollector._managed_market = _managed_market_with_transport_retry
    FastExecutableMarketCollector._okx = _okx_with_transport_retry

    setattr(FastExecutableMarketCollector, _INSTALL_MARKER, True)


__all__ = [
    "PROVIDER_SURFACE_ATTEMPTS",
    "PROVIDER_RETRY_DELAY_SECONDS",
    "collect_aave_liquidations_singleflight",
    "collect_coinbase_trade_flow_singleflight",
    "collect_okx_options_singleflight",
    "install_source_flap_repair",
]

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from inefficiency_engine.adapters.velora import VeloraPriceRouteAdapter
from inefficiency_engine.dex_shadow import (
    DexRouteQuoteRecord,
    DexRouteShadowCycle,
    build_shadow_observation,
    route_signature,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind
from inefficiency_engine.service import OpportunityService


SleepFn = Callable[[float], Awaitable[None]]


class DexTierShadowService:
    """Periodic multi-notional DEX route survival evidence.

    This is quote-only. Initial quotes are requested sequentially to avoid burst
    loading the public route API. Verification re-quotes are concurrency-bounded
    and always reuse the exact original source amount.
    """

    def __init__(
        self,
        core: OpportunityService,
        *,
        adapter: VeloraPriceRouteAdapter | None = None,
        evidence_store: EvidenceStore | None = None,
        sleep: SleepFn = asyncio.sleep,
    ):
        self.core = core
        self.settings = core.settings
        self.adapter = adapter or VeloraPriceRouteAdapter()
        self.evidence_store = evidence_store if evidence_store is not None else core.evidence_store
        self.sleep = sleep

    async def _reference_prices(self) -> dict[str, float]:
        snapshot = await self.core.collect_live_evidence()
        prices: dict[str, list[float]] = {}
        for quote in snapshot.market_quotes:
            if quote.market_kind != MarketKind.SPOT or quote.asset.upper() not in {"BTC", "ETH"}:
                continue
            prices.setdefault(quote.asset.upper(), []).append(quote.mid)
        return {
            asset: sorted(values)[len(values) // 2]
            for asset, values in prices.items()
            if values
        }

    async def collect_initial_quotes(self, notionals_usd: tuple[float, ...] | None = None):
        notionals = notionals_usd or self.settings.dex_route_tier_shadow_notionals_usd
        references = await self._reference_prices()
        quotes = []
        for notional in notionals:
            if notional <= 0:
                continue
            for asset in ("BTC", "ETH"):
                reference = references.get(asset)
                if reference is None or reference <= 0:
                    continue
                for direction in ("buy_asset", "sell_asset"):
                    try:
                        quote = await self.adapter.quote(
                            asset,
                            direction,
                            notional_usd=notional,
                            reference_price=reference,
                        )
                    except Exception:
                        continue
                    quotes.append(quote)
        return quotes

    async def _requote_bounded(self, initial_quotes):
        semaphore = asyncio.Semaphore(max(1, self.settings.dex_route_tier_shadow_max_concurrency))

        async def one(initial):
            async with semaphore:
                try:
                    return await self.adapter.requote(initial), None
                except Exception as exc:
                    return None, type(exc).__name__

        return await asyncio.gather(*(one(initial) for initial in initial_quotes))

    async def run_cycle(
        self,
        *,
        notionals_usd: tuple[float, ...] | None = None,
        horizons_seconds: tuple[float, ...] | None = None,
    ) -> DexRouteShadowCycle:
        horizons = tuple(sorted(set(
            max(0.0, value)
            for value in (horizons_seconds or self.settings.shadow_horizons_seconds)
        )))
        if not horizons:
            horizons = (max(0.0, self.settings.shadow_delay_seconds),)

        cycle_id = uuid.uuid4().hex
        started_at = datetime.now(timezone.utc)
        initial_quotes = await self.collect_initial_quotes(notionals_usd)
        records: list[DexRouteQuoteRecord] = []
        initial_records: list[DexRouteQuoteRecord] = []
        for quote in initial_quotes:
            record = DexRouteQuoteRecord(
                cycle_id=cycle_id,
                phase="initial",
                horizon_seconds=0.0,
                route_signature=route_signature(quote),
                observed_at=quote.observed_at,
                quote=quote,
            )
            initial_records.append(record)
            records.append(record)

        observations = []
        elapsed = 0.0
        for horizon in horizons:
            wait = max(0.0, horizon - elapsed)
            if wait > 0:
                await self.sleep(wait)
            elapsed = horizon
            outcomes = await self._requote_bounded(initial_quotes)
            verified_at = datetime.now(timezone.utc)
            for initial_record, (quote, failure_type) in zip(initial_records, outcomes):
                verification_record = None
                if quote is not None:
                    verification_record = DexRouteQuoteRecord(
                        cycle_id=cycle_id,
                        phase="verification",
                        horizon_seconds=horizon,
                        route_signature=initial_record.route_signature,
                        observed_at=quote.observed_at,
                        quote=quote,
                    )
                    records.append(verification_record)
                observations.append(build_shadow_observation(
                    cycle_id=cycle_id,
                    initial_record=initial_record,
                    verification_record=verification_record,
                    delay_seconds=horizon,
                    verified_at=verified_at,
                    failure_type=failure_type,
                ))

        cycle = DexRouteShadowCycle(
            cycle_id=cycle_id,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            horizons_seconds=list(horizons),
            initial_quote_count=len(initial_records),
            observations=observations,
            paper_only=True,
        )
        if self.evidence_store is not None:
            self.evidence_store.record_dex_route_shadow_cycle(cycle, records)
        return cycle

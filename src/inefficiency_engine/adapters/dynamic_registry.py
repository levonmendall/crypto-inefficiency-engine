from __future__ import annotations

from inefficiency_engine.adapters.registry import PublicAdapterRegistry
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.volume_universe import resolve_top_volume_assets


class DynamicVolumePublicAdapterRegistry(PublicAdapterRegistry):
    """Public adapter registry whose bounded CEX universe follows rolling volume.

    Hyperliquid remains full-universe. Only default-managed Coinbase, Bybit,
    Kraken and OKX adapters are updated; explicitly supplied/custom adapters are
    never mutated. Universe selection is research routing only and creates no
    allocation or execution authority.
    """

    def __init__(
        self,
        *,
        evidence_store: EvidenceStore | None = None,
        hyperliquid: object | None = None,
        coinbase: object | None = None,
        bybit: object | None = None,
        kraken: object | None = None,
        okx: object | None = None,
        provider_surface_timeout_seconds: float = 8.0,
        order_book_timeout_seconds: float = 8.0,
    ):
        self.evidence_store = evidence_store
        self._managed_coinbase = coinbase is None
        self._managed_bybit = bybit is None
        self._managed_kraken = kraken is None
        self._managed_okx = okx is None
        super().__init__(
            hyperliquid=hyperliquid,
            coinbase=coinbase,
            bybit=bybit,
            kraken=kraken,
            okx=okx,
            provider_surface_timeout_seconds=provider_surface_timeout_seconds,
            order_book_timeout_seconds=order_book_timeout_seconds,
        )

    async def _refresh_managed_assets(self) -> tuple[str, ...] | None:
        if self.evidence_store is None:
            return None
        assets = await resolve_top_volume_assets(self.evidence_store)
        if self._managed_coinbase:
            self.coinbase.assets = assets
        if self._managed_bybit:
            self.bybit.assets = assets
        if self._managed_kraken:
            self.kraken.assets = assets
        if self._managed_okx:
            self.okx.assets = assets
        return assets

    async def collect_inputs(self):
        await self._refresh_managed_assets()
        return await super().collect_inputs()

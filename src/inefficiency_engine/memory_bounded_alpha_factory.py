from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import select

from inefficiency_engine.bounded_alpha_factory import BoundedExpandedAlphaFactoryService
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote


class MemoryBoundedExpandedAlphaFactoryService(BoundedExpandedAlphaFactoryService):
    """Exact active-strategy alpha discovery without materializing unused history.

    The prior bridge path rebuilt the entire configured alpha history window on each
    projection. That was semantically unnecessary: every active strategy already
    applies a shorter explicit lookback, and strategies can only produce candidates
    for instruments present in the current snapshot. This implementation preserves
    those exact strategy windows while streaming persisted quote payloads and keeping
    only currently relevant venue/asset/market series in memory.
    """

    def _effective_history_hours(self) -> float:
        settings = self._expanded_settings
        active_lookbacks = (
            float(settings.alpha_momentum_lookback_hours),
            float(settings.alpha_reversion_lookback_hours),
            float(settings.alpha_cross_sectional_lookback_hours),
            float(settings.alpha_microstructure_lookback_hours),
            float(settings.alpha_event_max_age_hours),
        )
        required = max(1.0, *active_lookbacks)
        return min(max(1.0, float(self.settings.alpha_history_hours)), required)

    def _history_for_snapshot(
        self,
        snapshot: ScanSnapshot,
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        current_keys = {
            (quote.venue, quote.asset.upper(), quote.market_kind)
            for quote in snapshot.market_quotes
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}
        }
        if not current_keys:
            return {}

        cutoff = snapshot.completed_at - timedelta(hours=self._effective_history_hours())
        query = (
            select(self.store.market_quotes.c.payload_json)
            .where(self.store.market_quotes.c.observed_at >= cutoff.isoformat())
            .where(self.store.market_quotes.c.observed_at <= snapshot.completed_at.isoformat())
            .order_by(self.store.market_quotes.c.observed_at)
        )
        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = defaultdict(list)
        with self.store.engine.connect() as db:
            payloads = db.execution_options(stream_results=True).execute(query).scalars()
            for payload in payloads:
                quote = MarketQuote.model_validate_json(payload)
                key = (quote.venue, quote.asset.upper(), quote.market_kind)
                if key in current_keys:
                    grouped[key].append(quote)
        return grouped

    def discover(
        self,
        snapshot: ScanSnapshot,
        *,
        total_capital_usd: float,
    ):
        return self.registry.discover(
            snapshot,
            self._history_for_snapshot(snapshot),
            self._expanded_settings,  # type: ignore[arg-type]
            total_capital_usd=total_capital_usd,
        )

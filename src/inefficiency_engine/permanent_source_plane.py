from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Awaitable, Callable

from inefficiency_engine.adapters.dynamic_registry import DynamicVolumePublicAdapterRegistry
from inefficiency_engine.alpha_coverage_strategies import EventLedger, EventObservation
from inefficiency_engine.alpha_extensions import FundamentalFactorLedger, FundamentalFactorObservation
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, OpportunityLeg, Side
from inefficiency_engine.priority_source_collection import (
    ALPHA_L2_WORKER_ID,
    PrioritySourceCollectionService,
)
from inefficiency_engine.provider_gap_collection import ProviderAdmissionLedger
from inefficiency_engine.research_mechanisms import VolatilityResearchService, YieldResearchService
from inefficiency_engine.source_coverage import SourceCoveragePlane
from inefficiency_engine.source_market_cadence import (
    FastExecutableMarketCollector,
    rotating_assets,
)


PERMANENT_SOURCE_WORKER_ID = "canonical-source-operating-loop"
RESEARCH_MARKET_WORKER_ID = "research-market-universe-refresh"
DEFAULT_SOURCE_MARKET_INTERVAL_SECONDS = 30.0
DEFAULT_SOURCE_PRIORITY_INTERVAL_SECONDS = 60.0
DEFAULT_SOURCE_RESEARCH_INTERVAL_SECONDS = 300.0
DEFAULT_SOURCE_EXECUTABLE_DEADLINE_SECONDS = 45.0
DEFAULT_SOURCE_L2_ASSET_BATCH_SIZE = 4
MAX_SOURCE_L2_ASSET_BATCH_SIZE = 8


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _env_float(name: str, default: float, *, minimum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value)


def source_market_interval_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_MARKET_INTERVAL_SECONDS",
        DEFAULT_SOURCE_MARKET_INTERVAL_SECONDS,
        minimum=10.0,
    )


def source_priority_interval_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_PRIORITY_INTERVAL_SECONDS",
        DEFAULT_SOURCE_PRIORITY_INTERVAL_SECONDS,
        minimum=30.0,
    )


def source_research_interval_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_RESEARCH_INTERVAL_SECONDS",
        DEFAULT_SOURCE_RESEARCH_INTERVAL_SECONDS,
        minimum=60.0,
    )


def source_executable_deadline_seconds() -> float:
    return _env_float(
        "CIE_SOURCE_EXECUTABLE_DEADLINE_SECONDS",
        DEFAULT_SOURCE_EXECUTABLE_DEADLINE_SECONDS,
        minimum=15.0,
    )


def permanent_source_plane_current(
    store: EvidenceStore,
    *,
    max_age_seconds: float = 120.0,
    now: datetime | None = None,
) -> bool:
    """Return whether the permanent executable-source owner is currently alive.

    A degraded heartbeat still means the permanent source loop owns provider retries.
    Heavy research must not duplicate network work merely because one venue is
    degraded. Missing, errored or stale ownership remains fail-safe and allows the
    disposable path to recover acquisition if the permanent owner truly stops.
    """

    try:
        heartbeat = store.latest_worker_heartbeat(PERMANENT_SOURCE_WORKER_ID)
    except Exception:
        return False
    if heartbeat is None:
        return False
    observed_at = heartbeat.observed_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    current = now or _now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age = max(
        0.0,
        (
            current.astimezone(timezone.utc)
            - observed_at.astimezone(timezone.utc)
        ).total_seconds(),
    )
    return bool(
        age <= max(30.0, float(max_age_seconds))
        and str(heartbeat.state or "") in {"running", "success", "degraded"}
    )


class _PersistedEventSink:
    """Minimal ledgers required by provider/source collectors."""

    def __init__(self, store: EvidenceStore):
        self.ledger = EventLedger(store)
        self.fundamentals = FundamentalFactorLedger(store)
        self._l2_refresh: Callable[[], Awaitable[ScanSnapshot]] | None = None

    def record_event_observation(self, observation: EventObservation) -> str:
        return self.ledger.record(observation)

    def record_fundamental_observation(
        self,
        observation: FundamentalFactorObservation,
    ) -> str:
        return self.fundamentals.record(observation)

    def bind_l2_refresh(self, refresh: Callable[[], Awaitable[ScanSnapshot]]) -> None:
        self._l2_refresh = refresh

    async def refresh_l2_source_snapshot(self) -> ScanSnapshot:
        if self._l2_refresh is None:
            raise RuntimeError("permanent source L2 refresh is not bound")
        return await self._l2_refresh()


class PermanentSourcePlane:
    """Always-resident provider acquisition independent from disposable research.

    Executable evidence and broad research-history acquisition deliberately have
    different time budgets. ``refresh_market_l2_snapshot`` refreshes only a rotating
    four-asset executable cohort and is hard-deadlined well inside the downstream
    120-second freshness window. ``refresh_research_market_snapshot`` performs the
    broader top-volume sweep without visible L2 and is scheduled independently.
    Priority protocol/options/event sources remain a third independent cadence.
    """

    def __init__(self, store: EvidenceStore):
        self.store = store
        self.registry = DynamicVolumePublicAdapterRegistry(evidence_store=store)
        self.fast_market = FastExecutableMarketCollector(self.registry)
        self.coverage = SourceCoveragePlane(store)
        self.event_sink = _PersistedEventSink(store)
        self.admissions = ProviderAdmissionLedger(store)
        self.volatility = VolatilityResearchService(store)
        self.yield_service = YieldResearchService(store)
        self.priority = PrioritySourceCollectionService(
            store=store,
            alpha_factory=self.event_sink,
            admissions=self.admissions,
            volatility_service=self.volatility,
            yield_service=self.yield_service,
            source_coverage=self.coverage,
        )
        self.event_sink.bind_l2_refresh(self.refresh_market_l2_snapshot)
        self._market_cycle = 0
        self._last_priority_started_at: datetime | None = None

    def _l2_batch_size(self) -> int:
        try:
            requested = int(
                os.getenv(
                    "CIE_SOURCE_L2_ASSET_BATCH_SIZE",
                    str(DEFAULT_SOURCE_L2_ASSET_BATCH_SIZE),
                )
            )
        except ValueError:
            requested = DEFAULT_SOURCE_L2_ASSET_BATCH_SIZE
        return max(1, min(MAX_SOURCE_L2_ASSET_BATCH_SIZE, requested))

    def _selected_assets(self, market_quotes: list[object] | None = None) -> tuple[str, ...]:
        """Return the current rotating executable cohort without a network lookup."""

        selected = rotating_assets(
            self.store,
            self.registry,
            cycle=self._market_cycle,
            count=self._l2_batch_size(),
        )
        if selected:
            return selected

        ordered: list[str] = []
        seen: set[str] = set()
        for quote in list(market_quotes or []):
            asset = str(getattr(quote, "asset", "")).upper().strip()
            if asset and asset not in seen:
                ordered.append(asset)
                seen.add(asset)
        return tuple(ordered[: self._l2_batch_size()])

    @staticmethod
    def _book_requests(
        market_quotes: list[object], selected_assets: tuple[str, ...]
    ) -> list[object]:
        selected = set(selected_assets)
        opportunities: list[object] = []
        seen: set[tuple[str, str, str, str]] = set()
        for quote in market_quotes:
            asset = str(getattr(quote, "asset", "")).upper()
            market_kind = getattr(quote, "market_kind", None)
            if asset not in selected or market_kind not in {
                MarketKind.SPOT,
                MarketKind.PERPETUAL,
            }:
                continue
            venue = str(getattr(quote, "venue", ""))
            symbol = str(getattr(quote, "symbol", ""))
            contract_key = str(getattr(quote, "contract_key", "") or symbol)
            key = (
                venue,
                asset,
                str(getattr(market_kind, "value", market_kind)),
                contract_key,
            )
            if key in seen:
                continue
            seen.add(key)
            opportunities.append(
                SimpleNamespace(
                    legs=[
                        OpportunityLeg(
                            venue=venue,
                            asset=asset,
                            market_kind=market_kind,
                            side=Side.LONG,
                            symbol=symbol,
                            quote_currency=str(
                                getattr(quote, "quote_currency", "USD")
                            ),
                            contract_key=getattr(quote, "contract_key", None),
                            expires_at=getattr(quote, "expires_at", None),
                            reference_price=float(getattr(quote, "mid")),
                        )
                    ]
                )
            )
        return opportunities

    async def _collect_executable_snapshot_payload(self):
        selected_assets = self._selected_assets()
        funding, markets, providers = await self.fast_market.collect_inputs(selected_assets)
        if not selected_assets:
            selected_assets = self._selected_assets(list(markets))
        requests = self._book_requests(list(markets), selected_assets)
        books, book_statuses = await self.fast_market.collect_books(requests)
        return selected_assets, funding, markets, providers, requests, books, book_statuses

    async def refresh_market_l2_snapshot(self) -> ScanSnapshot:
        """Persist one hard-deadlined executable market/funding/L2 snapshot."""

        started_at = _now()
        deadline = source_executable_deadline_seconds()
        try:
            (
                selected_assets,
                funding,
                markets,
                providers,
                requests,
                books,
                book_statuses,
            ) = await asyncio.wait_for(
                self._collect_executable_snapshot_payload(),
                timeout=deadline,
            )
        except TimeoutError as exc:
            raise TimeoutError(
                f"ExecutableSourceRefreshDeadlineExceeded:{deadline:.1f}s"
            ) from exc

        completed_at = _now()
        scan_id = self.store.record_scan(
            funding_quotes=list(funding),
            market_quotes=list(markets),
            opportunities=[],
            providers=[*providers, *book_statuses],
            started_at=started_at,
            completed_at=completed_at,
            order_books=list(books),
            analysis_config={
                "permanent_source_plane": True,
                "executable_hot_path": True,
                "broad_research_sweep": False,
                "whole_cycle_deadline_seconds": deadline,
                "disposable_research_required": False,
                "selected_l2_assets": list(selected_assets),
                "l2_asset_batch_size": len(selected_assets),
                "market_quote_count": len(markets),
                "funding_quote_count": len(funding),
                "order_book_count": len(books),
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
        self._market_cycle += 1
        l2_state = "success" if books else "degraded"
        self.store.record_worker_heartbeat(
            worker_id=ALPHA_L2_WORKER_ID,
            state=l2_state,
            scan_id=scan_id,
            error_type=None if books else "PermanentSourceL2Empty",
            detail={
                "permanent_source_plane": True,
                "executable_hot_path": True,
                "selected_assets": list(selected_assets),
                "requested_instrument_count": len(requests),
                "retained_book_count": len(books),
                "provider_status_count": len(book_statuses),
                "whole_cycle_deadline_seconds": deadline,
                "structural_opportunity_required": False,
                "qualification_thresholds_unchanged": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
        return self.store.load_scan(scan_id)

    async def refresh_research_market_snapshot(self) -> ScanSnapshot:
        """Persist the broad market/funding sweep without blocking executable L2."""

        started_at = _now()
        funding, markets, providers = await self.registry.collect_inputs()
        completed_at = _now()
        scan_id = self.store.record_scan(
            funding_quotes=list(funding),
            market_quotes=list(markets),
            opportunities=[],
            providers=list(providers),
            started_at=started_at,
            completed_at=completed_at,
            order_books=[],
            analysis_config={
                "permanent_source_plane": False,
                "executable_hot_path": False,
                "research_market_sweep": True,
                "broad_research_sweep": True,
                "market_quote_count": len(markets),
                "funding_quote_count": len(funding),
                "order_book_count": 0,
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
        return self.store.load_scan(scan_id)

    def _priority_due(self, now: datetime) -> bool:
        if self._last_priority_started_at is None:
            return True
        age = max(
            0.0,
            (now - self._last_priority_started_at).total_seconds(),
        )
        return age >= source_priority_interval_seconds()

    async def run_cycle(self) -> dict[str, object]:
        """Compatibility cycle; production schedules each source cadence separately."""

        cycle_started = _now()
        self.store.record_worker_heartbeat(
            worker_id=PERMANENT_SOURCE_WORKER_ID,
            state="running",
            detail={
                "stage": "cycle_start",
                "resident_with_portfolio_process": True,
                "separate_python_process": False,
                "executable_hot_path": True,
                "disposable_research_required": False,
                "paper_only": True,
                "allocation_authority": False,
                "live_execution_authority": False,
            },
        )

        detail: dict[str, object] = {
            "resident_with_portfolio_process": True,
            "separate_python_process": False,
            "executable_hot_path": True,
            "disposable_research_required": False,
            "paper_only": True,
            "qualification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
        }
        error_types: list[str] = []
        try:
            snapshot = await self.refresh_market_l2_snapshot()
            detail.update(
                {
                    "market_scan_id": snapshot.scan_id,
                    "market_quote_count": len(snapshot.market_quotes),
                    "funding_quote_count": len(snapshot.funding_quotes),
                    "order_book_count": len(snapshot.order_books),
                    "market_refresh_complete": True,
                }
            )
        except Exception as exc:
            error_types.append(type(exc).__name__)
            detail.update(
                {
                    "market_refresh_complete": False,
                    "market_refresh_error_type": type(exc).__name__,
                }
            )

        if self._priority_due(cycle_started):
            self._last_priority_started_at = cycle_started
            try:
                priority = await self.priority.run_cycle()
                source_refresh = (
                    priority.get("source_refresh", {})
                    if isinstance(priority, dict)
                    else {}
                )
                detail.update(
                    {
                        "priority_refresh_complete": True,
                        "priority_refresh_state": source_refresh.get("state")
                        if isinstance(source_refresh, dict)
                        else None,
                        "priority_failed_sources": list(
                            source_refresh.get("failed_sources") or []
                        )
                        if isinstance(source_refresh, dict)
                        else [],
                        "priority_memory_deferred_sources": list(
                            source_refresh.get("memory_deferred_sources") or []
                        )
                        if isinstance(source_refresh, dict)
                        else [],
                    }
                )
                if (
                    isinstance(source_refresh, dict)
                    and source_refresh.get("state") == "degraded"
                ):
                    error_types.append("PrioritySourceDegraded")
            except Exception as exc:
                error_types.append(type(exc).__name__)
                detail.update(
                    {
                        "priority_refresh_complete": False,
                        "priority_refresh_error_type": type(exc).__name__,
                    }
                )
        else:
            detail["priority_refresh_complete"] = None
            detail["priority_refresh_state"] = "not_due"

        detail["cycle_runtime_seconds"] = max(
            0.0,
            (_now() - cycle_started).total_seconds(),
        )
        detail["subsystem_error_types"] = sorted(set(error_types))
        state = "degraded" if error_types else "success"
        self.store.record_worker_heartbeat(
            worker_id=PERMANENT_SOURCE_WORKER_ID,
            state=state,
            error_type=error_types[0] if error_types else None,
            detail=detail,
        )
        return {"state": state, **detail}

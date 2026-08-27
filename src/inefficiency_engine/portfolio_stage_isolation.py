from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from types import SimpleNamespace
from typing import Awaitable, Callable, Mapping, Sequence

from inefficiency_engine import __version__
from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocationPlan, UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService


STAGE_RESULT_PREFIX = "CIE_STAGE_RESULT="
STAGE_CAPITAL_ENV = "CIE_STAGE_CAPITAL_USD"
STAGE_ALLOCATOR_KWARGS_ENV = "CIE_STAGE_ALLOCATOR_KWARGS_JSON"

PORTFOLIO_SCAN_STAGE_TIMEOUT_SECONDS = 30.0
PORTFOLIO_ALLOCATION_STAGE_TIMEOUT_SECONDS = 45.0
ALLOCATION_CERTIFICATION_STAGE_TIMEOUT_SECONDS = 30.0
OPERATING_CERTIFICATION_STAGE_TIMEOUT_SECONDS = 30.0
_STAGE_TERMINATE_GRACE_SECONDS = 5.0

StageRunner = Callable[..., Awaitable[dict[str, object]]]


class PortfolioStageError(RuntimeError):
    pass


class PortfolioStageTimeout(PortfolioStageError):
    pass


class PortfolioStageFailed(PortfolioStageError):
    pass


class PortfolioStageProtocolError(PortfolioStageError):
    pass


class PortfolioScanStageTimeout(PortfolioStageTimeout):
    pass


class PortfolioScanStageFailed(PortfolioStageFailed):
    pass


class PortfolioAllocationStageTimeout(PortfolioStageTimeout):
    pass


class PortfolioAllocationStageFailed(PortfolioStageFailed):
    pass


class AllocationCertificationStageTimeout(PortfolioStageTimeout):
    pass


class AllocationCertificationStageFailed(PortfolioStageFailed):
    pass


class OperatingCertificationStageTimeout(PortfolioStageTimeout):
    pass


class OperatingCertificationStageFailed(PortfolioStageFailed):
    pass


def default_stage_command(stage_command: str) -> list[str]:
    return [sys.executable, "-m", "inefficiency_engine.cli", stage_command]


def _stage_process_group_exists(pgid: int) -> bool:
    if os.name != "posix" or not hasattr(os, "killpg"):
        return False
    try:
        os.killpg(int(pgid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _signal_stage_process_tree(
    process: asyncio.subprocess.Process,
    sig: signal.Signals,
) -> bool:
    if os.name == "posix" and hasattr(os, "killpg") and getattr(process, "pid", None):
        try:
            os.killpg(int(process.pid), sig)
            return True
        except ProcessLookupError:
            return False
        except OSError:
            pass
    if process.returncode is not None:
        return False
    if sig == signal.SIGTERM:
        process.terminate()
    else:
        process.kill()
    return True


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    group_mode = bool(
        os.name == "posix"
        and hasattr(os, "killpg")
        and getattr(process, "pid", None)
    )
    if group_mode:
        _signal_stage_process_tree(process, signal.SIGTERM)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _STAGE_TERMINATE_GRACE_SECONDS
        while _stage_process_group_exists(int(process.pid)) and loop.time() < deadline:
            await asyncio.sleep(0.01)
        if _stage_process_group_exists(int(process.pid)):
            _signal_stage_process_tree(process, signal.SIGKILL)
        if process.returncode is None:
            try:
                await asyncio.wait_for(process.wait(), timeout=1.0)
            except TimeoutError:
                _signal_stage_process_tree(process, signal.SIGKILL)
                await process.wait()
        return

    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=_STAGE_TERMINATE_GRACE_SECONDS)
    except TimeoutError:
        process.kill()
        await process.wait()


def _tail(value: bytes | None, *, limit: int = 2000) -> str:
    if not value:
        return ""
    text = value.decode("utf-8", errors="replace")
    return text[-limit:]


async def run_stage_subprocess(
    command: Sequence[str],
    *,
    stage_name: str,
    timeout_seconds: float,
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Run one provider-heavy stage behind a hard OS-process boundary.

    `asyncio.wait_for` alone cannot interrupt synchronous blocking work that has
    frozen its own event loop. This function runs the entire stage in a disposable
    subprocess. The parent event loop remains responsive and can terminate the
    stage process even if the stage's Python runtime is wedged.
    """

    merged_env = os.environ.copy()
    if env:
        merged_env.update({str(key): str(value) for key, value in env.items()})

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=merged_env,
        start_new_session=(os.name == "posix"),
    )
    try:
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=max(0.05, float(timeout_seconds)),
            )
        except TimeoutError as exc:
            await _terminate_process(process)
            raise PortfolioStageTimeout(
                f"{stage_name} exceeded hard process deadline of {timeout_seconds:.3f}s"
            ) from exc
        except asyncio.CancelledError:
            await _terminate_process(process)
            raise
    finally:
        if process.returncode is None or (
            os.name == "posix"
            and getattr(process, "pid", None)
            and _stage_process_group_exists(int(process.pid))
        ):
            await _terminate_process(process)

    if process.returncode != 0:
        detail = _tail(stderr) or _tail(stdout)
        raise PortfolioStageFailed(
            f"{stage_name} exited with code {process.returncode}: {detail}"
        )

    marker: str | None = None
    for raw_line in stdout.decode("utf-8", errors="replace").splitlines():
        if raw_line.startswith(STAGE_RESULT_PREFIX):
            marker = raw_line[len(STAGE_RESULT_PREFIX):]
    if marker is None:
        raise PortfolioStageProtocolError(
            f"{stage_name} completed without a {STAGE_RESULT_PREFIX!r} result marker"
        )
    try:
        payload = json.loads(marker)
    except json.JSONDecodeError as exc:
        raise PortfolioStageProtocolError(
            f"{stage_name} returned malformed JSON stage result"
        ) from exc
    if not isinstance(payload, dict):
        raise PortfolioStageProtocolError(f"{stage_name} result must be a JSON object")
    return payload


def _capital_from_env() -> float:
    raw = os.getenv(STAGE_CAPITAL_ENV)
    if raw is None:
        raise RuntimeError(f"{STAGE_CAPITAL_ENV} is required for this stage")
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{STAGE_CAPITAL_ENV} must be positive")
    return value


def _allocator_kwargs_from_env() -> dict[str, object]:
    raw = os.getenv(STAGE_ALLOCATOR_KWARGS_ENV)
    if not raw:
        return {}
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"{STAGE_ALLOCATOR_KWARGS_ENV} must contain a JSON object")
    allowed = {"max_venue_fraction", "max_asset_fraction", "max_allocations"}
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(f"unsupported allocator stage arguments: {sorted(unknown)}")
    return payload


def _build_stage_services(
    service: OpportunityService,
    store: EvidenceStore,
) -> tuple[
    ExpandedAlphaFactoryService,
    UnifiedPaperAllocatorService,
    AllocationForwardCertificationService,
    OperatingCertificationService,
]:
    universal = UniversalOpportunityService(service)
    composite = CexDexCompositeEvidenceService(service, universal=universal)
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    promotion = CexDexPaperPromotionService(service, composite, store)
    allocator = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
    allocation_certification = AllocationForwardCertificationService(service, allocator, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        alpha_factory,
        allocation_certification,
        version=__version__,
    )
    return alpha_factory, allocator, allocation_certification, operating_certification


async def execute_portfolio_stage_command(
    stage_command: str,
    *,
    service: OpportunityService,
    store: EvidenceStore,
) -> dict[str, object]:
    """Execute exactly one stage inside a disposable CLI subprocess."""

    if stage_command == "portfolio-scan-stage":
        snapshot = await service.collect_live_executability()
        if snapshot.scan_id == "unpersisted":
            raise RuntimeError("portfolio scan stage requires persisted evidence")
        return {
            "scan_id": snapshot.scan_id,
            "completed_at": snapshot.completed_at.isoformat(),
        }

    _, allocator, allocation_certification, operating_certification = _build_stage_services(
        service, store
    )
    capital_usd = _capital_from_env()

    if stage_command == "portfolio-allocation-stage":
        plan = await allocator.allocate(
            total_capital_usd=capital_usd,
            **_allocator_kwargs_from_env(),
        )
        return {"plan": plan.model_dump(mode="json")}

    if stage_command == "allocation-certification-stage":
        cycle = await allocation_certification.run_cycle(total_capital_usd=capital_usd)
        return {"cycle_id": cycle.cycle_id}

    if stage_command == "operating-certification-stage":
        cycle = await operating_certification.run_cycle(total_capital_usd=capital_usd)
        return {"cycle_id": cycle.cycle_id}

    raise ValueError(f"unknown portfolio stage command: {stage_command}")


def emit_stage_result(payload: dict[str, object]) -> None:
    print(
        STAGE_RESULT_PREFIX + json.dumps(payload, sort_keys=True, separators=(",", ":")),
        flush=True,
    )


async def _run_with_stage_errors(
    runner: StageRunner,
    command: Sequence[str],
    *,
    stage_name: str,
    timeout_seconds: float,
    env: Mapping[str, str] | None,
    timeout_error: type[PortfolioStageTimeout],
    failed_error: type[PortfolioStageFailed],
) -> dict[str, object]:
    try:
        return await runner(
            command,
            stage_name=stage_name,
            timeout_seconds=timeout_seconds,
            env=env,
        )
    except PortfolioStageTimeout as exc:
        raise timeout_error(str(exc)) from exc
    except (PortfolioStageFailed, PortfolioStageProtocolError) as exc:
        raise failed_error(str(exc)) from exc


class IsolatedOpportunityCoreProxy:
    """Delegate core behavior except live executability, which is process-isolated."""

    def __init__(
        self,
        core: OpportunityService,
        store: EvidenceStore,
        *,
        runner: StageRunner = run_stage_subprocess,
        timeout_seconds: float = PORTFOLIO_SCAN_STAGE_TIMEOUT_SECONDS,
    ):
        self._core = core
        self._store = store
        self._runner = runner
        self._timeout_seconds = timeout_seconds

    def __getattr__(self, name: str):
        return getattr(self._core, name)

    async def collect_live_executability(self):
        payload = await _run_with_stage_errors(
            self._runner,
            default_stage_command("portfolio-scan-stage"),
            stage_name="portfolio_live_executability_scan",
            timeout_seconds=self._timeout_seconds,
            env=None,
            timeout_error=PortfolioScanStageTimeout,
            failed_error=PortfolioScanStageFailed,
        )
        scan_id = payload.get("scan_id")
        if not isinstance(scan_id, str) or not scan_id:
            raise PortfolioScanStageFailed("portfolio scan stage did not return a persisted scan_id")
        try:
            return self._store.load_scan(scan_id)
        except Exception as exc:
            raise PortfolioScanStageFailed(
                f"portfolio scan stage returned unreadable persisted scan_id {scan_id!r}"
            ) from exc


class IsolatedAllocatorProxy:
    """Delegate allocator metadata while process-isolating actual allocation."""

    def __init__(
        self,
        allocator: UnifiedPaperAllocatorService,
        *,
        runner: StageRunner = run_stage_subprocess,
        timeout_seconds: float = PORTFOLIO_ALLOCATION_STAGE_TIMEOUT_SECONDS,
    ):
        self._allocator = allocator
        self._runner = runner
        self._timeout_seconds = timeout_seconds

    def __getattr__(self, name: str):
        return getattr(self._allocator, name)

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        kwargs: dict[str, object] = {}
        if max_venue_fraction is not None:
            kwargs["max_venue_fraction"] = max_venue_fraction
        if max_asset_fraction is not None:
            kwargs["max_asset_fraction"] = max_asset_fraction
        if max_allocations is not None:
            kwargs["max_allocations"] = max_allocations
        payload = await _run_with_stage_errors(
            self._runner,
            default_stage_command("portfolio-allocation-stage"),
            stage_name="portfolio_unified_allocation",
            timeout_seconds=self._timeout_seconds,
            env={
                STAGE_CAPITAL_ENV: str(total_capital_usd),
                STAGE_ALLOCATOR_KWARGS_ENV: json.dumps(kwargs, sort_keys=True),
            },
            timeout_error=PortfolioAllocationStageTimeout,
            failed_error=PortfolioAllocationStageFailed,
        )
        plan_payload = payload.get("plan")
        if not isinstance(plan_payload, dict):
            raise PortfolioAllocationStageFailed("allocation stage did not return a plan object")
        try:
            return UnifiedPaperAllocationPlan.model_validate(plan_payload)
        except Exception as exc:
            raise PortfolioAllocationStageFailed("allocation stage returned an invalid plan") from exc


class IsolatedAllocationCertificationProxy:
    def __init__(
        self,
        *,
        runner: StageRunner = run_stage_subprocess,
        timeout_seconds: float = ALLOCATION_CERTIFICATION_STAGE_TIMEOUT_SECONDS,
    ):
        self._runner = runner
        self._timeout_seconds = timeout_seconds

    async def run_cycle(self, *, total_capital_usd: float):
        payload = await _run_with_stage_errors(
            self._runner,
            default_stage_command("allocation-certification-stage"),
            stage_name="allocation_forward_certification",
            timeout_seconds=self._timeout_seconds,
            env={STAGE_CAPITAL_ENV: str(total_capital_usd)},
            timeout_error=AllocationCertificationStageTimeout,
            failed_error=AllocationCertificationStageFailed,
        )
        cycle_id = payload.get("cycle_id")
        if not isinstance(cycle_id, str) or not cycle_id:
            raise AllocationCertificationStageFailed(
                "allocation certification stage did not return cycle_id"
            )
        return SimpleNamespace(cycle_id=cycle_id)


class IsolatedOperatingCertificationProxy:
    def __init__(
        self,
        *,
        runner: StageRunner = run_stage_subprocess,
        timeout_seconds: float = OPERATING_CERTIFICATION_STAGE_TIMEOUT_SECONDS,
    ):
        self._runner = runner
        self._timeout_seconds = timeout_seconds

    async def run_cycle(self, *, total_capital_usd: float):
        payload = await _run_with_stage_errors(
            self._runner,
            default_stage_command("operating-certification-stage"),
            stage_name="operating_profitability_certification",
            timeout_seconds=self._timeout_seconds,
            env={STAGE_CAPITAL_ENV: str(total_capital_usd)},
            timeout_error=OperatingCertificationStageTimeout,
            failed_error=OperatingCertificationStageFailed,
        )
        cycle_id = payload.get("cycle_id")
        if not isinstance(cycle_id, str) or not cycle_id:
            raise OperatingCertificationStageFailed(
                "operating certification stage did not return cycle_id"
            )
        return SimpleNamespace(cycle_id=cycle_id)

import json
import sys

import pytest

from inefficiency_engine.portfolio_stage_isolation import (
    IsolatedAllocationCertificationProxy,
    IsolatedAllocatorProxy,
    IsolatedOperatingCertificationProxy,
    PortfolioAllocationStageTimeout,
    PortfolioStageFailed,
    PortfolioStageProtocolError,
    PortfolioStageTimeout,
    STAGE_RESULT_PREFIX,
    run_stage_subprocess,
)


@pytest.mark.asyncio
async def test_stage_subprocess_returns_marker_payload():
    payload = {"ok": True, "value": 7}
    command = [
        sys.executable,
        "-c",
        (
            "import json; "
            f"print({STAGE_RESULT_PREFIX!r} + json.dumps({payload!r}))"
        ),
    ]

    result = await run_stage_subprocess(
        command,
        stage_name="unit-success",
        timeout_seconds=2.0,
    )

    assert result == payload


@pytest.mark.asyncio
async def test_stage_subprocess_hard_times_out_blocking_process():
    command = [sys.executable, "-c", "import time; time.sleep(5)"]

    with pytest.raises(PortfolioStageTimeout):
        await run_stage_subprocess(
            command,
            stage_name="unit-timeout",
            timeout_seconds=0.1,
        )


@pytest.mark.asyncio
async def test_stage_subprocess_rejects_nonzero_exit():
    command = [
        sys.executable,
        "-c",
        "import sys; print('stage failed', file=sys.stderr); sys.exit(3)",
    ]

    with pytest.raises(PortfolioStageFailed, match="stage failed"):
        await run_stage_subprocess(
            command,
            stage_name="unit-failure",
            timeout_seconds=2.0,
        )


@pytest.mark.asyncio
async def test_stage_subprocess_rejects_missing_result_marker():
    command = [sys.executable, "-c", "print('ordinary output')"]

    with pytest.raises(PortfolioStageProtocolError):
        await run_stage_subprocess(
            command,
            stage_name="unit-protocol",
            timeout_seconds=2.0,
        )


@pytest.mark.asyncio
async def test_allocator_proxy_translates_generic_timeout_to_allocation_timeout():
    async def timeout_runner(*args, **kwargs):
        del args, kwargs
        raise PortfolioStageTimeout("blocked")

    proxy = IsolatedAllocatorProxy(object(), runner=timeout_runner)  # type: ignore[arg-type]

    with pytest.raises(PortfolioAllocationStageTimeout):
        await proxy.allocate(total_capital_usd=250000.0)


@pytest.mark.asyncio
async def test_certification_proxies_return_cycle_ids_without_running_in_parent_process():
    calls: list[tuple[str, dict[str, str]]] = []

    async def runner(command, *, stage_name, timeout_seconds, env=None):
        del command, timeout_seconds
        calls.append((stage_name, dict(env or {})))
        return {"cycle_id": f"{stage_name}-cycle"}

    allocation = IsolatedAllocationCertificationProxy(runner=runner)
    operating = IsolatedOperatingCertificationProxy(runner=runner)

    allocation_cycle = await allocation.run_cycle(total_capital_usd=250000.0)
    operating_cycle = await operating.run_cycle(total_capital_usd=250000.0)

    assert allocation_cycle.cycle_id == "allocation_forward_certification-cycle"
    assert operating_cycle.cycle_id == "operating_profitability_certification-cycle"
    assert len(calls) == 2
    assert all(float(env["CIE_STAGE_CAPITAL_USD"]) == 250000.0 for _, env in calls)

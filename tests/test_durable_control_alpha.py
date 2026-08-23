from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from inefficiency_engine.durable_control_alpha import DurableControlAlphaFactoryService
from inefficiency_engine.permanent_control_worker import _deadline_seconds


class _ExplodingRegistry:
    def book_request(self, *args, **kwargs):
        raise AssertionError("canonical control must not touch provider L2")


def test_durable_control_missing_depth_fails_closed_without_provider_request():
    service = object.__new__(DurableControlAlphaFactoryService)
    service._durable_missing_depth_count = 0
    service.core = SimpleNamespace(adapter_registry=_ExplodingRegistry())

    result = asyncio.run(
        service._bounded_current_l2_cost(
            SimpleNamespace(candidate_id="qualified-alpha-without-persisted-book")
        )
    )

    assert result is None
    diagnostics = service.durable_promotion_diagnostics()
    assert diagnostics["provider_requests_allowed"] is False
    assert diagnostics["provider_requests_used"] == 0
    assert diagnostics["missing_current_executable_depth_count"] == 1
    assert diagnostics["missing_depth_policy"] == "fail_closed"


def test_durable_control_direct_live_l2_hook_is_forbidden():
    service = object.__new__(DurableControlAlphaFactoryService)

    with pytest.raises(RuntimeError, match="ProviderAccessForbiddenInCanonicalControl"):
        asyncio.run(service._current_l2_cost(SimpleNamespace()))


def test_control_cycle_deadline_is_explicit_and_bounded(monkeypatch):
    monkeypatch.setenv("CIE_CONTROL_CYCLE_DEADLINE_SECONDS", "17")
    assert _deadline_seconds() == 17.0

    monkeypatch.setenv("CIE_CONTROL_CYCLE_DEADLINE_SECONDS", "not-a-number")
    assert _deadline_seconds() == 25.0

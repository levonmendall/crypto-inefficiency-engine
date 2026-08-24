from __future__ import annotations

from inefficiency_engine import permanent_source_plane
from inefficiency_engine import render_combined_postbind_lane_repair as render_repair
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService
from inefficiency_engine.permanent_source_worker_lane_repair import (
    _collect_hyperliquid_distress_with_retries,
    install_remaining_source_lane_repairs,
)
from inefficiency_engine.source_lane_repair_runtime import RemainingSourceLaneRepairService


def test_source_worker_installs_repaired_priority_and_distress_services(monkeypatch):
    original_priority = permanent_source_plane.PrioritySourceCollectionService
    original_distress = ResilientProviderGapCollectionService._collect_hyperliquid_distress_surface
    try:
        install_remaining_source_lane_repairs()
        assert permanent_source_plane.PrioritySourceCollectionService is RemainingSourceLaneRepairService
        assert (
            ResilientProviderGapCollectionService._collect_hyperliquid_distress_surface
            is _collect_hyperliquid_distress_with_retries
        )
    finally:
        permanent_source_plane.PrioritySourceCollectionService = original_priority
        ResilientProviderGapCollectionService._collect_hyperliquid_distress_surface = original_distress


def test_render_child_command_routes_only_source_through_repair(monkeypatch):
    base = render_repair.base.base
    original = base._BASE_RUNTIME_CHILD_COMMANDS
    monkeypatch.delattr(base, "_remaining_source_lane_repair_installed", raising=False)
    try:
        render_repair.install_source_repair_child_command()
        commands = base._BASE_RUNTIME_CHILD_COMMANDS(10000)
        assert commands["source"] == render_repair.SOURCE_REPAIR_COMMAND
        assert commands["portfolio"][-1] == "inefficiency_engine.lightweight_portfolio_worker"
        assert commands["api"][-1] == "10000"
    finally:
        base._BASE_RUNTIME_CHILD_COMMANDS = original
        monkeypatch.delattr(base, "_remaining_source_lane_repair_installed", raising=False)

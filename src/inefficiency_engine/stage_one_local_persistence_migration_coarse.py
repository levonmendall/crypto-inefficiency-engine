from __future__ import annotations

from inefficiency_engine import _install_stage_one_runtime_memory_guard
from inefficiency_engine import postgres_local_migration as migration
from inefficiency_engine import stage_one_local_persistence_migration as stage_one
from inefficiency_engine.stage_one_bounded_coarse_market_history import (
    BoundedStageOneCoarsePartitionedMarketHistory as CoarsePartitionedMarketHistory,
)


def main() -> int:
    """Run canonical Stage 1 with bounded coarse market-history verification."""

    stage_one.install_stage_one_repair()
    _install_stage_one_runtime_memory_guard()
    migration.PartitionedMarketHistory = CoarsePartitionedMarketHistory
    return migration.main()


if __name__ == "__main__":
    raise SystemExit(main())

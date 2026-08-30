from __future__ import annotations

from inefficiency_engine import _install_stage_one_runtime_memory_guard
from inefficiency_engine import postgres_local_migration as migration
from inefficiency_engine import stage_one_local_persistence_migration as stage_one
from inefficiency_engine.coarse_partitioned_market_history import (
    CoarsePartitionedMarketHistory,
)


def main() -> int:
    """Run canonical Stage 1 with only the physical market-history writer replaced."""

    _install_stage_one_runtime_memory_guard()
    migration.PartitionedMarketHistory = CoarsePartitionedMarketHistory
    return stage_one.main()


if __name__ == "__main__":
    raise SystemExit(main())

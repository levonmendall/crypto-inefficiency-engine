from __future__ import annotations

from inefficiency_engine import permanent_source_plane as source_plane
from inefficiency_engine import permanent_source_worker as base
from inefficiency_engine.source_lane_repair_runtime import RemainingSourceLaneRepairService


def install_remaining_source_lane_repairs() -> None:
    """Install the repaired priority-source service before the source plane is built."""

    source_plane.PrioritySourceCollectionService = RemainingSourceLaneRepairService


def main() -> int:
    install_remaining_source_lane_repairs()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())

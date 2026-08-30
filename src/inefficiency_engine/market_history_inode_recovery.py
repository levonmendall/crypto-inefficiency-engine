from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from inefficiency_engine.local_storage import local_storage_paths
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory


MARKET_HISTORY_LAYOUT = "venue_day_multi_asset_v2"
BOOTSTRAP_FREE_INODES = 128
BOOTSTRAP_UNLINK_LIMIT = 512


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _inode_capacity(path: Path) -> tuple[int | None, int | None]:
    probe = path if path.exists() else path.parent
    try:
        filesystem = os.statvfs(probe)
    except OSError:
        return None, None
    return int(filesystem.f_files), int(filesystem.f_ffree)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str))
    os.replace(temporary, path)


def _bootstrap_release_inodes(
    root: Path,
    *,
    minimum_free: int = BOOTSTRAP_FREE_INODES,
    unlink_limit: int = BOOTSTRAP_UNLINK_LIMIT,
) -> int:
    """Free a tiny bootstrap reserve without creating any filesystem objects.

    This is used only after the local market-history target has been proven unverified
    and selected for a full rebuild from authoritative PostgreSQL. Deleting a bounded
    number of old Parquet fragments is therefore safe: none of those fragments can
    become authoritative, and the durable source high-water remains unchanged.
    """

    if not root.exists():
        return 0
    deleted = 0
    for directory, _, files in os.walk(root):
        for name in files:
            if not name.endswith((".parquet", ".tmp")):
                continue
            try:
                Path(directory, name).unlink()
            except FileNotFoundError:
                continue
            deleted += 1
            _, free = _inode_capacity(root)
            if free is not None and free >= minimum_free:
                return deleted
            if deleted >= unlink_limit:
                return deleted
    return deleted


def _market_report(progress: dict[str, Any]) -> dict[str, Any] | None:
    tables = progress.get("tables")
    if not isinstance(tables, dict):
        return None
    market = tables.get("market_quotes")
    return market if isinstance(market, dict) else None


def market_history_rebuild_required(progress: dict[str, Any]) -> bool:
    """Return whether the unverified target still requires the coarse-layout reset."""

    market = _market_report(progress)
    if market is None or market.get("verified") is True:
        return False
    high_water = market.get("high_water_primary_key")
    if not isinstance(high_water, list) or not high_water:
        return False
    if market.get("market_history_rebuild_pending") is True:
        return True
    return market.get("local_history_layout") != MARKET_HISTORY_LAYOUT


def prepare_unverified_market_history_rebuild(
    progress_path: Path,
    *,
    market_history_root: Path | None = None,
) -> dict[str, object] | None:
    """Reset only the unverified local market target for a coarse-layout recopy.

    The captured source high-water and source inventory remain intact. All other table
    reports are left byte-for-byte equivalent as JSON values. A pending marker makes
    recursive deletion crash-resumable: if the service exits mid-delete, the next
    supervisor invocation continues the reset before launching the migration child.
    """

    try:
        progress = json.loads(progress_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    if not isinstance(progress, dict) or not market_history_rebuild_required(progress):
        return None
    market = _market_report(progress)
    if market is None:
        return None

    root = Path(market_history_root or local_storage_paths().market_history)
    inode_total_before, inode_free_before = _inode_capacity(root)
    previous_checkpoint = market.get("last_primary_key")
    high_water = market.get("high_water_primary_key")
    bootstrap_files_deleted = 0

    if market.get("market_history_rebuild_pending") is not True:
        if inode_free_before is not None and inode_free_before < BOOTSTRAP_FREE_INODES:
            bootstrap_files_deleted = _bootstrap_release_inodes(root)

        market["market_history_rebuild_pending"] = True
        market["market_history_rebuild_started_at"] = _now()
        market["market_history_rebuild_previous_last_primary_key"] = previous_checkpoint
        market["local_history_layout"] = MARKET_HISTORY_LAYOUT
        market["verified"] = False
        market.pop("last_primary_key", None)
        market.pop("destination_inventory", None)
        market.pop("verified_rows", None)
        market.pop("row_digest", None)
        market.pop("last_progress_at", None)
        progress["state"] = "running"
        progress["current_table"] = "market_quotes"
        progress.pop("completed_at", None)
        progress.pop("error_type", None)
        progress.pop("error", None)
        try:
            _atomic_write_json(progress_path, progress)
        except OSError:
            bootstrap_files_deleted += _bootstrap_release_inodes(
                root,
                minimum_free=BOOTSTRAP_FREE_INODES * 2,
                unlink_limit=BOOTSTRAP_UNLINK_LIMIT * 2,
            )
            _atomic_write_json(progress_path, progress)

    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    progress = json.loads(progress_path.read_text())
    market = _market_report(progress)
    if market is None:
        raise RuntimeError("market_quotes progress disappeared during history rebuild")
    market["market_history_rebuild_pending"] = False
    market["market_history_rebuild_completed_at"] = _now()
    market["local_history_layout"] = MARKET_HISTORY_LAYOUT
    market["verified"] = False
    market.pop("last_primary_key", None)
    market.pop("destination_inventory", None)
    progress["state"] = "running"
    progress["current_table"] = "market_quotes"
    progress.pop("completed_at", None)
    progress.pop("error_type", None)
    progress.pop("error", None)
    _atomic_write_json(progress_path, progress)

    inode_total_after, inode_free_after = _inode_capacity(root)
    return {
        "state": "complete",
        "reason": "unverified_market_history_rebuilt_for_coarse_layout",
        "started_at": market.get("market_history_rebuild_started_at"),
        "completed_at": market.get("market_history_rebuild_completed_at"),
        "inode_total_before": inode_total_before,
        "inode_free_before": inode_free_before,
        "inode_total_after": inode_total_after,
        "inode_free_after": inode_free_after,
        "bootstrap_files_deleted": bootstrap_files_deleted,
        "preserved_high_water_primary_key": high_water,
        "previous_last_primary_key": previous_checkpoint,
        "local_history_layout": MARKET_HISTORY_LAYOUT,
    }


class InodeRecoveryPartitionedMarketHistory(PartitionedMarketHistory):
    """Partitioned history specialization for production-scale inode recovery."""

    def _reap_compaction_garbage(self) -> int:
        with self._connect() as db:
            rows = list(
                db.execute(
                    "SELECT garbage.path, partitions.path "
                    "FROM compaction_garbage AS garbage "
                    "LEFT JOIN partitions ON partitions.path = garbage.path"
                )
            )
            live = [str(path) for path, partition_path in rows if partition_path is not None]
            if live:
                db.executemany(
                    "DELETE FROM compaction_garbage WHERE path = ?",
                    [(path,) for path in live],
                )
        removable = [str(path) for path, partition_path in rows if partition_path is None]
        cleared: list[str] = []
        for relative in removable:
            try:
                (self.root / relative).unlink()
            except FileNotFoundError:
                pass
            except OSError:
                continue
            cleared.append(relative)
        if cleared:
            with self._connect() as db:
                db.executemany(
                    "DELETE FROM compaction_garbage WHERE path = ?",
                    [(relative,) for relative in cleared],
                )
        return len(cleared)


__all__ = [
    "BOOTSTRAP_FREE_INODES",
    "InodeRecoveryPartitionedMarketHistory",
    "MARKET_HISTORY_LAYOUT",
    "market_history_rebuild_required",
    "prepare_unverified_market_history_rebuild",
]

from __future__ import annotations

from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory


class InodeRecoveryPartitionedMarketHistory(PartitionedMarketHistory):
    """Partitioned history specialization for production-scale inode recovery.

    The normal compaction garbage set is intentionally tiny because steady-state
    compaction is bounded per logical group. The one-time recovery can retire very
    large fragment sets, so it must not open one SQLite connection per retired file.
    """

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


__all__ = ["InodeRecoveryPartitionedMarketHistory"]

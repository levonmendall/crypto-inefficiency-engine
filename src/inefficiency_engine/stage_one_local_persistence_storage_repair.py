from __future__ import annotations

from typing import Any

from inefficiency_engine import postgres_local_migration as migration
from inefficiency_engine import stage_one_local_persistence_migration as stage_one


MARKET_QUOTES_MIGRATION_MODE = "captured_primary_key_high_water"
MARKET_QUOTES_RESUME_BATCH_SIZE = 10_000
_ORIGINAL_MIGRATE = migration.migrate


def _market_quotes_resume_checkpoint(progress: dict[str, Any]) -> bool:
    """Return whether durable state proves the final market_quotes resume is isolated."""

    if str(progress.get("current_table") or "") != "market_quotes":
        return False
    tables = progress.get("tables")
    if not isinstance(tables, dict) or not tables:
        return False
    market = tables.get("market_quotes")
    if not isinstance(market, dict) or market.get("verified") is True:
        return False
    if market.get("migration_mode") != MARKET_QUOTES_MIGRATION_MODE:
        return False
    for table_name, table_report in tables.items():
        if table_name == "market_quotes":
            continue
        if not isinstance(table_report, dict) or table_report.get("verified") is not True:
            return False
    checkpoint = market.get("last_primary_key")
    high_water = market.get("high_water_primary_key")
    return bool(
        isinstance(checkpoint, list)
        and checkpoint
        and isinstance(high_water, list)
        and high_water
    )


def _storage_repaired_migrate(
    source_url: str,
    *,
    batch_size: int = migration.BATCH_SIZE,
) -> dict[str, object]:
    """Use a larger batch only for the isolated final market-history resume.

    The original 2,000-row source batch can touch many venue/asset/day partitions and
    therefore create many small immutable Parquet files. The larger batch is enabled
    only when every other durable table report is already verified and market_quotes
    retains its captured high-water plus keyset checkpoint. Fresh or mixed-table
    migrations keep the canonical base batch size.
    """

    progress = migration._load_progress(migration._progress_path())
    effective_batch_size = int(batch_size)
    if _market_quotes_resume_checkpoint(progress):
        effective_batch_size = max(effective_batch_size, MARKET_QUOTES_RESUME_BATCH_SIZE)
    return _ORIGINAL_MIGRATE(source_url, batch_size=effective_batch_size)


def install_storage_capacity_repair() -> None:
    stage_one.install_stage_one_repair()
    migration.migrate = _storage_repaired_migrate


def main() -> int:
    install_storage_capacity_repair()
    try:
        return migration.main()
    finally:
        migration.migrate = _ORIGINAL_MIGRATE


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "MARKET_QUOTES_RESUME_BATCH_SIZE",
    "install_storage_capacity_repair",
    "main",
]

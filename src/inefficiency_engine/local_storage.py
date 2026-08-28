from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path


DEFAULT_PRODUCTION_STORAGE_ROOT = "/var/data/cie"


def production_storage_root() -> Path:
    """Return the canonical durable root, never a temporary/spool directory."""

    configured = os.getenv("CIE_STORAGE_ROOT", "").strip()
    root = Path(configured or DEFAULT_PRODUCTION_STORAGE_ROOT).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


@dataclass(frozen=True)
class LocalStoragePaths:
    root: Path
    metadata_db: Path
    market_history: Path
    migration: Path
    spool: Path


def local_storage_paths(root: str | Path | None = None) -> LocalStoragePaths:
    durable_root = Path(root).expanduser().resolve() if root else production_storage_root()
    paths = LocalStoragePaths(
        root=durable_root,
        metadata_db=durable_root / "metadata" / "cie.sqlite3",
        market_history=durable_root / "history" / "market_quotes",
        migration=durable_root / "migration",
        spool=Path(os.getenv("CIE_SPOOL_ROOT", "/tmp/cie-spool")).expanduser().resolve(),
    )
    for directory in (
        paths.metadata_db.parent,
        paths.market_history,
        paths.migration,
        paths.spool,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    return paths


_SAFE_COMPONENT = re.compile(r"[^A-Za-z0-9._-]+")


def safe_partition_component(value: str) -> str:
    normalized = _SAFE_COMPONENT.sub("_", value.strip())[:80]
    digest = hashlib.sha256(value.encode()).hexdigest()[:10]
    return f"{normalized or 'unknown'}-{digest}"


__all__ = [
    "DEFAULT_PRODUCTION_STORAGE_ROOT",
    "LocalStoragePaths",
    "local_storage_paths",
    "production_storage_root",
    "safe_partition_component",
]

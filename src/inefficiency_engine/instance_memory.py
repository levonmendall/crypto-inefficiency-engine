from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


MIB = 1024.0 * 1024.0
DEFAULT_SOFT_RATIO = 0.70
DEFAULT_START_BLOCK_RATIO = 0.775
DEFAULT_TERMINATE_RATIO = 0.825


@dataclass(frozen=True)
class InstanceMemorySnapshot:
    usage_mb: float | None
    limit_mb: float | None
    soft_mb: float | None
    start_block_mb: float | None
    terminate_mb: float | None
    source: str

    @property
    def soft_exceeded(self) -> bool:
        return bool(self.usage_mb is not None and self.soft_mb is not None and self.usage_mb >= self.soft_mb)

    @property
    def start_blocked(self) -> bool:
        return bool(
            self.usage_mb is not None
            and self.start_block_mb is not None
            and self.usage_mb >= self.start_block_mb
        )

    @property
    def terminate_required(self) -> bool:
        return bool(
            self.usage_mb is not None
            and self.terminate_mb is not None
            and self.usage_mb >= self.terminate_mb
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "usage_mb": self.usage_mb,
            "limit_mb": self.limit_mb,
            "soft_mb": self.soft_mb,
            "start_block_mb": self.start_block_mb,
            "terminate_mb": self.terminate_mb,
            "source": self.source,
            "soft_exceeded": self.soft_exceeded,
            "start_blocked": self.start_blocked,
            "terminate_required": self.terminate_required,
        }


def _read_int(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw or raw == "max":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _cgroup_v2_paths() -> tuple[Path, Path]:
    direct = (Path("/sys/fs/cgroup/memory.current"), Path("/sys/fs/cgroup/memory.max"))
    if direct[0].exists():
        return direct
    try:
        rows = Path("/proc/self/cgroup").read_text(encoding="utf-8").splitlines()
    except OSError:
        return direct
    for row in rows:
        parts = row.split(":", 2)
        if len(parts) == 3 and parts[0] == "0" and parts[1] == "":
            relative = parts[2].lstrip("/")
            base = Path("/sys/fs/cgroup") / relative
            return base / "memory.current", base / "memory.max"
    return direct


def _cgroup_v1_paths() -> tuple[Path, Path]:
    direct = (
        Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    )
    if direct[0].exists():
        return direct
    try:
        rows = Path("/proc/self/cgroup").read_text(encoding="utf-8").splitlines()
    except OSError:
        return direct
    for row in rows:
        parts = row.split(":", 2)
        if len(parts) != 3:
            continue
        controllers = set(parts[1].split(","))
        if "memory" not in controllers:
            continue
        relative = parts[2].lstrip("/")
        base = Path("/sys/fs/cgroup/memory") / relative
        return base / "memory.usage_in_bytes", base / "memory.limit_in_bytes"
    return direct


def _bounded_ratio(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return min(0.95, max(0.10, value))


def _threshold(name: str, limit_mb: float, default_ratio: float) -> float:
    raw = os.getenv(name)
    if raw:
        try:
            explicit = float(raw)
        except ValueError:
            explicit = 0.0
        if explicit > 0:
            return min(limit_mb * 0.95, explicit)
    return limit_mb * _bounded_ratio(f"{name}_RATIO", default_ratio)


def instance_memory_snapshot() -> InstanceMemorySnapshot:
    """Read aggregate service/container memory from cgroups when available.

    Render enforces memory at the service/container boundary, not per Python process.
    Cgroup usage therefore captures API + portfolio + disposable heavy children in one
    number. If cgroups are unavailable, the snapshot is intentionally unavailable
    rather than pretending process RSS represents the whole service.
    """

    for source, paths in (("cgroup_v2", _cgroup_v2_paths()), ("cgroup_v1", _cgroup_v1_paths())):
        usage_bytes = _read_int(paths[0])
        limit_bytes = _read_int(paths[1])
        if usage_bytes is None or limit_bytes is None or limit_bytes <= 0:
            continue
        usage_mb = usage_bytes / MIB
        limit_mb = limit_bytes / MIB
        # Some cgroup-v1 hosts expose an effectively-unlimited sentinel. Treat it as
        # unavailable instead of deriving meaningless thresholds from exabytes.
        if limit_mb > 1024.0 * 1024.0:
            continue
        soft = _threshold("CIE_INSTANCE_MEMORY_SOFT_MB", limit_mb, DEFAULT_SOFT_RATIO)
        start_block = _threshold(
            "CIE_INSTANCE_MEMORY_START_BLOCK_MB", limit_mb, DEFAULT_START_BLOCK_RATIO
        )
        terminate = _threshold(
            "CIE_INSTANCE_MEMORY_TERMINATE_MB", limit_mb, DEFAULT_TERMINATE_RATIO
        )
        start_block = max(soft, start_block)
        terminate = max(start_block, terminate)
        return InstanceMemorySnapshot(
            usage_mb=usage_mb,
            limit_mb=limit_mb,
            soft_mb=soft,
            start_block_mb=start_block,
            terminate_mb=terminate,
            source=source,
        )

    return InstanceMemorySnapshot(
        usage_mb=None,
        limit_mb=None,
        soft_mb=None,
        start_block_mb=None,
        terminate_mb=None,
        source="unavailable",
    )

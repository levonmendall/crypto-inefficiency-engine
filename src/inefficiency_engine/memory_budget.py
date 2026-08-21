from __future__ import annotations

import os
import resource
import sys


DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB = 1536.0


class MemoryBudgetDeferred(RuntimeError):
    """Optional research work was deferred to preserve process availability."""


def current_rss_mb() -> float | None:
    """Best-effort current resident set size without adding a runtime dependency."""
    try:
        with open("/proc/self/statm", "r", encoding="utf-8") as handle:
            fields = handle.read().split()
        if len(fields) >= 2:
            page_size = os.sysconf("SC_PAGE_SIZE")
            return float(int(fields[1]) * int(page_size)) / (1024.0 * 1024.0)
    except (OSError, ValueError, TypeError):
        return None
    return None


def max_rss_mb() -> float | None:
    """Best-effort process high-water RSS in MiB."""
    try:
        value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (OSError, ValueError, TypeError):
        return None
    # Linux reports KiB; macOS reports bytes.
    divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
    return value / divisor


def memory_snapshot() -> dict[str, float | None]:
    return {
        "rss_mb": current_rss_mb(),
        "max_rss_mb": max_rss_mb(),
    }


def memory_budget_exceeded(limit_mb: float) -> bool:
    rss = current_rss_mb()
    return bool(rss is not None and rss >= max(1.0, float(limit_mb)))

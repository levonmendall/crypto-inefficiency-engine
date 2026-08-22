from __future__ import annotations

import os


BYBIT_PUBLIC_ENABLED_ENV = "CIE_BYBIT_PUBLIC_ENABLED"

_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}


def env_flag(name: str, *, default: bool = True) -> bool:
    """Return a conservative boolean environment flag.

    Unknown non-empty values preserve the supplied default rather than silently
    enabling a provider. This helper controls network routing only; it never changes
    source, statistical, qualification, allocation, settlement or execution gates.
    """

    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    value = raw.strip().lower()
    if value in _FALSE_VALUES:
        return False
    if value in _TRUE_VALUES:
        return True
    return bool(default)


def bybit_public_enabled() -> bool:
    """Whether the runtime may use Bybit public-data surfaces."""

    return env_flag(BYBIT_PUBLIC_ENABLED_ENV, default=True)

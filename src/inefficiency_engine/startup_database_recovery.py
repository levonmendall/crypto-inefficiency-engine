from __future__ import annotations

import time
from typing import Any

from sqlalchemy.exc import OperationalError


STARTUP_DATABASE_RECOVERY_DEADLINE_SECONDS = 300.0
STARTUP_DATABASE_RECOVERY_RETRY_SECONDS = 5.0
_PATCH_MARKER = "_cie_startup_database_recovery_installed"
_POSTGRES_RECOVERY_MARKERS = (
    "database system is in recovery mode",
    "database system is starting up",
    "cannot connect now",
)


def is_transient_postgres_recovery_error(exc: OperationalError) -> bool:
    """Recognize only PostgreSQL startup/recovery errors proven safe to retry."""

    message = str(exc).lower()
    return any(marker in message for marker in _POSTGRES_RECOVERY_MARKERS)


def _dispose_store(store: Any | None) -> None:
    if store is None:
        return
    engine = getattr(store, "engine", None)
    dispose = getattr(engine, "dispose", None)
    if callable(dispose):
        try:
            dispose()
        except Exception:
            pass


def install_startup_database_recovery(postbind_module: Any) -> None:
    """Make only the serialized pre-child schema bootstrap recovery-aware.

    Render can start a web service while the attached PostgreSQL instance is briefly in
    crash/recovery/startup mode. The canonical post-bind entrypoint intentionally performs
    schema bootstrap before any permanent child starts; a transient connection failure at
    that exact boundary must therefore wait for PostgreSQL instead of crashing the deploy.

    Retry is deliberately narrow and bounded. Authentication, schema, programming, and
    other operational errors still fail immediately. Every retry reconstructs the store
    and disposes the failed engine so a dead pooled connection is never inherited.
    """

    if bool(getattr(postbind_module, _PATCH_MARKER, False)):
        return

    def recovered_bootstrap() -> None:
        settings = postbind_module.base.Settings.from_env()
        deadline = time.monotonic() + STARTUP_DATABASE_RECOVERY_DEADLINE_SECONDS
        attempt = 0

        while True:
            attempt += 1
            store = None
            try:
                store = postbind_module.base.build_evidence_store(settings.evidence_db_path)
                if store is None:
                    raise RuntimeError("combined runtime requires durable evidence persistence")
                postbind_module.base._build_control_services(settings, store)
                postbind_module.ensure_durable_control_cache_schema(store)
                print(
                    "permanent runtime schema bootstrap complete before child startup: "
                    f"{store.safe_database_url}; attempt={attempt}",
                    flush=True,
                )
                return
            except OperationalError as exc:
                _dispose_store(store)
                if not is_transient_postgres_recovery_error(exc):
                    raise

                remaining = max(0.0, deadline - time.monotonic())
                if remaining <= 0.0:
                    print(
                        "PostgreSQL remained in startup/recovery state through the bounded "
                        f"{STARTUP_DATABASE_RECOVERY_DEADLINE_SECONDS:.0f}s bootstrap window; "
                        "failing closed",
                        flush=True,
                    )
                    raise

                retry_seconds = min(STARTUP_DATABASE_RECOVERY_RETRY_SECONDS, remaining)
                print(
                    "PostgreSQL is temporarily in startup/recovery state during serialized "
                    f"schema bootstrap; attempt={attempt}; retrying in {retry_seconds:.1f}s",
                    flush=True,
                )
                time.sleep(retry_seconds)

    postbind_module.bootstrap_permanent_runtime_schema = recovered_bootstrap
    setattr(postbind_module, _PATCH_MARKER, True)


__all__ = [
    "STARTUP_DATABASE_RECOVERY_DEADLINE_SECONDS",
    "STARTUP_DATABASE_RECOVERY_RETRY_SECONDS",
    "install_startup_database_recovery",
    "is_transient_postgres_recovery_error",
]

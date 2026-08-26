from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import inspect, text


CONTROL_GATE_INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "market_quotes": ("venue", "observed_at"),
    "funding_quotes": ("venue", "observed_at"),
    "order_books": ("venue", "observed_at"),
    "opportunities": ("observed_at",),
    "provider_statuses": ("provider", "id"),
    "source_coverage_observations": ("source_id", "lane_id", "id"),
    "provider_gap_admissions": ("mechanism_id", "provider", "id"),
}

# Canonical cycle-history bootstrap reads one venue/asset/day from the append-only
# market quote ledger, then retains the newest source ids. Keep the existing
# venue/observed_at source-read index above and add this second, purpose-built index as
# a separate control-gate scope because one dict cannot represent two indexes for the
# same table. Canonical control must not start until this planner-usable access path is
# available.
CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "market_quotes": ("venue", "asset", "observed_at", "id"),
}

# These indexes improve bounded read paths but are not prerequisites for starting
# canonical control. In particular, the maker/transfer ledgers can exist in legacy
# production databases with an older schema. Their absence or schema drift must stay
# fail-closed for those individual evidence sources without freezing the whole control
# plane behind optional index DDL.
BACKGROUND_INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "maker_shadow_outcomes": ("observed_at",),
    "capital_transfer_outcomes": ("observed_at",),
    "alpha_forward_events": ("event_type", "strategy_id", "family"),
    "allocation_forward_trials": ("strategy", "settlement_supported", "id"),
    "allocation_forward_outcomes": ("strategy", "id"),
}

INDEX_SPECS: dict[str, tuple[str, ...]] = {
    **CONTROL_GATE_INDEX_SPECS,
    **BACKGROUND_INDEX_SPECS,
}

ProgressCallback = Callable[[dict[str, object]], None]

# Runtime index maintenance is deliberately outside API startup. Keep ordinary
# post-control optimization DDL short, but give the one exact cycle-history access path
# enough bounded time to complete on the small production PostgreSQL instance. API
# liveness remains independent while this build runs.
POSTGRES_INDEX_STATEMENT_TIMEOUT_MS = 30_000
CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS = 120_000
POSTGRES_INDEX_LOCK_TIMEOUT_MS = 5_000
POSTGRES_IDENTIFIER_MAX_BYTES = 63

# Interrupted CREATE INDEX CONCURRENTLY statements can leave invalid catalog entries.
# Replacement names are intentionally dynamic rather than a fixed _v2/_v3/_v4 set:
# every retry discovers already-used versions from pg_catalog and advances to a fresh
# deterministic suffix. Invalid predecessors are never dropped on the authority path.
_REPLACEMENT_VERSION_RE = re.compile(r"_v(?P<version>(?:[2-9]|[1-9][0-9]+))$")


class RuntimeIndexVerificationError(RuntimeError):
    """Raised when PostgreSQL cannot certify a required runtime index as usable."""


def _index_name(table_name: str, columns: tuple[str, ...]) -> str:
    return f"ix_runtime_{table_name}_{'_'.join(columns)}"


def _postgres_canonical_index_name(index_name: str) -> str:
    """Return the physical canonical name PostgreSQL already uses.

    PostgreSQL truncates unquoted identifiers to 63 bytes. Runtime index names are
    ASCII, so matching that truncation explicitly lets catalog verification address the
    same physical relation that older releases may already have created.
    """

    return index_name[:POSTGRES_IDENTIFIER_MAX_BYTES]


def _postgres_replacement_index_name(index_name: str, version: int) -> str:
    """Build a deterministic versioned name whose suffix survives PostgreSQL truncation.

    Older code appended ``_vN`` to names that were already near 63 bytes. PostgreSQL
    silently truncated the suffix, so every retry could target the same relation and
    surface a repeated ``ProgrammingError``. Preserve short historical names exactly;
    for long names reserve room for a compact hash plus the version suffix.
    """

    canonical = _postgres_canonical_index_name(index_name)
    suffix = f"_v{int(version)}"
    direct = f"{canonical}{suffix}"
    if len(direct) <= POSTGRES_IDENTIFIER_MAX_BYTES:
        return direct
    digest = hashlib.sha1(canonical.encode("ascii")).hexdigest()[:8]
    reserved = len(digest) + len(suffix) + 1
    prefix_length = max(1, POSTGRES_IDENTIFIER_MAX_BYTES - reserved)
    return f"{canonical[:prefix_length]}_{digest}{suffix}"


def _create_index_sql(
    *,
    dialect_name: str,
    index_name: str,
    table_name: str,
    columns: tuple[str, ...],
    if_not_exists: bool = True,
) -> str:
    concurrent = " CONCURRENTLY" if dialect_name == "postgresql" else ""
    existence = " IF NOT EXISTS" if if_not_exists else ""
    return (
        f"CREATE INDEX{concurrent}{existence} {index_name} "
        f"ON {table_name} ({','.join(columns)})"
    )


def _postgres_index_state(db: Any, *, index_name: str) -> dict[str, bool] | None:
    """Return PostgreSQL planner-usable state for one index in the active search path."""

    row = (
        db.execute(
            text(
                """
                SELECT i.indisvalid AS valid, i.indisready AS ready
                FROM pg_index AS i
                WHERE i.indexrelid = to_regclass(:index_name)
                """
            ),
            {"index_name": index_name},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return {
        "valid": bool(row.get("valid")),
        "ready": bool(row.get("ready")),
    }


def _postgres_index_is_usable(state: dict[str, bool] | None) -> bool:
    return bool(state is not None and state.get("valid") and state.get("ready"))


def _replacement_version(index_name: str, replacement_name: str) -> int | None:
    match = _REPLACEMENT_VERSION_RE.search(replacement_name)
    if match is None:
        return None
    version = int(match.group("version"))
    return (
        version
        if replacement_name == _postgres_replacement_index_name(index_name, version)
        else None
    )


def _postgres_replacement_index_states(
    db: Any,
    *,
    index_name: str,
) -> dict[str, dict[str, bool]]:
    """Return existing versioned replacements for one canonical runtime index.

    Long replacements may contain a compact hash before ``_vN`` so the version suffix
    survives PostgreSQL's 63-byte identifier limit. Query by a conservative literal
    canonical prefix, then accept only names that exactly match the deterministic naming
    function for their parsed version.
    """

    canonical = _postgres_canonical_index_name(index_name)
    prefix = canonical[: min(len(canonical), 40)]
    rows = (
        db.execute(
            text(
                """
                SELECT c.relname AS name, i.indisvalid AS valid, i.indisready AS ready
                FROM pg_class AS c
                JOIN pg_index AS i ON i.indexrelid = c.oid
                WHERE left(c.relname, char_length(:prefix)) = :prefix
                """
            ),
            {"prefix": prefix},
        )
        .mappings()
        .all()
    )
    states: dict[str, dict[str, bool]] = {}
    for row in rows:
        name = str(row.get("name") or "")
        if _replacement_version(canonical, name) is None:
            continue
        states[name] = {
            "valid": bool(row.get("valid")),
            "ready": bool(row.get("ready")),
        }
    return states


def _next_replacement_index_name(
    *,
    index_name: str,
    existing_states: Mapping[str, dict[str, bool]],
) -> str:
    versions = [
        version
        for name in existing_states
        if (version := _replacement_version(index_name, name)) is not None
    ]
    next_version = max(versions, default=1) + 1
    return _postgres_replacement_index_name(index_name, next_version)


def _usable_replacement_index_name(
    *,
    index_name: str,
    existing_states: Mapping[str, dict[str, bool]],
) -> str | None:
    usable: list[tuple[int, str]] = []
    for name, state in existing_states.items():
        version = _replacement_version(index_name, name)
        if version is not None and _postgres_index_is_usable(state):
            usable.append((version, name))
    if not usable:
        return None
    # Prefer the newest valid replacement so recovery stays monotonic across retries.
    return max(usable)[1]


def _statement_timeout_for_index(
    *,
    table_name: str,
    columns: tuple[str, ...],
) -> int:
    if (
        table_name == "market_quotes"
        and columns == CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS["market_quotes"]
    ):
        return CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    return POSTGRES_INDEX_STATEMENT_TIMEOUT_MS


def _configure_postgres_index_deadlines(
    db: Any,
    *,
    statement_timeout_ms: int,
) -> None:
    """Apply finite session deadlines before any runtime-index DDL is issued."""

    db.execute(text(f"SET statement_timeout TO '{statement_timeout_ms}ms'"))
    db.execute(text(f"SET lock_timeout TO '{POSTGRES_INDEX_LOCK_TIMEOUT_MS}ms'"))


def _verified_index_result(
    *,
    canonical_index_name: str,
    effective_index_name: str,
    repaired_invalid_index: bool,
    existing_index_reused: bool,
    ddl_required: bool,
    deferred_invalid_index_name: str | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "postgres_index_valid": True,
        "postgres_index_ready": True,
        "repaired_invalid_index": repaired_invalid_index,
        "existing_index_reused": existing_index_reused,
        "ddl_required": ddl_required,
        "canonical_index_name": canonical_index_name,
        "effective_index_name": effective_index_name,
        "replacement_index_used": effective_index_name != canonical_index_name,
    }
    if deferred_invalid_index_name is not None:
        result.update(
            {
                "invalid_index_cleanup_deferred": True,
                "deferred_invalid_index_name": deferred_invalid_index_name,
            }
        )
    return result


def _ensure_postgres_index(
    db: Any,
    *,
    index_name: str,
    table_name: str,
    columns: tuple[str, ...],
    statement_timeout_ms: int | None = None,
) -> dict[str, object]:
    """Verify first, then create or self-heal one PostgreSQL runtime index.

    A planner-usable existing index is returned immediately without issuing DDL.

    Missing indexes are built under finite statement/lock deadlines. If an interrupted
    concurrent build left the canonical name invalid, do not DROP it on the control-gate
    path. Discover every existing ``_vN`` replacement in pg_catalog, reuse the newest
    planner-usable one, or create the next unused version. Replacement names explicitly
    reserve room for the version suffix so PostgreSQL identifier truncation cannot turn
    repeated repairs into duplicate-relation ProgrammingErrors.

    A valid replacement has identical columns and therefore satisfies the required access
    path. Obsolete invalid catalog objects remain explicitly deferred for later fail-soft
    reclamation after canonical control is operating.
    """

    index_name = _postgres_canonical_index_name(index_name)
    state = _postgres_index_state(db, index_name=index_name)
    if _postgres_index_is_usable(state):
        return _verified_index_result(
            canonical_index_name=index_name,
            effective_index_name=index_name,
            repaired_invalid_index=False,
            existing_index_reused=True,
            ddl_required=False,
        )

    effective_timeout_ms = (
        statement_timeout_ms
        if statement_timeout_ms is not None
        else _statement_timeout_for_index(table_name=table_name, columns=columns)
    )
    _configure_postgres_index_deadlines(
        db,
        statement_timeout_ms=effective_timeout_ms,
    )

    if state is not None:
        replacement_states = _postgres_replacement_index_states(
            db,
            index_name=index_name,
        )
        reusable_name = _usable_replacement_index_name(
            index_name=index_name,
            existing_states=replacement_states,
        )
        if reusable_name is not None:
            result = _verified_index_result(
                canonical_index_name=index_name,
                effective_index_name=reusable_name,
                repaired_invalid_index=True,
                existing_index_reused=True,
                ddl_required=False,
                deferred_invalid_index_name=index_name,
            )
            result.update(
                {
                    "postgres_statement_timeout_ms": effective_timeout_ms,
                    "postgres_lock_timeout_ms": POSTGRES_INDEX_LOCK_TIMEOUT_MS,
                    "replacement_versions_observed": len(replacement_states),
                }
            )
            return result

        replacement_name = _next_replacement_index_name(
            index_name=index_name,
            existing_states=replacement_states,
        )
        create_statement = _create_index_sql(
            dialect_name="postgresql",
            index_name=replacement_name,
            table_name=table_name,
            columns=columns,
            if_not_exists=False,
        )
        db.execute(text(create_statement))
        replacement_state = _postgres_index_state(
            db,
            index_name=replacement_name,
        )
        if not _postgres_index_is_usable(replacement_state):
            raise RuntimeIndexVerificationError(
                "PostgreSQL replacement runtime index "
                f"{replacement_name} remains invalid or unready after repair"
            )
        result = _verified_index_result(
            canonical_index_name=index_name,
            effective_index_name=replacement_name,
            repaired_invalid_index=True,
            existing_index_reused=False,
            ddl_required=True,
            deferred_invalid_index_name=index_name,
        )
        result.update(
            {
                "postgres_statement_timeout_ms": effective_timeout_ms,
                "postgres_lock_timeout_ms": POSTGRES_INDEX_LOCK_TIMEOUT_MS,
                "replacement_versions_observed": len(replacement_states),
            }
        )
        return result

    create_statement = _create_index_sql(
        dialect_name="postgresql",
        index_name=index_name,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    db.execute(text(create_statement))
    state = _postgres_index_state(db, index_name=index_name)

    if state is None:
        raise RuntimeIndexVerificationError(
            f"PostgreSQL runtime index {index_name} is missing after maintenance"
        )
    if not _postgres_index_is_usable(state):
        raise RuntimeIndexVerificationError(
            f"PostgreSQL runtime index {index_name} remains invalid or unready after repair"
        )

    result = _verified_index_result(
        canonical_index_name=index_name,
        effective_index_name=index_name,
        repaired_invalid_index=False,
        existing_index_reused=False,
        ddl_required=True,
    )
    result.update(
        {
            "postgres_statement_timeout_ms": effective_timeout_ms,
            "postgres_lock_timeout_ms": POSTGRES_INDEX_LOCK_TIMEOUT_MS,
        }
    )
    return result


def ensure_runtime_indexes_after_api_bind(
    store: Any,
    *,
    index_specs: Mapping[str, tuple[str, ...]] | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Create bounded-read indexes outside the web-service startup critical path.

    Production PostgreSQL uses ``CREATE INDEX CONCURRENTLY`` in autocommit mode so
    multimillion-row index builds do not hold the normal table-write lock or prevent
    Render from binding the API port. PostgreSQL index existence alone is insufficient:
    every maintained index is verified as planner-usable through ``pg_index``. Invalid
    leftovers from interrupted concurrent builds are recovered through a verified,
    same-column dynamically versioned replacement instead of making the control gate
    depend on a potentially lock-blocked DROP.

    The exact cycle-history index gets a longer but still finite statement deadline than
    optional optimization indexes because production telemetry proved 30 seconds could
    leave repeated invalid concurrent-build artifacts on the small PostgreSQL instance.
    SQLite and test stores keep ordinary idempotent index creation.

    The helper validates each table's actual deployed columns before issuing DDL.
    Missing columns remain a hard failure for control-gate indexes, but background
    optimization indexes are terminally skipped. This makes legacy auxiliary schema
    drift observable without turning it into system-wide control unavailability.
    """

    requested = dict(index_specs or INDEX_SPECS)
    inspector = inspect(store.engine)
    available = set(inspector.get_table_names())
    dialect_name = str(getattr(store.engine.dialect, "name", ""))
    attempted: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []

    for table_name, columns in requested.items():
        if table_name not in available:
            continue
        logical_index_name = _index_name(table_name, columns)
        index_name = (
            _postgres_canonical_index_name(logical_index_name)
            if dialect_name == "postgresql"
            else logical_index_name
        )
        started = time.monotonic()
        actual_columns = {
            str(row.get("name"))
            for row in inspector.get_columns(table_name)
            if row.get("name") is not None
        }
        missing_columns = [column for column in columns if column not in actual_columns]
        if missing_columns:
            row = {
                "index": index_name,
                "logical_index": logical_index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": False,
                "error_type": "SchemaColumnMissing",
                "message": (
                    f"deployed table {table_name} is missing runtime-index columns: "
                    + ",".join(missing_columns)
                ),
                "missing_columns": missing_columns,
                "schema_compatible": False,
            }
            attempted.append(row)
            if table_name in BACKGROUND_INDEX_SPECS:
                skipped_row = {**row, "skipped": True, "optional": True}
                skipped.append(skipped_row)
                if progress is not None:
                    progress({"phase": "skipped", **skipped_row})
                continue
            failures.append(row)
            if progress is not None:
                progress({"phase": "failed", **row})
            continue

        statement = _create_index_sql(
            dialect_name=dialect_name,
            index_name=index_name,
            table_name=table_name,
            columns=columns,
        )
        if progress is not None:
            progress(
                {
                    "phase": "starting",
                    "index": index_name,
                    "logical_index": logical_index_name,
                    "table": table_name,
                    "concurrent": dialect_name == "postgresql",
                    "schema_compatible": True,
                }
            )
        try:
            postgres_state: dict[str, object] = {}
            if dialect_name == "postgresql":
                with store.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as db:
                    postgres_state = _ensure_postgres_index(
                        db,
                        index_name=index_name,
                        table_name=table_name,
                        columns=columns,
                        statement_timeout_ms=_statement_timeout_for_index(
                            table_name=table_name,
                            columns=columns,
                        ),
                    )
            else:
                with store.engine.begin() as db:
                    db.execute(text(statement))
            row = {
                "index": index_name,
                "logical_index": logical_index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": True,
                "schema_compatible": True,
                **postgres_state,
            }
            attempted.append(row)
            if progress is not None:
                progress({"phase": "complete", **row})
        except Exception as exc:
            failure = {
                "index": index_name,
                "logical_index": logical_index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": False,
                "error_type": type(exc).__name__,
                "message": str(exc)[:500],
                "schema_compatible": True,
            }
            attempted.append(failure)
            failures.append(failure)
            if progress is not None:
                progress({"phase": "failed", **failure})

    return {
        "complete": not failures,
        "dialect": dialect_name,
        "attempted": attempted,
        "failures": failures,
        "skipped": skipped,
        "requested_tables": list(requested),
        "startup_critical_path": False,
        "api_bound_before_maintenance": True,
        "postgres_index_validity_verified": dialect_name == "postgresql",
        "postgres_identifier_limit_bytes": (
            POSTGRES_IDENTIFIER_MAX_BYTES if dialect_name == "postgresql" else None
        ),
    }

from __future__ import annotations

from typing import Any, Callable

from sqlalchemy import func, select

from inefficiency_engine.source_coverage_catalog import SOURCES


PERMANENT_SOURCE_WORKER_ID = "canonical-source-operating-loop"
_PROJECTED_TABLES = {"market_quotes", "funding_quotes", "order_books"}
_INSTALL_MARKER = "_cie_current_source_scan_probe_runtime"
_SCAN_CACHE_ATTR = "_cie_current_source_scan_id"
_PROBE_CACHE_ATTR = "_cie_current_source_probe_cache"
_PROVIDER_CACHE_ATTR = "_cie_operational_provider_status_cache"
_MISSING = object()


def _catalog_provider_ids() -> tuple[str, ...]:
    """Return only provider ids that can contribute to the source-coverage contract."""

    return tuple(
        sorted(
            {
                str(provider)
                for source in SOURCES
                for provider in list(source.get("provider") or [])
                if str(provider)
            }
        )
    )


_CATALOG_PROVIDER_IDS = _catalog_provider_ids()


def _latest_successful_source_scan_id(store: Any) -> str | None:
    """Return the newest completed executable-source scan without touching quote history."""

    table = getattr(store, "worker_heartbeats", None)
    if table is None:
        return None
    try:
        query = (
            select(table.c.scan_id)
            .where(
                table.c.worker_id == PERMANENT_SOURCE_WORKER_ID,
                table.c.state == "success",
                table.c.scan_id.is_not(None),
            )
            .order_by(table.c.id.desc())
            .limit(1)
        )
        with store.engine.connect() as db:
            value = db.execute(query).scalar_one_or_none()
    except Exception:
        return None
    return str(value) if value not in (None, "") else None


def _source_scan_id(self: Any) -> str | None:
    cached = getattr(self, _SCAN_CACHE_ATTR, _MISSING)
    if cached is not _MISSING:
        return str(cached) if cached not in (None, "") else None
    value = _latest_successful_source_scan_id(self.store)
    setattr(self, _SCAN_CACHE_ATTR, value)
    return value


def _latest_catalog_provider_rows(
    self: Any,
    available: set[str],
) -> list[dict[str, object]]:
    """Read source-relevant provider status with bounded indexed point seeks.

    The generic reconciliation runtime previously grouped the entire append-only
    ``provider_statuses`` ledger on every 30-second source-coverage publication. In
    production that exact stage now consumes roughly 8-11 seconds by itself. Source
    coverage can only use provider ids named by ``SOURCES``, so seek the newest row for
    those ids directly through the existing ``(provider, id)`` runtime index instead.

    Instrument-specific L2 provider ids append an asset/symbol suffix and therefore do
    not have an exact catalog-id row. They are deliberately not recovered by scanning
    provider history here: their stronger executable-depth/order-book truth is already
    resolved from the current permanent-source scan by ``_current_source_scan_candidate``.
    Missing current table evidence therefore remains fail-closed.
    """

    if "provider_statuses" not in available:
        return []
    cached = getattr(self, _PROVIDER_CACHE_ATTR, _MISSING)
    if cached is not _MISSING:
        return [dict(row) for row in list(cached or [])]

    table = getattr(self.store, "provider_statuses", None)
    if table is None:
        setattr(self, _PROVIDER_CACHE_ATTR, [])
        return []

    rows: list[dict[str, object]] = []
    try:
        with self.store.engine.connect() as db:
            for provider in _CATALOG_PROVIDER_IDS:
                row = db.execute(
                    select(
                        table.c.provider,
                        table.c.ok,
                        table.c.item_count,
                        table.c.error_type,
                        table.c.observed_at,
                    )
                    .where(table.c.provider == provider)
                    .order_by(table.c.id.desc())
                    .limit(1)
                ).mappings().first()
                if row is not None:
                    rows.append(dict(row))
    except Exception:
        # Source reconciliation is diagnostic/fail-closed. A database read failure
        # must never create provider eligibility or fall back to an unbounded scan.
        rows = []

    setattr(self, _PROVIDER_CACHE_ATTR, rows)
    return [dict(row) for row in rows]


def _current_source_scan_candidate(
    self: Any,
    spec: dict[str, object],
    available: set[str],
) -> dict[str, object] | None:
    """Resolve executable market evidence from one bounded durable source scan.

    The append-only market/funding/L2 histories remain authoritative history, but
    operational source reconciliation must not sort those unbounded tables merely to
    find current truth. The permanent source worker already publishes the exact scan
    id of each completed executable cycle. Restricting the probe to that scan makes
    runtime independent of historical table size. Missing evidence in the current
    scan fails closed; there is deliberately no historical fallback.
    """

    probe = spec.get("table")
    if not isinstance(probe, tuple) or len(probe) != 3:
        return None
    table_name, column, value = probe
    if table_name not in _PROJECTED_TABLES:
        return None
    if table_name not in available or column != "venue" or value in (None, ""):
        return None

    cache = getattr(self, _PROBE_CACHE_ATTR, None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(self, _PROBE_CACHE_ATTR, cache)
    key = (str(table_name), str(column), str(value))
    if key in cache:
        cached = cache[key]
        return dict(cached) if isinstance(cached, dict) else None

    scan_id = _source_scan_id(self)
    if scan_id is None:
        cache[key] = None
        return None
    table = getattr(self.store, str(table_name), None)
    if table is None:
        cache[key] = None
        return None

    try:
        # scan_id is indexed on these evidence tables. MAX(observed_at) is exact
        # within the bounded current scan and avoids any global history sort.
        query = select(func.max(table.c.observed_at)).where(
            table.c.scan_id == scan_id,
            table.c.venue == str(value),
        )
        with self.store.engine.connect() as db:
            observed_at = db.execute(query).scalar_one_or_none()
    except Exception:
        cache[key] = None
        return None
    if observed_at in (None, ""):
        cache[key] = None
        return None

    candidate = {
        "healthy": True,
        "observed_at": observed_at,
        "item_count": 1,
        "classes": list(spec["classes"]),
        "authoritative": bool(spec.get("authoritative", True)),
        "commercial": True,
        "point_in_time": True,
        "source_reference": f"durable:current-source-scan:{scan_id}:{table_name}",
        "economic_fields_complete": True,
        "forward_testable_evidence": True,
    }
    cache[key] = candidate
    return dict(candidate)


def install_current_source_scan_probe_runtime() -> None:
    """Keep operational source probes bounded by current truth and indexed seeks."""

    from inefficiency_engine.source_coverage import SourceCoveragePlane

    if bool(getattr(SourceCoveragePlane, _INSTALL_MARKER, False)):
        return
    original: Callable[..., dict[str, object] | None] = SourceCoveragePlane._table_candidate

    def bounded_table_candidate(
        self: Any,
        spec: dict[str, object],
        available: set[str],
    ) -> dict[str, object] | None:
        probe = spec.get("table")
        if isinstance(probe, tuple) and len(probe) == 3 and probe[0] in _PROJECTED_TABLES:
            return _current_source_scan_candidate(self, spec, available)
        return original(self, spec, available)

    SourceCoveragePlane._provider_rows = _latest_catalog_provider_rows  # type: ignore[method-assign]
    SourceCoveragePlane._table_candidate = bounded_table_candidate  # type: ignore[method-assign]
    setattr(SourceCoveragePlane, _INSTALL_MARKER, True)

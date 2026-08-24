from __future__ import annotations


def main() -> int:
    """Compute and persist exactly one source-coverage snapshot without provider calls."""

    from inefficiency_engine.config import Settings
    from inefficiency_engine.durable_source_coverage_runtime import (
        SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    )
    from inefficiency_engine.evidence import build_evidence_store
    from inefficiency_engine.source_coverage import SourceCoveragePlane
    from inefficiency_engine.source_runtime_safety import (
        install_source_coverage_reconciliation_runtime,
    )

    install_source_coverage_reconciliation_runtime()
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("source coverage snapshot executor requires durable persistence")

    snapshot = SourceCoveragePlane(store).snapshot()
    heartbeat = store.latest_worker_heartbeat(SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
    if heartbeat is None:
        raise RuntimeError("source coverage snapshot publication was not persisted")
    detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
    if detail.get("snapshot_observed_at") != snapshot.observed_at.isoformat():
        raise RuntimeError("source coverage snapshot publication does not match calculation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

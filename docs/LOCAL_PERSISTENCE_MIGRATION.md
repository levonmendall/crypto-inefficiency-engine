# Local persistence migration architecture

## Decision

SQLite WAL plus partitioned Parquet is suitable for the consolidated Render Standard service. The workload has one application instance, moderate authoritative-state writes, append-heavy quote history, bounded range reads, paper-only execution, and no requirement for cross-host consensus. SQLite owns transactional metadata/control state; immutable Parquet files own high-volume quote history. SQLite `BEGIN IMMEDIATE`, a 30-second busy timeout, WAL, `synchronous=FULL`, atomic file rename, directory fsync, and a WAL manifest provide crash and multiprocess safety.

This is a staged migration. PostgreSQL compatibility remains in the package and the importer can read it through a migration-only secret. No PostgreSQL URL is committed. Certification stays fail-closed until 180-day partition coverage is verified.

## Production layout

| Responsibility | Durable location | Mechanism |
|---|---|---|
| Heartbeats, checkpoints, control and certification | `/var/data/cie/metadata/cie.sqlite3` | SQLAlchemy models on SQLite WAL |
| Portfolio, accounting, reconciliation and bridge state | `/var/data/cie/metadata/cie.sqlite3` | Existing transactional tables and authority boundaries |
| Current scan compatibility projection | `/var/data/cie/metadata/cie.sqlite3` | Existing `market_quotes` interface during staged cutover |
| Authoritative raw market history | `/var/data/cie/history/market_quotes/venue=*/asset=*/date=*/*.parquet` | Immutable Zstandard Parquet partitions |
| Partition visibility and deduplication | `history/market_quotes/manifest.sqlite3` | WAL manifest, unique lineage hash, monotonic history ID |
| Migration state | `/var/data/cie/migration/postgres-import-progress.json` | Atomic, explicit progress document |
| Temporary files | `/tmp/cie-spool` and hidden `*.tmp` files | Non-canonical; readers ignore them |

## PostgreSQL dependency audit and ownership map

| Category | Current owners | Migration treatment |
|---|---|---|
| Metadata/control | `evidence.py`, `durable_control_*`, `operating_*`, `allocation_certification.py`, `qualified_opportunity.py` | Existing SQLAlchemy tables run on canonical SQLite WAL |
| Heartbeat/telemetry | `bounded_heartbeat_runtime.py`, `runtime_heartbeat_snapshot_child.py`, worker/supervisor modules | Existing append-only heartbeat tables on SQLite; writes serialize in SQLite |
| Checkpoint | `durable_control_cache.py`, history/backfill/research supervisors, `heavy_work_lease.py` | SQLite transactions; no provider or allocation authority changes |
| Portfolio/accounting | `canonical_paper_portfolio.py`, `universal_paper_portfolio.py`, `resilient_paper_portfolio.py`, reconciliation/bridge modules | SQLite tables unchanged; paper-only boundaries preserved |
| Source history | `source_coverage_history.py` and migration/snapshot supervisors | SQLite append-only tables; PostgreSQL upsert has a SQLite-compatible path added during table-specific cutover |
| Raw market history | `evidence.py:market_quotes`, alpha/history readers | Atomic venue/asset/date Parquet plus manifest; current relational projection temporarily retained for interface compatibility |
| Cycle history | `cycle_probation.py`, `batched_cycle_history.py`, `durable_control_cycle_history*.py` | Existing separated 180-day source history remains; durable raw bucket reconstruction reads verified Parquet when enabled |
| Runtime-index-only | `runtime_index_maintenance.py`, `runtime_index_ddl_coordination_repair.py`, `cycle_history_*index*`, `cycle_history_brin_runtime.py` | Retained for PostgreSQL compatibility, but bypassed by the filesystem readiness gate in local production |

The repository-wide audit found PostgreSQL/runtime configuration references in 76 source, test, and deployment files. PostgreSQL-specific behavior is concentrated in:

- `coinbase_trade_flow.py` and `source_coverage_history.py`: `sqlalchemy.dialects.postgresql.insert` conflict handling.
- `cycle_probation.py`: PostgreSQL and SQLite conflict implementations selected by dialect.
- `runtime_index_maintenance.py`: catalog validity/readiness, replacement-index naming and `CREATE INDEX CONCURRENTLY`.
- `cycle_history_index_supervisor_probe.py`: `pg_stat_progress_create_index` and `pg_catalog` diagnostic query.
- `cycle_history_brin_runtime.py`: PostgreSQL-only BRIN DDL.
- `cycle_history_exact_index_direct.py`, `cycle_history_index_maintenance_child.py`, `runtime_index_ddl_coordination_repair.py`, and `render_combined_postbind.py`: PostgreSQL index supervision and serialization.
- Recovery/telemetry modules matching `DATABASE_URL`, PostgreSQL connection-loss classes, statement timeout and connection keepalive behavior.

No application code uses PostgreSQL advisory locks. PostgreSQL planner/index assumptions are limited to the runtime-index modules above. Ordinary `FOR UPDATE`/locking behavior remains SQLAlchemy-mediated; SQLite serializes durable writes at the database boundary.

## File-history correctness contract

1. Quotes are grouped by stable venue, uppercase asset and UTC date.
2. Payload lineage hashes deduplicate retries across processes and redeploys.
3. A writer takes a manifest `BEGIN IMMEDIATE` reservation, writes a hidden temporary Parquet file, fsyncs it, atomically renames it, fsyncs the directory, and commits manifest visibility.
4. Readers consult only committed manifest rows and never glob temporary/orphan files.
5. Range results are ordered by `observed_at` and durable monotonic history ID, preserving the former `(observed_at, id)` tie-break behavior.
6. Certification physically opens every committed Parquet file and verifies its checksum, exact schema, row count, recorded range, partition identity/date, and manifest lineage references. Missing or corrupt files fail closed.
7. Coverage is evaluated independently for every required venue/asset identity. The full required window must have no start, end, or internal gap greater than 12 hours; global earliest/latest timestamps cannot certify a lane.
8. The importer preserves source lineage hashes, compares the sorted distinct-lineage digest/count, identity scope, and observed-time coverage against the physically verified destination, and reports source row count separately from intentional lineage deduplication.
9. Every relational table is copied in foreign-key dependency order using deterministic primary-key keyset pagination. The atomic progress file records each committed last primary key, so restarts resume after the durable checkpoint rather than returning to offset zero.
10. Historical imports do not create forward outcomes, candidates, forward samples, or allocation authority.

## Staged cutover

1. Deploy code with PostgreSQL compatibility and run tests. Do not change production storage yet.
2. Attach the disk and configure the migration-only `CIE_MIGRATION_POSTGRES_URL` secret manually. Run `python -m inefficiency_engine.postgres_local_migration`; it is idempotent and publishes table progress.
3. Require verified table counts and partition coverage. Any absent target schema or count shortfall fails closed.
4. Start the service with `CIE_STORAGE_ROOT` and `CIE_MARKET_HISTORY_BACKEND=parquet`. PostgreSQL exact-index readiness is no longer consulted for cycle-history certification.
5. Keep the old PostgreSQL resource available read-only until a redeploy/restart persistence test and source-history/portfolio/reconciliation equivalence checks pass.
6. In a later cleanup PR, after production evidence proves all direct raw-history readers use the file adapter, bound/remove the relational current-scan compatibility projection. PostgreSQL migration/index code remains until that verification.

## Unchanged invariants

Paper-only and `live_execution_authority=false` remain explicit. No provider/universe scope, economic logic, qualification threshold, strategy, positive-candidate requirement, portfolio authority, bridge authority, reconciliation authority, source-history semantics, or 180-day coverage requirement changes in this migration.

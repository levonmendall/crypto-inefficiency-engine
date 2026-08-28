# Local persistence migration architecture

## Decision

SQLite WAL plus partitioned Parquet is suitable for the consolidated Render Standard service. The workload has one application instance, moderate authoritative-state writes, append-heavy quote history, bounded range reads, paper-only execution, and no requirement for cross-host consensus. SQLite owns transactional metadata/control state; immutable Parquet files own high-volume quote history. SQLite `BEGIN IMMEDIATE`, a 30-second busy timeout, WAL, `synchronous=FULL`, atomic file rename, directory fsync, and a WAL manifest provide crash and multiprocess safety.

This is a staged migration. PostgreSQL compatibility remains in the package and the importer can read it through a migration-only binding. No PostgreSQL URL is committed. Certification stays fail-closed until 180-day partition coverage is verified.

## Production layout

| Responsibility | Durable location | Mechanism |
|---|---|---|
| Heartbeats, checkpoints, control and certification | `/var/data/cie/metadata/cie.sqlite3` | SQLAlchemy models on SQLite WAL |
| Portfolio, accounting, reconciliation and bridge state | `/var/data/cie/metadata/cie.sqlite3` | Existing transactional tables and authority boundaries |
| Current scan compatibility projection | `/var/data/cie/metadata/cie.sqlite3` | Existing `market_quotes` interface during staged cutover |
| Authoritative raw market history | `/var/data/cie/history/market_quotes/venue=*/asset=*/date=*/*.parquet` | Immutable Zstandard Parquet partitions |
| Partition visibility and deduplication | `history/market_quotes/manifest.sqlite3` | WAL manifest, unique lineage hash, monotonic history ID |
| Migration state | `/var/data/cie/migration/postgres-import-progress.json` | Atomic, explicit progress document |
| Migration supervisor state | `/var/data/cie/migration/postgres-import-supervisor.json` | Atomic stage-one status; no authority |
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
9. Every table captures a deterministic primary-key high-water while PostgreSQL remains live. Keyset paging never copies beyond that boundary, so new production writes cannot invalidate the verified stage-one snapshot. Progress records the committed checkpoint and high-water for restart/catch-up.
10. Historical imports do not create forward outcomes, candidates, forward samples, allocation authority, or cutover authority.

## Automatic stage-one population

`CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION=true` enables a guarded background supervisor in the existing combined service. It refuses to run unless the durable storage root exists, the migration URL exactly matches the authoritative PostgreSQL URL, PostgreSQL is still the normal runtime authority, and neither SQLite nor Parquet production authority has been enabled. It waits for the API port to bind before starting the importer, uses a filesystem lock to prevent duplicate importers, terminates its child cleanly on service shutdown, and treats a persisted `state=verified` progress document as completed work after a redeploy.

Progress is observable without Render shell access at `/v3/internal/local-persistence-migration`. The endpoint exposes only bounded migration metadata and explicitly reports `postgresql_authoritative=true`, `cutover_ready=false`, `allocation_authority=false`, and `live_execution_authority=false`; it never returns a database URL or secret.

## Staged cutover

1. **Disk/population stage:** attach the disk while retaining the existing `DATABASE_URL` binding and `databases:` resource. Do not set `CIE_MARKET_HISTORY_BACKEND=parquet`; production continues reading and writing PostgreSQL. The migration URL is sourced from the same managed database binding without exposing its value.
2. After the public API binds, the guarded supervisor automatically runs `python -m inefficiency_engine.postgres_local_migration`. It bootstraps every reflected production table—not only `EvidenceStore` tables—then performs the idempotent, keyset-checkpointed copy to captured primary-key high waters and publishes progress.
3. Require `state=verified`, physical partition validity and the expected table/market snapshot evidence. Exercise a restart/redeploy while PostgreSQL remains authoritative and confirm the verified progress survives on the disk. This stage-one verification is not cutover authority.
4. **Separate cutover PR:** only after the stage-one evidence is reviewed, perform a final quiesced catch-up, re-verify equivalence, set the local SQLite path and `CIE_MARKET_HISTORY_BACKEND=parquet`, then remove `DATABASE_URL` and the Blueprint database resource. PostgreSQL exact-index readiness ceases to be authoritative only in this later stage.
5. Keep the old PostgreSQL resource available read-only until post-cutover source-history, portfolio, control, bridge and reconciliation equivalence checks pass.
6. In a later cleanup PR, after production evidence proves all direct raw-history readers use the file adapter, bound/remove the relational current-scan compatibility projection. PostgreSQL migration/index code remains until that verification.

## Unchanged invariants

Paper-only and `live_execution_authority=false` remain explicit. No provider/universe scope, economic logic, qualification threshold, strategy, positive-candidate requirement, portfolio authority, bridge authority, reconciliation authority, source-history semantics, or 180-day coverage requirement changes in this migration.

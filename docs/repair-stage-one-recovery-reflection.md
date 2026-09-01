# Stage 1 PostgreSQL recovery-mode startup repair

Production release `2b3c5577cd627ccc4fed0e593b62e4ec1822a07d` preserved the durable `market_quotes` checkpoint but exhausted opaque child restarts when PostgreSQL returned `FATAL: the database system is in recovery mode` during child startup.

The canonical PostgreSQL migration already classifies that exact message as a transient source-read condition and retries bounded restart-safe reads. The uncovered boundary was `source_metadata.reflect(source)`, which runs before the migration's protected retry/terminal-status path.

This repair adds a Stage-1-coarse startup wrapper that:

- reuses the existing transient PostgreSQL classifier;
- reuses the existing retry delay tuple and therefore does not raise retry ceilings;
- retries only while durable migration truth remains nonterminal;
- records the retry against the current table's existing `source_transport_retries` telemetry;
- never retries non-transient errors;
- never re-enters after durable `failed`, `interrupted`, or `verified` state;
- does not modify market high-water, checkpoints, batch sizes, PostgreSQL authority, verification rules, cutover gates, or paper/live authority.

Regression tests cover recovery, bounded exhaustion, terminal-state refusal, and non-transient refusal while asserting the market checkpoint and high-water remain unchanged.

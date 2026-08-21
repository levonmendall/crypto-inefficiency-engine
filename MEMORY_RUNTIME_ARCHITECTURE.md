# Disposable heavy-work runtime

Production Render memory is enforced at the service/container boundary, so the runtime is designed around aggregate memory rather than per-process RSS.

## Permanent processes

Only two Python processes remain resident:

1. the read-only API;
2. the canonical paper portfolio process.

The canonical portfolio consumes durable qualified-opportunity state and fresh settlement/valuation market evidence. It does not build a second alpha/replay research graph.

## Disposable heavy jobs

Research and historical maintenance execute as mutually exclusive subprocesses. Each invocation performs one bounded unit of work, persists durable results, and exits so the operating system reclaims the complete Python heap.

- Research: one bounded research/certification cycle.
- History: one bounded top-volume-universe batch, default four assets.

A PostgreSQL/SQLite heavy-work lease prevents research and history from becoming active at the same time even if a job is invoked outside the parent supervisor. Lease expiry makes hard-kill recovery fail-safe.

## Aggregate memory budget

The parent reads Linux cgroup memory usage for the entire service. Thresholds default to fractions of the actual container limit:

- 70%: soft-pressure telemetry;
- 77.5%: do not start another heavyweight subprocess;
- 82.5%: terminate the active disposable heavyweight subprocess so the service retains headroom below Render's hard limit.

For a 2 GiB service these are approximately 1.43 GiB, 1.59 GiB and 1.69 GiB. They can be overridden with `CIE_INSTANCE_MEMORY_SOFT_MB`, `CIE_INSTANCE_MEMORY_START_BLOCK_MB`, and `CIE_INSTANCE_MEMORY_TERMINATE_MB`.

## Top-40 scaling property

Historical maintenance uses small resumable asset batches and SQL-filtered replay. Research never performs inline historical backfill. Increasing the active universe therefore increases elapsed work and durable data volume rather than requiring all assets' historical/replay state to coexist in memory.

All changes remain paper-only. Missing or deferred heavy evidence cannot create allocation authority or weaken profitability, statistical, risk, executability, or settlement gates.

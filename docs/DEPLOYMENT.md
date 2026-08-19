# Persistent Shadow Deployment

## Objective

Run paper-only market observation continuously and preserve scans, executability decisions, shadow outcomes and worker heartbeats across process restarts and deploys.

## Render topology

The checked-in `render.yaml` defines:

1. `cie-shadow-worker` — always-on Python background worker running `cie worker`.
2. `cie-evidence` — managed PostgreSQL evidence store.
3. `cie-shadow-api` — read-only FastAPI service for health, diagnostics, evidence, replay and shadow summaries.

The worker and API receive the database connection as `DATABASE_URL`. Credentials are not committed and are not persisted in analysis configuration snapshots.

## Runtime behavior

The worker records start/running/success/error/stopped heartbeats, backs off on transient errors and continuously performs the multi-horizon shadow study. v0.10 routes public CEX market/funding and visible-L2 requests through a single adapter registry, including OKX.

The API exposes operational checks including:

- `GET /health` — process/database health;
- `GET /v1/providers/diagnostic` — public provider surface + representative visible-L2 diagnostics;
- `GET /v1/worker/health` — latest worker heartbeat and stale-heartbeat determination;
- `GET /v1/evidence/counts` — durable ledger counts;
- `GET /v1/shadow/summary` — aggregate shadow evidence.

The diagnostic endpoint is read-only and paper-only. It reports degraded/empty public surfaces rather than treating zero data as success.

## Storage selection

Resolution order:

1. `CIE_DATABASE_URL`
2. `DATABASE_URL`
3. `CIE_EVIDENCE_DB_PATH`

Database URLs are resolved outside `Settings` so credentials are excluded from persisted analysis configuration.

## Deployment gate

Before deployment:

```bash
pip install -e '.[dev]'
pytest
python -m compileall -q src
```

After deployment, verify the actual running release rather than inferring deployment from a merge:

```text
GET /health
GET /v1/providers/diagnostic
GET /v1/worker/health
GET /v1/evidence/counts
```

A healthy provider diagnostic should show non-empty market/funding surfaces and successful representative L2 requests for the supported public venues. Provider degradation does not authorize lowering evidence standards.

Do not enable live trading. This deployment exists only to collect and evaluate public-data shadow evidence.

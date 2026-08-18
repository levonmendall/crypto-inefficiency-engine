# Persistent Shadow Deployment

## Objective

Run paper-only market observation continuously and preserve every scan, executability decision, shadow outcome, and worker heartbeat across process restarts and deploys.

## Render topology

The checked-in `render.yaml` defines:

1. `cie-shadow-worker` — always-on Python background worker running `cie worker`.
2. `cie-evidence` — private-network managed PostgreSQL database.
3. `cie-shadow-api` — lightweight read-only FastAPI web service for health, evidence counts, replay, and shadow summaries.

The worker and API receive the database's internal `connectionString` as `DATABASE_URL`. PostgreSQL credentials are not committed to source and are not stored in scan configuration snapshots.

## Why PostgreSQL rather than a worker-local SQLite file

Worker filesystems are not the canonical evidence boundary. Managed PostgreSQL lets evidence survive redeploys/restarts and allows the observational API to read the same ledger without sharing a filesystem.

SQLite remains the preferred local/test backend because it requires no infrastructure and exercises the same persistence API.

## Blueprint cost boundary

The Blueprint uses a `starter` background worker and `basic-256mb` Postgres. These are paid resources. A background worker has no free instance type, and free Render Postgres expires after 30 days. Syncing/creating the Blueprint can therefore incur charges; committing `render.yaml` alone does not create resources.

## Runtime behavior

The worker:

- records `starting` before entering the loop;
- records `running` before each cycle;
- records `success` with cycle/scan IDs after a successful cycle;
- records `error` plus exception type after a failed cycle;
- backs off after transient errors instead of terminating;
- handles SIGTERM/SIGINT through a stop event and records its final state.

The API exposes:

- `GET /health` — process/database health;
- `GET /v1/worker/health` — latest worker heartbeat and stale-heartbeat determination;
- `GET /v1/evidence/counts` — durable ledger row counts;
- `GET /v1/shadow/summary` — aggregate shadow survival evidence.

## Storage selection

Resolution order:

1. `CIE_DATABASE_URL`
2. `DATABASE_URL`
3. `CIE_EVIDENCE_DB_PATH`

Database URLs are resolved outside the `Settings` dataclass so credentials are not included in persisted `analysis_config` payloads.

## Deployment gate

Before syncing the Blueprint:

```bash
pip install -e '.[dev]'
pytest
python -m compileall -q src
```

Do not enable live trading. This deployment exists only to collect public-data shadow evidence.

# Render consolidation migration

## Target topology

The crypto engine now targets one paid Render compute service plus PostgreSQL:

```text
cie-shadow-worker (Standard web service)
  -> python -m inefficiency_engine.render_combined
       -> governed worker: python -m inefficiency_engine.cli worker
       -> read API: python -m uvicorn inefficiency_engine.read_api_research_deploy:app --host 0.0.0.0 --port $PORT

cie-evidence (managed PostgreSQL)
```

The former standalone `cie-shadow-api` service is removed from the Blueprint. The combined supervisor treats both the worker and API as critical children; if either exits, it terminates the sibling and exits so Render restarts the complete runtime.

## One-time Render migration constraint

Render does not allow changing an existing service's type after creation. The current paid `cie-shadow-worker` resource is a Background Worker, while the consolidated runtime must be a Web Service so Render can route public HTTP traffic to the API.

Therefore the existing `cie-shadow-worker` resource must be recreated once as the Blueprint-defined Standard web service. This is a service-type migration, not an additional long-term paid service.

## Cutover procedure

1. Wait until Render Builds and Deploys are operational.
2. Confirm the latest worker evidence is durably stored in `cie-evidence`; no canonical state is stored only on the worker filesystem.
3. Delete the existing Render Background Worker named `cie-shadow-worker`.
4. Delete the obsolete `cie-shadow-api` Free web service if Render does not remove it during Blueprint reconciliation.
5. Immediately sync the Blueprint from `main`.
6. Approve creation of `cie-shadow-worker` as a **Standard Web Service** if Render prompts.
7. Confirm the service starts `python -m inefficiency_engine.render_combined` and binds the public `$PORT`.
8. Verify `/health` returns HTTP 200 and `/ready` confirms PostgreSQL readiness.
9. Verify `/v1/worker/health` shows fresh worker heartbeats and the canonical portfolio/research loops continue advancing.

The managed PostgreSQL database remains unchanged and is referenced by the same `DATABASE_URL` Blueprint binding.

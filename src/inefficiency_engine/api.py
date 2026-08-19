from __future__ import annotations

from fastapi import FastAPI, HTTPException

from inefficiency_engine import __version__
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.replay import replay_scan
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.shadow_summary import summarize_evidence_store


settings = Settings.from_env()
evidence_store = build_evidence_store(settings.evidence_db_path)
app = FastAPI(title="Crypto Inefficiency Engine", version=__version__)
service = OpportunityService(settings=settings, evidence_store=evidence_store)


@app.get("/health")
def health():
    payload = {
        "status": "ok",
        "version": __version__,
        "paper_only": True,
        "evidence_persistence": evidence_store is not None,
    }
    if evidence_store is not None:
        payload["evidence_backend"] = evidence_store.backend
        payload["database_ok"] = evidence_store.ping()
    return payload


@app.get("/v1/detectors")
def detector_registry():
    manifests = service.detector_manifests()
    return {
        "count": len(manifests),
        "paper_only": True,
        "detectors": [item.model_dump(mode="json") for item in manifests],
    }


@app.get("/v1/opportunities/demo")
def demo_opportunities():
    opportunities = service.demo_scan()
    return {"count": len(opportunities), "opportunities": [o.model_dump(mode="json") for o in opportunities]}


@app.get("/v1/opportunities/live")
async def live_opportunities():
    try:
        snapshot = await service.collect_live_evidence()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"live public-data scan failed: {type(exc).__name__}") from exc
    return {
        "scan_id": snapshot.scan_id,
        "count": len(snapshot.opportunities),
        "paper_only": True,
        "providers": [status.model_dump(mode="json") for status in snapshot.providers],
        "opportunities": [o.model_dump(mode="json") for o in snapshot.opportunities],
    }


@app.get("/v1/graph/live")
async def live_market_graph():
    try:
        graph, providers, opportunities = await service.collect_live_graph()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"live graph scan failed: {type(exc).__name__}") from exc
    return {
        "paper_only": True,
        "summary": graph.summary(),
        "provider_status": [status.model_dump(mode="json") for status in providers],
        "discovered_opportunity_count": len(opportunities),
        "graph": graph.model_dump(mode="json"),
    }


@app.get("/v1/evidence/{scan_id}/replay")
def replay_evidence(scan_id: str):
    if evidence_store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        result = replay_scan(evidence_store, service, scan_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="scan not found") from exc
    return result.model_dump(mode="json")


@app.get("/v1/executability/live")
async def live_executability():
    try:
        snapshot = await service.collect_live_executability()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"live executability scan failed: {type(exc).__name__}") from exc
    qualified = [item for item in snapshot.executability if item.max_qualified_notional_usd > 0]
    return {
        "scan_id": snapshot.scan_id,
        "paper_only": True,
        "opportunity_count": len(snapshot.opportunities),
        "qualified_opportunity_count": len(qualified),
        "order_book_count": len(snapshot.order_books),
        "capital_tiers_usd": list(settings.capital_tiers_usd),
        "latency_model": service.empirical_latency_model().model_dump(mode="json"),
        "providers": [status.model_dump(mode="json") for status in snapshot.providers],
        "executability": [item.model_dump(mode="json") for item in snapshot.executability],
    }


@app.get("/v1/opportunities/ranked/live")
async def ranked_live_opportunities():
    try:
        snapshot = await service.collect_live_executability()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"live ranking scan failed: {type(exc).__name__}") from exc
    ranked = service.rank_snapshot(snapshot)
    return {
        "scan_id": snapshot.scan_id,
        "paper_only": True,
        "rank_basis": "capital_adjusted_net_annualized_return",
        "allocator_authority": False,
        "count": len(ranked),
        "opportunities": [item.model_dump(mode="json") for item in ranked],
    }


@app.post("/v1/shadow/cycle")
async def run_shadow_cycle():
    try:
        cycle = await service.run_shadow_cycle()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"shadow cycle failed: {type(exc).__name__}") from exc
    return cycle.model_dump(mode="json")


@app.get("/v1/shadow/summary")
def shadow_summary():
    if evidence_store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    payload = summarize_evidence_store(evidence_store)
    payload["empirical_latency_model"] = service.empirical_latency_model().model_dump(mode="json")
    return payload


@app.get("/v1/latency/model")
def empirical_latency_model(
    strategy: str | None = None,
    venue_pair: str | None = None,
    asset: str | None = None,
    notional_usd_per_leg: float | None = None,
):
    if evidence_store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    model = service.empirical_latency_model(
        notional_usd_per_leg=notional_usd_per_leg,
        strategy=strategy,
        venue_pair_name=venue_pair,
        asset=asset,
    )
    return model.model_dump(mode="json")


@app.get("/v1/worker/health")
def worker_health():
    if evidence_store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    return evidence_store.worker_health(stale_after_seconds=settings.worker_heartbeat_stale_seconds)


@app.get("/v1/evidence/counts")
def evidence_counts():
    if evidence_store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    return evidence_store.counts().__dict__

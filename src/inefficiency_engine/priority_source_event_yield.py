from __future__ import annotations

from datetime import datetime, timezone

import httpx

from inefficiency_engine.alpha_coverage_strategies import EventObservation
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import parse_morpho_markets, parse_snapshot_proposals, stable_id
from inefficiency_engine.research_mechanisms import YieldObservation
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation

SNAPSHOT_GRAPHQL_URL = "https://hub.snapshot.org/graphql"
MORPHO_GRAPHQL_URL = "https://api.morpho.org/graphql"
DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def collect_snapshot_governance(coverage: SourceCoveragePlane, alpha_factory) -> SourceProbeResult:
    query = '''query SourceCoverageProposals {
      proposals(first: 50, orderBy: "created", orderDirection: desc) {
        id title state created start end space { id symbol }
      }
    }'''
    async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control":"no-cache","User-Agent":"crypto-inefficiency-engine/source-coverage"}) as client:
        response = await client.post(SNAPSHOT_GRAPHQL_URL, json={"query":query})
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload,dict) and payload.get("errors"):
        raise ValueError("Snapshot GraphQL returned errors")
    proposals = parse_snapshot_proposals(payload)
    observed_at = _now()
    for proposal in proposals:
        event_at = proposal["event_at"]
        assert isinstance(event_at,datetime)
        coverage.record_event(SourceEventObservation(
            event_id=stable_id("snapshot-governance",proposal["id"]), lane_id="event_driven",
            source_id="snapshot-governance", event_type="governance_proposal", event_at=event_at,
            observed_at=observed_at, asset=str(proposal.get("space_symbol") or "") or None,
            source_reference=SNAPSHOT_GRAPHQL_URL,
            payload={key:value for key,value in proposal.items() if key != "event_at"},
        ))
        symbol = str(proposal.get("space_symbol") or "")
        # Zero surprise makes this provider evidence forward-testable without fabricating alpha.
        if symbol and symbol.isalnum() and len(symbol) <= 16:
            alpha_factory.record_event_observation(EventObservation(
                event_id=stable_id("snapshot-alpha",proposal["id"]), provider="snapshot:hub-graphql",
                asset=symbol, event_type="governance_proposal_observed", known_at=min(event_at,observed_at),
                event_at=event_at, observed_at=observed_at, surprise_score=0.0, confidence=0.50,
                source_reference=SNAPSHOT_GRAPHQL_URL, authoritative=True,
                commercial_use_permitted=True, point_in_time=True, paper_only=True,
            ))
    return SourceProbeResult(
        source_id="snapshot-governance", item_count=len(proposals), source_reference=SNAPSHOT_GRAPHQL_URL,
        evidence_by_lane={"event_driven":["timestamped_events","event_identity"]},
        forward_testable_evidence=True, detail={"zero_surprise_alpha_seed":True},
    )


async def collect_morpho_markets(coverage: SourceCoveragePlane, yield_service) -> SourceProbeResult:
    query = '''query SourceCoverageMarkets {
      markets(first: 50) {
        items { uniqueKey loanAsset { symbol } state { supplyApy supplyAssetsUsd liquidityAssetsUsd } }
      }
    }'''
    async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control":"no-cache","User-Agent":"crypto-inefficiency-engine/source-coverage"}) as client:
        response = await client.post(MORPHO_GRAPHQL_URL, json={"query":query})
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload,dict) and payload.get("errors"):
        raise ValueError("Morpho GraphQL returned errors")
    markets = parse_morpho_markets(payload)
    observed_at = _now()
    for row in markets[:25]:
        yield_service.record(YieldObservation(
            observation_id=stable_id("morpho-markets",row["market_id"],observed_at.strftime("%Y-%m-%dT%H")),
            provider="morpho:graphql-markets", protocol="Morpho", venue_or_chain="multi-chain",
            asset=str(row["asset"]), kind="lending", observed_at=observed_at, as_of_at=observed_at,
            gross_apy=float(row["supply_apy"]), capacity_usd=float(row["liquidity_usd"]), holding_hours=24.0,
            entry_exit_cost_bps=0.0, credit_or_protocol_risk_haircut_apy=0.0,
            slashing_or_liquidation_risk_haircut_apy=0.0, incentive_decay_haircut_apy=0.0,
            withdrawal_or_lockup_hours=0.0, source_reference=MORPHO_GRAPHQL_URL,
            authoritative=True, commercial_use_permitted=True, point_in_time=True, paper_only=True,
        ))
    return SourceProbeResult(
        source_id="morpho-markets", item_count=len(markets), source_reference=MORPHO_GRAPHQL_URL,
        evidence_by_lane={"yield":["yield_rate","capacity","exit_liquidity"],"fundamental_onchain":["protocol_fundamentals"]},
        economic_fields_complete=True, detail={"risk_calibration_complete":False,"paper_allocation_authority":False},
    )


async def collect_defillama_protocols() -> SourceProbeResult:
    async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control":"no-cache","User-Agent":"crypto-inefficiency-engine/source-coverage"}) as client:
        response = await client.get(DEFILLAMA_PROTOCOLS_URL)
        response.raise_for_status()
        payload = response.json()
    rows = [row for row in payload if isinstance(row,dict) and row.get("name") and row.get("tvl") is not None] if isinstance(payload,list) else []
    if not rows:
        raise ValueError("DefiLlama protocols returned no usable protocol metrics")
    return SourceProbeResult(
        source_id="defillama-protocols", item_count=len(rows), source_reference=DEFILLAMA_PROTOCOLS_URL,
        evidence_by_lane={"fundamental_onchain":["protocol_fundamentals"]},
        authoritative=False, economic_fields_complete=False,
        detail={"secondary_discovery_only":True,"alpha_authority":False},
    )

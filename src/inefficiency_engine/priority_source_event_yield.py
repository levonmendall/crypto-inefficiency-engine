from __future__ import annotations

import statistics
from datetime import datetime, timezone

import httpx

from inefficiency_engine.alpha_coverage_strategies import EventObservation
from inefficiency_engine.alpha_extensions import FundamentalFactorLedger, FundamentalFactorObservation
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import parse_morpho_markets, parse_snapshot_proposals, stable_id
from inefficiency_engine.research_mechanisms import YieldObservation
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation

SNAPSHOT_GRAPHQL_URL = "https://hub.snapshot.org/graphql"
MORPHO_GRAPHQL_URL = "https://api.morpho.org/graphql"
DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _alpha_asset(symbol: str) -> str:
    """Normalize protocol wrappers only when they represent the same underlying asset."""

    value = str(symbol).upper().strip()
    return {"WETH": "ETH", "WBTC": "BTC"}.get(value, value)


def _liquidity_coverage_score(*, liquidity_usd: float, supply_usd: float) -> float:
    """Normalize immediately withdrawable liquidity coverage to [-1, 1].

    This is an explicit research factor, not a profitability claim. 50% liquidity
    coverage is neutral, full coverage is +1, and zero coverage is -1. Forward alpha
    testing determines whether this protocol-health factor has predictive value.
    """

    if supply_usd <= 0:
        return -1.0
    ratio = max(0.0, min(1.0, liquidity_usd / supply_usd))
    return max(-1.0, min(1.0, 2.0 * ratio - 1.0))


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
    by_alpha_asset: dict[str, list[dict[str, object]]] = {}
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
        by_alpha_asset.setdefault(_alpha_asset(str(row["asset"])), []).append(row)

    # Source coverage previously counted Morpho protocol fundamentals while the alpha
    # ledger consumed only Ethereum chain factors. Persist a conservative normalized
    # protocol-health factor and, when a recent authoritative chain observation exists,
    # combine it into one point-in-time observation. This makes the strategy actually
    # digest both evidence classes required by the lane instead of satisfying a source
    # checkbox with data that never enters the signal.
    factor_ledger = FundamentalFactorLedger(coverage.store)
    protocol_factor_count = 0
    for asset, asset_rows in by_alpha_asset.items():
        chain = factor_ledger.latest(
            asset,
            before=observed_at,
            max_age_hours=24.0,
            require_authoritative=True,
            require_commercial_use=True,
        )
        if chain is None:
            continue
        coverage_scores = [
            _liquidity_coverage_score(
                liquidity_usd=float(row["liquidity_usd"]),
                supply_usd=float(row["supply_usd"]),
            )
            for row in asset_rows
            if float(row["supply_usd"]) > 0
        ]
        if not coverage_scores:
            continue
        factors = dict(chain.factor_scores)
        factors["morpho_liquidity_coverage"] = statistics.median(coverage_scores)
        combined = FundamentalFactorObservation(
            observation_id=stable_id(
                "ethereum-morpho-composite",
                asset,
                chain.observation_id,
                observed_at.strftime("%Y-%m-%dT%H:%M"),
            ),
            provider="ethereum+morpho:composite",
            asset=asset,
            observed_at=observed_at,
            as_of_at=min(observed_at, chain.as_of_at),
            factor_scores=factors,
            source_reference=f"{chain.source_reference}|{MORPHO_GRAPHQL_URL}",
            authoritative=True,
            commercial_use_permitted=True,
            point_in_time=True,
            paper_only=True,
        )
        factor_ledger.record(combined)
        protocol_factor_count += 1

    return SourceProbeResult(
        source_id="morpho-markets", item_count=len(markets), source_reference=MORPHO_GRAPHQL_URL,
        evidence_by_lane={"yield":["yield_rate","capacity","exit_liquidity"],"fundamental_onchain":["protocol_fundamentals"]},
        # Capacity/liquidity are real, but protocol-loss calibration is still unknown;
        # do not describe the yield economics as complete merely because the API fields exist.
        economic_fields_complete=False,
        detail={
            "risk_calibration_complete":False,
            "protocol_factor_observation_count":protocol_factor_count,
            "fundamental_signal_consumes_protocol_evidence":True,
            "paper_allocation_authority":False,
        },
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

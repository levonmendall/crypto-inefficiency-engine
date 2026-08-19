from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

from inefficiency_engine.market_graph import MarketGraphSnapshot
from inefficiency_engine.models import MarketQuote
from inefficiency_engine.universal_models import (
    BridgeQuote,
    DexPoolSnapshot,
    ExternalOpportunitySignal,
    OptionQuote,
    StablecoinConversionEdge,
    StablecoinConversionObservation,
    UniversalCandidate,
    UniversalEdge,
    UniversalFamily,
    UniversalGraphSnapshot,
    UniversalNode,
)


STABLECOINS = {"USDC", "USDT", "USD"}
KNOWN_CANONICAL_ASSETS = {
    "WETH": "ETH", "WBTC": "BTC", "BTC": "BTC", "ETH": "ETH", "SOL": "SOL",
    "USDC": "USDC", "USDT": "USDT", "USD": "USD",
}


def currency_node_id(symbol: str) -> str:
    return f"currency:{symbol.upper()}"


def dex_pool_node_id(pool: DexPoolSnapshot) -> str:
    return f"dex-pool:{pool.chain_id}:{pool.dex_id}:{pool.pair_address.lower()}"


def build_conversion_edges(
    observations: list[StablecoinConversionObservation], *, depeg_multiplier: float = 1.5,
    risk_floor_bps: float = 2.0,
) -> list[StablecoinConversionEdge]:
    edges: list[StablecoinConversionEdge] = []
    for row in observations:
        spread_bps = max(0.0, (row.ask - row.bid) / row.mid * 10_000.0)
        parity_target = 1.0 if {row.base_currency.upper(), row.quote_currency.upper()} <= STABLECOINS else row.mid
        depeg_bps = abs(row.mid / parity_target - 1.0) * 10_000.0 if parity_target > 0 else 0.0
        haircut = max(risk_floor_bps, depeg_bps * depeg_multiplier)
        half_spread = spread_bps / 2.0
        edges.append(StablecoinConversionEdge(
            source_currency=row.base_currency.upper(), target_currency=row.quote_currency.upper(), venue=row.venue,
            rate=row.bid, spread_bps=spread_bps, depeg_bps=depeg_bps, risk_haircut_bps=haircut,
            total_conversion_cost_bps=half_spread + haircut, observed_at=row.observed_at, source=row.source,
        ))
        edges.append(StablecoinConversionEdge(
            source_currency=row.quote_currency.upper(), target_currency=row.base_currency.upper(), venue=row.venue,
            rate=1.0 / row.ask, spread_bps=spread_bps, depeg_bps=depeg_bps, risk_haircut_bps=haircut,
            total_conversion_cost_bps=half_spread + haircut, observed_at=row.observed_at, source=row.source,
        ))
    return edges


class StablecoinConversionModel:
    def __init__(self, edges: list[StablecoinConversionEdge]):
        self.edges = [edge for edge in edges if edge.usable]

    def best_path(self, source: str, target: str) -> tuple[float, float, list[StablecoinConversionEdge]] | None:
        source, target = source.upper(), target.upper()
        if source == target:
            return 1.0, 0.0, []
        direct = [edge for edge in self.edges if edge.source_currency == source and edge.target_currency == target]
        if direct:
            best = min(direct, key=lambda edge: edge.total_conversion_cost_bps)
            return best.rate, best.total_conversion_cost_bps, [best]
        best_path = None
        for first in self.edges:
            if first.source_currency != source:
                continue
            for second in self.edges:
                if second.source_currency != first.target_currency or second.target_currency != target:
                    continue
                cost = first.total_conversion_cost_bps + second.total_conversion_cost_bps
                rate = first.rate * second.rate
                if best_path is None or cost < best_path[1]:
                    best_path = (rate, cost, [first, second])
        return best_path


def detect_stablecoin_dislocations(
    observations: list[StablecoinConversionObservation], *, minimum_edge_bps: float = 8.0,
) -> list[UniversalCandidate]:
    results: list[UniversalCandidate] = []
    for row in observations:
        if row.base_currency.upper() not in {"USDC", "USDT"} or row.quote_currency.upper() != "USD":
            continue
        deviation_bps = abs(row.mid - 1.0) * 10_000.0
        spread_bps = (row.ask - row.bid) / row.mid * 10_000.0
        gross, modeled = max(0.0, deviation_bps), max(0.0, spread_bps)
        risk = max(5.0, deviation_bps * 0.5)
        if gross < minimum_edge_bps:
            continue
        direction = "sell_stablecoin_for_usd" if row.mid > 1 else "buy_stablecoin_with_usd"
        raw = f"stable:{row.venue}:{row.base_currency}:{row.observed_at.isoformat()}"
        results.append(UniversalCandidate(
            candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20], family=UniversalFamily.STABLECOIN_DISLOCATION,
            asset=row.base_currency.upper(), gross_edge_bps=gross, modeled_cost_bps=modeled, risk_haircut_bps=risk,
            net_edge_bps=gross - modeled - risk, observed_at=row.observed_at,
            expires_at=row.observed_at + timedelta(seconds=60), executable_eligible=False,
            blocked_reason="inventory/redemption and exact execution path are not modeled",
            evidence={"venue": row.venue, "direction": direction, "mid": row.mid, "bid": row.bid, "ask": row.ask,
                      "deviation_bps": deviation_bps},
        ))
    return results


def _cex_asset_prices(market_quotes: list[MarketQuote]) -> dict[str, list[MarketQuote]]:
    grouped: dict[str, list[MarketQuote]] = defaultdict(list)
    for quote in market_quotes:
        if quote.market_kind.value != "spot":
            continue
        grouped[KNOWN_CANONICAL_ASSETS.get(quote.asset.upper(), quote.asset.upper())].append(quote)
    return grouped


def detect_dex_routes(
    market_quotes: list[MarketQuote], pools: list[DexPoolSnapshot], *, minimum_edge_bps: float = 12.0,
    liquidity_risk_floor_bps: float = 8.0,
) -> list[UniversalCandidate]:
    cex = _cex_asset_prices(market_quotes)
    results: list[UniversalCandidate] = []
    pools_by_asset: dict[str, list[DexPoolSnapshot]] = defaultdict(list)
    for pool in pools:
        canonical = pool.base_token.canonical_asset or KNOWN_CANONICAL_ASSETS.get(pool.base_token.symbol.upper())
        if canonical and pool.price_usd:
            pools_by_asset[canonical].append(pool)
            for quote in cex.get(canonical, []):
                gross = abs(pool.price_usd / quote.mid - 1.0) * 10_000.0
                if gross < minimum_edge_bps:
                    continue
                liquidity_risk = max(liquidity_risk_floor_bps, 50_000.0 / max(pool.liquidity_usd or 1.0, 1.0) * 10_000.0)
                raw = f"cex-dex:{canonical}:{quote.venue}:{pool.chain_id}:{pool.pair_address}:{pool.observed_at.isoformat()}"
                results.append(UniversalCandidate(
                    candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20], family=UniversalFamily.CEX_DEX,
                    asset=canonical, gross_edge_bps=gross, risk_haircut_bps=liquidity_risk,
                    net_edge_bps=gross - liquidity_risk, capacity_usd=pool.liquidity_usd,
                    observed_at=min(quote.observed_at, pool.observed_at),
                    expires_at=min(quote.observed_at, pool.observed_at) + timedelta(seconds=60),
                    executable_eligible=False,
                    blocked_reason="DEX aggregator liquidity is a discovery proxy, not an exact executable swap curve",
                    evidence={"cex_venue": quote.venue, "cex_mid": quote.mid, "dex_id": pool.dex_id,
                              "chain_id": pool.chain_id, "pair_address": pool.pair_address,
                              "dex_price_usd": pool.price_usd, "liquidity_usd": pool.liquidity_usd,
                              "depth_model": pool.depth_model},
                ))
    for canonical, asset_pools in pools_by_asset.items():
        for i, left in enumerate(asset_pools):
            for right in asset_pools[i + 1:]:
                if not left.price_usd or not right.price_usd:
                    continue
                gross = abs(left.price_usd / right.price_usd - 1.0) * 10_000.0
                if gross < minimum_edge_bps:
                    continue
                capacity = min(left.liquidity_usd or 0.0, right.liquidity_usd or 0.0)
                risk = max(liquidity_risk_floor_bps * 2.0, 75_000.0 / max(capacity, 1.0) * 10_000.0)
                raw = f"dex-dex:{canonical}:{left.pair_address}:{right.pair_address}:{max(left.observed_at, right.observed_at).isoformat()}"
                results.append(UniversalCandidate(
                    candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20], family=UniversalFamily.DEX_DEX,
                    asset=canonical, gross_edge_bps=gross, risk_haircut_bps=risk, net_edge_bps=gross-risk,
                    capacity_usd=capacity, observed_at=min(left.observed_at, right.observed_at),
                    expires_at=min(left.observed_at, right.observed_at) + timedelta(seconds=60), executable_eligible=False,
                    blocked_reason="DEX pool snapshots do not provide exact route-specific executable depth",
                    evidence={"left_pool": left.pair_address, "right_pool": right.pair_address,
                              "left_chain": left.chain_id, "right_chain": right.chain_id,
                              "left_price_usd": left.price_usd, "right_price_usd": right.price_usd},
                ))
    return sorted(results, key=lambda item: item.net_edge_bps, reverse=True)


def evaluate_bridge_quote(quote: BridgeQuote) -> UniversalCandidate:
    gross = max(0.0, (quote.output_amount / quote.input_amount - 1.0) * 10_000.0)
    raw = f"bridge:{quote.provider}:{quote.asset}:{quote.origin_chain_id}:{quote.destination_chain_id}:{quote.observed_at.isoformat()}"
    return UniversalCandidate(
        candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20], family=UniversalFamily.CROSS_CHAIN,
        asset=quote.asset, gross_edge_bps=gross, modeled_cost_bps=quote.fee_bps,
        risk_haircut_bps=quote.settlement_risk_haircut_bps,
        net_edge_bps=gross-quote.fee_bps-quote.settlement_risk_haircut_bps, observed_at=quote.observed_at,
        expires_at=quote.expires_at, executable_eligible=quote.executable_eligible,
        blocked_reason=quote.blocked_reason or (None if quote.executable_eligible else "bridge quote is capability evidence only"),
        evidence={"provider": quote.provider, "origin_chain_id": quote.origin_chain_id,
                  "destination_chain_id": quote.destination_chain_id, "expected_fill_seconds": quote.expected_fill_seconds},
    )


def detect_option_relative_value(
    quotes: list[OptionQuote], *, minimum_iv_deviation: float = 8.0,
) -> list[UniversalCandidate]:
    groups: dict[tuple[str, datetime, str], list[OptionQuote]] = defaultdict(list)
    for quote in quotes:
        if quote.mark_iv is not None:
            groups[(quote.asset, quote.expires_at, quote.option_type)].append(quote)
    results: list[UniversalCandidate] = []
    for (asset, expiry, option_type), rows in groups.items():
        if len(rows) < 3:
            continue
        benchmark = median([row.mark_iv for row in rows if row.mark_iv is not None])
        for row in rows:
            if row.mark_iv is None:
                continue
            deviation = abs(row.mark_iv - benchmark)
            if deviation < minimum_iv_deviation:
                continue
            gross, risk = deviation * 100.0, max(25.0, deviation * 50.0)
            raw = f"option-rv:{row.instrument_name}:{row.observed_at.isoformat()}"
            results.append(UniversalCandidate(
                candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20], family=UniversalFamily.OPTION_RELATIVE_VALUE,
                asset=asset, gross_edge_bps=gross, risk_haircut_bps=risk, net_edge_bps=gross-risk,
                observed_at=row.observed_at, expires_at=min(expiry, row.observed_at + timedelta(minutes=5)),
                executable_eligible=False,
                blocked_reason="volatility-surface anomaly lacks delta/vega hedge and option execution-cost model",
                evidence={"instrument_name": row.instrument_name, "option_type": option_type, "strike": row.strike,
                          "mark_iv": row.mark_iv, "surface_median_iv": benchmark,
                          "iv_deviation_points": deviation, "underlying_price": row.underlying_price,
                          "open_interest": row.open_interest},
            ))
    return sorted(results, key=lambda item: item.net_edge_bps, reverse=True)


def candidate_from_external_signal(signal: ExternalOpportunitySignal) -> UniversalCandidate:
    raw = f"{signal.family}:{signal.provider}:{signal.asset}:{signal.observed_at.isoformat()}"
    family = UniversalFamily.LIQUIDATION_BACKSTOP if signal.family == "liquidation_backstop" else UniversalFamily.SOLVER
    eligible = signal.executable_eligible and signal.authoritative_capacity
    return UniversalCandidate(
        candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20], family=family, asset=signal.asset,
        gross_edge_bps=signal.gross_edge_bps, modeled_cost_bps=signal.modeled_cost_bps,
        risk_haircut_bps=signal.risk_haircut_bps, net_edge_bps=signal.net_edge_bps,
        capacity_usd=signal.capacity_usd, observed_at=signal.observed_at, expires_at=signal.expires_at,
        executable_eligible=eligible,
        blocked_reason=None if eligible else "external signal lacks authoritative capacity/execution evidence",
        evidence={"provider": signal.provider, "source": signal.source},
    )


def build_universal_graph(
    core_graph: MarketGraphSnapshot, *, conversion_edges: list[StablecoinConversionEdge],
    dex_pools: list[DexPoolSnapshot], option_quotes: list[OptionQuote], candidates: list[UniversalCandidate],
) -> UniversalGraphSnapshot:
    nodes: dict[str, UniversalNode] = {}
    edges: dict[str, UniversalEdge] = {}
    for asset in core_graph.assets:
        nodes[asset.asset_id] = UniversalNode(node_id=asset.asset_id, kind="asset", label=asset.symbol)
    for venue in core_graph.venues:
        nodes[venue.venue_id] = UniversalNode(node_id=venue.venue_id, kind="venue", label=venue.name)
    for instrument in core_graph.instruments:
        nodes[instrument.instrument_id] = UniversalNode(
            node_id=instrument.instrument_id, kind="cex_instrument",
            label=f"{instrument.venue}:{instrument.asset}:{instrument.contract_key}",
            metadata={"venue": instrument.venue, "asset": instrument.asset, "market_kind": instrument.market_kind.value,
                      "quote_currency": instrument.quote_currency, "contract_key": instrument.contract_key},
        )
    for edge in core_graph.edges:
        edges[edge.edge_id] = UniversalEdge(edge_id=edge.edge_id, source_id=edge.source_id, target_id=edge.target_id,
                                             kind=edge.relationship.value, executable_eligible=True, metadata=edge.metadata)
    for edge in conversion_edges:
        src, dst = currency_node_id(edge.source_currency), currency_node_id(edge.target_currency)
        nodes.setdefault(src, UniversalNode(node_id=src, kind="currency", label=edge.source_currency))
        nodes.setdefault(dst, UniversalNode(node_id=dst, kind="currency", label=edge.target_currency))
        edge_id = f"conversion:{edge.venue}:{edge.source_currency}->{edge.target_currency}"
        edges[edge_id] = UniversalEdge(edge_id=edge_id, source_id=src, target_id=dst, kind="currency_conversion",
            cost_bps=edge.total_conversion_cost_bps, risk_haircut_bps=edge.risk_haircut_bps, executable_eligible=False,
            metadata={"venue": edge.venue, "rate": edge.rate, "source": edge.source})
    for pool in dex_pools:
        pool_id = dex_pool_node_id(pool)
        for token in (pool.base_token, pool.quote_token):
            nodes[token.token_id] = UniversalNode(node_id=token.token_id, kind="chain_token",
                label=f"{token.symbol}@{pool.chain_id}", metadata=token.model_dump(mode="json"))
        nodes[pool_id] = UniversalNode(node_id=pool_id, kind="dex_pool",
            label=f"{pool.dex_id}:{pool.base_token.symbol}/{pool.quote_token.symbol}",
            metadata={"chain_id": pool.chain_id, "pair_address": pool.pair_address,
                      "liquidity_usd": pool.liquidity_usd, "depth_model": pool.depth_model})
        for token_id, direction in ((pool.base_token.token_id, "base"), (pool.quote_token.token_id, "quote")):
            edge_id = f"pool-token:{pool_id}:{token_id}"
            edges[edge_id] = UniversalEdge(edge_id=edge_id, source_id=pool_id, target_id=token_id,
                                             kind="pool_contains", executable_eligible=False,
                                             metadata={"side": direction})
    for quote in option_quotes:
        option_id, asset_id = f"option:deribit:{quote.instrument_name}", f"crypto:asset:{quote.asset.upper()}"
        nodes.setdefault(asset_id, UniversalNode(node_id=asset_id, kind="asset", label=quote.asset.upper()))
        nodes[option_id] = UniversalNode(node_id=option_id, kind="option", label=quote.instrument_name,
                                          metadata=quote.model_dump(mode="json"))
        edge_id = f"option-underlying:{option_id}->{asset_id}"
        edges[edge_id] = UniversalEdge(edge_id=edge_id, source_id=option_id, target_id=asset_id,
                                         kind="option_underlying", executable_eligible=False)
    for asset in ("USDC", "USDT", "ETH"):
        for source_chain, target_chain in (("ethereum", "base"), ("ethereum", "arbitrum"), ("base", "arbitrum")):
            source_id, target_id = f"bridge-capability:{asset}:{source_chain}", f"bridge-capability:{asset}:{target_chain}"
            nodes.setdefault(source_id, UniversalNode(node_id=source_id, kind="bridge_asset", label=f"{asset}@{source_chain}"))
            nodes.setdefault(target_id, UniversalNode(node_id=target_id, kind="bridge_asset", label=f"{asset}@{target_chain}"))
            edge_id = f"bridge-capability:{asset}:{source_chain}->{target_chain}"
            edges[edge_id] = UniversalEdge(edge_id=edge_id, source_id=source_id, target_id=target_id,
                kind="bridge_capability", executable_eligible=False,
                metadata={"blocked_reason": "fresh authoritative bridge quote required"})
    times = [core_graph.observed_at, *[e.observed_at for e in conversion_edges], *[p.observed_at for p in dex_pools],
             *[q.observed_at for q in option_quotes]]
    return UniversalGraphSnapshot(
        observed_at=max(times) if times else datetime.now(timezone.utc),
        nodes=sorted(nodes.values(), key=lambda item: item.node_id), edges=sorted(edges.values(), key=lambda item: item.edge_id),
        candidates=sorted(candidates, key=lambda item: item.net_edge_bps, reverse=True),
        capability_status={
            "cex_spot_perp_futures": "executable qualification supported",
            "stablecoin_conversion": "risk-aware graph/search; conversion execution blocked",
            "dex_pool_discovery": "public liquidity proxy; exact swap execution blocked",
            "cex_dex_dex_dex": "search supported; execution blocked on exact DEX quote/depth",
            "cross_chain": "bridge quote interface + settlement-risk model; live authoritative quote required",
            "liquidation_backstop": "typed external interface; authoritative source required",
            "solver": "typed external interface; authoritative source required",
            "options_relative_value": "Deribit surface search; option hedge/execution model blocked",
            "paper_allocator": "qualified CEX opportunities only; no execution authority",
        },
    )

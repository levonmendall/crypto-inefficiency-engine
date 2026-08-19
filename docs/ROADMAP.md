# Roadmap

## Milestone 1 — Discovery core — complete
- Canonical quote/funding models
- Public Hyperliquid and Coinbase adapters
- Funding-dispersion and spot/perp-basis detectors
- Conservative screening hurdle
- Paper-only execution boundary

## Milestone 2 — Point-in-time evidence — foundation complete
- Append-only SQLite/PostgreSQL observation store
- Provider health and source timestamps
- Opportunity/order-book/executability snapshots
- Lineage hashes and exact analysis configuration
- Deterministic replay harness

## Milestone 3 — Executability and economic realism — core complete
- L2 depth/VWAP/slippage
- Exact-base paired-leg sizing
- Capital tiers and capacity frontier
- Freshness/skew gates
- Explicit venue fees, collateral and borrow economics
- Conservative latency and hedge-recovery fallbacks

## Milestone 4 — v0.9 Universal Opportunity Graph / broader alpha — complete
- v0.9.0 canonical graph, detector registry and common ranking
- v0.9.1 Bybit/Kraken breadth, dated-futures basis and CEX↔CEX spot dislocation
- v0.9.2 stablecoin, DEX-pool, bridge, solver/liquidation, options and paper-allocation research surfaces

## v0.10 — Evidence maturation — active

### v0.10.0 — public adapter registry / OKX promotion — complete
- Central public adapter registry for quotes, funding, L2 routing and provider attribution.
- OKX promoted into core discovery/qualification/shadow/learning.
- Empty public surfaces fail closed and live diagnostics are exposed.

### v0.10.1 — amount-specific DEX route evidence — complete
- Quote-only Velora BTC/ETH↔USDC routes.
- Exact amounts, route composition, block/gas/latency lineage.
- No capacity or execution authority.

### v0.10.2 — durable DEX route survival — complete
- Exact-source re-quotes at 1/5/15/30/60-second horizons.
- Durable survival/adverse-price/route-change/gas/latency evidence.
- DEX evidence failures isolated from core CEX worker success.

### v0.10.3 — multi-notional DEX route frontier — complete
- Sequential $1k/$5k/$10k/$25k probes by default.
- Conservative contiguous acceptable quote frontier.
- Durable evidence with `capacity_claimed=false`.

### v0.10.4 — explicit DEX/CEX quote-currency conversion — complete
- Fresh observed conversion path required for USDC DEX vs USD/USDT CEX comparisons.
- Direct/two-hop conversion paths, directional normalization, spread embedded once, depeg risk charged separately.
- Missing/stale conversion evidence fails closed.

### v0.10.5 — amount-specific stablecoin conversion depth — complete
- Public Coinbase Exchange level-2 `USDC-USD` and `USDT-USD` books.
- Exact source-amount visible-depth reconstruction for both directions.
- USDC↔USDT two-hop conversions carry the actual intermediate USD amount.
- Full fill, book freshness and multi-book timestamp skew are mandatory.
- Per-leg effective rate, slippage, levels, timestamps and request latency are retained.
- Read-only stablecoin depth-quote API.
- `visible_depth_only=true`, `capacity_claimed=false`, `executable_eligible=false`.

### same-notional CEX↔DEX composite evidence — complete
- Each quoted DEX route tier is joined to stablecoin conversion depth and CEX hedge economics at the same economic amount.
- DEX-sell proceeds and DEX-buy hedge proceeds use the exact directional conversion-depth output.
- CEX taker fees, DEX gas and stablecoin depeg/risk haircuts are charged separately without double-counting conversion spread/slippage.
- Incomplete depth/fee rows fail independently; complete rows remain research-only.

### v0.10.6 — DEX statistical research qualification — current
- Independent DEX route cycles, not repeated horizons, define effective sample size.
- Route survival and repeated frontier acceptance use 95% Wilson confidence intervals by default.
- Confidence-width, minimum-effective-sample and adverse-tail-sample gates are mandatory.
- p95 adverse route deterioration must remain below a configured ceiling.
- Qualification is asset, direction, notional and horizon specific; evidence at one capital tier cannot certify another tier.
- A live same-notional composite row can become `research_qualified=true` only after both current economics and historical statistical gates pass.
- Research qualification still has `capacity_claimed=false`, `allocation_eligible=false` and `executable_eligible=false`.

### Next v0.10 evidence work
1. Extend multi-horizon route shadowing across the larger $5k/$10k/$25k frontier tiers so those tiers can accumulate their own independent statistical evidence.
2. Persist composite CEX↔DEX observations so net-edge survival can be calibrated directly rather than combining current economics with route-only history.
3. Build cross-venue inventory/settlement state and explicit atomic hedge/recovery models before CEX↔DEX can enter paper allocation.
4. Add stablecoin conversion-depth shadow/persistence evidence and effective sample gates.
5. Add authoritative bridge quote evidence only when a reliable supported source exists.
6. Add option L2 and hedge-aware execution economics before options can enter allocation.
7. Add authoritative liquidation/solver feeds before those families can enter allocation.

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg hedge recovery, concentration limits, dead-man switch and kill switch. Remains blocked until accumulated shadow evidence is statistically convincing and live execution is separately authorized.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing

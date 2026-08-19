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

Completing v0.9 means each targeted opportunity family has a canonical place in the graph and a fail-closed path to future evidence. It does not mean every family is executable.

## Milestone 5 — Shadow evidence runtime — v0.7 complete
- Durable worker/PostgreSQL topology
- Multi-horizon 1/5/15/30/60s observation
- Failure causes and segmented survival
- Provider degradation scoped to dependent venues

## v0.8 — Empirical fill/latency modeling — complete
- L2 request latency measurement
- Partial-fill and unhedged-exposure reconstruction
- Hedge-recovery loss distributions
- Hierarchical empirical cohorts
- Interval-censored conservative interpolation
- Effective independent sample size
- Wilson confidence intervals and confidence-width gates
- Conservative fixed fallbacks whenever empirical gates fail

## v0.10 — Evidence maturation — active

### v0.10.0 — adapter registry / OKX core promotion — complete
- One public-adapter registry owns quote/funding collection, visible-L2 routing and provider→venue attribution.
- OKX spot/perpetual/funding is promoted into core discovery, qualification, shadow observation and empirical-learning eligibility.
- Explicit OKX fee configuration is enforced in executable economics.
- Empty provider surfaces are degraded rather than treated as successful zero-result scans.
- Live provider diagnostics inspect public market/funding surfaces and representative visible L2, including request latency.

### v0.10.1 — amount-specific DEX route evidence — current
- Quote-only Velora `/prices` v6.2 integration for Ethereum BTC/ETH↔USDC route evidence.
- $1,000 buy/sell evidence probes retain exact input/output, block number, route composition, gas estimate and request latency.
- RFQ liquidity is excluded from the route probe.
- Successful probes are not treated as capacity evidence.
- Route-quoted CEX↔DEX candidates remain blocked from allocation pending inventory/settlement, stablecoin conversion, quote survival and hedge-recovery evidence.
- Remove duplicate universal-layer OKX collection after its v0.10.0 core promotion.

### Next v0.10 evidence work
1. Persist amount-specific DEX route observations and build multi-horizon route-survival/price-decay evidence.
2. Probe multiple notionals and infer capacity only from a conservative, observed quote-size frontier.
3. Qualify stablecoin conversion paths jointly with DEX route comparisons.
4. Add cross-venue inventory/settlement and hedge-recovery models before CEX↔DEX can enter paper allocation.
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

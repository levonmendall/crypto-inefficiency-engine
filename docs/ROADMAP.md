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

## Milestone 4 — Universal opportunity graph / broader alpha — in progress
### v0.9.0 complete
- Canonical asset/venue/instrument identities
- Provider-symbol aliases
- Economic-equivalence graph edges
- Detector registry
- Common qualified-opportunity ranking surface

### v0.9.1 complete
- Bybit public spot/perpetual/dated-futures market data
- Kraken public USD spot depth
- Contract-specific dated-future identity
- Dated futures cash-and-carry detector
- CEX↔CEX same-quote spot-dislocation detector
- Explicit Bybit/Kraken fee models
- Quote-currency compatibility gates
- Opportunity-scoped provider-failure attribution
- CEX spot short remains fail-closed without borrow/inventory economics

### Next breadth
1. Stablecoin identity and conversion-risk model so USD/USDC/USDT relationships can be compared explicitly rather than assumed equivalent.
2. Additional direct CEX/perpetual venues behind the same adapter contract.
3. DEX pool and token/chain identity graph.
4. CEX↔DEX and DEX↔DEX route evaluation.
5. Cross-chain liquidity/bridge costs and settlement risk.
6. Liquidation/backstop and solver opportunity interfaces.
7. Options/volatility-relative-value graph.
8. Portfolio capital allocator only after multiple independent strategy families have sufficient evidence.

## Milestone 5 — Shadow evidence runtime — v0.7 complete
- Durable worker/PostgreSQL topology
- Multi-horizon 1/5/15/30/60s observation
- Failure causes and segmented survival
- Provider degradation fail-closed, now scoped to dependent venues

## v0.8 — Empirical fill/latency modeling — complete
- L2 request latency measurement
- Partial-fill and unhedged-exposure reconstruction
- Hedge-recovery loss distributions
- Hierarchical empirical cohorts
- Interval-censored conservative interpolation
- Effective independent sample size
- Wilson confidence intervals and confidence-width gates
- Conservative fixed fallbacks whenever empirical gates fail

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg hedge recovery, concentration limits, dead-man switch and kill switch. Remains blocked until accumulated shadow evidence is statistically convincing and live execution is separately authorized.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing

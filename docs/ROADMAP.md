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

### v0.9.0 — graph foundation — complete
- Canonical asset/venue/instrument identities
- Provider-symbol aliases
- Economic-equivalence graph edges
- Detector registry
- Common qualified-opportunity ranking surface

### v0.9.1 — CEX breadth — complete
- Bybit public spot/perpetual/dated-futures market data
- Kraken public USD spot depth
- Contract-specific dated-future identity
- Dated futures cash-and-carry detector
- CEX↔CEX same-quote spot-dislocation detector
- Explicit Bybit/Kraken fee models
- Quote-currency compatibility gates
- Opportunity-scoped provider-failure attribution
- CEX spot short remains fail-closed without borrow/inventory economics

### v0.9.2 — universal alpha graph completion — complete
- Stablecoin USD/USDC/USDT identity and conversion edges with spread/depeg risk haircuts
- Public OKX spot/perpetual/funding adapter for additional venue discovery
- Chain-token and DEX-pool identity using public DexScreener pool discovery
- CEX↔DEX and DEX↔DEX research candidate generation
- DEX liquidity is explicitly a discovery proxy; exact executable swap depth remains blocked
- Cross-chain bridge quote/cost/settlement-risk model and graph capability edges
- Liquidation/backstop and solver typed opportunity interfaces with authoritative-capacity gates
- Public Deribit options surface ingestion and volatility-relative-value anomaly discovery
- Options remain non-executable until option L2, fee, delta/vega hedge and paired-capacity models exist
- Deterministic paper capital allocator over already-qualified CEX opportunities only
- Allocator enforces capital, venue, asset, capacity and shared-instrument conflict constraints
- Cash/no allocation remains valid and allocator has no execution authority
- Universal read-only API surfaces for graph, candidates, stablecoin conversion, interfaces and paper allocation

### v0.9 evidence boundary
Completing v0.9 means every originally targeted opportunity family now has a canonical place in the graph and a fail-closed path to future execution evidence. It does **not** mean every family is executable. DEX route depth, bridge quotes, solver/liquidation capacity and option hedging intentionally remain blocked until authoritative evidence is available.

## Milestone 5 — Shadow evidence runtime — v0.7 complete
- Durable worker/PostgreSQL topology
- Multi-horizon 1/5/15/30/60s observation
- Failure causes and segmented survival
- Provider degradation fail-closed, scoped to dependent venues

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

### v0.10.0 — adapter registry / OKX core promotion
- One public-adapter registry owns quote/funding collection, visible-L2 routing and provider→venue attribution.
- OKX spot/perpetual/funding is promoted into core discovery, qualification, shadow observation and empirical-learning eligibility.
- Explicit OKX fee configuration is enforced in executable economics.
- Empty provider surfaces are degraded rather than treated as successful zero-result scans.
- Live provider diagnostics inspect public market/funding surfaces and representative visible L2, including request latency.
- CLI/API diagnostic surfaces remain read-only and paper-only.

### Next v0.10 evidence work
1. Observe production provider diagnostics and shadow evidence across Coinbase, Kraken, Hyperliquid, Bybit and OKX.
2. Replace DEX discovery proxies with exact route-specific quote evidence without adding signing or transaction submission.
3. Persist DEX quote/path lineage and build a route-survival shadow study before DEX can enter allocation.
4. Add authoritative bridge quote evidence only when a reliable supported source exists.
5. Add option L2 and hedge-aware execution economics before options can enter capital allocation.
6. Add authoritative liquidation/solver feeds before those families can enter allocation.
7. Promote additional strategy families into the allocator only after their own evidence gates are satisfied.

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg hedge recovery, concentration limits, dead-man switch and kill switch. Remains blocked until accumulated shadow evidence is statistically convincing and live execution is separately authorized.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing

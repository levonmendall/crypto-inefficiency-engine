# Roadmap

## Paper V1 — complete

The paper-first Crypto Inefficiency Engine has reached its V1 definition of done.

Its objective is to continuously search accessible crypto markets for structural inefficiencies, reconstruct conservative net economics after observable costs and risk, learn which apparent edges survive market contact, and allocate **paper capital only** to independently qualified opportunities.

### Foundation — complete
- Canonical asset, venue and instrument identity.
- Strategy-neutral market/opportunity graph and detector registry.
- Public provider registry with fail-closed provider diagnostics.
- Append-only SQLite/PostgreSQL point-in-time evidence and lineage hashes.
- Deterministic replay.
- Visible-L2 depth, VWAP/slippage, exact paired-leg sizing and capacity frontiers.
- Explicit fees, financing/borrow, collateral opportunity cost, latency and hedge-recovery economics.
- Multi-horizon shadow evidence and statistically gated empirical calibration.

### Core CEX opportunity families — paper allocatable
- Funding dispersion.
- Spot/perpetual basis.
- Dated-futures basis.
- CEX spot dislocation when required short-borrow economics are available.

These families use the common CEX executability pipeline and cannot allocate capital unless their exact capital tier passes current economics and qualification gates.

### CEX↔DEX evidence maturation — complete for paper promotion
- Amount-specific quote-only DEX routes.
- Exact-source multi-horizon route re-quotes.
- $1k/$5k/$10k/$25k route frontiers and independent tier evidence.
- Fresh observed USD/USDC/USDT conversion paths.
- Amount-specific Coinbase stablecoin L2 depth and two-hop reconstruction.
- Same-notional DEX route + conversion depth + CEX hedge economics.
- Append-only fully costed composite-edge shadow history.
- Independent route/frontier statistics with Wilson confidence gates.
- Independent stablecoin conversion-depth survival and tail statistics.
- Direct composite net-edge survival statistics, including p95 deterioration and low-tail retained edge.
- Explicit pre-funded paper inventory requirements.
- No synchronous bridge/deposit/withdrawal assumption during a qualifying opportunity.
- Independent CEX hedge-recovery venue and recovery reserve.
- Final statistically haircutted capture edge before paper allocation eligibility.

### Strategy-neutral paper allocator — complete
The unified allocator compares independently qualified core CEX and CEX↔DEX opportunities using conservative expected return on reserved capital for the **current deployment**. It does not pretend that a fast arbitrage edge can be continuously annualized.

The allocator enforces:
- total paper-capital limits;
- venue concentration;
- asset concentration;
- shared instrument/route conflicts;
- explicit two-leg capital reservation;
- cash as a valid outcome.

Allocation never authorizes execution.

## Research families intentionally fail closed

These families remain searchable in the universal graph, but Paper V1 does **not** upgrade incomplete evidence into trading authority.

### DEX↔DEX — research only
Blocked until independent pool-specific, route-specific executable depth can be observed and shadowed rather than inferred from pool-liquidity proxies.

### Stablecoin dislocation — research only
Amount-specific conversion depth and stability evidence exist, but a market-neutral redemption/convergence path is not modeled. Paper V1 will not promote directional peg speculation as arbitrage.

### Cross-chain — research only
Blocked until a fresh authoritative bridge-quote source provides amount-specific fees, fill time and settlement-risk evidence that can be persisted and statistically evaluated.

### Solver — research only
Blocked until an authoritative auction/capacity/settlement feed is connected. A typed capability interface exists; synthetic solver capacity does not create allocation authority.

### Liquidation/backstop — research only
Blocked until authoritative liquidation capacity, expiry, cost and recovery evidence are available.

### Option relative value — research only
Public option-surface discovery exists. Paper promotion remains blocked pending option L2, fee economics, delta hedge construction, vega/gamma risk and paired capacity evidence.

## Post-V1 work

### Evidence accumulation / production proof
The deployed worker must accumulate enough independent observations for statistical gates to pass at the asset, direction and notional tiers where real opportunities occur. A family being architecturally promotable does not imply current live evidence has already reached its sample thresholds.

### Broader venue and strategy maturation
Additional public CEX/DEX venues can be promoted through the common adapter/evidence contracts. Research-only families should be promoted only when their missing authoritative evidence becomes available; thresholds must not be weakened simply to generate candidates.

### Tiny-capital controlled execution — intentionally blocked
Real-money execution is **not part of Paper V1**. Any future live executor must be a separate service with separate explicit authorization, credentials, hard capital caps, paired-leg recovery, concentration controls, dead-man/kill switches and production evidence proving the relevant strategy statistically convincing.

### Machine-paid intelligence API — optional commercialization
- API keys and usage metering.
- Per-query pricing.
- Bot/agent endpoints.
- Optional machine-payment gateway.
- Never expose private positions or proprietary execution timing.

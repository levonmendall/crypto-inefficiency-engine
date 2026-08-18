# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and proving whether apparent edge survives point-in-time evidence, real visible depth, explicit fees, capital usage, latency risk, and repeated shadow observation. V0.5 does not place live orders and does not require exchange trading keys.

## Current capabilities

1. Pull predicted perpetual funding across venues from Hyperliquid public market data.
2. Normalize funding intervals so venue rates are comparable.
3. Detect cross-venue funding dispersion opportunities.
4. Pull public Coinbase spot quotes and Hyperliquid perpetual context.
5. Detect spot/perp basis candidates.
6. Persist append-only scans with provider health, lineage hashes, and the exact analysis configuration.
7. Replay stored scans to detect research/configuration drift.
8. Parse Hyperliquid perpetual and Coinbase spot L2 order books.
9. Qualify two-leg opportunities at configured capital tiers using the same base quantity on both legs.
10. Estimate the continuous **capacity frontier**: the largest visible notional that still clears the return hurdle.
11. Charge explicit conservative venue taker fees rather than relying only on a generic transaction-cost guess.
12. Measure returns on modeled capital required across **both** legs, not a single-leg notional denominator.
13. Charge configurable collateral opportunity cost, book-age/hedge-latency risk, and a hedge-recovery buffer.
14. Require extra visible hedge liquidity beyond the intended fill quantity.
15. Fail closed when short-spot borrow cost is required but unavailable.
16. Run live **shadow cycles** that re-scan after a delay and record whether the same economic opportunity remains executable at the original target size.
17. Persist shadow survival evidence and expose aggregate survival statistics.
18. Expose a read-only/API-first surface designed to become a future paid machine-to-machine intelligence service.

## Conservative fee defaults

The default execution model assumes taker execution for entry and exit until measured shadow/live evidence justifies anything less conservative:

- Coinbase Exchange spot: **60 bps per taker fill** by default (lowest-volume public tier). Override `CIE_COINBASE_SPOT_TAKER_FEE_BPS` only with verified account-tier economics.
- Hyperliquid perps: **4.5 bps per taker fill** base rate by default. Override `CIE_HYPERLIQUID_PERP_TAKER_FEE_BPS` when verified account-specific fee data is available.

The detector-level generic cost remains only a floor; explicit venue fees replace it when higher so the same cost category is not double-counted.

## Non-negotiable safety boundary

- `paper_only=true` is hard-coded into the execution boundary.
- No private keys, trading API secrets, custody, deposits, withdrawals, or live order placement.
- An opportunity with stale/incomplete data is rejected.
- Unknown venue fees fail closed during executable qualification.
- Short spot fails closed without an explicit borrow-cost assumption.
- Visible order-book depth is evidence, not a promise of a future fill.
- Shadow survival is evidence of persistence, not proof that a real order would have received the same queue position or fill.
- Venue names in third-party market data are observations, not assertions that a venue is legally accessible to a given user.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
export CIE_EVIDENCE_DB_PATH=data/cie-evidence.sqlite3
uvicorn inefficiency_engine.api:app --reload
```

Useful commands:

```bash
cie demo
cie live
cie executability
cie shadow-once
cie shadow-loop
```

Endpoints:

- `GET /health`
- `GET /v1/opportunities/demo`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/evidence/{scan_id}/replay`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`

## Why this architecture?

Finding a spread is easy. The valuable question is whether the spread remained available **after real fee schedules, capital requirements, depth, slippage, latency exposure, hedge risk, and time**. V0.5 begins measuring that question instead of assuming it.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/ROADMAP.md`](docs/ROADMAP.md).

# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and proving whether the apparent edge survives point-in-time evidence and execution constraints. V0.3 does not place live orders and does not require exchange trading keys.

## Current capabilities

1. Pull predicted perpetual funding across venues from Hyperliquid's public info API.
2. Normalize funding intervals so venue rates are comparable.
3. Detect cross-venue funding dispersion opportunities.
4. Pull public Coinbase spot quotes and Hyperliquid perpetual context.
5. Detect spot/perp basis candidates.
6. Apply explicit transaction-cost, freshness, and safety-haircut assumptions.
7. Persist append-only scans with provider health, lineage hashes, and the exact analysis configuration.
8. Replay a stored scan to detect research/configuration drift.
9. Parse Hyperliquid perpetual and Coinbase spot L2 order books.
10. Qualify two-leg opportunities at $1K/$10K/$25K/$50K/$100K tiers using the same base quantity on both legs.
11. Recalculate net return after observed depth, entry slippage, conservative exit slippage, static costs, and the safety buffer.
12. Fail closed when either leg lacks a supported book, is stale, has excessive timestamp skew, or cannot fill the target hedge quantity.
13. Persist order books and executability decisions so execution qualification can be deterministically replayed.
14. Expose a read-only API designed to become a future paid machine-to-machine intelligence service.

## Non-negotiable safety boundary

- `paper_only=true` is hard-coded into the execution boundary.
- No private keys, trading API secrets, custody, deposits, withdrawals, or live order placement.
- An opportunity with stale/incomplete data is rejected.
- Apparent edge below modeled costs + safety buffer is rejected.
- Visible order-book depth is treated as evidence, not a promise of a future fill.
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

Endpoints:

- `GET /health`
- `GET /v1/opportunities/demo`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/evidence/{scan_id}/replay`

## Why this architecture?

Finding a spread is easy. Proving the spread existed **point-in-time, after costs, at executable size on both hedge legs** is the difficult part. The project is deliberately building evidence and executability before adding live capital.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/ROADMAP.md`](docs/ROADMAP.md).

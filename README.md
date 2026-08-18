# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies. V1 does not place orders and does not require exchange API keys.

## V1 scope

1. Pull predicted perpetual funding across venues from Hyperliquid's public info API.
2. Normalize funding intervals so venue rates are comparable.
3. Detect cross-venue funding dispersion opportunities.
4. Pull public Coinbase spot quotes and Hyperliquid perpetual context for selected assets.
5. Detect spot/perp basis candidates.
6. Apply explicit transaction-cost, freshness, liquidity, and safety-haircut assumptions.
7. Rank opportunities by estimated **net annualized return**, never gross spread alone.
8. Expose read-only API endpoints suitable for a future paid machine-to-machine API.

## Non-negotiable V1 safety boundary

- `paper_only=true` is hard-coded into the execution boundary.
- No private keys, API secrets, custody, deposits, withdrawals, or live order placement.
- An opportunity with stale/incomplete data is rejected.
- Apparent edge below modeled costs + safety buffer is rejected.
- Venue names in third-party market data are observations, **not assertions that the venue is legally accessible to a given user**.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
uvicorn inefficiency_engine.api:app --reload
```

Then open:

- `GET /health`
- `GET /v1/opportunities/demo`
- `GET /v1/opportunities/live` (requires outbound internet from the runtime)

## Why funding dispersion first?

Funding is a mechanical transfer between longs and shorts. When the same asset has meaningfully different normalized funding across venues, a hedged pair can potentially capture the difference. V1 models the full paired round-trip cost and refuses opportunities that do not clear it.

## Roadmap

See [`docs/ROADMAP.md`](docs/ROADMAP.md). The next major milestones are persistent point-in-time data, order-book executable sizing, live shadow fills, additional venue adapters, strategy attribution, and only then a separately authorized tiny-capital execution service.

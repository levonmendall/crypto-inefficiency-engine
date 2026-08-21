# Source Coverage Plane

The Source Coverage Plane is a paper-only, diagnostic evidence layer for the 13 canonical profit mechanisms. It answers a different question from profitability qualification: **does each mechanism have sufficiently broad, fresh and authoritative source evidence to be evaluated honestly?**

It does not grant allocation or execution authority. Strategy economics, independent forward outcomes, statistical promotion, portfolio risk, execution qualification and settlement remain separate downstream gates.

## Source sufficiency

Each lane declares the evidence classes it needs. A source is counted only when its latest observation is healthy, fresh, point-in-time, commercially usable and authoritative. The default maturity target is two independent authoritative source groups plus complete coverage of that lane's required evidence classes.

The plane distinguishes `provider_gap`, `evidence_class_gap`, `concentration_risk`, and `sufficient`. Downstream calibration gaps remain visible even after the source layer becomes sufficient.

## Priority evidence expansion

The current expansion closes the highest-value source gaps in this order:

1. **Liquidation / distress** — Bybit all-liquidation WebSocket, Aave V3 `LiquidationCall` logs, plus existing Bybit ADL/insurance and Hyperliquid distress state. Raw liquidation events never fabricate capture or settlement probabilities.
2. **Event driven** — existing Bybit/Coinbase listing catalogs plus Snapshot governance proposals. Tokenomist is catalogued as an optional credential-gated future source rather than called without authorization/credentials.
3. **Yield** — Lido plus Morpho protocol-native market rate, capacity and available-liquidity evidence. Yield observations remain subject to protocol-loss and realized-yield forward calibration.
4. **Options / volatility** — Deribit plus Bybit and OKX option quotes/Greeks, with OKX executable book depth. Hedge-path and gap/vega/gamma validation remains downstream.
5. **On-chain fundamentals** — Ethereum finalized RPC plus Morpho protocol state, with DefiLlama as secondary discovery/normalization only. Secondary sources do not count as authoritative redundancy or create alpha authority.

## API

- `GET /v2/source-coverage`
- `GET /v2/source-coverage/{lane_id}`

Both are read-only. Every returned lane remains `paper_only=true`, `allocation_authority=false`, and `live_execution_authority=false`.

# Dynamic Stablecoin Exclusion for the Top-40 Volume Universe

The directional/CEX research universe is ranked strictly by CoinGecko rolling 24-hour `total_volume` after eligibility filtering.

Stable-value eligibility is now data-driven. Every refresh requires both:

1. the market-wide `/coins/markets?order=volume_desc` feed; and
2. CoinGecko's `/coins/markets?category=stablecoins` classification feed.

CoinGecko asset IDs present in the live stablecoin category are excluded before the remaining assets are ranked. A small ticker denylist remains only as defense in depth; it is no longer the authority for determining whether a newly issued asset is stable-value.

The snapshot method is versioned as `marketwide_24h_trading_volume_usd_dynamic_stable_v2`, which intentionally invalidates previously persisted top-40 snapshots that were created with the static-list-only filter. If a new dynamically classified snapshot cannot be obtained and there is no prior v2 snapshot, the universe fails closed rather than reverting to a static or legacy list.

This change does not alter alpha qualification, forward-evidence requirements, sizing, risk controls, settlement gates, or paper-only execution authority.

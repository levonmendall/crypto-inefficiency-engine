# v3.5.11 portfolio hot-path repair

Production evidence after v3.5.10 showed that canonical account snapshots were advancing again while market-evidence time remained frozen and each account update was still a fallback snapshot. That proves the watchdog restored accounting liveness, but the portfolio cycle continued to time out downstream of the initial executable market scan.

The canonical ledger currently settles only spot directional-long alpha positions. Before v3.5.11 the portfolio cycle nevertheless ran the full unified allocator, including core multi-leg CEX and CEX↔DEX qualification paths that can never be opened by the canonical ledger. Those paths add additional public-provider, DEX-route, stablecoin-depth and statistical work to the liveness-critical accounting cycle. Alpha promotion also performed a second direct L2 provider request instead of reusing the point-in-time order book already collected in the executable scan.

v3.5.11 makes the runtime boundary match settlement capability:

- full research and forward certification still evaluate the complete mechanism surface;
- canonical accounting uses a settlement-compatible allocator restricted to alpha positions it can actually settle;
- canonical allocation consumes the latest executable scan already persisted by the portfolio cycle instead of initiating another market scan;
- alpha promotion reuses the matching fresh L2 book from that scan;
- only when the scan lacks a matching fresh book may alpha promotion make a direct provider request, and that fallback is bounded by the adapter registry order-book timeout;
- all live-money authority remains disabled and no qualification threshold is relaxed.

The intended production result is that a successful executable scan can advance canonical market-evidence time and complete the accounting cycle without waiting on unsupported mechanism families. Full mechanism certification remains independent and may continue to report zero certified mechanisms until forward statistical gates are genuinely satisfied.

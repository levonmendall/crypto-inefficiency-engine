# V2.4 Forward Allocation Certification

V2.4 adds an append-only scorecard for the unified allocator's actual paper decisions.

The earlier forward-alpha ledger tests whether a strategy signal works. This layer tests a harder question:

> After statistical promotion, adaptive health sizing, cross-strategy ranking and portfolio risk constraints, did the allocation the engine actually chose deliver the economics it predicted?

## Point-in-time decision envelope

Predictive allocations now preserve the source timestamp, venue symbol, market kind, entry reference price and point-in-time modeled round-trip cost that were available when the allocator made its decision. These fields are evidence only and do not create execution authority.

Every selected allocation is written to the append-only allocation trial ledger with its predicted profit, return on reserved capital, exposure kind, capital reservation and settlement support status.

## Fail-closed settlement coverage

V2.4 does not invent realized P&L for an opportunity family simply because the allocator selected it.

The first authoritative settlement method is deliberately narrow:

- directional-long predictive alpha;
- spot market exposure;
- one identified venue/symbol;
- a known entry reference price;
- a precommitted modeled round-trip cost;
- a defined holding horizon.

At maturity, the forward spot move is measured from a later public point-in-time price and the original modeled round-trip cost is subtracted. The resulting realized net return and paper profit are compared with the predicted paper profit.

The method is explicitly labeled `spot_mid_forward_minus_point_in_time_roundtrip_cost`. It is an early forward economic reconstruction, not a claim of a live fill.

## What remains unsupported

Unsupported allocations are still persisted as decisions but excluded from realized-profit statistics.

- perpetual directional shorts remain unsupported until realized funding accrual can be reconstructed over the exact holding interval;
- funding/basis and other market-neutral CEX opportunities require two-leg entry/exit and carry settlement;
- CEX↔DEX opportunities require family-specific route, hedge and settlement evidence.

This prevents missing execution evidence from becoming synthetic profitability.

## Forward scorecard

The certification summary reports:

- total recorded allocation decisions;
- supported vs unsupported settlement decisions;
- supported outcomes that have matured;
- settlement coverage;
- predicted profit for settled trials;
- reconstructed realized profit for settled trials;
- aggregate and median profit-capture ratios;
- mean prediction error;
- profitable-trial rate.

Missing future market data leaves a supported trial pending rather than assigning a result.

## Continuous operation

The durable worker runs allocation certification on its own staggered cadence, separate from route, stablecoin, composite-edge and alpha-forward evidence cycles. The API exposes:

- `GET /v2/allocation/certification/summary`
- `POST /v2/allocation/certification/cycle`

## Authority boundary

V2.4 remains paper-only. Certification observes and measures decisions after the fact. It cannot promote a strategy, override the unified allocator, authorize live execution, access private balances, sign transactions or submit orders.

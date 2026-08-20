# Cycle-Aware Directional Alpha

## Objective

Capture sustained directional crypto trends without turning the Bitcoin halving calendar into a mechanical buy/sell rule.

The strategy is **paper-first, evidence-gated and fail-closed**. It competes for capital through the existing unified allocator only after the same forward statistical qualification, current L2 cost reconstruction, adaptive health controls and portfolio risk limits applied to other predictive alpha families.

## Signal hierarchy

`7 / 30 / 90 / 180-day price trend`

→ `multi-horizon directional agreement`

→ `BTC regime confirmation`

→ `market breadth confirmation`

→ `bounded Bitcoin-halving-cycle prior`

→ `holding-period cost reconstruction`

→ `forward evidence`

→ `paper qualification`

→ `unified capital competition`

The default forward holding horizon is 72 hours. Overlapping forward cohorts are not counted as independent evidence.

## Four-year cycle rule

The halving cycle is never an independent trade trigger. Completed Bitcoin halvings estimate a rough structural cycle position. That position may tilt the trend forecast by **at most 10%** and cannot reverse the direction selected by observed price trend.

## Directional construction

Long candidates require a spot instrument. Short candidates require a perpetual instrument with a current matching funding observation. Favorable funding is ignored as forecast alpha; adverse funding expected over the modeled holding period is charged as a cost.

The strategy also rejects a candidate when its direction is strongly contradicted by the BTC regime or broad-market participation.

## Memory boundary

Fast alpha strategies retain exact high-frequency history over their active windows. Long-horizon trend evidence is read from persisted market history as a compact daily sample, while the recent exact window overlaps the cache refresh interval. This avoids materializing months of high-frequency quotes in memory merely to estimate a slow trend.

If the compact long-history read fails, only this strategy loses sufficient evidence. Existing alpha families continue operating.

## Promotion and authority

Discovery alone creates no capital authority. A cycle-aware trend candidate must still pass independent forward samples, confidence lower bounds, hit-rate and regime gates, current L2 economics, modeled carry costs, recent strategy-health controls, portfolio risk limits, and competition against every other qualified opportunity and cash.

The strategy remains paper-only and cannot submit live orders.

# Card truth resolver repair

This repair makes the dashboard card read model resolve source/provider truth separately from stale research and certification projections.

Key invariants:

- current canonical source truth owns provider status and the displayed current authoritative item count;
- stale research cannot turn a connected source into a provider gap;
- stale source evidence is distinct from a missing provider;
- legacy projected row-tail counts are diagnostic only and cannot be displayed as observation counts;
- research continues to own signals, forward outcomes, statistical qualification, promotion, settlement, and certification;
- no paper allocation or live-execution authority is created by presentation reconciliation.

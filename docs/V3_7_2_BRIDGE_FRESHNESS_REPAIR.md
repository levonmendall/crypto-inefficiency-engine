# v3.7.2 — Qualified-opportunity freshness separation

## Production symptom

The canonical $250,000 paper account remained exact and market evidence continued to advance, but the dashboard reported `Cycle degraded` and exactly one degraded opportunity family.

The v3.7 qualified-opportunity envelope expired on the same clock as its underlying market evidence (normally about 120 seconds). The research worker publishes the envelope near the beginning of a sequential bounded research cycle, then continues through DEX shadow and other research stages. A healthy research cycle can therefore outlive the market-data TTL before the next bridge publication.

The canonical allocator correctly failed closed when that envelope expired, but the resulting `QualifiedOpportunitySnapshotUnavailableOrStale` family failure incorrectly classified the whole portfolio cycle as degraded even when accounting and market collection were healthy.

## Repair

v3.7.2 separates two independent clocks:

1. **Bridge control freshness** — proves that the research-to-accounting bridge has completed successfully recently enough to be considered operational. The control envelope now spans the expected sequential research cadence.
2. **Candidate evidence freshness** — remains short and point-in-time. Every candidate is independently checked against `max_quote_age_seconds` before it can reserve paper capital.

A stale candidate is now dropped to cash with a diagnostic skip reason and no family-level degradation. It is never allocated from the longer-lived control envelope.

A genuinely failed bridge publication remains fail-closed and degraded immediately through the dedicated `qualified-opportunity-bridge` heartbeat. An expired control envelope also remains a family failure.

## Invariants preserved

- Paper-only; no live execution authority.
- No profitability, statistical, liquidity, cost, settlement, or risk threshold is weakened.
- Stale market evidence cannot create a paper position.
- Heavy research is never rerun inside the canonical accounting hot path.
- The v3.7.1 memory-bounded scan/history repair remains in force.
- The complete discovery universe remains persisted; this change only changes freshness semantics at the bridge boundary.

## Regression coverage

The v3.7.2 tests prove that:

- stale candidate evidence produces cash without a false family degradation;
- fresh candidate evidence remains allocatable;
- an expired bridge control envelope still fails closed;
- an explicit degraded bridge heartbeat overrides an otherwise active envelope;
- the bridge-control TTL is longer than the candidate market-data TTL.

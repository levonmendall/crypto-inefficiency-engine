# v3.7.1 Memory-bounded qualified-opportunity bridge repair

## Production symptom

After v3.7.0 reached Render, the consolidated service repeatedly exceeded the 2 GB memory limit and restarted. The qualified-opportunity architecture was correct, but its first implementation reconstructed the newest persisted `ScanSnapshot` with `EvidenceStore.load_scan()` before publishing the bridge envelope.

The memory-bounded research service intentionally persists the complete discovered universe while retaining only a rotating bounded L2/executability working set in memory. Reconstructing the full scan inside the bridge defeated that boundary and could materialize provider rows, funding, every market quote, every discovered opportunity, every order book and every executability payload at once.

The bridge also invoked alpha promotion on every bridge projection. Alpha discovery rebuilt the configured historical quote window as a raw JSON list and then as Pydantic objects, creating an avoidable second peak.

## Repair

v3.7.1 keeps the Universal Qualified-Opportunity Bridge and canonical settlement design but makes the research-to-portfolio transfer memory-bounded.

The production bridge now reads only:

- latest scan metadata;
- current spot/perpetual market quotes needed by active alpha strategies;
- the already-bounded order-book working set;
- the already-bounded executability rows; and
- only the exact structural opportunities referenced by those executability rows.

It deliberately does not load provider rows, funding rows, unrelated discovery rows, or the complete persisted scan. Full discovery remains durable and unchanged on disk.

The production alpha factory now:

- streams historical quote rows from the database instead of first materializing the raw JSON result set;
- retains history only for venue/asset/market series present in the current snapshot; and
- uses the maximum lookback actually consumed by the active strategies, subject to the existing configured history cap.

With the current strategy set, the longest history window that affects discovery is the 48-hour cross-sectional window. Momentum and mean reversion use 24 hours, microstructure uses 6 hours, and event research uses 24 hours. Fundamental/event regime labels consume only the trailing minimum-history sample set. Therefore dropping persisted rows older than the active strategy lookbacks does not change a candidate decision.

## Governance preserved

This repair does not:

- lower profitability or statistical thresholds;
- reduce the persisted discovery universe;
- reduce the rotating research coverage set;
- authorize live execution;
- change the $250,000 canonical paper portfolio;
- make stale evidence allocatable; or
- put provider-heavy research back into canonical accounting.

The portfolio still consumes only fresh, paper-only qualified-opportunity envelopes and fails closed to cash if that envelope is unavailable or stale.

## Regression protection

Tests explicitly make `EvidenceStore.load_scan()` raise if the bridge attempts to call it and verify that an unrelated persisted opportunity is not materialized into the bridge projection. Additional coverage verifies that alpha history excludes non-current instruments and rows older than the longest active strategy lookback while retaining the exact relevant history.

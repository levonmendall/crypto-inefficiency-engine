# Research Qualification Reset

## Objective

Increase the rate at which the engine can learn whether crypto opportunities have real net edge without lowering the standards for meaningful paper capital and without creating any live-execution path.

The operating principle is:

> Easy to research, progressively harder to trust, extremely hard to allocate meaningful capital.

## Research funnel

The alpha path now has three practical evidence levels:

1. **Diagnostic shadow** — any directional candidate with valid source evidence, a usable price reference, and measurable execution cost can accumulate diagnostic outcomes. Missing/stale source or unavailable cost still fails closed.
2. **Provisional paper** — candidates with at least 12 independent forward observations, at least one observed regime, positive conservative net expectancy after a partial multiple-testing penalty, healthy recent forward behavior, current executable economics, and forward-test-sufficient source coverage may receive at most $1,000 of paper notional per candidate. This tier is explicitly not full qualification.
3. **Fully qualified paper** — the ordinary 30-sample target, full multiple-testing-adjusted mean-return hurdle, regime requirements, current L2/economic revalidation, strategy health, and allocation-grade independent-source redundancy remain in force.

The repository remains paper-only. Neither the diagnostic nor provisional tier can authorize live execution.

## Strategy-specific qualification

A universal hit-rate confidence veto is inappropriate for asymmetric strategies. Under `research-reset-v1`:

- trend/momentum, on-chain fundamental, cross-sectional relative value, and event-driven alpha are **expectancy-primary**. Hit rate remains measured and contributes to health/sizing, but it is not by itself a full-qualification veto.
- mean-reversion and microstructure alpha remain **hit-rate-primary** and retain the existing hit-rate confidence gate.

All families continue to require the existing full forward-sample target, confidence-adjusted net-return hurdle, multiple-testing penalty, regime coverage, current execution economics, health checks, and allocation-source requirements before full paper qualification.

## Provisional paper controls

Default provisional controls:

- 12 independent forward samples
- 1 observed regime
- conservative forward mean lower bound above 50% of the multiple-testing penalty
- current net return above 2 bps after current costs
- recent health must pass the existing drawdown, capture-ratio, decay, and loss-streak controls
- fresh source evidence must be sufficient to measure the forward outcome
- maximum $1,000 paper notional per candidate
- maximum 1% of supplied portfolio capital per candidate before the absolute $1,000 cap

The normal minimum paper position still applies. A provisional candidate is kept at stage `research` and labeled `qualification_tier=provisional_paper` so it cannot be confused with `paper_qualified` alpha.

## Broader shadow learning

The old Candidate Opportunity Observatory shadowed selected near misses within a 25 bp gap to the full current hurdle plus non-selected valid execution variants.

The reset expands this: every directional net-hurdle rejection that already has valid source evidence, a usable price reference, and a modeled execution cost may be shadowed. The observatory remains append-only and diagnostic-only. Shadow outcomes do not increment canonical qualification sample counts.

## Execution fee truth

Conservative configured fees remain the fallback. The runtime now accepts optional verified account-specific execution overrides so research can use the economics the project can actually obtain rather than permanently assuming entry-tier taker pricing.

Override names are generated from venue and market kind, for example:

- `CIE_EXECUTION_TAKER_FEE_BPS_OKX_SPOT`
- `CIE_EXECUTION_MAKER_FEE_BPS_OKX_SPOT`
- `CIE_EXECUTION_EXPECTED_MAKER_FRACTION_OKX_SPOT`

A global `CIE_EXECUTION_EXPECTED_MAKER_FRACTION` may be used when the same empirically justified maker fraction applies broadly. Do not set these values from aspirational fee tiers; use only verified operating/account economics. If no override is configured, existing conservative fee assumptions are unchanged.

## Scientific decision point

The reset publishes an append-only `research_qualification_reset_snapshots` record and exposes it at:

`GET /v3/research/qualification-reset`

The default checkpoint waits for 120 realized provisional allocation outcomes and requires at least 20 outcomes for a strategy to count as mature. It then reports one of:

- `collect_more_evidence`
- `concentrate_on_positive_strategies`
- `strategy_universe_change_recommended`

This checkpoint is diagnostic. It does not automatically add strategies, lower thresholds, or grant execution authority. Its purpose is to stop endless threshold tuning: if a sufficiently large provisional evidence set still shows no positive net-expectancy strategy, the next action should be to change the strategy universe rather than loosen governance again.

## Preserved safeguards

The reset does not remove:

- paper-only enforcement
- fail-closed stale/incomplete source handling
- current L2 and execution-cost validation
- settlement and portfolio constraints
- adaptive health and drawdown controls
- multiple-testing protection
- independent forward evidence
- full allocation-source redundancy
- append-only evidence lineage

The change is intentionally aggressive about learning and conservative about authority.

from __future__ import annotations

from inefficiency_engine import control_cycle_executor as base


_ORIGINAL_CACHE_STATUS = base._cache_status


def _truthful_cache_status() -> dict[str, object]:
    """Distinguish an uninitialized disposable strategy cache from an incomplete one.

    The strategy cache is intentionally process-local and is lazily initialized only
    when strategy evidence is actually read. A fresh disposable executor therefore has
    ``cache_count == 0`` before that read. Reporting that state as
    ``all_caches_complete=false`` made diagnostics look like a durable-history failure.
    Keep the aggregate fail-closed until initialization, but label the state explicitly
    and never treat it as cycle-history progress or certification authority.
    """

    status = dict(_ORIGINAL_CACHE_STATUS())
    strategy = status.get("strategy")
    strategy = dict(strategy) if isinstance(strategy, dict) else {}
    outcomes = status.get("outcomes")
    outcomes = dict(outcomes) if isinstance(outcomes, dict) else {}

    cache_count = int(strategy.get("cache_count") or 0)
    initialized = cache_count > 0
    strategy["cache_initialized"] = initialized
    strategy["completion_state"] = (
        "complete"
        if initialized and strategy.get("all_caches_complete") is True
        else "rebuilding"
        if initialized
        else "not_initialized_in_this_executor"
    )
    strategy["uninitialized_is_not_durable_cache_failure"] = not initialized
    if not initialized:
        # ``None`` means not yet observed in this disposable interpreter. Qualification
        # remains fail-closed because aggregate ``complete`` below still requires True.
        strategy["all_caches_complete"] = None

    status["strategy"] = strategy
    status["outcomes"] = outcomes
    status["strategy_cache_initialized"] = initialized
    status["complete"] = bool(
        strategy.get("all_caches_complete") is True
        and outcomes.get("all_caches_complete") is True
    )
    status["diagnostic_only"] = True
    status["qualification_thresholds_unchanged"] = True
    status["allocation_authority"] = False
    status["live_execution_authority"] = False
    status["paper_only"] = True
    return status


def main() -> int:
    base._cache_status = _truthful_cache_status
    return int(base.main())


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["_truthful_cache_status", "main"]

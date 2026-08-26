from __future__ import annotations

from inefficiency_engine import read_api_end_to_end_certification_deploy as base


END_TO_END_PATH = "/v3/operations/end-to-end-certification"


def repaired_end_to_end_certification_payload() -> dict[str, object]:
    """Separate exact cycle-history truth from generic historical-cache telemetry.

    The legacy endpoint fell back from a missing ``cycle_history_cache_progress`` field
    to the unrelated strategy/outcome ``historical_cache_progress`` object, and also
    allowed generic historical-cache completion to satisfy the cycle-history serving
    target check. This repair recomputes that check from the raw control heartbeat's
    explicit cycle-history field plus the certified background target only.
    """

    payload = dict(base.end_to_end_certification_payload())

    raw_control: dict[str, object] = {}
    try:
        ready = dict(base.active.deployment_readiness())
        runtime = ready.get("runtime_heartbeats")
        workers = runtime.get("workers") if isinstance(runtime, dict) else {}
        raw_control = base._worker(workers, "canonical_control")
    except Exception:
        # The base endpoint already failed closed on readiness. A second diagnostic read
        # is advisory; if unavailable, never promote generic history into cycle history.
        raw_control = {}

    background = payload.get("cycle_history_backfill")
    background = dict(background) if isinstance(background, dict) else {}
    background_progress = background.get("progress")
    if not isinstance(background_progress, dict):
        background_progress = {}

    raw_cycle_progress = raw_control.get("cycle_history_cache_progress")
    if not isinstance(raw_cycle_progress, dict):
        raw_cycle_progress = {}
    raw_cycle_complete = bool(raw_control.get("cycle_history_cache_complete"))

    background_cycle_complete = bool(
        background.get("available")
        and not background.get("stale")
        and background.get("cache_complete")
        and background.get("serving_scan_id")
    )
    cycle_history_serving_target_certified = bool(
        raw_cycle_complete or background_cycle_complete
    )

    checks = payload.get("checks")
    checks = dict(checks) if isinstance(checks, dict) else {}
    checks["cycle_history_serving_target_certified"] = (
        cycle_history_serving_target_certified
    )
    blockers = [name for name, passed in checks.items() if not bool(passed)]
    operationally_certified = not blockers

    historical_progress = raw_control.get("historical_cache_progress")
    if not isinstance(historical_progress, dict):
        historical_progress = {}
    cycle_progress = raw_cycle_progress or background_progress
    progress_source = (
        "canonical_control"
        if raw_cycle_progress
        else "background_backfill"
        if background_progress
        else "unavailable"
    )

    control = payload.get("control")
    control = dict(control) if isinstance(control, dict) else {}
    control.update(
        {
            "cycle_history_cache_complete": raw_cycle_complete,
            "cycle_history_cache_progress": dict(cycle_progress),
            "cycle_history_cache_progress_source": progress_source,
            "historical_cache_progress": dict(historical_progress),
            "cycle_history_generic_cache_fallback_disabled": True,
        }
    )

    strategy = historical_progress.get("strategy")
    if isinstance(strategy, dict):
        control["strategy_cache_initialized"] = strategy.get(
            "cache_initialized",
            bool(strategy.get("cache_count")),
        )
        control["strategy_cache_completion_state"] = strategy.get(
            "completion_state"
        )

    payload.update(
        {
            "certified": operationally_certified,
            "operationally_certified": operationally_certified,
            "status": "certified" if operationally_certified else "blocked",
            "checks": checks,
            "blockers": blockers,
            "control": control,
            "cycle_history_certification_source": (
                "canonical_control"
                if raw_cycle_complete
                else "background_backfill"
                if background_cycle_complete
                else "none"
            ),
            "cycle_history_generic_cache_fallback_disabled": True,
        }
    )
    return payload


__all__ = [
    "END_TO_END_PATH",
    "repaired_end_to_end_certification_payload",
]

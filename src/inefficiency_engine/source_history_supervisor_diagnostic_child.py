from __future__ import annotations

import json
import signal
import sys
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage_history_migration_child import (
    SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID,
)


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return _utc(parsed)


def _fresh_concrete_child_failure(store, *, started_at: datetime) -> bool:
    try:
        heartbeat = store.latest_worker_heartbeat(
            SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID
        )
    except Exception:
        return False
    if heartbeat is None or str(heartbeat.state or "") not in {"degraded", "error"}:
        return False
    try:
        observed_at = _utc(heartbeat.observed_at)
    except Exception:
        return False
    return observed_at >= _utc(started_at)


def _termination_signal(return_code: int | None) -> tuple[int | None, str | None]:
    if return_code is None or int(return_code) >= 0:
        return None, None
    number = abs(int(return_code))
    try:
        name = signal.Signals(number).name
    except (ValueError, OSError):
        name = None
    return number, name


def publish(payload: dict[str, object]) -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        return 1

    started_at = _parse_time(payload["attempt_started_at"])
    if bool(payload.get("preserve_fresh_child_failure")) and _fresh_concrete_child_failure(
        store,
        started_at=started_at,
    ):
        return 0

    return_code_raw = payload.get("child_return_code")
    return_code = int(return_code_raw) if return_code_raw is not None else None
    signal_number, signal_name = _termination_signal(return_code)
    store.record_worker_heartbeat(
        worker_id=SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID,
        state="degraded",
        error_type=str(payload.get("error_type") or "SourceHistorySupervisorFailure"),
        detail={
            "stage": payload.get("stage"),
            "message": payload.get("message"),
            "supervisor_observation": True,
            "supervisor_executes_migration": False,
            "attempt_number": int(payload.get("attempt_number") or 0),
            "attempt_started_at": started_at.isoformat(),
            "executor_deadline_seconds": float(payload.get("executor_deadline_seconds") or 0.0),
            "child_return_code": return_code,
            "child_timed_out": bool(payload.get("child_timed_out")),
            "process_termination_observed_by_supervisor": return_code is not None,
            "termination_signal_number": signal_number,
            "termination_signal": signal_name,
            "possible_oom_or_external_kill": signal_number == int(signal.SIGKILL),
            "oom_kill_proven": False,
            "retrying": True,
            "retry_seconds": float(payload.get("retry_seconds") or 0.0),
            "migration_owner": "independent-bounded-history-child",
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "candidate_level_history_synthesized": False,
            "historical_counts_as_forward": False,
            "qualification_thresholds_unchanged": True,
            "qualification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        },
    )
    return 0


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    try:
        raw = json.loads(sys.argv[1])
        if not isinstance(raw, dict):
            return 2
        return publish(raw)
    except Exception as exc:
        print(
            f"source-history diagnostic child failed: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["publish", "main"]

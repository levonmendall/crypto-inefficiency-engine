from __future__ import annotations

from typing import Any


RUNTIME_INDEX_LABEL = "runtime_index_maintenance"
RUNTIME_INDEX_WORKER_ID = "source-coverage-runtime-index-maintenance"
RUNTIME_INDEX_STALE_AFTER_SECONDS = 1800.0
_INSTALL_MARKER = "_cie_runtime_index_health_observability_installed"


def _detail_payload(base: Any) -> dict[str, object]:
    try:
        store = base._store()  # noqa: SLF001 - deploy-layer observability hook
    except Exception:
        return {}
    if store is None:
        return {}
    try:
        heartbeat = store.latest_worker_heartbeat(RUNTIME_INDEX_WORKER_ID)
    except Exception:
        return {}
    if heartbeat is None:
        return {}
    detail = getattr(heartbeat, "detail", None)
    return dict(detail) if isinstance(detail, dict) else {}


def install_runtime_index_health_observability(base: Any) -> None:
    """Expose the post-bind runtime-index gate through the public health payload.

    The index-maintenance supervisor already persists per-index progress. This hook
    makes that durable state visible through the existing runtime heartbeat contract
    without changing startup, index, control, provider, or trading behavior.
    """

    if bool(getattr(base, _INSTALL_MARKER, False)):
        return

    base._RUNTIME_HEARTBEATS[RUNTIME_INDEX_LABEL] = RUNTIME_INDEX_WORKER_ID  # noqa: SLF001
    base._RUNTIME_STALE_AFTER_SECONDS[RUNTIME_INDEX_LABEL] = (  # noqa: SLF001
        RUNTIME_INDEX_STALE_AFTER_SECONDS
    )

    original = base._runtime_heartbeats  # noqa: SLF001

    def runtime_heartbeats_with_index_gate() -> dict[str, object]:
        payload = original()
        workers = payload.get("workers")
        if not isinstance(workers, dict):
            return payload
        worker = workers.get(RUNTIME_INDEX_LABEL)
        if not isinstance(worker, dict) or not bool(worker.get("available")):
            return payload

        detail = _detail_payload(base)
        result = detail.get("result") if isinstance(detail.get("result"), dict) else {}
        failures = result.get("failures") if isinstance(result, dict) else None
        worker.update(
            {
                "attempt": detail.get("attempt"),
                "scope": detail.get("scope"),
                "current_index": detail.get("current_index"),
                "current_table": detail.get("current_table"),
                "current_index_runtime_seconds": detail.get(
                    "current_index_runtime_seconds"
                ),
                "current_index_ok": detail.get("current_index_ok"),
                "current_index_concurrent": detail.get("current_index_concurrent"),
                "message": detail.get("message"),
                "control_gate_released": detail.get("control_gate_released"),
                "background_indexes_complete": detail.get(
                    "background_indexes_complete"
                ),
                "maintenance_runtime_seconds": detail.get("runtime_seconds"),
                "maintenance_result_complete": (
                    result.get("complete") if isinstance(result, dict) else None
                ),
                "maintenance_dialect": (
                    result.get("dialect") if isinstance(result, dict) else None
                ),
                "maintenance_failures": (
                    failures if isinstance(failures, list) else []
                ),
            }
        )
        workers[RUNTIME_INDEX_LABEL] = worker
        payload["runtime_index_gate_observability"] = True
        return payload

    base._runtime_heartbeats = runtime_heartbeats_with_index_gate  # type: ignore[attr-defined]  # noqa: SLF001
    setattr(base, _INSTALL_MARKER, True)

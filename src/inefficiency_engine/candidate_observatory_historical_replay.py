from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, func, inspect, insert, select, update

from inefficiency_engine.alpha_factory import AlphaForwardSignal


DEFAULT_REPLAY_START = datetime(2026, 8, 21, 0, 0, tzinfo=timezone.utc)
REPLAY_START_ENV = "CIE_CANDIDATE_OBSERVATORY_REPLAY_START"
REPLAY_WORKER_ID = "candidate-observatory-historical-replay"
REPLAY_SCHEMA_VERSION = 1
REPLAY_COMPLETE_EXIT_CODE = 3
DEFAULT_BATCH_SIZE = 100
ALPHA_RESEARCH_WORKER_ID = "shadow-research-auxiliary"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _utc(parsed)


def replay_start_from_env() -> datetime:
    raw = os.getenv(REPLAY_START_ENV)
    parsed = _parse_time(raw) if raw else None
    return parsed or DEFAULT_REPLAY_START


def _serialized(value: dict[str, object]) -> tuple[str, str]:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return raw, hashlib.sha256(raw.encode()).hexdigest()


def _replay_id(source_table: str, source_row_id: int, record_type: str) -> str:
    return hashlib.sha256(
        f"{source_table}:{source_row_id}:{record_type}:v{REPLAY_SCHEMA_VERSION}".encode()
    ).hexdigest()


class HistoricalCandidateReplayLedger:
    """Compact append-only diagnostic replay index over already persisted evidence.

    The source ledgers remain canonical. This table stores only enough normalized
    observability to recover the missing Aug-21-to-live-observatory view. Nothing in
    this ledger is eligible for forward qualification, allocation, or execution.
    """

    def __init__(self, store, *, create_schema: bool = True):
        self.store = store
        metadata = MetaData()
        self.records = Table(
            "candidate_observatory_historical_replay",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("replay_id", String(64), nullable=False, unique=True),
            Column("record_type", String(32), nullable=False),
            Column("source_table", String(64), nullable=False),
            Column("source_row_id", Integer, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.checkpoints = Table(
            "candidate_observatory_historical_replay_checkpoints",
            metadata,
            Column("stream", String(64), primary_key=True),
            Column("last_source_id", Integer, nullable=False),
            Column("updated_at", Text, nullable=False),
        )
        Index("ix_candidate_historical_replay_type", self.records.c.record_type)
        Index("ix_candidate_historical_replay_observed", self.records.c.observed_at)
        Index("ix_candidate_historical_replay_source", self.records.c.source_table, self.records.c.source_row_id)
        if create_schema:
            metadata.create_all(store.engine)

    def checkpoint(self, stream: str) -> int:
        with self.store.engine.connect() as db:
            value = db.execute(
                select(self.checkpoints.c.last_source_id).where(
                    self.checkpoints.c.stream == stream
                )
            ).scalar_one_or_none()
        return int(value or 0)

    def advance_checkpoint(self, stream: str, source_id: int) -> None:
        source_id = max(0, int(source_id))
        with self.store.engine.begin() as db:
            current = db.execute(
                select(self.checkpoints.c.last_source_id).where(
                    self.checkpoints.c.stream == stream
                )
            ).scalar_one_or_none()
            if current is None:
                db.execute(
                    insert(self.checkpoints),
                    {
                        "stream": stream,
                        "last_source_id": source_id,
                        "updated_at": _now().isoformat(),
                    },
                )
            elif source_id > int(current):
                db.execute(
                    update(self.checkpoints)
                    .where(self.checkpoints.c.stream == stream)
                    .values(last_source_id=source_id, updated_at=_now().isoformat())
                )

    def record(
        self,
        *,
        record_type: str,
        source_table: str,
        source_row_id: int,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> bool:
        replay_id = _replay_id(source_table, source_row_id, record_type)
        normalized = {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "replay_id": replay_id,
            "record_type": record_type,
            "historical_replay": True,
            "diagnostic_only": True,
            "historical_counts_as_forward": False,
            "qualification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
            **payload,
        }
        raw, lineage = _serialized(normalized)
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.records.c.replay_id).where(
                    self.records.c.replay_id == replay_id
                )
            ).scalar_one_or_none()
            if exists is not None:
                return False
            db.execute(
                insert(self.records),
                {
                    "replay_id": replay_id,
                    "record_type": record_type,
                    "source_table": source_table,
                    "source_row_id": int(source_row_id),
                    "observed_at": _utc(observed_at).isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                },
            )
        return True


def _table_names(store) -> set[str]:
    try:
        return set(inspect(store.engine).get_table_names())
    except Exception:
        return set()


def live_observatory_started_at(store) -> datetime | None:
    """Return the first genuine live observatory timestamp, never a replay timestamp."""

    available = _table_names(store)
    candidates: list[datetime] = []
    with store.engine.connect() as db:
        if "candidate_observatory_events" in available:
            raw = db.execute(
                select(func.min(Table(
                    "candidate_observatory_events", MetaData(), autoload_with=store.engine
                ).c.observed_at))
            ).scalar_one_or_none()
            parsed = _parse_time(raw)
            if parsed is not None:
                candidates.append(parsed)
        if "candidate_observatory_snapshots" in available:
            raw = db.execute(
                select(func.min(Table(
                    "candidate_observatory_snapshots", MetaData(), autoload_with=store.engine
                ).c.observed_at))
            ).scalar_one_or_none()
            parsed = _parse_time(raw)
            if parsed is not None:
                candidates.append(parsed)
    return min(candidates) if candidates else None


def _cutoff_clause(column, boundary: datetime):
    return column < boundary.isoformat()


def _process_alpha_signals(
    store,
    ledger: HistoricalCandidateReplayLedger,
    *,
    start: datetime,
    boundary: datetime,
    batch_size: int,
) -> dict[str, object]:
    available = _table_names(store)
    if "alpha_forward_events" not in available:
        return {"advanced": 0, "recorded": 0, "drained": True, "parse_errors": 0}
    table = Table("alpha_forward_events", MetaData(), autoload_with=store.engine)
    checkpoint = ledger.checkpoint("alpha_forward_signals")
    query = (
        select(table.c.id, table.c.payload_json, table.c.observed_at)
        .where(table.c.id > checkpoint)
        .where(table.c.event_type == "signal")
        .where(table.c.observed_at >= start.isoformat())
        .where(_cutoff_clause(table.c.observed_at, boundary))
        .order_by(table.c.id)
        .limit(batch_size)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query).mappings())
    recorded = 0
    parse_errors = 0
    for row in rows:
        source_id = int(row["id"])
        try:
            signal = AlphaForwardSignal.model_validate_json(str(row["payload_json"]))
            candidate = signal.candidate
            payload = {
                "reconstruction_method": "exact_alpha_forward_signal_ledger",
                "exact_persisted_evidence": True,
                "source_event_id": source_id,
                "signal_id": signal.signal_id,
                "observed_at": candidate.observed_at.isoformat(),
                "due_at": signal.due_at.isoformat(),
                "stage": "forward_candidate_selected",
                "selected_for_forward_test": True,
                "candidate": candidate.model_dump(mode="json"),
            }
            if ledger.record(
                record_type="selected_candidate",
                source_table="alpha_forward_events",
                source_row_id=source_id,
                observed_at=candidate.observed_at,
                payload=payload,
            ):
                recorded += 1
        except Exception:
            parse_errors += 1
        finally:
            ledger.advance_checkpoint("alpha_forward_signals", source_id)
    return {
        "advanced": len(rows),
        "recorded": recorded,
        "drained": len(rows) < batch_size,
        "parse_errors": parse_errors,
    }


def _process_alpha_funnels(
    store,
    ledger: HistoricalCandidateReplayLedger,
    *,
    start: datetime,
    boundary: datetime,
    batch_size: int,
) -> dict[str, object]:
    available = _table_names(store)
    if "worker_heartbeats" not in available:
        return {"advanced": 0, "recorded": 0, "drained": True, "parse_errors": 0}
    table = Table("worker_heartbeats", MetaData(), autoload_with=store.engine)
    checkpoint = ledger.checkpoint("alpha_funnel_heartbeats")
    query = (
        select(table.c.id, table.c.payload_json, table.c.observed_at, table.c.cycle_id)
        .where(table.c.id > checkpoint)
        .where(table.c.worker_id == ALPHA_RESEARCH_WORKER_ID)
        .where(table.c.observed_at >= start.isoformat())
        .where(_cutoff_clause(table.c.observed_at, boundary))
        .order_by(table.c.id)
        .limit(batch_size)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query).mappings())
    recorded = 0
    parse_errors = 0
    for row in rows:
        source_id = int(row["id"])
        try:
            heartbeat = json.loads(str(row["payload_json"]))
            detail = heartbeat.get("detail") if isinstance(heartbeat, dict) else None
            funnels = detail.get("alpha_discovery_funnel") if isinstance(detail, dict) else None
            if isinstance(funnels, dict) and funnels:
                observed_at = _parse_time(row.get("observed_at")) or start
                payload = {
                    "reconstruction_method": "exact_alpha_research_heartbeat",
                    "exact_persisted_evidence": True,
                    "source_heartbeat_id": source_id,
                    "cycle_id": row.get("cycle_id"),
                    "observed_at": observed_at.isoformat(),
                    "funnels": funnels,
                    "candidate_level_rejections_reconstructable": False,
                    "candidate_level_rejections_note": (
                        "legacy rejected-candidate identities were not persisted; aggregate rejection counts are exact"
                    ),
                }
                if ledger.record(
                    record_type="alpha_funnel",
                    source_table="worker_heartbeats",
                    source_row_id=source_id,
                    observed_at=observed_at,
                    payload=payload,
                ):
                    recorded += 1
        except Exception:
            parse_errors += 1
        finally:
            ledger.advance_checkpoint("alpha_funnel_heartbeats", source_id)
    return {
        "advanced": len(rows),
        "recorded": recorded,
        "drained": len(rows) < batch_size,
        "parse_errors": parse_errors,
    }


def _process_structural_funnels(
    store,
    ledger: HistoricalCandidateReplayLedger,
    *,
    start: datetime,
    boundary: datetime,
    batch_size: int,
) -> dict[str, object]:
    available = _table_names(store)
    table_name = "research_closure_cycle_summaries"
    if table_name not in available:
        return {"advanced": 0, "recorded": 0, "drained": True, "parse_errors": 0}
    table = Table(table_name, MetaData(), autoload_with=store.engine)
    checkpoint = ledger.checkpoint("structural_funnel_summaries")
    query = (
        select(table.c.id, table.c.payload_json)
        .where(table.c.id > checkpoint)
        .order_by(table.c.id)
        .limit(batch_size)
    )
    with store.engine.connect() as db:
        source_rows = list(db.execute(query).mappings())

    recorded = 0
    parse_errors = 0
    eligible_source_rows = 0
    last_scanned = checkpoint
    for row in source_rows:
        source_id = int(row["id"])
        last_scanned = source_id
        try:
            summary = json.loads(str(row["payload_json"]))
            observed_at = _parse_time(summary.get("observed_at") if isinstance(summary, dict) else None)
            if observed_at is None or observed_at < start or observed_at >= boundary:
                continue
            eligible_source_rows += 1
            source_funnels = summary.get("rejection_funnels") if isinstance(summary, dict) else None
            if not isinstance(source_funnels, dict):
                continue
            trusted: dict[str, object] = {}
            omitted: list[str] = []
            for mechanism_id, funnel in source_funnels.items():
                if mechanism_id in {"price_discrepancy", "carry"}:
                    trusted[str(mechanism_id)] = funnel
                elif mechanism_id == "microstructure":
                    if isinstance(funnel, dict) and funnel.get("same_cycle_candidate_funnel") is True:
                        trusted["microstructure"] = funnel
                    else:
                        omitted.append("microstructure")
            if not trusted:
                continue
            payload = {
                "reconstruction_method": "exact_research_closure_ledger",
                "exact_persisted_evidence": True,
                "source_summary_id": summary.get("summary_id"),
                "source_scan_id": summary.get("source_scan_id"),
                "observed_at": observed_at.isoformat(),
                "funnels": trusted,
                "omitted_untrusted_legacy_funnels": sorted(set(omitted)),
                "candidate_level_rejections_reconstructable": False,
            }
            if ledger.record(
                record_type="structural_funnel",
                source_table=table_name,
                source_row_id=source_id,
                observed_at=observed_at,
                payload=payload,
            ):
                recorded += 1
        except Exception:
            parse_errors += 1
        finally:
            ledger.advance_checkpoint("structural_funnel_summaries", source_id)

    # If all returned rows were beyond the eventual live boundary, advancing their
    # checkpoints is safe: this historical replay deliberately ends at live start.
    drained = len(source_rows) < batch_size
    return {
        "advanced": len(source_rows),
        "eligible": eligible_source_rows,
        "recorded": recorded,
        "drained": drained,
        "parse_errors": parse_errors,
        "last_scanned": last_scanned,
    }


def run_historical_candidate_replay_batch(
    store,
    *,
    start: datetime | None = None,
    now: datetime | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    """Advance a bounded replay batch from persisted evidence only.

    No detector is re-run and no forward outcome is synthesized. Selected alpha
    candidates come from the exact forward-signal ledger; raw/emitted rejection
    counts come from the exact historical worker/closure summaries that were actually
    persisted. This avoids hindsight and code-version reconstruction bias.
    """

    start = _utc(start or replay_start_from_env())
    current = _utc(now or _now())
    live_start = live_observatory_started_at(store)
    boundary = live_start or current
    ledger = HistoricalCandidateReplayLedger(store)
    bounded = max(1, min(500, int(batch_size)))

    alpha_signals = _process_alpha_signals(
        store, ledger, start=start, boundary=boundary, batch_size=bounded
    )
    alpha_funnels = _process_alpha_funnels(
        store, ledger, start=start, boundary=boundary, batch_size=bounded
    )
    structural = _process_structural_funnels(
        store, ledger, start=start, boundary=boundary, batch_size=bounded
    )
    complete = bool(
        live_start is not None
        and alpha_signals["drained"]
        and alpha_funnels["drained"]
        and structural["drained"]
    )
    result = {
        "replay_start": start.isoformat(),
        "replay_boundary": boundary.isoformat(),
        "live_observatory_started_at": live_start.isoformat() if live_start else None,
        "complete": complete,
        "waiting_for_live_observatory_boundary": live_start is None,
        "alpha_signals": alpha_signals,
        "alpha_funnels": alpha_funnels,
        "structural_funnels": structural,
        "historical_replay": True,
        "diagnostic_only": True,
        "historical_counts_as_forward": False,
        "qualification_thresholds_unchanged": True,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    try:
        store.record_worker_heartbeat(
            worker_id=REPLAY_WORKER_ID,
            state="success" if complete else "running",
            detail=result,
        )
    except Exception:
        pass
    return result


def read_historical_candidate_replay(store, *, limit: int = 100) -> dict[str, object]:
    """Read the compact replay index without creating schema from the API process."""

    available = _table_names(store)
    table_name = "candidate_observatory_historical_replay"
    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(REPLAY_WORKER_ID)
    except Exception:
        heartbeat = None
    runtime = heartbeat.model_dump(mode="json") if heartbeat is not None else None
    if table_name not in available:
        return {
            "available": False,
            "replay_start": replay_start_from_env().isoformat(),
            "complete": False,
            "runtime": runtime,
            "selected_candidates": [],
            "alpha_funnels": [],
            "structural_funnels": [],
            "historical_replay": True,
            "diagnostic_only": True,
            "historical_counts_as_forward": False,
            "qualification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

    table = Table(table_name, MetaData(), autoload_with=store.engine)
    bounded = max(1, min(500, int(limit)))
    by_type: dict[str, list[dict[str, object]]] = {}
    with store.engine.connect() as db:
        count_rows = list(
            db.execute(
                select(table.c.record_type, func.count(table.c.id)).group_by(table.c.record_type)
            )
        )
        for record_type in ("selected_candidate", "alpha_funnel", "structural_funnel"):
            raws = list(
                db.execute(
                    select(table.c.payload_json)
                    .where(table.c.record_type == record_type)
                    .order_by(table.c.id.desc())
                    .limit(bounded)
                ).scalars()
            )
            parsed: list[dict[str, object]] = []
            for raw in raws:
                try:
                    payload = json.loads(str(raw))
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(payload, dict):
                    parsed.append(payload)
            by_type[record_type] = parsed

    counts = {str(record_type): int(count) for record_type, count in count_rows}
    detail = runtime.get("detail") if isinstance(runtime, dict) else {}
    return {
        "available": bool(sum(counts.values())),
        "replay_start": (
            detail.get("replay_start")
            if isinstance(detail, dict) and detail.get("replay_start")
            else replay_start_from_env().isoformat()
        ),
        "live_observatory_started_at": (
            detail.get("live_observatory_started_at") if isinstance(detail, dict) else None
        ),
        "complete": bool(detail.get("complete")) if isinstance(detail, dict) else False,
        "counts": counts,
        "selected_candidates": by_type.get("selected_candidate", []),
        "alpha_funnels": by_type.get("alpha_funnel", []),
        "structural_funnels": by_type.get("structural_funnel", []),
        "candidate_level_rejections_reconstructable": False,
        "candidate_level_rejections_note": (
            "legacy rejected-candidate identities were never persisted; exact aggregate rejection funnels are replayed instead"
        ),
        "runtime": runtime,
        "historical_replay": True,
        "diagnostic_only": True,
        "historical_counts_as_forward": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


__all__ = [
    "DEFAULT_REPLAY_START",
    "REPLAY_COMPLETE_EXIT_CODE",
    "REPLAY_WORKER_ID",
    "HistoricalCandidateReplayLedger",
    "live_observatory_started_at",
    "read_historical_candidate_replay",
    "run_historical_candidate_replay_batch",
]

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, inspect, select, text

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.evidence_velocity import dynamic_lane_priority, evidence_freshness_seconds
from inefficiency_engine.runtime_provider_policy import env_flag
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class SourceCoverageObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    source_id: str
    lane_id: str
    observed_at: datetime = Field(default_factory=_now)
    healthy: bool
    item_count: int = Field(default=0, ge=0)
    evidence_classes: list[str] = Field(default_factory=list)
    authoritative: bool = True
    commercial_use_permitted: bool = True
    point_in_time: bool = True
    source_reference: str | None = None
    economic_fields_complete: bool = False
    forward_testable_evidence: bool = False
    error_type: str | None = None
    detail: dict[str, object] = Field(default_factory=dict)
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False


class SourceEventObservation(BaseModel):
    event_id: str
    lane_id: str
    source_id: str
    event_type: str
    event_at: datetime
    observed_at: datetime = Field(default_factory=_now)
    asset: str | None = None
    source_reference: str | None = None
    payload: dict[str, object] = Field(default_factory=dict)
    authoritative: bool = True
    commercial_use_permitted: bool = True
    point_in_time: bool = True
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False


class SourceCoverageLedger:
    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "source_coverage_observations",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("source_id", Text, nullable=False),
            Column("lane_id", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index("ix_source_coverage_lane", self.rows.c.lane_id, self.rows.c.observed_at)
        Index("ix_source_coverage_source", self.rows.c.source_id, self.rows.c.observed_at)
        metadata.create_all(store.engine)

    def record(self, row: SourceCoverageObservation) -> str:
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.observation_id).where(
                    self.rows.c.observation_id == row.observation_id
                )
            ).scalar_one_or_none()
            if exists is None:
                db.execute(
                    insert(self.rows),
                    {
                        "observation_id": row.observation_id,
                        "source_id": row.source_id,
                        "lane_id": row.lane_id,
                        "observed_at": row.observed_at.isoformat(),
                        "payload_json": row.model_dump_json(),
                    },
                )
        return row.observation_id

    def latest(self) -> dict[tuple[str, str], SourceCoverageObservation]:
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(self.rows.c.payload_json).order_by(self.rows.c.id.desc()).limit(3000)
                ).scalars()
            )
        result: dict[tuple[str, str], SourceCoverageObservation] = {}
        for payload in payloads:
            row = SourceCoverageObservation.model_validate_json(payload)
            result.setdefault((row.source_id, row.lane_id), row)
        return result


class SourceEventLedger:
    """Raw point-in-time events; recording one never creates investment authority."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "source_event_observations",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_id", String(64), nullable=False, unique=True),
            Column("lane_id", Text, nullable=False),
            Column("source_id", Text, nullable=False),
            Column("event_type", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("event_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index("ix_source_event_lane", self.rows.c.lane_id, self.rows.c.observed_at)
        Index("ix_source_event_source", self.rows.c.source_id, self.rows.c.observed_at)
        metadata.create_all(store.engine)

    def record(self, row: SourceEventObservation) -> str:
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.rows.c.event_id).where(self.rows.c.event_id == row.event_id)
            ).scalar_one_or_none()
            if exists is None:
                db.execute(
                    insert(self.rows),
                    {
                        "event_id": row.event_id,
                        "lane_id": row.lane_id,
                        "source_id": row.source_id,
                        "event_type": row.event_type,
                        "observed_at": row.observed_at.isoformat(),
                        "event_at": row.event_at.isoformat(),
                        "payload_json": row.model_dump_json(),
                    },
                )
        return row.event_id


class CandidateSourceSufficiency(BaseModel):
    lane_id: str
    required_evidence_classes: list[str]
    covered_evidence_classes: list[str]
    missing_evidence_classes: list[str]
    admitted_source_groups: list[str]
    primary_source_groups: list[str] = Field(default_factory=list)
    primary_group_satisfied: bool = True
    research_eligible: bool = False
    forward_test_eligible: bool = False
    allocation_source_qualified: bool = False
    blockers: list[str] = Field(default_factory=list)
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False


class LaneSourceCoverage(BaseModel):
    lane_id: str
    name: str
    required_evidence_classes: list[str]
    covered_evidence_classes: list[str]
    missing_evidence_classes: list[str]
    downstream_evidence_gaps: list[str] = Field(default_factory=list)
    target_independent_authoritative_sources: int = 2
    healthy_source_count: int
    independent_authoritative_source_count: int
    admitted_authoritative_source_groups: list[str] = Field(default_factory=list)
    missing_authoritative_source_count: int = 0
    policy_disabled_source_ids: list[str] = Field(default_factory=list)
    source_redundancy_satisfied: bool
    evidence_class_coverage_satisfied: bool
    research_eligible: bool = False
    forward_test_eligible: bool = False
    allocation_source_qualified: bool = False
    source_layer_sufficient: bool
    source_state: str
    sources: list[dict[str, object]]
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False


class SourceCoverageSnapshot(BaseModel):
    observed_at: datetime
    lane_count: int
    sufficient_lane_count: int
    insufficient_lane_count: int
    research_eligible_lane_count: int = 0
    forward_test_eligible_lane_count: int = 0
    allocation_source_qualified_lane_count: int = 0
    priority_order: list[str]
    lanes: list[LaneSourceCoverage]
    paper_only: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False


class SourceCoveragePlane:
    """Thirteen-lane evidence plane with separate research and allocation gates.

    Research may advance with one admitted authoritative source once all evidence
    classes needed to measure a forward outcome are present. Allocation keeps the
    existing two-independent-source requirement. This prevents source redundancy
    from blocking learning while preserving the fail-closed investment boundary.
    """

    def __init__(self, store: EvidenceStore, *, max_age_hours: float | None = None):
        self.store = store
        self.ledger = SourceCoverageLedger(store)
        self.events = SourceEventLedger(store)
        self.max_age_hours = float(
            max_age_hours
            if max_age_hours is not None
            else os.getenv("CIE_SOURCE_COVERAGE_MAX_AGE_HOURS", "24")
        )
        # Explicit max_age_hours remains a deterministic test/override contract.
        self.class_specific_freshness = max_age_hours is None

    def record(self, row: SourceCoverageObservation) -> str:
        return self.ledger.record(row)

    def record_event(self, row: SourceEventObservation) -> str:
        return self.events.record(row)

    def _provider_rows(self, available: set[str]) -> list[dict[str, object]]:
        if "provider_statuses" not in available:
            return []
        with self.store.engine.connect() as db:
            rows = list(
                db.execute(
                    text(
                        "SELECT provider,ok,item_count,error_type,observed_at "
                        "FROM provider_statuses ORDER BY id DESC LIMIT 1000"
                    )
                ).mappings()
            )
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            latest.setdefault(str(row["provider"]), dict(row))
        return list(latest.values())

    def _admissions(self, available: set[str]) -> list[dict[str, object]]:
        if "provider_gap_admissions" not in available:
            return []
        with self.store.engine.connect() as db:
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM provider_gap_admissions "
                        "ORDER BY id DESC LIMIT 1000"
                    )
                ).scalars()
            )
        latest: dict[tuple[str, str], dict[str, object]] = {}
        for raw in raws:
            try:
                payload = json.loads(str(raw))
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                latest.setdefault(
                    (
                        str(payload.get("mechanism_id") or ""),
                        str(payload.get("provider") or ""),
                    ),
                    payload,
                )
        return list(latest.values())

    def _table_candidate(
        self,
        spec: dict[str, object],
        available: set[str],
    ) -> dict[str, object] | None:
        probe = spec.get("table")
        if not isinstance(probe, tuple) or len(probe) != 3:
            return None
        table_name, column, value = probe
        safe_tables = {
            "market_quotes",
            "funding_quotes",
            "order_books",
            "opportunities",
            "maker_shadow_outcomes",
            "capital_transfer_outcomes",
        }
        if table_name not in safe_tables or table_name not in available:
            return None
        clause, params = "", {}
        if column is not None:
            if column not in {"venue", "asset", "strategy"}:
                return None
            clause, params = f" WHERE {column}=:value", {"value": value}
        try:
            with self.store.engine.connect() as db:
                row = db.execute(
                    text(
                        f"SELECT observed_at FROM {table_name}{clause} "
                        "ORDER BY observed_at DESC LIMIT 1"
                    ),
                    params,
                ).first()
        except Exception:
            return None
        if row is None:
            return None
        return {
            "healthy": True,
            "observed_at": row[0],
            "item_count": 1,
            "classes": list(spec["classes"]),
            "authoritative": bool(spec.get("authoritative", True)),
            "commercial": True,
            "point_in_time": True,
            "source_reference": f"durable:{table_name}",
            "economic_fields_complete": True,
            "forward_testable_evidence": True,
        }

    def _freshness_seconds(self, classes: list[str]) -> float:
        fallback = max(1.0, self.max_age_hours * 3600.0)
        if not self.class_specific_freshness:
            return fallback
        return evidence_freshness_seconds(classes, fallback_seconds=fallback)

    def _source_status(
        self,
        spec: dict[str, object],
        lane_id: str,
        now: datetime,
        direct: dict[tuple[str, str], SourceCoverageObservation],
        providers: list[dict[str, object]],
        admissions: list[dict[str, object]],
        available: set[str],
    ) -> dict[str, object]:
        credential = spec.get("credential")
        base = {
            "source_id": spec["id"],
            "name": spec["name"],
            "classes": spec["classes"],
            "group": spec["group"],
            "tier": spec["tier"],
            "authoritative": bool(spec.get("authoritative", True)),
        }
        enabled_env = spec.get("enabled_env")
        if enabled_env and not env_flag(str(enabled_env), default=True):
            return {
                **base,
                "state": "not_applicable",
                "healthy": False,
                "fresh": False,
                "admitted": False,
                "observed_at": None,
                "item_count": 0,
                "policy_disabled": True,
                "enabled_env": str(enabled_env),
            }
        if credential and not os.getenv(str(credential)):
            return {
                **base,
                "state": "credential_required",
                "healthy": False,
                "fresh": False,
                "admitted": False,
                "observed_at": None,
                "item_count": 0,
                "credential_env": credential,
            }

        candidates: list[dict[str, object]] = []
        row = direct.get((str(spec["id"]), lane_id))
        if row is not None:
            candidates.append(
                {
                    "healthy": row.healthy,
                    "observed_at": row.observed_at,
                    "item_count": row.item_count,
                    "classes": row.evidence_classes or list(spec["classes"]),
                    "authoritative": row.authoritative,
                    "commercial": row.commercial_use_permitted,
                    "point_in_time": row.point_in_time,
                    "error_type": row.error_type,
                    "source_reference": row.source_reference,
                    "economic_fields_complete": row.economic_fields_complete,
                    "forward_testable_evidence": row.forward_testable_evidence,
                }
            )

        prefixes = [str(value) for value in list(spec.get("provider") or [])]
        for admission in admissions:
            provider = str(admission.get("provider") or "")
            if prefixes and any(provider.startswith(prefix) for prefix in prefixes):
                candidates.append(
                    {
                        "healthy": bool(admission.get("healthy")),
                        "observed_at": admission.get("observed_at"),
                        "item_count": int(admission.get("item_count") or 0),
                        "classes": list(spec["classes"]),
                        "authoritative": bool(
                            admission.get("authoritative", spec.get("authoritative", True))
                        ),
                        "commercial": bool(admission.get("commercial_use_permitted", True)),
                        "point_in_time": bool(admission.get("point_in_time", True)),
                        "error_type": admission.get("error_type"),
                        "source_reference": admission.get("source_reference"),
                        "economic_fields_complete": bool(
                            admission.get("economic_fields_complete", False)
                        ),
                        "forward_testable_evidence": bool(
                            admission.get("forward_testable_evidence", False)
                        ),
                    }
                )

        for provider_row in providers:
            provider = str(provider_row.get("provider") or "")
            if prefixes and any(provider.startswith(prefix) for prefix in prefixes):
                candidates.append(
                    {
                        "healthy": bool(provider_row.get("ok")),
                        "observed_at": provider_row.get("observed_at"),
                        "item_count": int(provider_row.get("item_count") or 0),
                        "classes": list(spec["classes"]),
                        "authoritative": bool(spec.get("authoritative", True)),
                        "commercial": True,
                        "point_in_time": True,
                        "error_type": provider_row.get("error_type"),
                        "source_reference": provider,
                        "economic_fields_complete": False,
                        "forward_testable_evidence": False,
                    }
                )

        table_candidate = self._table_candidate(spec, available)
        if table_candidate:
            candidates.append(table_candidate)
        if not candidates:
            return {
                **base,
                "state": "unobserved",
                "healthy": False,
                "fresh": False,
                "admitted": False,
                "observed_at": None,
                "item_count": 0,
                "active": bool(spec.get("active", True)),
            }

        candidates.sort(
            key=lambda item: _time(item.get("observed_at"))
            or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        latest = candidates[0]
        observed = _time(latest.get("observed_at"))
        age_seconds = max(0.0, (now - observed).total_seconds()) if observed else None
        classes = [str(item) for item in list(latest.get("classes") or spec["classes"])]
        freshness_seconds = self._freshness_seconds(classes)
        fresh = bool(age_seconds is not None and age_seconds <= freshness_seconds)
        admitted = bool(
            latest.get("healthy")
            and fresh
            and latest.get("authoritative", True)
            and latest.get("commercial", True)
            and latest.get("point_in_time", True)
        )
        state: Literal["healthy", "stale", "failed"] = (
            "failed"
            if not latest.get("healthy")
            else "healthy"
            if fresh
            else "stale"
        )
        return {
            **base,
            "state": state,
            "healthy": bool(latest.get("healthy")),
            "fresh": fresh,
            "admitted": admitted,
            "classes": classes,
            "authoritative": bool(latest.get("authoritative", spec.get("authoritative", True))),
            "observed_at": observed.isoformat() if observed else None,
            "age_hours": age_seconds / 3600.0 if age_seconds is not None else None,
            "age_seconds": age_seconds,
            "freshness_ttl_seconds": freshness_seconds,
            "freshness_policy": "evidence_class_specific"
            if self.class_specific_freshness
            else "explicit_uniform_override",
            "item_count": int(latest.get("item_count") or 0),
            "error_type": latest.get("error_type"),
            "source_reference": latest.get("source_reference"),
            "economic_fields_complete": bool(latest.get("economic_fields_complete")),
            "forward_testable_evidence": bool(latest.get("forward_testable_evidence")),
        }

    def snapshot(self, *, now: datetime | None = None) -> SourceCoverageSnapshot:
        now = now or _now()
        available = set(inspect(self.store.engine).get_table_names())
        direct = self.ledger.latest()
        providers = self._provider_rows(available)
        admissions = self._admissions(available)
        rows: list[LaneSourceCoverage] = []

        for lane_id, definition in LANES.items():
            source_rows = [
                self._source_status(
                    spec,
                    lane_id,
                    now,
                    direct,
                    providers,
                    admissions,
                    available,
                )
                for spec in SOURCES
                if lane_id in list(spec["lanes"])
            ]
            admitted = [row for row in source_rows if row.get("admitted")]
            covered = sorted(
                {
                    str(cls)
                    for row in admitted
                    for cls in list(row.get("classes") or [])
                }
            )
            required = [str(value) for value in list(definition["required"])]
            missing = [value for value in required if value not in covered]
            groups = {
                str(row["group"])
                for row in admitted
                if bool(row.get("authoritative"))
            }
            groups_sorted = sorted(groups)
            policy_disabled_source_ids = sorted(
                str(row.get("source_id") or "")
                for row in source_rows
                if row.get("state") == "not_applicable" and row.get("source_id")
            )
            redundancy = len(groups) >= 2
            class_ok = not missing
            research_eligible = bool(admitted)
            forward_test_eligible = research_eligible and class_ok
            allocation_source_qualified = forward_test_eligible and redundancy
            state = (
                "sufficient"
                if allocation_source_qualified
                else "provider_gap"
                if not admitted
                else "evidence_class_gap"
                if missing
                else "concentration_risk"
            )
            rows.append(
                LaneSourceCoverage(
                    lane_id=lane_id,
                    name=str(definition["name"]),
                    required_evidence_classes=required,
                    covered_evidence_classes=covered,
                    missing_evidence_classes=missing,
                    downstream_evidence_gaps=[
                        str(value) for value in list(definition.get("downstream") or [])
                    ],
                    healthy_source_count=len(admitted),
                    independent_authoritative_source_count=len(groups),
                    admitted_authoritative_source_groups=groups_sorted,
                    missing_authoritative_source_count=max(0, 2 - len(groups)),
                    policy_disabled_source_ids=policy_disabled_source_ids,
                    source_redundancy_satisfied=redundancy,
                    evidence_class_coverage_satisfied=class_ok,
                    research_eligible=research_eligible,
                    forward_test_eligible=forward_test_eligible,
                    allocation_source_qualified=allocation_source_qualified,
                    source_layer_sufficient=allocation_source_qualified,
                    source_state=state,
                    sources=source_rows,
                )
            )

        priority_order = dynamic_lane_priority(self.store, rows)
        return SourceCoverageSnapshot(
            observed_at=now,
            lane_count=len(rows),
            sufficient_lane_count=sum(row.source_layer_sufficient for row in rows),
            insufficient_lane_count=sum(not row.source_layer_sufficient for row in rows),
            research_eligible_lane_count=sum(row.research_eligible for row in rows),
            forward_test_eligible_lane_count=sum(row.forward_test_eligible for row in rows),
            allocation_source_qualified_lane_count=sum(
                row.allocation_source_qualified for row in rows
            ),
            priority_order=priority_order,
            lanes=rows,
        )

    def lane(self, lane_id: str) -> LaneSourceCoverage:
        for row in self.snapshot().lanes:
            if row.lane_id == lane_id:
                return row
        raise KeyError(lane_id)

    def candidate_sufficiency(
        self,
        lane_id: str,
        *,
        required_evidence_classes: list[str] | None = None,
        primary_groups: set[str] | None = None,
        now: datetime | None = None,
    ) -> CandidateSourceSufficiency:
        lane = next(
            (row for row in self.snapshot(now=now).lanes if row.lane_id == lane_id),
            None,
        )
        if lane is None:
            raise KeyError(lane_id)

        required = [
            str(item)
            for item in (
                required_evidence_classes
                if required_evidence_classes is not None
                else lane.required_evidence_classes
            )
        ]
        admitted = [row for row in lane.sources if bool(row.get("admitted"))]
        covered = sorted(
            {
                str(cls)
                for row in admitted
                for cls in list(row.get("classes") or [])
                if str(cls) in required
            }
        )
        missing = [item for item in required if item not in covered]
        groups = sorted(
            {
                str(row.get("group") or "")
                for row in admitted
                if bool(row.get("authoritative")) and str(row.get("group") or "")
            }
        )
        normalized_primary = {
            str(item).strip().lower() for item in (primary_groups or set()) if str(item).strip()
        }
        primary_ok = not normalized_primary or any(
            str(row.get("group") or "").strip().lower() in normalized_primary
            for row in admitted
        )
        research_eligible = bool(admitted) and primary_ok
        forward_test_eligible = research_eligible and not missing
        allocation_source_qualified = forward_test_eligible and len(groups) >= 2
        blockers: list[str] = []
        if not admitted:
            blockers.append("no fresh admitted authoritative source")
        if not primary_ok:
            blockers.append("candidate primary venue/protocol source is not freshly admitted")
        if missing:
            blockers.extend(f"missing evidence class:{item}" for item in missing)
        if forward_test_eligible and len(groups) < 2:
            blockers.append("independent-source redundancy remains required for allocation")

        return CandidateSourceSufficiency(
            lane_id=lane_id,
            required_evidence_classes=required,
            covered_evidence_classes=covered,
            missing_evidence_classes=missing,
            admitted_source_groups=groups,
            primary_source_groups=sorted(normalized_primary),
            primary_group_satisfied=primary_ok,
            research_eligible=research_eligible,
            forward_test_eligible=forward_test_eligible,
            allocation_source_qualified=allocation_source_qualified,
            blockers=blockers,
        )

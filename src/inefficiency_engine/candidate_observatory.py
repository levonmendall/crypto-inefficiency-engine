from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.all_lane_alpha_factory import ALPHA_LANES

OBSERVATORY_WORKER_ID = "candidate-opportunity-observatory"
DIAGNOSTIC_SHADOW_MAX_GAP_TO_HURDLE = 0.0025
MAX_DIAGNOSTIC_SHADOW_SIGNALS_PER_CYCLE = 100
MAX_NEAR_MISSES = 20


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: BaseModel | dict[str, object]) -> str:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


class CandidateObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cycle_id: str | None = None
    source_scan_id: str
    observed_at: datetime
    lane_id: str
    candidate_id: str
    signal_candidate_id: str
    strategy_id: str
    family: str
    asset: str
    direction: str
    stage: str
    venue: str
    signal_reference_venue: str | None = None
    market_kind: str
    symbol: str
    horizon_hours: float
    entry_reference_price: float
    expected_gross_return: float
    estimated_cost_return: float | None = None
    expected_net_return: float | None = None
    required_net_return: float
    gap_to_hurdle: float | None = None
    notional_usd: float
    expected_profit_usd: float | None = None
    confidence_score: float
    source_groups: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    selected_for_forward_test: bool = False
    diagnostic_shadow_eligible: bool = False
    qualification_thresholds_unchanged: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True

    @property
    def qualification_key(self) -> tuple[str, str, str]:
        return self.strategy_id, self.asset.upper(), self.direction


class CandidateDiagnosticShadowSignal(BaseModel):
    signal_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    shadow_key: str
    observation: CandidateObservation
    due_at: datetime
    recorded_at: datetime = Field(default_factory=_now)
    qualification_authority: bool = False
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class CandidateDiagnosticShadowOutcome(BaseModel):
    signal_id: str
    shadow_key: str
    observation_id: str
    matured_at: datetime
    exit_price: float
    realized_gross_return: float
    realized_net_return: float
    correct_direction: bool
    qualification_authority: bool = False
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class CandidateObservatorySnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cycle_id: str
    observed_at: datetime
    source_scan_id: str
    candidate_event_count: int = 0
    raw_signal_count: int = 0
    terminal_candidate_count: int = 0
    forward_candidate_count: int = 0
    diagnostic_shadow_signals_recorded: int = 0
    diagnostic_shadow_outcomes_matured: int = 0
    near_misses: list[dict[str, object]] = Field(default_factory=list)
    lane_priorities: list[dict[str, object]] = Field(default_factory=list)
    qualification_requirements: dict[str, object] = Field(default_factory=dict)
    qualification_thresholds_unchanged: bool = True
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class CandidateObservatoryLedger:
    """Append-only research observability isolated from qualification authority."""

    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.events = Table(
            "candidate_observatory_events", metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observation_id", String(64), nullable=False, unique=True),
            Column("cycle_id", String(64), nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("lane_id", Text, nullable=False),
            Column("stage", Text, nullable=False),
            Column("strategy_id", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.snapshots = Table(
            "candidate_observatory_snapshots", metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_id", String(64), nullable=False, unique=True),
            Column("cycle_id", String(64), nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.shadow_events = Table(
            "candidate_observatory_shadow_events", metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_id", String(64), nullable=False, unique=True),
            Column("signal_id", String(64), nullable=False),
            Column("shadow_key", Text, nullable=False),
            Column("event_type", String(16), nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("due_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_candidate_observatory_cycle", self.events.c.cycle_id)
        Index("ix_candidate_observatory_lane", self.events.c.lane_id)
        Index("ix_candidate_observatory_stage", self.events.c.stage)
        Index("ix_candidate_observatory_snapshot_observed", self.snapshots.c.observed_at)
        Index("ix_candidate_observatory_shadow_signal", self.shadow_events.c.signal_id)
        Index("ix_candidate_observatory_shadow_key", self.shadow_events.c.shadow_key)
        Index("ix_candidate_observatory_shadow_due", self.shadow_events.c.due_at)
        metadata.create_all(store.engine)

    def record_observations(self, cycle_id: str, rows: list[CandidateObservation]) -> list[CandidateObservation]:
        rows = [row.model_copy(update={"cycle_id": cycle_id}) for row in rows]
        values = []
        for row in rows:
            raw = _json(row)
            values.append({
                "observation_id": row.observation_id, "cycle_id": cycle_id,
                "observed_at": row.observed_at.isoformat(), "lane_id": row.lane_id,
                "stage": row.stage, "strategy_id": row.strategy_id, "asset": row.asset,
                "payload_json": raw, "lineage_hash": _hash(raw),
            })
        if values:
            with self.store.engine.begin() as db:
                db.execute(insert(self.events), values)
        return rows

    def record_snapshot(self, row: CandidateObservatorySnapshot) -> None:
        raw = _json(row)
        with self.store.engine.begin() as db:
            db.execute(insert(self.snapshots), {
                "snapshot_id": row.snapshot_id, "cycle_id": row.cycle_id,
                "observed_at": row.observed_at.isoformat(), "payload_json": raw,
                "lineage_hash": _hash(raw),
            })

    def latest_snapshot(self) -> CandidateObservatorySnapshot | None:
        with self.store.engine.connect() as db:
            raw = db.execute(select(self.snapshots.c.payload_json).order_by(self.snapshots.c.id.desc()).limit(1)).scalar_one_or_none()
        return CandidateObservatorySnapshot.model_validate_json(raw) if raw else None

    def _open_shadow_exists(self, shadow_key: str) -> bool:
        with self.store.engine.connect() as db:
            ids = list(db.execute(select(self.shadow_events.c.signal_id).where(
                self.shadow_events.c.event_type == "signal", self.shadow_events.c.shadow_key == shadow_key
            )).scalars())
            if not ids:
                return False
            completed = set(db.execute(select(self.shadow_events.c.signal_id).where(
                self.shadow_events.c.event_type == "outcome", self.shadow_events.c.signal_id.in_(ids)
            )).scalars())
        return any(signal_id not in completed for signal_id in ids)

    def record_shadow_signal(self, observation: CandidateObservation) -> bool:
        if not observation.diagnostic_shadow_eligible or observation.direction not in {"long", "short"}:
            return False
        shadow_key = ":".join((observation.strategy_id, observation.asset.upper(), observation.direction,
                               observation.venue, observation.market_kind, observation.symbol, observation.stage))
        if self._open_shadow_exists(shadow_key):
            return False
        signal = CandidateDiagnosticShadowSignal(
            shadow_key=shadow_key, observation=observation,
            due_at=observation.observed_at + timedelta(hours=observation.horizon_hours),
        )
        raw = _json(signal)
        with self.store.engine.begin() as db:
            db.execute(insert(self.shadow_events), {
                "event_id": uuid.uuid4().hex, "signal_id": signal.signal_id, "shadow_key": shadow_key,
                "event_type": "signal", "observed_at": observation.observed_at.isoformat(),
                "due_at": signal.due_at.isoformat(), "payload_json": raw, "lineage_hash": _hash(raw),
            })
        return True

    def pending_shadow_signals(self, *, now: datetime) -> list[CandidateDiagnosticShadowSignal]:
        with self.store.engine.connect() as db:
            rows = list(db.execute(select(self.shadow_events.c.signal_id, self.shadow_events.c.payload_json).where(
                self.shadow_events.c.event_type == "signal", self.shadow_events.c.due_at <= now.isoformat()
            ).order_by(self.shadow_events.c.id)))
            completed = set(db.execute(select(self.shadow_events.c.signal_id).where(
                self.shadow_events.c.event_type == "outcome"
            )).scalars())
        return [CandidateDiagnosticShadowSignal.model_validate_json(raw) for signal_id, raw in rows if signal_id not in completed]

    def record_shadow_outcome(self, row: CandidateDiagnosticShadowOutcome) -> None:
        raw = _json(row)
        with self.store.engine.begin() as db:
            db.execute(insert(self.shadow_events), {
                "event_id": uuid.uuid4().hex, "signal_id": row.signal_id, "shadow_key": row.shadow_key,
                "event_type": "outcome", "observed_at": row.matured_at.isoformat(),
                "due_at": row.matured_at.isoformat(), "payload_json": raw, "lineage_hash": _hash(raw),
            })


def settle_diagnostic_shadows(ledger: CandidateObservatoryLedger, snapshot) -> int:
    quotes = {(q.venue, q.asset.upper(), q.market_kind.value, q.symbol): q for q in snapshot.market_quotes}
    matured = 0
    for signal in ledger.pending_shadow_signals(now=snapshot.completed_at):
        row = signal.observation
        quote = quotes.get((row.venue, row.asset.upper(), row.market_kind, row.symbol))
        if quote is None or quote.mid <= 0 or row.entry_reference_price <= 0:
            continue
        raw = quote.mid / row.entry_reference_price - 1.0
        directional = raw if row.direction == "long" else -raw
        cost = max(0.0, float(row.estimated_cost_return or 0.0))
        ledger.record_shadow_outcome(CandidateDiagnosticShadowOutcome(
            signal_id=signal.signal_id, shadow_key=signal.shadow_key, observation_id=row.observation_id,
            matured_at=snapshot.completed_at, exit_price=float(quote.mid), realized_gross_return=directional,
            realized_net_return=directional - cost, correct_direction=directional > 0,
        ))
        matured += 1
    return matured


def build_observatory_snapshot(*, cycle_id: str, observed_at: datetime, source_scan_id: str,
                               diagnostics: dict[str, dict[str, object]], observations: list[CandidateObservation],
                               qualifications: dict[tuple[str, str, str], dict[str, object]], required_samples: int,
                               research_capital_usd: float, shadow_signals_recorded: int,
                               shadow_outcomes_matured: int) -> CandidateObservatorySnapshot:
    terminal = [row for row in observations if row.stage != "raw_signal"]
    required_samples = max(1, int(required_samples))
    near_misses = []
    for row in terminal:
        if row.expected_net_return is None and not row.selected_for_forward_test:
            continue
        qual = qualifications.get(row.qualification_key, {})
        samples = int(qual.get("sample_count") or 0)
        near_misses.append({
            "observation_id": row.observation_id, "lane_id": row.lane_id, "strategy_id": row.strategy_id,
            "asset": row.asset, "direction": row.direction, "venue": row.venue, "stage": row.stage,
            "gross_return": row.expected_gross_return, "cost_return": row.estimated_cost_return,
            "net_return": row.expected_net_return, "required_net_return": row.required_net_return,
            "net_minus_current_hurdle": row.gap_to_hurdle, "research_notional_usd": row.notional_usd,
            "expected_profit_usd": row.expected_profit_usd, "forward_sample_count": samples,
            "forward_sample_deficit": max(0, required_samples - samples),
            "hit_rate_confidence_lower_bound": qual.get("hit_rate_ci_lower"),
            "mean_net_return_confidence_lower_bound": qual.get("mean_realized_net_return_ci_lower"),
            "regime_count": int(qual.get("regime_count") or 0),
            "qualification_blockers": list(qual.get("blockers") or []),
            "selected_for_forward_test": row.selected_for_forward_test,
            "diagnostic_shadow_eligible": row.diagnostic_shadow_eligible, "allocation_authority": False,
        })
    near_misses.sort(key=lambda row: (
        bool(row["selected_for_forward_test"]),
        float(row["net_return"]) if isinstance(row.get("net_return"), (int, float)) else float("-inf"),
        -int(row["forward_sample_deficit"]), float(row.get("expected_profit_usd") or 0.0),
    ), reverse=True)

    by_lane = {lane: [] for lane in ALPHA_LANES}
    for row in terminal:
        if row.lane_id in by_lane:
            by_lane[row.lane_id].append(row)
    capital = max(1.0, float(research_capital_usd))
    priorities = []
    for lane in ALPHA_LANES:
        diagnostic = diagnostics.get(lane, {})
        rows = by_lane[lane]
        quals = [qualifications.get(key, {}) for key in {row.qualification_key for row in rows}]
        samples = max((int(q.get("sample_count") or 0) for q in quals), default=0)
        hit_lowers = [float(q["hit_rate_ci_lower"]) for q in quals if isinstance(q.get("hit_rate_ci_lower"), (int, float))]
        persistence = max(hit_lowers, default=(0.10 if samples == 0 else 0.25))
        best_raw = diagnostic.get("best_net_economics")
        best_net = float(best_raw) if isinstance(best_raw, (int, float)) else 0.0
        notional = max((row.notional_usd for row in rows), default=0.0)
        deficit = max(1, required_samples - samples)
        raw_count = int(diagnostic.get("raw_candidate_count") or 0)
        score = raw_count * max(0.0, best_net) * max(0.05, persistence) * max(0.05, min(1.0, notional / capital)) / deficit
        priorities.append({
            "lane_id": lane, "raw_signal_count": raw_count,
            "post_gate_candidate_count": int(diagnostic.get("post_gate_candidate_count") or 0),
            "forward_candidate_count": int(diagnostic.get("emitted_candidate_count") or 0),
            "best_net_return": best_raw if isinstance(best_raw, (int, float)) else None,
            "persistence_proxy": persistence, "research_notional_proxy_usd": notional,
            "forward_sample_count": samples, "forward_sample_deficit": deficit, "priority_score": score,
            "dominant_rejection_gate": diagnostic.get("dominant_rejection_gate"),
            "priority_is_diagnostic_only": True, "allocation_authority": False,
        })
    priorities.sort(key=lambda row: float(row["priority_score"]), reverse=True)
    hurdle = next((row.required_net_return for row in observations), None)
    return CandidateObservatorySnapshot(
        cycle_id=cycle_id, observed_at=observed_at, source_scan_id=source_scan_id,
        candidate_event_count=len(observations), raw_signal_count=sum(row.stage == "raw_signal" for row in observations),
        terminal_candidate_count=len(terminal), forward_candidate_count=sum(row.selected_for_forward_test for row in terminal),
        diagnostic_shadow_signals_recorded=shadow_signals_recorded,
        diagnostic_shadow_outcomes_matured=shadow_outcomes_matured,
        near_misses=near_misses[:MAX_NEAR_MISSES], lane_priorities=priorities,
        qualification_requirements={
            "minimum_independent_forward_samples": required_samples, "current_net_return_hurdle": hurdle,
            "source_redundancy_required_for_allocation": True, "regime_coverage_required": True,
            "confidence_adjusted_profitability_required": True,
        },
    )

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import statistics
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, insert, inspect, select, text

from inefficiency_engine.candidate_observatory_runtime import (
    CandidateObservedAllLaneEvidenceFactoryService,
)


RESEARCH_RESET_WORKER_ID = "research-qualification-reset"
RESEARCH_RESET_POLICY_VERSION = "research-reset-v1"
HIT_RATE_BLOCKER = "forward hit-rate confidence lower bound is below hurdle"
LOCAL_SAMPLE_BLOCKER = "insufficient candidate-specific forward samples for cross-asset pooling"

# Hit rate remains diagnostic for asymmetric-payoff families. Mean reversion and
# microstructure keep it as a full-qualification gate because repeated capture is
# part of their expected economic shape.
EXPECTANCY_PRIMARY_LANES = frozenset(
    {
        "trend_momentum",
        "fundamental_onchain",
        "cross_sectional_relative_value",
        "event_driven",
    }
)
HIT_RATE_PRIMARY_LANES = frozenset({"mean_reversion", "microstructure"})


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw in (None, "") else int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw in (None, "") else float(raw)


def _safe_fee_token(value: object) -> str:
    text_value = getattr(value, "value", value)
    return re.sub(r"[^A-Z0-9]+", "_", str(text_value).upper()).strip("_")


@dataclass(frozen=True)
class ResearchResetPolicy:
    provisional_min_samples: int = 12
    provisional_min_regimes: int = 1
    provisional_current_net_return: float = 0.0002
    provisional_penalty_fraction: float = 0.50
    provisional_max_notional_usd: float = 1000.0
    provisional_max_capital_fraction: float = 0.01
    decision_min_total_outcomes: int = 120
    decision_min_outcomes_per_strategy: int = 20

    @classmethod
    def from_env(cls) -> "ResearchResetPolicy":
        return cls(
            provisional_min_samples=max(3, _env_int("CIE_PROVISIONAL_MIN_FORWARD_SAMPLES", 12)),
            provisional_min_regimes=max(1, _env_int("CIE_PROVISIONAL_MIN_REGIMES", 1)),
            provisional_current_net_return=max(
                0.0, _env_float("CIE_PROVISIONAL_MIN_CURRENT_NET_RETURN", 0.0002)
            ),
            provisional_penalty_fraction=max(
                0.0, min(1.0, _env_float("CIE_PROVISIONAL_MULTIPLE_TESTING_PENALTY_FRACTION", 0.50))
            ),
            provisional_max_notional_usd=max(
                0.0, _env_float("CIE_PROVISIONAL_MAX_NOTIONAL_USD", 1000.0)
            ),
            provisional_max_capital_fraction=max(
                0.0, min(0.05, _env_float("CIE_PROVISIONAL_MAX_CAPITAL_FRACTION", 0.01))
            ),
            decision_min_total_outcomes=max(
                1, _env_int("CIE_RESEARCH_RESET_DECISION_MIN_TOTAL_OUTCOMES", 120)
            ),
            decision_min_outcomes_per_strategy=max(
                1, _env_int("CIE_RESEARCH_RESET_DECISION_MIN_OUTCOMES_PER_STRATEGY", 20)
            ),
        )


class ResearchQualificationResetSnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime = Field(default_factory=_now)
    source_scan_id: str | None = None
    policy_version: str = RESEARCH_RESET_POLICY_VERSION
    expectancy_primary_lanes: list[str] = Field(default_factory=list)
    hit_rate_primary_lanes: list[str] = Field(default_factory=list)
    full_forward_sample_target: int = Field(ge=1)
    provisional_forward_sample_target: int = Field(ge=1)
    provisional_current_net_return_hurdle: float = Field(ge=0)
    provisional_max_notional_usd: float = Field(ge=0)
    broad_diagnostic_shadow_count: int = Field(ge=0)
    extra_diagnostic_shadows_scheduled: int = Field(ge=0)
    provisional_candidate_count: int = Field(ge=0)
    full_candidate_count: int = Field(ge=0)
    execution_fee_override_keys: list[str] = Field(default_factory=list)
    provisional_outcome_count: int = Field(ge=0)
    provisional_strategy_outcomes: list[dict[str, object]] = Field(default_factory=list)
    scientific_checkpoint: str
    scientific_checkpoint_reason: str
    paper_only: bool = True
    live_execution_authority: bool = False


class ResearchQualificationResetLedger:
    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.snapshots = Table(
            "research_qualification_reset_snapshots",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_id", String(64), nullable=False, unique=True),
            Column("observed_at", Text, nullable=False),
            Column("source_scan_id", Text),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        metadata.create_all(store.engine)

    def record(self, snapshot: ResearchQualificationResetSnapshot) -> str:
        raw = json.dumps(snapshot.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        lineage = hashlib.sha256(raw.encode()).hexdigest()
        with self.store.engine.begin() as db:
            existing = db.execute(
                select(self.snapshots.c.snapshot_id).where(
                    self.snapshots.c.snapshot_id == snapshot.snapshot_id
                )
            ).scalar_one_or_none()
            if existing is None:
                db.execute(
                    insert(self.snapshots),
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "observed_at": snapshot.observed_at.isoformat(),
                        "source_scan_id": snapshot.source_scan_id,
                        "payload_json": raw,
                        "lineage_hash": lineage,
                    },
                )
        return snapshot.snapshot_id

    def provisional_outcome_summary(self) -> tuple[int, list[dict[str, object]]]:
        available = set(inspect(self.store.engine).get_table_names())
        if not {"allocation_forward_trials", "allocation_forward_outcomes"}.issubset(available):
            return 0, []
        query = text(
            "SELECT t.strategy AS strategy, o.payload_json AS payload_json "
            "FROM allocation_forward_trials t "
            "JOIN allocation_forward_outcomes o ON o.trial_id = t.trial_id "
            "WHERE t.candidate_id LIKE 'provisional:%' "
            "ORDER BY o.id"
        )
        grouped: dict[str, list[float]] = {}
        with self.store.engine.connect() as db:
            rows = list(db.execute(query).mappings())
        for row in rows:
            try:
                payload = json.loads(str(row["payload_json"]))
                value = float(payload["realized_net_return"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
            grouped.setdefault(str(row["strategy"]), []).append(value)
        summaries = []
        for strategy, values in sorted(grouped.items()):
            summaries.append(
                {
                    "strategy_id": strategy,
                    "outcome_count": len(values),
                    "mean_realized_net_return": statistics.fmean(values),
                    "profitable_fraction": sum(value > 0 for value in values) / len(values),
                    "worst_realized_net_return": min(values),
                }
            )
        return sum(int(row["outcome_count"]) for row in summaries), summaries


def _scientific_checkpoint(
    *,
    total_outcomes: int,
    strategy_summaries: list[dict[str, object]],
    policy: ResearchResetPolicy,
) -> tuple[str, str]:
    if total_outcomes < policy.decision_min_total_outcomes:
        return (
            "collect_more_evidence",
            f"{total_outcomes}/{policy.decision_min_total_outcomes} provisional outcomes collected",
        )
    mature = [
        row
        for row in strategy_summaries
        if int(row.get("outcome_count") or 0) >= policy.decision_min_outcomes_per_strategy
    ]
    if not mature:
        return (
            "collect_more_evidence",
            "aggregate target reached but no strategy has enough strategy-specific outcomes",
        )
    positive = [row for row in mature if float(row.get("mean_realized_net_return") or 0.0) > 0.0]
    if positive:
        names = ", ".join(str(row.get("strategy_id")) for row in positive[:5])
        return (
            "concentrate_on_positive_strategies",
            f"mature provisional evidence is net-positive for: {names}",
        )
    return (
        "strategy_universe_change_recommended",
        "mature provisional evidence shows no positive mean net-return strategy",
    )


class ResearchResetAllLaneEvidenceFactoryService(CandidateObservedAllLaneEvidenceFactoryService):
    """High-throughput research with strict separation between learning and authority.

    The reset deliberately broadens research and tiny paper experimentation while
    preserving full paper qualification, fail-closed source/L2 economics, health,
    settlement, portfolio constraints, and the repository-wide prohibition on live
    execution. Provisional paper evidence cannot masquerade as full qualification.
    """

    def __init__(self, core, store, *args, **kwargs):
        super().__init__(core, store, *args, **kwargs)
        self.research_reset_policy = ResearchResetPolicy.from_env()
        self.research_reset_ledger = ResearchQualificationResetLedger(store)
        self._last_reset_provisional_candidates = []
        self._last_reset_extra_shadows = 0

    def _one_way_fee_bps(self, venue, market_kind):
        base = super()._one_way_fee_bps(venue, market_kind)
        venue_token = _safe_fee_token(venue)
        kind_token = _safe_fee_token(market_kind)
        suffix = f"{venue_token}_{kind_token}"
        taker_raw = os.getenv(f"CIE_EXECUTION_TAKER_FEE_BPS_{suffix}")
        maker_raw = os.getenv(f"CIE_EXECUTION_MAKER_FEE_BPS_{suffix}")
        maker_fraction_raw = os.getenv(f"CIE_EXECUTION_EXPECTED_MAKER_FRACTION_{suffix}")
        if maker_fraction_raw in (None, ""):
            maker_fraction_raw = os.getenv("CIE_EXECUTION_EXPECTED_MAKER_FRACTION")
        if taker_raw in (None, "") and maker_raw in (None, ""):
            return base
        taker = float(taker_raw) if taker_raw not in (None, "") else base
        maker = float(maker_raw) if maker_raw not in (None, "") else taker
        if taker is None:
            taker = maker
        if taker is None:
            return None
        fraction = 0.0 if maker_fraction_raw in (None, "") else float(maker_fraction_raw)
        fraction = max(0.0, min(1.0, fraction))
        blended = fraction * float(maker) + (1.0 - fraction) * float(taker)
        return blended

    @staticmethod
    def configured_execution_fee_overrides() -> list[str]:
        prefixes = (
            "CIE_EXECUTION_TAKER_FEE_BPS_",
            "CIE_EXECUTION_MAKER_FEE_BPS_",
            "CIE_EXECUTION_EXPECTED_MAKER_FRACTION_",
        )
        return sorted(
            key for key, value in os.environ.items() if value not in (None, "") and key.startswith(prefixes)
        )

    def qualification(self, candidate):
        qualification = super().qualification(candidate)
        lane = self._lane_for_candidate(candidate)
        if lane not in EXPECTANCY_PRIMARY_LANES or HIT_RATE_BLOCKER not in qualification.blockers:
            return qualification
        blockers = [item for item in qualification.blockers if item != HIT_RATE_BLOCKER]
        qualified = not blockers
        updated = qualification.model_copy(deep=True)
        updated.blockers = blockers
        updated.statistically_qualified = qualified
        updated.paper_allocation_authority = qualified
        return updated

    def discover(self, snapshot, *, total_capital_usd: float):
        rows = super().discover(snapshot, total_capital_usd=total_capital_usd)
        # Once a candidate has a valid source, executable cost, and directional price
        # reference, observe its realized economics even when it misses the current
        # full-paper hurdle by more than the old 25bp near-miss window.
        for observation in self._last_candidate_observations:
            if (
                observation.stage == "net_hurdle_rejected"
                and observation.direction in {"long", "short"}
                and observation.estimated_cost_return is not None
                and observation.expected_net_return is not None
                and math.isfinite(float(observation.entry_reference_price))
            ):
                observation.diagnostic_shadow_eligible = True
        return rows

    def _costed_research_candidates(self):
        rows = []
        for observation in self._last_candidate_observations:
            if observation.stage not in {
                "net_hurdle_rejected",
                "forward_candidate_selected",
                "execution_variant_not_selected",
            }:
                continue
            if observation.expected_net_return is None or observation.estimated_cost_return is None:
                continue
            source = self._last_observatory_candidate_refs.get(observation.candidate_id)
            if source is None:
                continue
            item = source.model_copy(deep=True)
            item.estimated_cost_return = float(observation.estimated_cost_return)
            item.expected_net_return = float(observation.expected_net_return)
            item.expected_profit_usd = item.notional_usd * item.expected_net_return
            item.features.update(
                {
                    "source_forward_test_eligible": True,
                    "source_group_count": len(observation.source_groups),
                    "research_reset_observed_stage": observation.stage,
                }
            )
            rows.append(item)
        best = {}
        for candidate in rows:
            key = self._competition_key(candidate)
            previous = best.get(key)
            if previous is None or (candidate.expected_net_return, candidate.confidence_score) > (
                previous.expected_net_return,
                previous.confidence_score,
            ):
                best[key] = candidate
        return list(best.values())

    def _provisional_statistical_gate(self, candidate, qualification) -> tuple[bool, list[str], float]:
        policy = self.research_reset_policy
        blockers: list[str] = []
        required = max(
            0.0,
            float(qualification.multiple_testing_penalty_return) * policy.provisional_penalty_fraction,
        )
        if qualification.sample_count < policy.provisional_min_samples:
            blockers.append("insufficient forward samples for provisional paper")
        if LOCAL_SAMPLE_BLOCKER in qualification.blockers:
            blockers.append(LOCAL_SAMPLE_BLOCKER)
        if (
            qualification.mean_realized_net_return_ci_lower is None
            or qualification.mean_realized_net_return_ci_lower <= required
        ):
            blockers.append("forward expectancy confidence lower bound is below provisional hurdle")
        if qualification.regime_count < policy.provisional_min_regimes:
            blockers.append("insufficient regime coverage for provisional paper")
        return not blockers, blockers, required

    async def _generic_provisional_candidates(
        self,
        snapshot,
        *,
        total_capital_usd: float,
        promoted,
    ):
        promoted_keys = {
            (item.strategy_id, item.asset.upper(), item.direction) for item in promoted
        }
        source_snapshot = self.source_plane.snapshot(now=snapshot.completed_at)
        provisional = []
        max_notional = min(
            self.research_reset_policy.provisional_max_notional_usd,
            total_capital_usd * self.research_reset_policy.provisional_max_capital_fraction,
        )
        if max_notional < float(self.settings.alpha_min_notional_usd):
            return []

        for candidate in self._costed_research_candidates():
            key = (candidate.strategy_id, candidate.asset.upper(), candidate.direction)
            if key in promoted_keys:
                continue
            qualification = self.qualification(candidate)
            eligible, _, required = self._provisional_statistical_gate(candidate, qualification)
            if not eligible:
                continue
            health = self.strategy_health(candidate)
            if not health.healthy_for_paper_allocation:
                continue
            try:
                source_gate = self._source_gate(candidate, source_snapshot)
            except Exception:
                continue
            if source_gate is None or not source_gate.forward_test_eligible:
                continue

            book = self._snapshot_book(candidate, snapshot)
            current_cost = (
                self._cost_from_book(candidate, book)
                if book is not None
                else await self._bounded_current_l2_cost(candidate)
            )
            if current_cost is None:
                continue
            current_cost += self._holding_carry_cost(candidate)
            current_net = float(candidate.expected_gross_return) - float(current_cost)
            if current_net <= self.research_reset_policy.provisional_current_net_return:
                continue
            forward_lower = float(qualification.mean_realized_net_return_ci_lower or 0.0)
            conservative = min(current_net, forward_lower)
            if conservative <= self.research_reset_policy.provisional_current_net_return:
                continue

            item = candidate.model_copy(deep=True)
            original_notional = max(1e-9, float(item.notional_usd))
            item.candidate_id = f"provisional:{candidate.candidate_id}"
            item.notional_usd = min(float(item.notional_usd), max_notional)
            item.capital_required_usd = float(item.capital_required_usd) * (
                item.notional_usd / original_notional
            )
            item.estimated_cost_return = current_cost
            item.expected_net_return = conservative
            item.expected_profit_usd = item.notional_usd * conservative
            item.stage = "research"
            item.paper_allocation_eligible = True
            item.live_execution_eligible = False
            item.features.update(
                {
                    "qualification_tier": "provisional_paper",
                    "provisional_paper": True,
                    "provisional_forward_samples": qualification.sample_count,
                    "provisional_required_mean_lower": required,
                    "provisional_current_net_hurdle": self.research_reset_policy.provisional_current_net_return,
                    "provisional_max_notional_usd": self.research_reset_policy.provisional_max_notional_usd,
                    "provisional_source_policy": "forward_test_sufficient",
                    "source_allocation_qualified": bool(source_gate.allocation_source_qualified),
                    "full_allocation_source_redundancy_required": True,
                    "health_score": health.health_score,
                    "paper_only": True,
                    "live_execution_authority": False,
                }
            )
            provisional.append(item)

        provisional.sort(
            key=lambda item: (item.expected_net_return, item.expected_profit_usd), reverse=True
        )
        return provisional

    async def promoted_candidates(self, snapshot, *, total_capital_usd: float):
        promoted = await super().promoted_candidates(
            snapshot, total_capital_usd=total_capital_usd
        )
        provisional = await self._generic_provisional_candidates(
            snapshot,
            total_capital_usd=total_capital_usd,
            promoted=promoted,
        )
        self._last_reset_provisional_candidates = provisional
        rows = [*promoted, *provisional]
        rows.sort(
            key=lambda item: (item.expected_net_return, item.expected_profit_usd), reverse=True
        )
        return rows

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        alpha = await super().run_evidence_cycle(total_capital_usd=total_capital_usd)
        snapshot = self._last_discovery_snapshot
        if snapshot is None:
            return alpha
        try:
            # The parent observatory caps new diagnostic shadows per cycle. Schedule
            # remaining eligible costed rejects here; duplicate/open keys are ignored
            # by the append-only observatory ledger.
            extra = 0
            for observation in sorted(
                (row for row in self._last_candidate_observations if row.diagnostic_shadow_eligible),
                key=lambda row: (
                    float(row.expected_net_return) if row.expected_net_return is not None else -1.0,
                    float(row.expected_gross_return),
                ),
                reverse=True,
            ):
                extra += self.candidate_observatory.record_shadow_signal(observation)
            self._last_reset_extra_shadows = extra

            total_outcomes, strategy_summaries = self.research_reset_ledger.provisional_outcome_summary()
            checkpoint, reason = _scientific_checkpoint(
                total_outcomes=total_outcomes,
                strategy_summaries=strategy_summaries,
                policy=self.research_reset_policy,
            )
            full_projection = 0
            for candidate in self._costed_research_candidates():
                try:
                    full_projection += int(self.qualification(candidate).statistically_qualified)
                except Exception:
                    continue
            provisional_projection = 0
            for candidate in self._costed_research_candidates():
                try:
                    qualification = self.qualification(candidate)
                    eligible, _, _ = self._provisional_statistical_gate(candidate, qualification)
                    provisional_projection += int(eligible and not qualification.statistically_qualified)
                except Exception:
                    continue
            reset_snapshot = ResearchQualificationResetSnapshot(
                observed_at=alpha.observed_at,
                source_scan_id=snapshot.scan_id,
                expectancy_primary_lanes=sorted(EXPECTANCY_PRIMARY_LANES),
                hit_rate_primary_lanes=sorted(HIT_RATE_PRIMARY_LANES),
                full_forward_sample_target=max(1, int(self.settings.alpha_min_forward_samples)),
                provisional_forward_sample_target=self.research_reset_policy.provisional_min_samples,
                provisional_current_net_return_hurdle=self.research_reset_policy.provisional_current_net_return,
                provisional_max_notional_usd=self.research_reset_policy.provisional_max_notional_usd,
                broad_diagnostic_shadow_count=sum(
                    1 for row in self._last_candidate_observations if row.diagnostic_shadow_eligible
                ),
                extra_diagnostic_shadows_scheduled=extra,
                provisional_candidate_count=provisional_projection,
                full_candidate_count=full_projection,
                execution_fee_override_keys=self.configured_execution_fee_overrides(),
                provisional_outcome_count=total_outcomes,
                provisional_strategy_outcomes=strategy_summaries,
                scientific_checkpoint=checkpoint,
                scientific_checkpoint_reason=reason,
            )
            self.research_reset_ledger.record(reset_snapshot)
            try:
                self.store.record_worker_heartbeat(
                    worker_id=RESEARCH_RESET_WORKER_ID,
                    state="success",
                    cycle_id=alpha.cycle_id,
                    observed_at=alpha.observed_at,
                    detail={
                        "policy_version": RESEARCH_RESET_POLICY_VERSION,
                        "broad_diagnostic_shadow_count": reset_snapshot.broad_diagnostic_shadow_count,
                        "extra_diagnostic_shadows_scheduled": extra,
                        "provisional_candidate_count": provisional_projection,
                        "full_candidate_count": full_projection,
                        "scientific_checkpoint": checkpoint,
                        "paper_only": True,
                        "live_execution_authority": False,
                    },
                )
            except Exception:
                pass
        except Exception as exc:
            try:
                self.store.record_worker_heartbeat(
                    worker_id=RESEARCH_RESET_WORKER_ID,
                    state="error",
                    cycle_id=alpha.cycle_id,
                    observed_at=alpha.observed_at,
                    error_type=type(exc).__name__,
                    detail={
                        "message": str(exc)[:500],
                        "reset_observability_failure_is_non_authoritative": True,
                        "paper_only": True,
                        "live_execution_authority": False,
                    },
                )
            except Exception:
                pass
        return alpha

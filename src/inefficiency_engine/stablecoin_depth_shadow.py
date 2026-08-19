from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from statistics import NormalDist

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, func, insert, select

from inefficiency_engine.config import Settings
from inefficiency_engine.conversion_depth import StablecoinConversionDepthQuote, quote_stablecoin_conversion_depth
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService


SleepFn = Callable[[float], Awaitable[None]]


class StablecoinDepthProbeSpec(BaseModel):
    source_currency: str
    target_currency: str
    input_amount: float = Field(gt=0)

    @property
    def key(self) -> str:
        raw = f"{self.source_currency.upper()}|{self.target_currency.upper()}|{self.input_amount:.8f}"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]


class StablecoinDepthRecord(BaseModel):
    record_id: str
    cycle_id: str
    phase: str
    horizon_seconds: float = Field(ge=0)
    probe_key: str
    observed_at: datetime
    quote: StablecoinConversionDepthQuote
    paper_only: bool = True


class StablecoinDepthObservation(BaseModel):
    cycle_id: str
    probe_key: str
    source_currency: str
    target_currency: str
    input_amount: float = Field(gt=0)
    horizon_seconds: float = Field(ge=0)
    initial_quote_id: str
    verification_quote_id: str | None = None
    initial_output_amount: float = Field(gt=0)
    verification_output_amount: float | None = Field(default=None, gt=0)
    output_change_bps: float | None = None
    adverse_deterioration_bps: float | None = Field(default=None, ge=0)
    initial_slippage_bps: float = Field(ge=0)
    verification_slippage_bps: float | None = Field(default=None, ge=0)
    slippage_expansion_bps: float | None = None
    survived: bool
    failure_type: str | None = None
    verified_at: datetime
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class StablecoinDepthShadowCycle(BaseModel):
    cycle_id: str
    started_at: datetime
    completed_at: datetime
    horizons_seconds: list[float]
    initial_quote_count: int = Field(ge=0)
    records: list[StablecoinDepthRecord] = Field(default_factory=list)
    observations: list[StablecoinDepthObservation] = Field(default_factory=list)
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class ProbabilityEstimate(BaseModel):
    successes: int = Field(ge=0)
    sample_count: int = Field(ge=0)
    probability: float | None = Field(default=None, ge=0, le=1)
    ci_lower: float | None = Field(default=None, ge=0, le=1)
    ci_upper: float | None = Field(default=None, ge=0, le=1)
    ci_width: float | None = Field(default=None, ge=0, le=1)


class StablecoinDepthStatisticalQualification(BaseModel):
    probe_key: str
    source_currency: str
    target_currency: str
    input_amount: float = Field(gt=0)
    reference_horizon_seconds: float = Field(ge=0)
    effective_sample_count: int = Field(ge=0)
    adverse_tail_sample_count: int = Field(ge=0)
    survival: ProbabilityEstimate
    p95_adverse_deterioration_bps: float | None = None
    p95_slippage_expansion_bps: float | None = None
    statistically_qualified: bool
    reasons: list[str] = Field(default_factory=list)
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


def default_probe_specs(settings: Settings) -> tuple[StablecoinDepthProbeSpec, ...]:
    notionals = tuple(
        float(value)
        for value in getattr(settings, "stablecoin_depth_shadow_notionals", (1000.0, 5000.0, 10000.0, 25000.0))
        if float(value) > 0
    )
    pairs = (
        ("USDC", "USD"),
        ("USD", "USDC"),
        ("USDT", "USD"),
        ("USD", "USDT"),
        ("USDC", "USDT"),
        ("USDT", "USDC"),
    )
    return tuple(
        StablecoinDepthProbeSpec(source_currency=source, target_currency=target, input_amount=amount)
        for amount in sorted(set(notionals))
        for source, target in pairs
    )


def _json(value: BaseModel) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * min(1.0, max(0.0, q))
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def _wilson(successes: int, n: int, confidence: float) -> ProbabilityEstimate:
    if n <= 0:
        return ProbabilityEstimate(successes=0, sample_count=0)
    confidence = min(0.999999, max(0.500001, confidence))
    z = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    phat = successes / n
    z2 = z * z
    denominator = 1.0 + z2 / n
    center = (phat + z2 / (2.0 * n)) / denominator
    margin = z * ((phat * (1.0 - phat) / n + z2 / (4.0 * n * n)) ** 0.5) / denominator
    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)
    return ProbabilityEstimate(
        successes=successes,
        sample_count=n,
        probability=phat,
        ci_lower=lower,
        ci_upper=upper,
        ci_width=upper - lower,
    )


class StablecoinDepthLedger:
    def __init__(self, evidence_store: EvidenceStore):
        self.engine = evidence_store.engine
        self.metadata = MetaData()
        self.records = Table(
            "stablecoin_depth_records",
            self.metadata,
            Column("record_id", String(64), primary_key=True),
            Column("cycle_id", String(64), nullable=False),
            Column("phase", Text, nullable=False),
            Column("horizon_seconds", Text, nullable=False),
            Column("probe_key", String(64), nullable=False),
            Column("source_currency", Text, nullable=False),
            Column("target_currency", Text, nullable=False),
            Column("input_amount", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.cycles = Table(
            "stablecoin_depth_shadow_cycles",
            self.metadata,
            Column("cycle_id", String(64), primary_key=True),
            Column("started_at", Text, nullable=False),
            Column("completed_at", Text, nullable=False),
            Column("initial_quote_count", Integer, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_stablecoin_depth_record_cycle", self.records.c.cycle_id)
        Index("ix_stablecoin_depth_record_key", self.records.c.probe_key)
        Index("ix_stablecoin_depth_cycle_completed", self.cycles.c.completed_at)
        self.metadata.create_all(self.engine)

    def record_cycle(self, cycle: StablecoinDepthShadowCycle) -> str:
        cycle_payload = _json(cycle)
        rows: list[dict[str, object]] = []
        for record in cycle.records:
            payload = _json(record)
            quote = record.quote
            rows.append({
                "record_id": record.record_id,
                "cycle_id": record.cycle_id,
                "phase": record.phase,
                "horizon_seconds": str(record.horizon_seconds),
                "probe_key": record.probe_key,
                "source_currency": quote.source_currency,
                "target_currency": quote.target_currency,
                "input_amount": str(quote.input_amount),
                "observed_at": record.observed_at.isoformat(),
                "payload_json": payload,
                "lineage_hash": hashlib.sha256(payload.encode()).hexdigest(),
            })
        with self.engine.begin() as db:
            db.execute(insert(self.cycles), {
                "cycle_id": cycle.cycle_id,
                "started_at": cycle.started_at.isoformat(),
                "completed_at": cycle.completed_at.isoformat(),
                "initial_quote_count": cycle.initial_quote_count,
                "payload_json": cycle_payload,
                "lineage_hash": hashlib.sha256(cycle_payload.encode()).hexdigest(),
            })
            if rows:
                db.execute(insert(self.records), rows)
        return cycle.cycle_id

    def cycles_all(self) -> list[StablecoinDepthShadowCycle]:
        with self.engine.connect() as db:
            payloads = list(db.execute(select(self.cycles.c.payload_json).order_by(self.cycles.c.completed_at)).scalars())
        return [StablecoinDepthShadowCycle.model_validate_json(payload) for payload in payloads]

    def load_cycle(self, cycle_id: str) -> StablecoinDepthShadowCycle:
        with self.engine.connect() as db:
            payload = db.execute(select(self.cycles.c.payload_json).where(self.cycles.c.cycle_id == cycle_id)).scalar_one_or_none()
        if payload is None:
            raise KeyError(f"unknown stablecoin depth cycle_id: {cycle_id}")
        return StablecoinDepthShadowCycle.model_validate_json(payload)

    def summary(self) -> dict[str, object]:
        cycles = self.cycles_all()
        observations = [row for cycle in cycles for row in cycle.observations]
        with self.engine.connect() as db:
            record_count = int(db.execute(select(func.count()).select_from(self.records)).scalar_one())
        return {
            "cycle_count": len(cycles),
            "record_count": record_count,
            "observation_count": len(observations),
            "survived_count": sum(row.survived for row in observations),
            "capacity_claimed": False,
            "executable_eligible": False,
            "paper_only": True,
        }


class StablecoinDepthShadowService:
    def __init__(
        self,
        depth_service: StablecoinConversionDepthService,
        *,
        evidence_store: EvidenceStore | None = None,
        sleep: SleepFn = asyncio.sleep,
        specs: tuple[StablecoinDepthProbeSpec, ...] | None = None,
    ):
        self.depth_service = depth_service
        self.settings = depth_service.settings
        self.sleep = sleep
        self.specs = specs or default_probe_specs(self.settings)
        self.ledger = StablecoinDepthLedger(evidence_store) if evidence_store is not None else None

    @staticmethod
    def _record(cycle_id: str, phase: str, horizon: float, spec: StablecoinDepthProbeSpec,
                quote: StablecoinConversionDepthQuote) -> StablecoinDepthRecord:
        raw = f"{cycle_id}:{phase}:{horizon}:{spec.key}:{quote.quote_id}"
        return StablecoinDepthRecord(
            record_id=hashlib.sha256(raw.encode()).hexdigest()[:32],
            cycle_id=cycle_id,
            phase=phase,
            horizon_seconds=horizon,
            probe_key=spec.key,
            observed_at=quote.observed_at,
            quote=quote,
        )

    async def _probe(self) -> dict[str, tuple[StablecoinDepthProbeSpec, StablecoinConversionDepthQuote | None, str | None]]:
        books = await self.depth_service.collect_books()
        now = datetime.now(timezone.utc)
        results: dict[str, tuple[StablecoinDepthProbeSpec, StablecoinConversionDepthQuote | None, str | None]] = {}
        for spec in self.specs:
            try:
                quote = quote_stablecoin_conversion_depth(
                    spec.source_currency,
                    spec.target_currency,
                    spec.input_amount,
                    books,
                    now=now,
                    max_book_age_seconds=self.settings.max_order_book_age_seconds,
                    max_book_skew_seconds=self.settings.max_order_book_skew_seconds,
                )
                results[spec.key] = (spec, quote, None)
            except Exception as exc:
                results[spec.key] = (spec, None, type(exc).__name__)
        return results

    async def run_cycle(self, *, horizons_seconds: tuple[float, ...] | None = None) -> StablecoinDepthShadowCycle:
        horizons = tuple(sorted(set(max(0.0, value) for value in (horizons_seconds or self.settings.shadow_horizons_seconds))))
        if not horizons:
            horizons = (max(0.0, self.settings.shadow_delay_seconds),)
        cycle_id = uuid.uuid4().hex
        started_at = datetime.now(timezone.utc)
        initial = await self._probe()
        records: list[StablecoinDepthRecord] = []
        for spec, quote, _ in initial.values():
            if quote is not None:
                records.append(self._record(cycle_id, "initial", 0.0, spec, quote))

        observations: list[StablecoinDepthObservation] = []
        elapsed = 0.0
        for horizon in horizons:
            wait = max(0.0, horizon - elapsed)
            if wait > 0:
                await self.sleep(wait)
            elapsed = horizon
            verification = await self._probe()
            verified_at = datetime.now(timezone.utc)
            for key, (spec, initial_quote, initial_failure) in initial.items():
                if initial_quote is None:
                    continue
                _, verification_quote, verification_failure = verification.get(key, (spec, None, "ProbeMissing"))
                if verification_quote is None:
                    observations.append(StablecoinDepthObservation(
                        cycle_id=cycle_id,
                        probe_key=key,
                        source_currency=spec.source_currency.upper(),
                        target_currency=spec.target_currency.upper(),
                        input_amount=spec.input_amount,
                        horizon_seconds=horizon,
                        initial_quote_id=initial_quote.quote_id,
                        initial_output_amount=initial_quote.output_amount,
                        initial_slippage_bps=initial_quote.total_slippage_bps,
                        survived=False,
                        failure_type=verification_failure or initial_failure or "ProbeMissing",
                        verified_at=verified_at,
                    ))
                    continue
                records.append(self._record(cycle_id, "verification", horizon, spec, verification_quote))
                output_change_bps = (verification_quote.output_amount / initial_quote.output_amount - 1.0) * 10_000.0
                slippage_expansion = verification_quote.total_slippage_bps - initial_quote.total_slippage_bps
                observations.append(StablecoinDepthObservation(
                    cycle_id=cycle_id,
                    probe_key=key,
                    source_currency=spec.source_currency.upper(),
                    target_currency=spec.target_currency.upper(),
                    input_amount=spec.input_amount,
                    horizon_seconds=horizon,
                    initial_quote_id=initial_quote.quote_id,
                    verification_quote_id=verification_quote.quote_id,
                    initial_output_amount=initial_quote.output_amount,
                    verification_output_amount=verification_quote.output_amount,
                    output_change_bps=output_change_bps,
                    adverse_deterioration_bps=max(0.0, -output_change_bps),
                    initial_slippage_bps=initial_quote.total_slippage_bps,
                    verification_slippage_bps=verification_quote.total_slippage_bps,
                    slippage_expansion_bps=slippage_expansion,
                    survived=True,
                    verified_at=verified_at,
                ))

        cycle = StablecoinDepthShadowCycle(
            cycle_id=cycle_id,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            horizons_seconds=list(horizons),
            initial_quote_count=sum(quote is not None for _, quote, _ in initial.values()),
            records=records,
            observations=observations,
        )
        if self.ledger is not None:
            self.ledger.record_cycle(cycle)
        return cycle


def build_stablecoin_depth_statistical_qualification(
    cycles: list[StablecoinDepthShadowCycle],
    spec: StablecoinDepthProbeSpec,
    settings: Settings,
) -> StablecoinDepthStatisticalQualification:
    horizon = float(getattr(settings, "stablecoin_depth_statistical_reference_horizon_seconds", settings.dex_statistical_reference_horizon_seconds))
    confidence = float(getattr(settings, "stablecoin_depth_statistical_confidence_level", settings.dex_statistical_confidence_level))
    min_samples = int(getattr(settings, "stablecoin_depth_statistical_min_effective_samples", settings.dex_statistical_min_effective_samples))
    min_tail = int(getattr(settings, "stablecoin_depth_statistical_min_tail_samples", settings.dex_statistical_min_tail_samples))
    min_survival = float(getattr(settings, "stablecoin_depth_statistical_min_survival_lower_bound", settings.dex_statistical_min_survival_lower_bound))
    max_ci_width = float(getattr(settings, "stablecoin_depth_statistical_max_ci_width", settings.dex_statistical_max_ci_width))
    max_p95 = float(getattr(settings, "stablecoin_depth_statistical_max_p95_deterioration_bps", 10.0))

    rows: list[StablecoinDepthObservation] = []
    for cycle in cycles:
        match = next((row for row in cycle.observations if row.probe_key == spec.key and abs(row.horizon_seconds - horizon) <= 1e-9), None)
        if match is not None:
            rows.append(match)
    survival = _wilson(sum(row.survived for row in rows), len(rows), confidence)
    adverse = [row.adverse_deterioration_bps for row in rows if row.survived and row.adverse_deterioration_bps is not None]
    slip = [max(0.0, row.slippage_expansion_bps) for row in rows if row.survived and row.slippage_expansion_bps is not None]
    p95_adverse = _quantile(adverse, 0.95)
    p95_slip = _quantile(slip, 0.95)

    reasons: list[str] = []
    if len(rows) < min_samples:
        reasons.append(f"effective samples {len(rows)} < {min_samples}")
    if len(adverse) < min_tail:
        reasons.append(f"adverse tail samples {len(adverse)} < {min_tail}")
    if survival.ci_lower is None or survival.ci_lower < min_survival:
        reasons.append("survival Wilson lower bound below configured minimum")
    if survival.ci_width is None or survival.ci_width > max_ci_width:
        reasons.append("survival confidence interval too wide")
    if p95_adverse is None or p95_adverse > max_p95:
        reasons.append("p95 conversion-depth deterioration exceeds configured ceiling or is unavailable")

    return StablecoinDepthStatisticalQualification(
        probe_key=spec.key,
        source_currency=spec.source_currency.upper(),
        target_currency=spec.target_currency.upper(),
        input_amount=spec.input_amount,
        reference_horizon_seconds=horizon,
        effective_sample_count=len(rows),
        adverse_tail_sample_count=len(adverse),
        survival=survival,
        p95_adverse_deterioration_bps=p95_adverse,
        p95_slippage_expansion_bps=p95_slip,
        statistically_qualified=not reasons,
        reasons=reasons,
    )


class StablecoinDepthStatisticalService:
    def __init__(self, ledger: StablecoinDepthLedger, settings: Settings):
        self.ledger = ledger
        self.settings = settings

    def model(self, source_currency: str, target_currency: str, input_amount: float) -> StablecoinDepthStatisticalQualification:
        spec = StablecoinDepthProbeSpec(
            source_currency=source_currency,
            target_currency=target_currency,
            input_amount=input_amount,
        )
        return build_stablecoin_depth_statistical_qualification(self.ledger.cycles_all(), spec, self.settings)

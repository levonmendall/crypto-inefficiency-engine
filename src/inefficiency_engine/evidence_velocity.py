from __future__ import annotations

import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import inspect, text


# Source validity is evidence-class specific. Execution still has its own stricter
# quote/L2 freshness gates; these values only determine whether evidence may remain
# in the research/forward-learning plane.
EVIDENCE_CLASS_FRESHNESS_SECONDS: dict[str, float] = {
    "market_quotes": 300.0,
    "executable_depth": 120.0,
    "order_book": 120.0,
    "trade_flow": 120.0,
    "funding_or_basis": 900.0,
    "yield_rate": 1_800.0,
    "capacity": 1_800.0,
    "exit_liquidity": 1_800.0,
    "market_history": 21_600.0,
    "multi_asset_market_history": 21_600.0,
    "execution_costs": 21_600.0,
    "chain_activity": 21_600.0,
    "protocol_fundamentals": 86_400.0,
    "option_quotes": 900.0,
    "option_greeks": 900.0,
    "option_depth": 900.0,
    "timestamped_events": 86_400.0,
    "event_identity": 86_400.0,
    "liquidation_events": 300.0,
    "liquidation_pressure": 300.0,
    "distress_state": 900.0,
    "venue_opportunity_history": 86_400.0,
    "transfer_costs": 86_400.0,
    "transfer_latency": 86_400.0,
    "maker_fill_outcomes": 21_600.0,
}

ALPHA_FAMILY_TO_LANE: dict[str, str] = {
    "directional_time_series": "trend_momentum",
    "directional_reversal": "mean_reversion",
    "onchain_fundamental": "fundamental_onchain",
    "cross_sectional_relative_value": "cross_sectional_relative_value",
    "event_driven": "event_driven",
    "microstructure_orderflow": "microstructure",
}

VENUE_SOURCE_GROUP: dict[str, str] = {
    "coinbase": "coinbase",
    "bybit": "bybit",
    "kraken": "kraken",
    "okx": "okx",
    "hlperp": "hyperliquid",
    "hyperliquid": "hyperliquid",
    "deribit": "deribit",
    "morpho": "morpho",
    "lido": "lido",
    "aave": "aave",
}

PROVISIONAL_FORWARD_MIN_OUTCOMES = 3
PROVISIONAL_FORWARD_MIN_HIT_RATE = 0.55
DEFAULT_STAGNATION_WINDOW = 50


@dataclass(frozen=True)
class StagnationDiagnostic:
    lane_id: str
    stagnant: bool
    observed_snapshots: int
    state: str | None
    progress_signature: tuple[int, int, int, int, int, int] | None
    remediation: str
    automatic_priority_boost: float
    thresholds_unchanged: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "lane_id": self.lane_id,
            "stagnant": self.stagnant,
            "observed_snapshots": self.observed_snapshots,
            "state": self.state,
            "progress_signature": list(self.progress_signature) if self.progress_signature else None,
            "remediation": self.remediation,
            "automatic_priority_boost": self.automatic_priority_boost,
            "thresholds_unchanged": self.thresholds_unchanged,
        }


def evidence_freshness_seconds(
    evidence_classes: Iterable[str],
    *,
    fallback_seconds: float,
) -> float:
    values = [
        EVIDENCE_CLASS_FRESHNESS_SECONDS[item]
        for item in evidence_classes
        if item in EVIDENCE_CLASS_FRESHNESS_SECONDS
    ]
    return max(1.0, min(values) if values else float(fallback_seconds))


def alpha_lane_for_family(family: str) -> str | None:
    return ALPHA_FAMILY_TO_LANE.get(str(family))


def source_group_for_venue(venue: str) -> str | None:
    return VENUE_SOURCE_GROUP.get(str(venue).strip().lower())


def provisional_forward_positive(
    *,
    outcome_count: int,
    mean_net_return: float | None,
    hit_rate: float | None,
) -> bool:
    return bool(
        int(outcome_count) >= PROVISIONAL_FORWARD_MIN_OUTCOMES
        and mean_net_return is not None
        and float(mean_net_return) > 0.0
        and hit_rate is not None
        and float(hit_rate) >= PROVISIONAL_FORWARD_MIN_HIT_RATE
    )


def _number(value: object | None) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _integer(value: object | None) -> int:
    parsed = _number(value)
    return max(0, int(parsed or 0))


def _json_dict(raw: object | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _operating_history(store, *, limit: int = DEFAULT_STAGNATION_WINDOW) -> list[dict[str, Any]]:
    try:
        if "operating_certification_snapshots" not in set(inspect(store.engine).get_table_names()):
            return []
        with store.engine.connect() as db:
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM operating_certification_snapshots "
                        "ORDER BY id DESC LIMIT :limit"
                    ),
                    {"limit": max(1, min(500, int(limit)))},
                ).scalars()
            )
    except Exception:
        return []
    return [row for raw in raws if (row := _json_dict(raw)) is not None]


def _mechanism_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in list(payload.get("mechanisms") or []):
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("mechanism_id") or "")
        if lane_id:
            result[lane_id] = row
    return result


def _progress_signature(row: dict[str, Any]) -> tuple[int, int, int, int, int, int]:
    return (
        _integer(row.get("authoritative_observation_count")),
        _integer(row.get("forward_signal_count")),
        _integer(row.get("independent_forward_outcome_count")),
        _integer(row.get("current_statistically_qualified_count")),
        _integer(row.get("current_promoted_count")),
        _integer(row.get("settled_allocator_outcome_count")),
    )


def _stagnation_remediation(row: dict[str, Any]) -> tuple[str, float]:
    state = str(row.get("state") or "collecting")
    stage = str(row.get("stage") or "")
    signature = _progress_signature(row)
    authoritative, signals, outcomes, statistical, promoted, settled = signature

    # Investment rejection is not an engineering defect. Keep collecting without
    # weakening any hurdle when economics/statistics are genuinely poor.
    if state == "poor_economics":
        return "observe_only_poor_economics", 0.0
    if state == "statistical_failure":
        return "observe_only_statistical_failure", 0.0
    if state == "certified":
        return "maintain_monitoring", 0.0

    if state == "provider_gap" or stage.startswith("waiting_for_source:"):
        return "prioritize_missing_or_stale_authoritative_source", 160.0
    if authoritative > 0 and signals <= 0:
        return "prioritize_candidate_generation_and_economic_projection", 120.0
    if signals > 0 and outcomes <= 0:
        return "prioritize_forward_settlement_and_maturity_collection", 90.0
    if outcomes > 0 and statistical <= 0:
        return "prioritize_independent_forward_sampling", 80.0
    if statistical > 0 and promoted <= 0:
        return "prioritize_execution_cost_capacity_and_l2_refresh", 120.0
    if promoted > 0 and settled <= 0:
        return "prioritize_allocator_settlement_evidence", 120.0
    return "prioritize_next_incomplete_funnel_boundary", 70.0


def stagnation_diagnostics(
    store,
    *,
    lane_ids: Iterable[str],
    window: int = DEFAULT_STAGNATION_WINDOW,
) -> dict[str, StagnationDiagnostic]:
    lane_ids = [str(item) for item in lane_ids]
    history = _operating_history(store, limit=max(1, int(window)))
    maps = [_mechanism_map(payload) for payload in history]
    result: dict[str, StagnationDiagnostic] = {}
    for lane_id in lane_ids:
        rows = [mapping[lane_id] for mapping in maps if lane_id in mapping]
        latest = rows[0] if rows else None
        signatures = [_progress_signature(row) for row in rows[:window]]
        stagnant = bool(
            len(signatures) >= window
            and signatures
            and all(item == signatures[0] for item in signatures)
        )
        remediation, boost = (
            _stagnation_remediation(latest) if stagnant and latest is not None else ("not_stagnant", 0.0)
        )
        result[lane_id] = StagnationDiagnostic(
            lane_id=lane_id,
            stagnant=stagnant,
            observed_snapshots=len(rows),
            state=str(latest.get("state")) if latest is not None else None,
            progress_signature=signatures[0] if signatures else None,
            remediation=remediation,
            automatic_priority_boost=boost,
        )
    return result


def _lane_attr(row: object, name: str, default: object = None) -> object:
    if isinstance(row, dict):
        return row.get(name, default)
    return getattr(row, name, default)


def dynamic_lane_priority(store, lanes: Sequence[object]) -> list[str]:
    """Rank lanes by marginal information value and distance to the next gate.

    This scheduler changes collection priority only. It never changes source,
    economic, statistical, risk, execution, settlement, or allocation thresholds.
    """

    lane_ids = [str(_lane_attr(row, "lane_id", "")) for row in lanes]
    lane_ids = [item for item in lane_ids if item]
    history = _operating_history(store, limit=1)
    operating = _mechanism_map(history[0]) if history else {}
    stagnation = stagnation_diagnostics(store, lane_ids=lane_ids)
    scored: list[tuple[float, str]] = []

    for lane in lanes:
        lane_id = str(_lane_attr(lane, "lane_id", ""))
        if not lane_id:
            continue
        research_eligible = bool(_lane_attr(lane, "research_eligible", False))
        forward_eligible = bool(_lane_attr(lane, "forward_test_eligible", False))
        allocation_source_qualified = bool(
            _lane_attr(
                lane,
                "allocation_source_qualified",
                _lane_attr(lane, "source_layer_sufficient", False),
            )
        )
        missing = len(list(_lane_attr(lane, "missing_evidence_classes", []) or []))
        score = 0.0
        if not research_eligible:
            score += 220.0
        elif not forward_eligible:
            score += 180.0 + 20.0 * missing
        elif not allocation_source_qualified:
            score += 100.0
        else:
            score += 30.0

        row = operating.get(lane_id, {})
        state = str(row.get("state") or "collecting")
        state_bonus = {
            "provider_gap": 160.0,
            "collecting": 100.0,
            "execution_blocked": 120.0,
            "settlement_blocked": 120.0,
            "certifying": 140.0,
            "poor_economics": -120.0,
            "statistical_failure": -50.0,
            "certified": -250.0,
        }.get(state, 0.0)
        score += state_bonus

        forward = _integer(row.get("independent_forward_outcome_count"))
        settled = _integer(row.get("settled_allocator_outcome_count"))
        if 0 < forward < 30:
            score += 60.0 * min(1.0, forward / 30.0)
        if 0 < settled < 20:
            score += 90.0 * min(1.0, settled / 20.0)
        score += stagnation.get(
            lane_id,
            StagnationDiagnostic(lane_id, False, 0, None, None, "not_stagnant", 0.0),
        ).automatic_priority_boost
        scored.append((score, lane_id))

    scored.sort(key=lambda item: (-item[0], item[1]))
    return [lane_id for _, lane_id in scored]


def prioritize_source_probes(store, lanes: Sequence[object], probes: Sequence[tuple]) -> list[tuple]:
    lane_order = dynamic_lane_priority(store, lanes)
    rank = {lane_id: index for index, lane_id in enumerate(lane_order)}

    def key(probe: tuple) -> tuple[int, int]:
        lane_ids = []
        if len(probe) > 1 and isinstance(probe[1], list):
            lane_ids = [str(item) for item in probe[1]]
        elif probe:
            lane_ids = [str(probe[0])]
        best = min((rank.get(lane_id, len(rank) + 100) for lane_id in lane_ids), default=len(rank) + 100)
        return (best, 0)

    return sorted(list(probes), key=key)

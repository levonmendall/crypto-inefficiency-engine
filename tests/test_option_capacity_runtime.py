from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.evidence_velocity import EVIDENCE_CLASS_FRESHNESS_SECONDS
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.mechanism_execution import MechanismTrialSpec
from inefficiency_engine.option_capacity import (
    OptionCapacityLedger,
    OptionCapacityObservation,
    _select_surface,
)
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


NOW = datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc)
EXPIRY = datetime(2026, 8, 28, 8, 0, tzinfo=timezone.utc)
NEXT_EXPIRY = datetime(2026, 9, 4, 8, 0, tzinfo=timezone.utc)
THIRD_EXPIRY = datetime(2026, 9, 11, 8, 0, tzinfo=timezone.utc)


def _capacity(observation_id: str = "cap-1") -> OptionCapacityObservation:
    return OptionCapacityObservation(
        observation_id=observation_id,
        instrument_name="BTC-28AUG26-60000-C",
        underlying="BTC",
        expiry=EXPIRY,
        strike=60_000.0,
        option_type="call",
        observed_at=NOW,
        underlying_price_usd=60_000.0,
        bid_visible_amount_underlying=1.0,
        ask_visible_amount_underlying=0.8,
        bid_capacity_usd=60_000.0,
        ask_capacity_usd=48_000.0,
        source_reference="deribit:test",
    )


def _spec() -> MechanismTrialSpec:
    return MechanismTrialSpec(
        mechanism_id="volatility",
        cohort_key="vol|Deribit|BTC|short_volatility",
        asset="BTC",
        venues=["Deribit"],
        source_observed_at=NOW,
        holding_hours=6.0,
        capital_usd=10_000.0,
        predicted_net_return=0.01,
        settlement_payload={
            "venue": "Deribit",
            "underlying": "BTC",
            "expiry": EXPIRY.isoformat(),
            "strike": 60_000.0,
            "option_type": "call",
            "direction": "short_volatility",
        },
        conflict_keys=["option:test"],
    )


def test_option_capacity_is_explicit_source_class_with_short_freshness():
    required = set(LANES["volatility"]["required"])
    capacity_sources = [
        source
        for source in SOURCES
        if "volatility" in source["lanes"] and "option_capacity" in source["classes"]
    ]

    assert "option_capacity" in required
    assert [source["id"] for source in capacity_sources] == ["deribit-option-capacity"]
    assert EVIDENCE_CLASS_FRESHNESS_SECONDS["option_capacity"] == 900.0


def test_deribit_capacity_surface_covers_two_expiries_but_remains_bounded():
    rows = []
    for expiry in (EXPIRY, NEXT_EXPIRY, THIRD_EXPIRY):
        for option_type, suffix in (("call", "C"), ("put", "P")):
            for strike in (55_000.0, 60_000.0, 65_000.0):
                rows.append(
                    (
                        f"BTC-{expiry:%d%b%y}-{int(strike)}-{suffix}".upper(),
                        "BTC",
                        expiry,
                        strike,
                        option_type,
                        60_000.0,
                    )
                )

    selected = _select_surface(rows)

    assert {row[2] for row in selected} == {EXPIRY, NEXT_EXPIRY}
    assert THIRD_EXPIRY not in {row[2] for row in selected}
    # Two expiries x two option sides x two near-ATM strikes.
    assert len(selected) == 8
    for expiry in (EXPIRY, NEXT_EXPIRY):
        for option_type in ("call", "put"):
            side = [row for row in selected if row[2] == expiry and row[4] == option_type]
            assert {row[3] for row in side} == {55_000.0, 60_000.0}


def test_capacity_observation_uses_underlying_amount_units_without_multiplier():
    row = _capacity()

    assert row.amount_unit == "underlying_base_currency"
    assert row.contract_size_underlying is None
    assert row.bid_capacity_usd == row.bid_visible_amount_underlying * row.underlying_price_usd
    assert row.ask_capacity_usd == row.ask_visible_amount_underlying * row.underlying_price_usd


def test_volatility_trial_is_bounded_to_five_percent_visible_capacity(tmp_path):
    store = EvidenceStore(tmp_path / "option-capacity.sqlite3")
    ledger = OptionCapacityLedger(store)
    ledger.record(_capacity())

    service = GovernedMechanismExecutionService.__new__(GovernedMechanismExecutionService)
    service.option_capacity = ledger
    service.settings = SimpleNamespace(alpha_min_notional_usd=100.0)

    bounded = service._capacity_bounded_volatility_spec(
        _spec(),
        before=NOW + timedelta(minutes=1),
    )

    assert bounded is not None
    # Smallest visible side is $48k; only 5% may be used by the paper trial.
    assert bounded.capital_usd == 2_400.0
    assert bounded.settlement_payload["visible_option_capacity_usd"] == 48_000.0
    assert bounded.settlement_payload["option_capacity_fraction_used"] == 0.05
    assert bounded.settlement_payload["hidden_option_depth_assumed"] is False
    evidence = bounded.settlement_payload["option_capacity_evidence"][0]
    assert evidence["observation_id"] == "cap-1"
    assert evidence["contract_size_underlying"] is None


def test_volatility_trial_fails_closed_without_exact_contract_capacity(tmp_path):
    store = EvidenceStore(tmp_path / "option-capacity-missing.sqlite3")
    service = GovernedMechanismExecutionService.__new__(GovernedMechanismExecutionService)
    service.option_capacity = OptionCapacityLedger(store)
    service.settings = SimpleNamespace(alpha_min_notional_usd=100.0)

    assert service._capacity_bounded_volatility_spec(
        _spec(),
        before=NOW + timedelta(minutes=1),
    ) is None

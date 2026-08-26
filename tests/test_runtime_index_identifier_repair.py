from __future__ import annotations

from inefficiency_engine.runtime_index_health_observability import (
    _index_maintenance_result,
)
from inefficiency_engine.runtime_index_maintenance import (
    POSTGRES_IDENTIFIER_MAX_BYTES,
    _next_replacement_index_name,
    _postgres_canonical_index_name,
    _postgres_replacement_index_name,
    _replacement_version,
)


def test_postgres_canonical_name_matches_server_identifier_truncation():
    logical = "ix_runtime_allocation_forward_trials_strategy_settlement_supported_id"

    physical = _postgres_canonical_index_name(logical)

    assert len(logical) > POSTGRES_IDENTIFIER_MAX_BYTES
    assert len(physical) == POSTGRES_IDENTIFIER_MAX_BYTES
    assert physical == logical[:POSTGRES_IDENTIFIER_MAX_BYTES]


def test_long_runtime_index_replacement_preserves_version_suffix():
    canonical = _postgres_canonical_index_name(
        "ix_runtime_allocation_forward_trials_strategy_settlement_supported_id"
    )

    replacement = _postgres_replacement_index_name(canonical, 2)

    assert len(replacement) <= POSTGRES_IDENTIFIER_MAX_BYTES
    assert replacement.endswith("_v2")
    assert replacement != canonical
    assert _replacement_version(canonical, replacement) == 2


def test_long_runtime_index_replacement_versions_do_not_collapse_to_same_name():
    canonical = _postgres_canonical_index_name(
        "ix_runtime_alpha_forward_events_event_type_strategy_id_family"
    )
    v2 = _postgres_replacement_index_name(canonical, 2)
    v10 = _postgres_replacement_index_name(canonical, 10)

    assert len(v2) <= POSTGRES_IDENTIFIER_MAX_BYTES
    assert len(v10) <= POSTGRES_IDENTIFIER_MAX_BYTES
    assert v2.endswith("_v2")
    assert v10.endswith("_v10")
    assert v2 != v10
    assert _replacement_version(canonical, v2) == 2
    assert _replacement_version(canonical, v10) == 10


def test_next_replacement_uses_highest_hashed_long_name_version():
    canonical = _postgres_canonical_index_name(
        "ix_runtime_source_coverage_observations_source_id_lane_id_id"
    )
    states = {
        _postgres_replacement_index_name(canonical, 2): {
            "valid": False,
            "ready": False,
        },
        _postgres_replacement_index_name(canonical, 9): {
            "valid": False,
            "ready": False,
        },
    }

    replacement = _next_replacement_index_name(
        index_name=canonical,
        existing_states=states,
    )

    assert replacement == _postgres_replacement_index_name(canonical, 10)
    assert replacement.endswith("_v10")
    assert len(replacement) <= POSTGRES_IDENTIFIER_MAX_BYTES


def test_final_background_index_heartbeat_exposes_nested_failure_rows():
    detail = {
        "background_indexes_complete": False,
        "failures": [
            {
                "scope": "post_control_source_strategy",
                "result": {
                    "complete": False,
                    "dialect": "postgresql",
                    "failures": [
                        {
                            "index": "ix_runtime_example",
                            "table": "example",
                            "error_type": "ProgrammingError",
                            "message": "duplicate relation",
                        }
                    ],
                },
            }
        ],
    }

    result = _index_maintenance_result(detail)

    assert result["complete"] is False
    assert result["dialect"] == "postgresql"
    assert result["failures"] == [
        {
            "scope": "post_control_source_strategy",
            "index": "ix_runtime_example",
            "table": "example",
            "error_type": "ProgrammingError",
            "message": "duplicate relation",
        }
    ]

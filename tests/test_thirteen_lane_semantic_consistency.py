from __future__ import annotations

from inefficiency_engine.mechanism_execution import MECHANISM_IDS
from inefficiency_engine.operating_certification import (
    _ALPHA_MECHANISM_FAMILIES,
    _CORE_MECHANISM_STRATEGIES,
)
from inefficiency_engine.profit_coverage import canonical_profit_mechanisms
from inefficiency_engine.source_coverage_catalog import LANES


def test_source_research_and_qualification_taxonomies_cover_the_same_thirteen_lanes():
    source_ids = set(LANES)
    research_ids = {
        row.mechanism_id for row in canonical_profit_mechanisms()
    }
    qualification_ids = (
        set(MECHANISM_IDS)
        | set(_ALPHA_MECHANISM_FAMILIES)
        | set(_CORE_MECHANISM_STRATEGIES)
    )

    assert len(source_ids) == 13
    assert research_ids == source_ids
    assert qualification_ids == source_ids
    assert (
        set(MECHANISM_IDS)
        & set(_ALPHA_MECHANISM_FAMILIES)
    ) == set()
    assert (
        set(MECHANISM_IDS)
        & set(_CORE_MECHANISM_STRATEGIES)
    ) == set()
    assert (
        set(_ALPHA_MECHANISM_FAMILIES)
        & set(_CORE_MECHANISM_STRATEGIES)
    ) == set()

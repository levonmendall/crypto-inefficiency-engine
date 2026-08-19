from inefficiency_engine.completion import paper_v1_status


def test_paper_v1_completion_contract_is_fail_closed():
    status = paper_v1_status("1.0.0")

    assert status.paper_v1_complete is True
    assert status.paper_only is True
    assert status.live_execution_available is False
    assert status.live_money_authorized is False
    assert status.unified_allocator_available is True
    assert status.durable_evidence_available is True
    assert status.deterministic_replay_available is True
    assert status.multi_horizon_shadow_available is True
    assert status.promotable_family_count == 5
    assert status.research_only_family_count == 6

    promotable = {row.family for row in status.families if row.paper_allocation_available}
    assert promotable == {
        "funding_dispersion",
        "spot_perp_basis",
        "futures_basis",
        "cex_spot_dislocation",
        "cex_dex",
    }

    research_only = [row for row in status.families if row.stage == "research_only"]
    assert all(row.paper_allocation_available is False for row in research_only)
    assert all(row.live_execution_available is False for row in status.families)
    assert all(row.blockers for row in research_only)

from __future__ import annotations

import asyncio

import httpx

from inefficiency_engine.adapters.hyperliquid import HyperliquidAdapter


def test_hyperliquid_retries_transient_status_without_changing_request():
    calls: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(__import__("json").loads(request.content)))
        if len(calls) == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "0"},
                request=request,
                json={"error": "rate limited"},
            )
        return httpx.Response(200, request=request, json={"BTC": "100000"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await HyperliquidAdapter(client)._post_payload({"type": "allMids"})

    payload = asyncio.run(run())
    assert payload == {"BTC": "100000"}
    assert calls == [{"type": "allMids"}, {"type": "allMids"}]


def test_hyperliquid_permanent_client_error_remains_fail_closed():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(403, request=request, json={"error": "forbidden"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await HyperliquidAdapter(client)._post_payload({"type": "metaAndAssetCtxs"})

    try:
        asyncio.run(run())
    except httpx.HTTPStatusError:
        pass
    else:
        raise AssertionError("permanent 4xx response must remain fail-closed")
    assert attempts == 1


def test_aave_runtime_repair_adds_independent_rpc_without_changing_query_policy():
    from inefficiency_engine import permanent_source_worker_lane_repair as repair
    from inefficiency_engine import production_source_recovery_runtime as recovery_v1
    from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2

    previous_urls = recovery_v1.AAVE_RPC_FALLBACK_URLS
    previous_v1_budget = recovery_v1.AAVE_TRANSPORT_BUDGET_SECONDS
    previous_v2_budget = recovery_v2.AAVE_TRANSPORT_BUDGET_SECONDS
    try:
        repair.install_remaining_source_lane_repairs()
        assert recovery_v1.AAVE_RPC_FALLBACK_URLS == (
            "https://eth.llamarpc.com",
            "https://cloudflare-eth.com/v1/mainnet",
            "https://eth.drpc.org",
            "https://public.1rpc.io/eth",
        )
        assert recovery_v1.AAVE_TRANSPORT_BUDGET_SECONDS == 4.0
        assert recovery_v2.AAVE_TRANSPORT_BUDGET_SECONDS == 4.0
        assert recovery_v2.AAVE_V3_ETHEREUM_POOL == recovery_v1.AAVE_V3_ETHEREUM_POOL
        assert recovery_v2.AAVE_LIQUIDATION_TOPIC == recovery_v1.AAVE_LIQUIDATION_TOPIC
    finally:
        recovery_v1.AAVE_RPC_FALLBACK_URLS = previous_urls
        recovery_v1.AAVE_TRANSPORT_BUDGET_SECONDS = previous_v1_budget
        recovery_v2.AAVE_TRANSPORT_BUDGET_SECONDS = previous_v2_budget


def test_mobile_dashboard_repairs_truth_label_and_narrow_cards():
    from inefficiency_engine.read_api_mobile_truth_deploy import repaired_dashboard_html

    html = repaired_dashboard_html()
    assert "Allocation family gates" in html
    assert "No family-level failures" in html
    assert "Opportunity families" not in html
    assert "mobile-truth-repair" in html
    assert "overflow-x:hidden" in html
    assert ".cardmetrics,.strip{grid-template-columns:1fr}" in html
    assert ".badge{white-space:normal" in html


def test_all_cards_render_zero_sample_forward_statistics_as_unavailable():
    from inefficiency_engine.read_api_mobile_truth_deploy import repaired_dashboard_html

    html = repaired_dashboard_html()

    # One shared formatter covers every mechanism card and strategy-detail card.
    assert "function forwardStatPct(outcomes,value){return (+outcomes||0)>0?pct(value):'—'}" in html
    assert "forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return)" in html
    assert "forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return_ci_lower)" in html
    assert "forwardStatPct(c.forward_outcome_count,c.forward_hit_rate)" in html
    assert "forwardStatPct(s.forward_outcomes,s.mean_forward_net_return)" in html
    assert "forwardStatPct(s.forward_outcomes,s.mean_forward_net_return_ci_lower)" in html
    assert "forwardStatPct(s.forward_outcomes,s.forward_hit_rate_ci_lower)" in html

    # Undefined zero-sample statistics no longer use the generic percentage formatter.
    assert "metric('Forward mean',pct(c.mean_forward_net_return)" not in html
    assert "metric('CI lower',pct(c.mean_forward_net_return_ci_lower)" not in html
    assert "metric('Hit rate',pct(c.forward_hit_rate)" not in html

    # Genuine zero P&L remains a real numeric value and is not gated by sample count.
    assert "metric('Realized P&L',money(c.allocator_realized_profit_usd),'paper allocator')" in html


def test_production_entrypoint_wraps_mobile_truth_with_database_independent_liveness():
    from inefficiency_engine import render_combined_postbind_lane_repair as runtime
    from inefficiency_engine import read_api_liveness_deploy as liveness

    assert runtime.BOUNDED_HEARTBEAT_API_APP == (
        "inefficiency_engine.read_api_liveness_deploy:app"
    )
    assert liveness.app.inner is not None

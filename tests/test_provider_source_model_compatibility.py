from __future__ import annotations

from inefficiency_engine import provider_readiness_read


def test_completed_source_plane_is_not_erased_by_narrow_legacy_probe(monkeypatch):
    monkeypatch.setattr(
        provider_readiness_read,
        "provider_readiness_snapshot",
        lambda store, now=None: {
            "volatility": {
                "mechanism_id": "volatility",
                "admitted_provider_count": 0,
                "providers": [
                    {
                        "provider": "legacy-primary",
                        "healthy": False,
                        "admitted": False,
                        "fresh": True,
                        "error_type": "TimeoutError",
                    }
                ],
            }
        },
    )
    payload = {
        "mechanisms": [
            {
                "mechanism_id": "volatility",
                "state": "collecting",
                "stage": "profitability_certifiable",
                "provider_ready": True,
                "primary_reason": "broader source plane is sufficient",
                "next_action": "continue forward evidence",
            }
        ]
    }

    result = provider_readiness_read.reconcile_provider_readiness(None, payload)
    row = result["mechanisms"][0]

    assert row["provider_ready"] is True
    assert row["state"] == "collecting"
    assert row["stage"] == "profitability_certifiable"
    assert row["primary_reason"] == "broader source plane is sufficient"

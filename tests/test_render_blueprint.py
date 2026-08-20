from pathlib import Path

import yaml


def test_render_blueprint_defines_durable_shadow_topology():
    blueprint = yaml.safe_load(Path("render.yaml").read_text())
    services = {service["name"]: service for service in blueprint["services"]}
    databases = {database["name"]: database for database in blueprint["databases"]}

    worker = services["cie-shadow-worker"]
    assert worker["type"] == "worker"
    assert worker["startCommand"] == "cie worker"
    # The existing worker's instance type is intentionally dashboard-managed.
    # Omitting `plan` prevents Blueprint syncs from downgrading a manual RAM
    # upgrade back to Render Starter (512 MB).
    assert "plan" not in worker
    assert worker["autoDeployTrigger"] == "checksPass"

    worker_env = {item["key"]: item for item in worker["envVars"]}
    assert worker_env["CIE_SHADOW_MAX_OPPORTUNITIES"]["value"] == "16"
    assert worker_env["CIE_SHADOW_MAX_CANDIDATES"]["value"] == "80"
    assert worker_env["CIE_DEX_ROUTE_TIER_SHADOW_MAX_CONCURRENCY"]["value"] == "1"

    api = services["cie-shadow-api"]
    assert api["type"] == "web"
    assert api["healthCheckPath"] == "/health"

    expected_build = "python -m pip install --retries 20 --timeout 120 ."
    assert worker["buildCommand"] == expected_build
    assert api["buildCommand"] == expected_build

    database = databases["cie-evidence"]
    assert database["plan"] != "free"
    assert database["ipAllowList"] == []

    for name in ("cie-shadow-worker", "cie-shadow-api"):
        database_env = next(item for item in services[name]["envVars"] if item["key"] == "DATABASE_URL")
        assert database_env["fromDatabase"]["name"] == "cie-evidence"
        assert database_env["fromDatabase"]["property"] == "connectionString"

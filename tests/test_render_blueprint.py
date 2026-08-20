from pathlib import Path

import yaml


def test_render_blueprint_defines_single_paid_combined_runtime():
    blueprint = yaml.safe_load(Path("render.yaml").read_text())
    services = {service["name"]: service for service in blueprint["services"]}
    databases = {database["name"]: database for database in blueprint["databases"]}

    assert set(services) == {"cie-shadow-worker"}

    runtime = services["cie-shadow-worker"]
    assert runtime["type"] == "web"
    assert runtime["plan"] == "standard"
    assert runtime["startCommand"] == "python -m inefficiency_engine.render_combined"
    assert runtime["healthCheckPath"] == "/health"
    assert runtime["autoDeployTrigger"] == "checksPass"
    assert runtime["buildCommand"] == "python -m pip install --retries 5 --timeout 30 ."
    assert runtime["maxShutdownDelaySeconds"] == 90

    runtime_env = {item["key"]: item for item in runtime["envVars"]}
    assert runtime_env["CIE_SHADOW_MAX_OPPORTUNITIES"]["value"] == "16"
    assert runtime_env["CIE_SHADOW_MAX_CANDIDATES"]["value"] == "80"
    assert runtime_env["CIE_DEX_ROUTE_TIER_SHADOW_MAX_CONCURRENCY"]["value"] == "1"

    database_env = runtime_env["DATABASE_URL"]
    assert database_env["fromDatabase"]["name"] == "cie-evidence"
    assert database_env["fromDatabase"]["property"] == "connectionString"

    database = databases["cie-evidence"]
    assert database["plan"] != "free"
    assert database["ipAllowList"] == []


def test_free_api_service_is_removed_from_blueprint():
    source = Path("render.yaml").read_text()
    assert "cie-shadow-api" not in source
    assert "plan: free" not in source

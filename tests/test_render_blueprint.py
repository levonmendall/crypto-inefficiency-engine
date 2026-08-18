from pathlib import Path

import yaml


def test_render_blueprint_defines_durable_shadow_topology():
    blueprint = yaml.safe_load(Path("render.yaml").read_text())
    services = {service["name"]: service for service in blueprint["services"]}
    databases = {database["name"]: database for database in blueprint["databases"]}

    worker = services["cie-shadow-worker"]
    assert worker["type"] == "worker"
    assert worker["startCommand"] == "cie worker"
    assert worker["plan"] != "free"
    assert worker["autoDeployTrigger"] == "checksPass"

    api = services["cie-shadow-api"]
    assert api["type"] == "web"
    assert api["healthCheckPath"] == "/health"

    database = databases["cie-evidence"]
    assert database["plan"] != "free"
    assert database["ipAllowList"] == []

    for name in ("cie-shadow-worker", "cie-shadow-api"):
        database_env = next(item for item in services[name]["envVars"] if item["key"] == "DATABASE_URL")
        assert database_env["fromDatabase"]["name"] == "cie-evidence"
        assert database_env["fromDatabase"]["property"] == "connectionString"

"""Standalone app boundary checks; parsing public YAML never contacts Docker."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]

def model():
    return yaml.safe_load((ROOT / "docker-compose.pretrip-app.yml").read_text(encoding="utf-8"))

def test_only_two_services_without_database_or_mlflow_credentials():
    data = model()
    assert set(data["services"]) == {"fare-inference", "dashboard"}
    assert "volumes" not in data
    for service in data["services"].values():
        assert "env_file" not in service and "container_name" not in service
        assert not any(key.startswith(("POSTGRES", "MLFLOW", "PGPASSWORD")) for key in service.get("environment", {}))

def test_model_mount_is_only_readonly_bind_and_no_host_inference_port():
    inference = model()["services"]["fare-inference"]
    assert "ports" not in inference
    assert inference["volumes"] == [{"type": "bind", "source": "${PRETRIP_APP_MODEL_ROOT:?Entrypoint verifies repository bundle}", "target": "/model-v22", "read_only": True, "bind": {"create_host_path": False}}]
    assert "volumes" not in model()["services"]["dashboard"]
    assert inference["read_only"] is True

def test_dashboard_is_loopback_pretrip_v22_with_explicit_network_optin():
    dashboard = model()["services"]["dashboard"]
    assert dashboard["ports"] == ["127.0.0.1:${PRETRIP_APP_PORT:-18522}:8501"]
    env = dashboard["environment"]
    assert env["NYC_TAXI_DASHBOARD_MODE"] == "pretrip"
    assert env["FARE_MODEL_VERSION"] == "v2.2"
    assert env["FARE_ADDRESS_ALLOW_NETWORK"] == "${FARE_ADDRESS_ALLOW_NETWORK:-0}"
    assert env["MAPBOX_ACCESS_TOKEN"] == "${MAPBOX_ACCESS_TOKEN:-}"
    assert dashboard["read_only"] is True

def test_network_and_services_require_same_explicit_owner():
    data = model()
    network = data["networks"]["app"]
    assert not network.get("external", False) and "name" not in network
    for service in data["services"].values():
        assert service["labels"] == network["labels"]
        assert service["networks"] == ["app"]
        assert service["pull_policy"] == "never"
    assert network["labels"]["com.nyc-taxi.pretrip-app.schema"] == "standalone-v1"

def test_fixed_serving_resources_and_command_never_run_worker_or_fit():
    service = model()["services"]["fare-inference"]
    assert service["cpus"] == 2.0 and service["mem_limit"] == "3g" and service["memswap_limit"] == "3g"
    assert service["pids_limit"] == 512
    assert "size=536870912" in service["tmpfs"][0]
    assert service["command"][:3] == ["python", "-m", "nyc_taxi.ml.inference_server"]
    assert service["command"][3:5] == ["--model-version", "v2.2"]

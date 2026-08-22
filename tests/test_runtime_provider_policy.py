from inefficiency_engine.runtime_provider_policy import bybit_public_enabled, env_flag


def test_provider_policy_defaults_enabled(monkeypatch):
    monkeypatch.delenv("CIE_BYBIT_PUBLIC_ENABLED", raising=False)
    assert bybit_public_enabled() is True


def test_provider_policy_disables_bybit_explicitly(monkeypatch):
    monkeypatch.setenv("CIE_BYBIT_PUBLIC_ENABLED", "false")
    assert bybit_public_enabled() is False


def test_provider_policy_does_not_enable_unknown_values(monkeypatch):
    monkeypatch.setenv("CIE_TEST_PROVIDER_FLAG", "maybe")
    assert env_flag("CIE_TEST_PROVIDER_FLAG", default=False) is False

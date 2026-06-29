"""Tests for openai_client config resolution + request redaction (no live calls)."""

from correctness_v2 import openai_client


def test_resolve_model_prefers_env(monkeypatch):
    monkeypatch.setenv(openai_client.MODEL_ENV, "gpt-test-model")
    assert openai_client.resolve_model() == "gpt-test-model"


def test_resolve_model_default_is_recent(monkeypatch):
    monkeypatch.delenv(openai_client.MODEL_ENV, raising=False)
    # Default must be a concrete, non-empty model id.
    model = openai_client.resolve_model()
    assert isinstance(model, str) and model.strip()


def test_temperature_omitted_by_default(monkeypatch):
    monkeypatch.delenv(openai_client.TEMPERATURE_ENV, raising=False)
    body = openai_client.build_request(
        [{"role": "user", "content": "hi"}], model="m"
    )
    # Newer models reject non-default temperature, so we omit it unless set.
    assert "temperature" not in body
    assert body["response_format"] == {"type": "json_object"}


def test_temperature_included_when_set(monkeypatch):
    monkeypatch.setenv(openai_client.TEMPERATURE_ENV, "0")
    body = openai_client.build_request(
        [{"role": "user", "content": "hi"}], model="m"
    )
    assert body["temperature"] == 0.0


def test_redacted_request_has_no_secret(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-should-not-appear")
    red = openai_client.redacted_request(
        [{"role": "user", "content": "hi"}], model="m"
    )
    assert red["secrets_included"] is False
    assert red["api_key"] == "<omitted>"
    assert "sk-should-not-appear" not in str(red)


def test_call_openai_json_missing_key_fails_closed(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    # Force key discovery to return nothing.
    import perizia_canonical_pipeline.llm_resolution_pack as pack

    monkeypatch.setattr(
        pack, "discover_openai_config", lambda: {"api_key": None, "model": "m"}
    )
    try:
        openai_client.call_openai_json([{"role": "user", "content": "hi"}])
        assert False, "expected OpenAIClientError"
    except openai_client.OpenAIClientError as exc:
        assert exc.reason_code == "OPENAI_API_KEY_MISSING"

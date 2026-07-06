"""Tests for feature flags and access guard (endpoint/job start gating)."""

import pytest

from correctness_v2 import feature_flags


@pytest.fixture(autouse=True)
def clear_flags(monkeypatch):
    for name in [
        feature_flags.FLAG_ENABLED,
        feature_flags.FLAG_ADMIN_ONLY,
        feature_flags.FLAG_AUTO_START,
        feature_flags.FLAG_SHADOW_MODE,
        feature_flags.FLAG_NO_OLD_FALLBACK,
        feature_flags.FLAG_JOB_MODE,
        feature_flags.FLAG_MAX_RUNTIME_SECONDS,
    ]:
        monkeypatch.delenv(name, raising=False)
    yield


def test_safe_defaults():
    assert feature_flags.is_enabled() is False
    assert feature_flags.is_admin_only() is True
    assert feature_flags.is_shadow_mode() is True
    assert feature_flags.no_old_fallback() is True
    assert feature_flags.job_mode() == "async"
    assert feature_flags.max_runtime_seconds() == 0


def test_auto_start_default_off():
    assert feature_flags.auto_start_enabled() is False


def test_auto_start_requires_feature_enabled(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_AUTO_START, "true")
    # Feature itself disabled -> auto-start never fires.
    assert feature_flags.auto_start_enabled() is False
    monkeypatch.setenv(feature_flags.FLAG_ENABLED, "true")
    assert feature_flags.auto_start_enabled() is True


def test_auto_start_not_gated_by_admin_only(monkeypatch):
    # ADMIN_ONLY restricts the manual endpoints, not the product auto-start.
    monkeypatch.setenv(feature_flags.FLAG_ENABLED, "true")
    monkeypatch.setenv(feature_flags.FLAG_ADMIN_ONLY, "true")
    monkeypatch.setenv(feature_flags.FLAG_AUTO_START, "true")
    assert feature_flags.auto_start_enabled() is True


def test_disabled_blocks_access_even_for_admin():
    # Feature disabled by default -> blocked regardless of admin.
    assert feature_flags.access_block_reason(is_admin=True) == "CORRECTNESS_V2_DISABLED"
    assert feature_flags.access_block_reason(is_admin=False) == "CORRECTNESS_V2_DISABLED"


def test_enabled_admin_only_blocks_non_admin(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_ENABLED, "true")
    # admin_only defaults to True
    assert feature_flags.access_block_reason(is_admin=False) == "ADMIN_ONLY_FEATURE"
    assert feature_flags.access_block_reason(is_admin=True) is None


def test_enabled_not_admin_only_allows_anyone(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_ENABLED, "true")
    monkeypatch.setenv(feature_flags.FLAG_ADMIN_ONLY, "false")
    assert feature_flags.access_block_reason(is_admin=False) is None


def test_garbage_flag_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_ENABLED, "banana")
    # Unknown token -> safe default (disabled).
    assert feature_flags.is_enabled() is False


def test_max_runtime_parsing(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_MAX_RUNTIME_SECONDS, "120")
    assert feature_flags.max_runtime_seconds() == 120
    monkeypatch.setenv(feature_flags.FLAG_MAX_RUNTIME_SECONDS, "not-a-number")
    assert feature_flags.max_runtime_seconds() == 0


def test_snapshot_shape():
    snap = feature_flags.snapshot()
    assert snap[feature_flags.FLAG_ENABLED] is False
    assert snap[feature_flags.FLAG_JOB_MODE] == "async"

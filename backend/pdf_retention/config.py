"""Fail-closed configuration for encrypted PDF retention."""

from __future__ import annotations

import base64
import binascii
import os
from pathlib import Path
from typing import Dict, Tuple

FLAG_ENABLED = "CORRECTNESS_V2_PDF_RETENTION_ENABLED"
DAYS_ENV = "CORRECTNESS_V2_PDF_RETENTION_DAYS"
ROOT_ENV = "CORRECTNESS_V2_PDF_RETENTION_ROOT"
ACTIVE_KEY_VERSION_ENV = "CORRECTNESS_V2_PDF_RETENTION_ACTIVE_KEY_VERSION"
KEY_PREFIX = "CORRECTNESS_V2_PDF_RETENTION_KEY_"

DEFAULT_DAYS = 30
MIN_DAYS = 1
MAX_DAYS = 365
DEFAULT_ROOT = Path("/srv/perizia/private/pdf_retention")
CONSENT_VERSION = "DIAGNOSTIC_RETENTION_V1"
ENCRYPTION_ALGORITHM = "AES-256-GCM"

_TRUE = frozenset({"1", "true", "yes", "on", "y", "t"})
_FALSE = frozenset({"0", "false", "no", "off", "n", "f", ""})


class RetentionConfigurationError(RuntimeError):
    """Configuration is unsafe or incomplete; callers must fail closed."""


def is_enabled() -> bool:
    raw = os.environ.get(FLAG_ENABLED)
    if raw is None:
        return False
    token = str(raw).strip().lower()
    if token in _TRUE:
        return True
    if token in _FALSE:
        return False
    return False


def retention_days() -> int:
    raw = os.environ.get(DAYS_ENV)
    try:
        value = DEFAULT_DAYS if raw is None else int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_DAYS
    return value if MIN_DAYS <= value <= MAX_DAYS else DEFAULT_DAYS


def retention_root() -> Path:
    raw = str(os.environ.get(ROOT_ENV) or "").strip()
    root = Path(raw) if raw else DEFAULT_ROOT
    if not root.is_absolute():
        raise RetentionConfigurationError("PDF_RETENTION_ROOT_NOT_ABSOLUTE")
    normalized = Path(os.path.abspath(str(root)))
    forbidden = (
        Path("/srv/perizia/app"),
        Path("/srv/perizia/_qa"),
        Path("/var/www"),
        Path("/usr/share/nginx"),
    )
    for candidate in forbidden:
        if normalized == candidate or candidate in normalized.parents:
            raise RetentionConfigurationError("PDF_RETENTION_ROOT_FORBIDDEN")
    return normalized


def diagnostic_temp_root() -> Path:
    raw = str(os.environ.get("CORRECTNESS_V2_PDF_RETENTION_DIAGNOSTIC_TMP_ROOT") or "").strip()
    root = Path(raw) if raw else Path("/run/periziascan/pdf_retention")
    if not root.is_absolute():
        raise RetentionConfigurationError("PDF_RETENTION_DIAGNOSTIC_ROOT_NOT_ABSOLUTE")
    normalized = Path(os.path.abspath(str(root)))
    if Path("/srv/perizia/app") in (normalized, *normalized.parents):
        raise RetentionConfigurationError("PDF_RETENTION_DIAGNOSTIC_ROOT_FORBIDDEN")
    return normalized


def _decode_key(raw: str, version: str) -> bytes:
    try:
        key = base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RetentionConfigurationError(f"PDF_RETENTION_KEY_INVALID:{version}") from exc
    if len(key) != 32:
        raise RetentionConfigurationError(f"PDF_RETENTION_KEY_LENGTH_INVALID:{version}")
    return key


def keyring() -> Tuple[str, Dict[str, bytes]]:
    active = str(os.environ.get(ACTIVE_KEY_VERSION_ENV) or "").strip().lower()
    if not active or not active.startswith("v") or not active[1:].isdigit():
        raise RetentionConfigurationError("PDF_RETENTION_ACTIVE_KEY_VERSION_INVALID")
    keys: Dict[str, bytes] = {}
    for name, raw in os.environ.items():
        if not name.startswith(KEY_PREFIX) or name == ACTIVE_KEY_VERSION_ENV:
            continue
        version = name[len(KEY_PREFIX) :].strip().lower()
        if version.startswith("v") and version[1:].isdigit() and str(raw).strip():
            keys[version] = _decode_key(str(raw).strip(), version)
    if active not in keys:
        raise RetentionConfigurationError("PDF_RETENTION_ACTIVE_KEY_MISSING")
    return active, keys


def key_for_version(version: str) -> bytes:
    _active, keys = keyring()
    try:
        return keys[str(version).strip().lower()]
    except KeyError as exc:
        raise RetentionConfigurationError("PDF_RETENTION_KEY_VERSION_UNAVAILABLE") from exc

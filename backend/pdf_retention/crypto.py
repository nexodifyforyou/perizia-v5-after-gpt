"""AES-256-GCM envelope encryption for retained PDF bytes."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from . import config

NONCE_BYTES = 12
TAG_BYTES = 16


class RetentionCryptoError(RuntimeError):
    """Authentication, key lookup, or metadata validation failed."""


@dataclass(frozen=True)
class EncryptedPdf:
    ciphertext: bytes
    metadata: Dict[str, Any]


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb64(value: Any, field: str, expected_len: int | None = None) -> bytes:
    try:
        decoded = base64.b64decode(str(value), validate=True)
    except Exception as exc:
        raise RetentionCryptoError(f"INVALID_ENCRYPTION_METADATA:{field}") from exc
    if expected_len is not None and len(decoded) != expected_len:
        raise RetentionCryptoError(f"INVALID_ENCRYPTION_METADATA:{field}")
    return decoded


def associated_data(
    *, retention_id: str, analysis_id: str, user_id: str, input_sha256: str, consent_version: str
) -> bytes:
    payload = {
        "analysis_id": str(analysis_id),
        "consent_version": str(consent_version),
        "input_sha256": str(input_sha256),
        "retention_id": str(retention_id),
        "user_id": str(user_id),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def encrypt_pdf(
    plaintext: bytes,
    *,
    retention_id: str,
    analysis_id: str,
    user_id: str,
    input_sha256: str,
    consent_version: str,
) -> EncryptedPdf:
    if not isinstance(plaintext, bytes):
        raise TypeError("PDF_RETENTION_PLAINTEXT_MUST_BE_BYTES")
    active_version, keys = config.keyring()
    kek = keys[active_version]
    aad = associated_data(
        retention_id=retention_id,
        analysis_id=analysis_id,
        user_id=user_id,
        input_sha256=input_sha256,
        consent_version=consent_version,
    )
    dek = os.urandom(32)
    content_nonce = os.urandom(NONCE_BYTES)
    wrapped_nonce = os.urandom(NONCE_BYTES)
    sealed = AESGCM(dek).encrypt(content_nonce, plaintext, aad)
    wrapped = AESGCM(kek).encrypt(wrapped_nonce, dek, retention_id.encode("utf-8"))
    ciphertext, auth_tag = sealed[:-TAG_BYTES], sealed[-TAG_BYTES:]
    wrapped_dek, wrapped_tag = wrapped[:-TAG_BYTES], wrapped[-TAG_BYTES:]
    return EncryptedPdf(
        ciphertext=ciphertext,
        metadata={
            "encryption_algorithm": config.ENCRYPTION_ALGORITHM,
            "encryption_format_version": "v1",
            "key_version": active_version,
            "nonce_b64": _b64(content_nonce),
            "auth_tag_b64": _b64(auth_tag),
            "wrapped_dek_b64": _b64(wrapped_dek),
            "wrapped_dek_nonce_b64": _b64(wrapped_nonce),
            "wrapped_dek_tag_b64": _b64(wrapped_tag),
            "ciphertext_size": len(ciphertext),
            "ciphertext_sha256": hashlib.sha256(ciphertext).hexdigest(),
        },
    )


def decrypt_pdf(ciphertext: bytes, record: Mapping[str, Any]) -> bytes:
    if record.get("encryption_algorithm") != config.ENCRYPTION_ALGORITHM:
        raise RetentionCryptoError("UNSUPPORTED_ENCRYPTION_ALGORITHM")
    if record.get("encryption_format_version") != "v1":
        raise RetentionCryptoError("UNSUPPORTED_ENCRYPTION_FORMAT")
    expected_digest = str(record.get("ciphertext_sha256") or "")
    if not expected_digest or hashlib.sha256(ciphertext).hexdigest() != expected_digest:
        raise RetentionCryptoError("CIPHERTEXT_INTEGRITY_FAILED")
    if int(record.get("ciphertext_size") or -1) != len(ciphertext):
        raise RetentionCryptoError("CIPHERTEXT_SIZE_MISMATCH")
    nonce = _unb64(record.get("nonce_b64"), "nonce", NONCE_BYTES)
    tag = _unb64(record.get("auth_tag_b64"), "auth_tag", TAG_BYTES)
    wrapped_nonce = _unb64(record.get("wrapped_dek_nonce_b64"), "wrapped_nonce", NONCE_BYTES)
    wrapped_tag = _unb64(record.get("wrapped_dek_tag_b64"), "wrapped_tag", TAG_BYTES)
    wrapped_dek = _unb64(record.get("wrapped_dek_b64"), "wrapped_dek")
    try:
        kek = config.key_for_version(str(record.get("key_version") or ""))
        dek = AESGCM(kek).decrypt(
            wrapped_nonce,
            wrapped_dek + wrapped_tag,
            str(record.get("retention_id") or "").encode("utf-8"),
        )
        aad = associated_data(
            retention_id=str(record.get("retention_id") or ""),
            analysis_id=str(record.get("analysis_id") or ""),
            user_id=str(record.get("user_id") or ""),
            input_sha256=str(record.get("input_sha256") or ""),
            consent_version=str(record.get("consent_version") or ""),
        )
        plaintext = AESGCM(dek).decrypt(nonce, ciphertext + tag, aad)
    except (InvalidTag, ValueError, config.RetentionConfigurationError) as exc:
        raise RetentionCryptoError("PDF_RETENTION_AUTHENTICATION_FAILED") from exc
    if hashlib.sha256(plaintext).hexdigest() != record.get("input_sha256"):
        raise RetentionCryptoError("PDF_RETENTION_LINEAGE_HASH_MISMATCH")
    return plaintext

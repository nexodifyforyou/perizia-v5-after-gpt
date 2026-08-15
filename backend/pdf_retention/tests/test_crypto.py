from __future__ import annotations

import base64
import hashlib
import logging
import os

import pytest

from pdf_retention import crypto
from pdf_retention.tests.fixtures import isolated_retention  # noqa: F401


def _encrypt(payload: bytes, suffix: str = "a"):
    digest = hashlib.sha256(payload).hexdigest()
    encrypted = crypto.encrypt_pdf(
        payload,
        retention_id=f"pdfr_{suffix * 32}",
        analysis_id=f"analysis_{suffix}",
        user_id=f"ret_test_{suffix}",
        input_sha256=digest,
        consent_version="DIAGNOSTIC_RETENTION_V1",
    )
    record = {
        "retention_id": f"pdfr_{suffix * 32}",
        "analysis_id": f"analysis_{suffix}",
        "user_id": f"ret_test_{suffix}",
        "input_sha256": digest,
        "consent_version": "DIAGNOSTIC_RETENTION_V1",
        **encrypted.metadata,
    }
    return encrypted, record


def test_ciphertext_is_not_plaintext_and_marker_is_absent():
    payload = b"%PDF-1.7\nUNIQUE_PLAINTEXT_MARKER_9f3d\n%%EOF"
    encrypted, _record = _encrypt(payload)
    assert encrypted.ciphertext != payload
    assert b"UNIQUE_PLAINTEXT_MARKER_9f3d" not in encrypted.ciphertext


def test_correct_key_decrypts_byte_identical_and_hash_matches():
    payload = b"%PDF synthetic exact original\x00\xff"
    encrypted, record = _encrypt(payload)
    decrypted = crypto.decrypt_pdf(encrypted.ciphertext, record)
    assert decrypted == payload
    assert hashlib.sha256(decrypted).hexdigest() == record["input_sha256"]


def test_wrong_key_fails_closed(monkeypatch):
    encrypted, record = _encrypt(b"wrong key test")
    monkeypatch.setenv(
        "CORRECTNESS_V2_PDF_RETENTION_KEY_V1",
        base64.b64encode(os.urandom(32)).decode("ascii"),
    )
    with pytest.raises(crypto.RetentionCryptoError, match="AUTHENTICATION_FAILED"):
        crypto.decrypt_pdf(encrypted.ciphertext, record)


def test_modified_ciphertext_fails_closed():
    encrypted, record = _encrypt(b"ciphertext tamper")
    damaged = bytearray(encrypted.ciphertext)
    damaged[0] ^= 1
    with pytest.raises(crypto.RetentionCryptoError, match="INTEGRITY_FAILED"):
        crypto.decrypt_pdf(bytes(damaged), record)


def test_modified_tag_fails_closed():
    encrypted, record = _encrypt(b"tag tamper")
    tag = bytearray(base64.b64decode(record["auth_tag_b64"]))
    tag[-1] ^= 1
    record["auth_tag_b64"] = base64.b64encode(tag).decode("ascii")
    with pytest.raises(crypto.RetentionCryptoError, match="AUTHENTICATION_FAILED"):
        crypto.decrypt_pdf(encrypted.ciphertext, record)


def test_nonce_unique_across_many_encryptions():
    nonces = set()
    for index in range(256):
        suffix = f"{index:032x}"
        payload = f"same-pdf-{index % 2}".encode()
        digest = hashlib.sha256(payload).hexdigest()
        encrypted = crypto.encrypt_pdf(
            payload,
            retention_id=f"pdfr_{suffix}", analysis_id=f"a{index}",
            user_id="ret_test_nonce", input_sha256=digest,
            consent_version="DIAGNOSTIC_RETENTION_V1",
        )
        nonce = encrypted.metadata["nonce_b64"]
        assert nonce not in nonces
        nonces.add(nonce)


def test_same_pdf_twice_has_different_ciphertext():
    payload = b"same exact PDF bytes"
    first, _ = _encrypt(payload, "a")
    second, _ = _encrypt(payload, "b")
    assert first.ciphertext != second.ciphertext
    assert first.metadata["nonce_b64"] != second.metadata["nonce_b64"]


def test_key_version_rotation_keeps_old_record_decryptable(monkeypatch):
    payload = b"rotation"
    encrypted, record = _encrypt(payload)
    monkeypatch.setenv(
        "CORRECTNESS_V2_PDF_RETENTION_KEY_V2",
        base64.b64encode(os.urandom(32)).decode("ascii"),
    )
    monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_ACTIVE_KEY_VERSION", "v2")
    assert crypto.decrypt_pdf(encrypted.ciphertext, record) == payload
    new_encrypted, _ = _encrypt(payload, "b")
    assert new_encrypted.metadata["key_version"] == "v2"


def test_key_nonce_plaintext_never_logged(caplog):
    payload = b"DO_NOT_LOG_PLAINTEXT_f47a"
    with caplog.at_level(logging.DEBUG):
        encrypted, record = _encrypt(payload)
        assert crypto.decrypt_pdf(encrypted.ciphertext, record) == payload
    rendered = caplog.text
    assert payload.decode() not in rendered
    assert record["nonce_b64"] not in rendered
    assert os.environ["CORRECTNESS_V2_PDF_RETENTION_KEY_V1"] not in rendered

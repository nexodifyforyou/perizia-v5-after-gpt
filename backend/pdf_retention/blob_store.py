"""Private ciphertext filesystem with strict path and link defenses."""

from __future__ import annotations

import errno
import os
import re
import stat
import uuid
from pathlib import Path
from typing import Iterator, Tuple

from . import config

STORAGE_ID_RE = re.compile(r"\Aobj_[0-9a-f]{32}\Z")
TEMP_NAME_RE = re.compile(r"\A(obj_[0-9a-f]{32})\.[0-9a-f]{16}\.tmp\Z")


class UnsafeStoragePath(RuntimeError):
    """A malformed identifier, link, or unexpected filesystem object."""


def new_storage_id() -> str:
    return f"obj_{uuid.uuid4().hex}"


def validate_storage_id(storage_id: str) -> str:
    value = str(storage_id or "")
    if not STORAGE_ID_RE.fullmatch(value):
        raise UnsafeStoragePath("MALFORMED_STORAGE_ID")
    return value


def _mkdir_private(path: Path) -> None:
    root = config.retention_root()
    if path == root and root == config.DEFAULT_ROOT:
        if not root.exists():
            raise config.RetentionConfigurationError("PDF_RETENTION_ROOT_NOT_PROVISIONED")
        st = root.lstat()
        if st.st_uid != 0 or stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            raise config.RetentionConfigurationError("PDF_RETENTION_ROOT_OWNERSHIP_INVALID")
        # The live backend is unprivileged. Deployment provisions root-owned
        # 0730: the service group may create/traverse opaque names but cannot
        # list/read the directory; world has no access. Subdirectories and
        # ciphertext files remain service-owned 0700/0600.
        if st.st_mode & 0o047:
            raise config.RetentionConfigurationError("PDF_RETENTION_ROOT_PERMISSIONS_INVALID")
        return
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path == root:
        root_stat = path.lstat()
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            raise UnsafeStoragePath("UNSAFE_STORAGE_ROOT")
        if root_stat.st_mode & 0o077:
            os.chmod(path, 0o700)
        return
    current = path
    while root in current.parents:
        st = current.lstat()
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            raise UnsafeStoragePath("UNSAFE_STORAGE_DIRECTORY")
        if st.st_mode & 0o077:
            os.chmod(current, 0o700)
        current = current.parent


def ensure_layout() -> Path:
    root = config.retention_root()
    _mkdir_private(root)
    _mkdir_private(root / "objects")
    _mkdir_private(root / "tmp")
    return root


def object_path(storage_id: str) -> Path:
    value = validate_storage_id(storage_id)
    root = config.retention_root()
    return root / "objects" / value[4:6] / value[6:8] / f"{value}.bin"


def _assert_private_regular(path: Path) -> os.stat_result:
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode):
        raise UnsafeStoragePath("UNSAFE_STORAGE_OBJECT_TYPE")
    if st.st_nlink != 1:
        raise UnsafeStoragePath("UNSAFE_STORAGE_HARDLINK")
    if st.st_mode & 0o077:
        raise UnsafeStoragePath("UNSAFE_STORAGE_PERMISSIONS")
    return st


def write_encrypted_temp(storage_id: str, ciphertext: bytes) -> Path:
    value = validate_storage_id(storage_id)
    root = ensure_layout()
    temp_path = root / "tmp" / f"{value}.{uuid.uuid4().hex[:16]}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temp_path, flags, 0o600)
    try:
        view = memoryview(ciphertext)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError(errno.EIO, "ciphertext write made no progress")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    _assert_private_regular(temp_path)
    return temp_path


def promote_temp(storage_id: str, temp_path: Path) -> Path:
    value = validate_storage_id(storage_id)
    root = ensure_layout()
    expected_parent = root / "tmp"
    if temp_path.parent != expected_parent or not TEMP_NAME_RE.fullmatch(temp_path.name):
        raise UnsafeStoragePath("UNSAFE_TEMP_PATH")
    if not temp_path.name.startswith(value + "."):
        raise UnsafeStoragePath("TEMP_STORAGE_ID_MISMATCH")
    _assert_private_regular(temp_path)
    final_path = object_path(value)
    _mkdir_private(final_path.parent)
    os.replace(temp_path, final_path)
    _assert_private_regular(final_path)
    dir_fd = os.open(final_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return final_path


def discard_temp(temp_path: Path) -> None:
    root = config.retention_root()
    if temp_path.parent != root / "tmp" or not TEMP_NAME_RE.fullmatch(temp_path.name):
        raise UnsafeStoragePath("UNSAFE_TEMP_PATH")
    try:
        _assert_private_regular(temp_path)
        temp_path.unlink()
    except FileNotFoundError:
        return


def read_ciphertext(storage_id: str) -> bytes:
    path = object_path(storage_id)
    _assert_private_regular(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
            raise UnsafeStoragePath("UNSAFE_STORAGE_OBJECT")
        chunks = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def delete_ciphertext(storage_id: str) -> bool:
    path = object_path(storage_id)
    try:
        _assert_private_regular(path)
    except FileNotFoundError:
        return False
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        # Another idempotent cleanup worker won the race after our lstat.
        return False


def ciphertext_exists(storage_id: str) -> bool:
    path = object_path(storage_id)
    try:
        _assert_private_regular(path)
        return True
    except FileNotFoundError:
        return False


def iter_objects() -> Iterator[Tuple[str | None, Path]]:
    root = config.retention_root() / "objects"
    if not root.exists():
        return
    for first in root.iterdir():
        if first.is_symlink() or not first.is_dir():
            continue
        for second in first.iterdir():
            if second.is_symlink() or not second.is_dir():
                continue
            for path in second.iterdir():
                match = re.fullmatch(r"(obj_[0-9a-f]{32})\.bin", path.name)
                yield (match.group(1) if match else None, path)


def iter_temps() -> Iterator[Tuple[str | None, Path]]:
    root = config.retention_root() / "tmp"
    if not root.exists():
        return
    for path in root.iterdir():
        match = TEMP_NAME_RE.fullmatch(path.name)
        yield (match.group(1) if match else None, path)

"""Delete named legacy ledger files once, and only inside the Railway volume.

LEGACY_DB_DELETE is a comma-separated list of absolute paths. Anything that
is not a regular file under the volume, or that is the live database (or its
SQLite -wal/-shm), is left untouched and logged.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass


@dataclass(frozen=True)
class CleanupResult:
    path: str
    status: str  # deleted | missing | skipped
    reason: str = ""


def volume_root_from_env() -> str:
    raw = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip() or "/data"
    return os.path.abspath(raw)


def protected_database_paths(db_path: str) -> set[str]:
    """The live ledger and the sidecars SQLite keeps open next to it."""
    base = os.path.abspath(db_path)
    return {base, base + "-wal", base + "-shm"}


def _inside(path: str, root: str) -> bool:
    path = os.path.abspath(path)
    root = os.path.abspath(root)
    try:
        common = os.path.commonpath([path, root])
    except ValueError:
        return False
    return common == root and path != root


def delete_legacy_files(
    raw_paths: list[str],
    *,
    volume_root: str,
    db_path: str,
) -> list[CleanupResult]:
    root = os.path.abspath(volume_root)
    protected = protected_database_paths(db_path)
    results: list[CleanupResult] = []
    for raw in raw_paths:
        path = (raw or "").strip()
        if not path:
            continue
        if not os.path.isabs(path):
            results.append(CleanupResult(path, "skipped", "путь не абсолютный"))
            continue
        normalized = os.path.abspath(path)
        if not _inside(normalized, root):
            results.append(CleanupResult(normalized, "skipped", "вне тома"))
            continue
        if normalized in protected:
            results.append(CleanupResult(normalized, "skipped", "это текущая база или её -wal/-shm"))
            continue
        if not os.path.lexists(normalized):
            results.append(CleanupResult(normalized, "missing", "файла нет"))
            continue
        info = os.lstat(normalized)
        if stat.S_ISDIR(info.st_mode):
            results.append(CleanupResult(normalized, "skipped", "это каталог"))
            continue
        if stat.S_ISLNK(info.st_mode):
            target = os.path.realpath(normalized)
            if target in protected or not _inside(target, root):
                results.append(CleanupResult(normalized, "skipped", "ссылка ведёт на текущую базу или наружу"))
                continue
        elif not stat.S_ISREG(info.st_mode):
            results.append(CleanupResult(normalized, "skipped", "не обычный файл"))
            continue
        else:
            real = os.path.realpath(normalized)
            if real in protected or not _inside(real, root):
                results.append(CleanupResult(normalized, "skipped", "файл разрешается в текущую базу или наружу"))
                continue
        os.unlink(normalized)
        results.append(CleanupResult(normalized, "deleted", ""))
    return results

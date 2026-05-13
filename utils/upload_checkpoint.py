"""
Persistente Upload-Checkpoints im Quellordner (_aero_upload_checkpoint.json).
Ermöglicht Fortsetzung von Teiluploads nach Absturz/Neustart (Dropbox + Custom API).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from typing import Any, Optional

CHECKPOINT_FILENAME = "_aero_upload_checkpoint.json"
CHECKPOINT_VERSION = 1

log = logging.getLogger(__name__)


def checkpoint_path(local_dir: str) -> str:
    return os.path.join(local_dir, CHECKPOINT_FILENAME)


def manifest_fingerprint(files_manifest: list[dict]) -> str:
    """Stabiler Fingerabdruck aus sortierten name+size (und optional type)."""
    lines = []
    for item in sorted(files_manifest, key=lambda x: x.get("name") or ""):
        lines.append(f"{item.get('name')}|{item.get('size')}|{item.get('type', '')}")
    raw = "\n".join(lines).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def load_checkpoint(local_dir: str) -> Optional[dict[str, Any]]:
    path = checkpoint_path(local_dir)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or data.get("version") != CHECKPOINT_VERSION:
            return None
        return data
    except Exception as e:
        log.warning("Checkpoint lesen fehlgeschlagen (%s): %s", path, e)
        return None


def save_checkpoint(local_dir: str, data: dict[str, Any]) -> None:
    path = checkpoint_path(local_dir)
    data = dict(data)
    data["version"] = CHECKPOINT_VERSION
    os.makedirs(local_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".aero_ck_", suffix=".json", dir=local_dir
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def clear_checkpoint(local_dir: str) -> None:
    path = checkpoint_path(local_dir)
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError as e:
        log.debug("Checkpoint entfernen: %s", e)


def should_skip_upload_file(filename: str) -> bool:
    """True für Marker, Systemdateien und Checkpoint."""
    if filename == CHECKPOINT_FILENAME:
        return True
    if filename in (
        "_fertig.txt",
        "_in_verarbeitung.txt",
        ".DS_Store",
        ".apdisk",
        "Thumbs.db",
        "desktop.ini",
    ):
        return True
    if filename.startswith("._"):
        return True
    return False

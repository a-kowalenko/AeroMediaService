"""Wartet auf unveränderten Ordnerinhalt, bevor ein Upload geclaimt wird."""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Literal

from core.upload_markers import MARKER_FERTIG

IGNORED_FILENAMES = frozenset({
    MARKER_FERTIG,
    "_in_verarbeitung.txt",
    "_aero_upload_checkpoint.json",
})

ObserveResult = Literal["waiting", "stable", "removed"]


@dataclass
class _PendingState:
    fingerprint: tuple[int, int]
    stable_since: float
    logged_waiting: bool = False


def folder_content_fingerprint(dir_path: str) -> tuple[int, int]:
    """Summe der Dateigrößen und Anzahl, ohne Marker/Checkpoint."""
    total_bytes = 0
    file_count = 0
    for root, _dirs, files in os.walk(dir_path):
        for name in files:
            if name in IGNORED_FILENAMES:
                continue
            file_path = os.path.join(root, name)
            try:
                total_bytes += os.path.getsize(file_path)
                file_count += 1
            except OSError:
                continue
    return total_bytes, file_count


class FolderStabilityTracker:
    """Merkt sich Ordner mit _fertig.txt bis der Inhalt stabil bleibt."""

    def __init__(
        self,
        required_stable_seconds: float,
        logger: logging.Logger | None = None,
    ) -> None:
        self._required = max(0.0, float(required_stable_seconds))
        self._log = logger or logging.getLogger(__name__)
        self._pending: dict[str, _PendingState] = {}

    def _key(self, dir_path: str) -> str:
        return os.path.normcase(os.path.abspath(dir_path))

    def set_required_seconds(self, seconds: float) -> None:
        self._required = max(0.0, float(seconds))

    def observe(self, dir_path: str) -> ObserveResult:
        key = self._key(dir_path)
        fertig_path = os.path.join(dir_path, MARKER_FERTIG)
        if not os.path.isfile(fertig_path):
            self._pending.pop(key, None)
            return "removed"

        if self._required <= 0:
            return "stable"

        fingerprint = folder_content_fingerprint(dir_path)
        now = time.monotonic()
        state = self._pending.get(key)

        if state is None or state.fingerprint != fingerprint:
            self._pending[key] = _PendingState(
                fingerprint=fingerprint,
                stable_since=now,
            )
            state = self._pending[key]

        if not state.logged_waiting:
            self._log.info(
                "Ordner '%s': Warte auf Datei-Stabilität (%.0f s unverändert)...",
                os.path.basename(dir_path),
                self._required,
            )
            state.logged_waiting = True

        elapsed = now - state.stable_since
        if elapsed >= self._required:
            self._pending.pop(key, None)
            self._log.info(
                "Ordner '%s': Inhalt stabil — Upload wird vorbereitet.",
                os.path.basename(dir_path),
            )
            return "stable"

        return "waiting"

    def discard(self, dir_path: str) -> None:
        self._pending.pop(self._key(dir_path), None)

    def clear(self) -> None:
        self._pending.clear()

"""Marker-Dateien im Überwachungsordner (_fertig / _in_verarbeitung)."""
from __future__ import annotations

import logging
import os
from typing import Optional

MARKER_FERTIG = "_fertig.txt"
MARKER_PROCESSING = "_in_verarbeitung.txt"


def marker_paths(folder_path: str) -> tuple[str, str]:
    return (
        os.path.join(folder_path, MARKER_FERTIG),
        os.path.join(folder_path, MARKER_PROCESSING),
    )


def read_marker_file(path: str, logger: Optional[logging.Logger] = None) -> str:
    """Liest eine Marker-Datei; UTF-8 zuerst, bei Decode-Fehler CP1252."""
    try:
        with open(path, "r", encoding="utf-8-sig") as marker_file:
            return marker_file.read().strip()
    except UnicodeDecodeError:
        pass

    with open(path, "r", encoding="cp1252") as marker_file:
        content = marker_file.read().strip()
    if logger:
        logger.warning("Marker %s nicht als UTF-8 lesbar, mit CP1252 gelesen.", path)
    return content


def read_marker_raw(folder_path: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
    """Liest Marker-Inhalt aus _in_verarbeitung.txt oder _fertig.txt."""
    fertig_path, processing_path = marker_paths(folder_path)
    for path in (processing_path, fertig_path):
        if not os.path.isfile(path):
            continue
        try:
            return read_marker_file(path, logger)
        except OSError:
            continue
    return None


def remove_upload_markers(folder_path: str, logger: Optional[logging.Logger] = None) -> None:
    """Entfernt Claim-Marker im Ordner (unabhängig vom Archiv-Move)."""
    for name in (MARKER_FERTIG, MARKER_PROCESSING):
        path = os.path.join(folder_path, name)
        if not os.path.isfile(path):
            continue
        try:
            os.remove(path)
            if logger:
                logger.debug("Marker entfernt: %s", path)
        except OSError as exc:
            if logger:
                logger.warning("Marker %s konnte nicht entfernt werden: %s", name, exc)


def discard_stale_fertig_marker(folder_path: str, logger: logging.Logger) -> bool:
    """
    Entfernt _fertig.txt in einem bereits übernommenen Ordner.
    Returns True, wenn eine Datei entfernt wurde.
    """
    fertig_path, _ = marker_paths(folder_path)
    if not os.path.isfile(fertig_path):
        return False
    try:
        os.remove(fertig_path)
        logger.warning(
            "Veraltetes _fertig.txt in bereits übernommenem Ordner '%s' entfernt.",
            os.path.basename(folder_path),
        )
        return True
    except OSError as exc:
        logger.error(
            "Veraltetes _fertig.txt konnte nicht entfernt werden (%s): %s",
            folder_path,
            exc,
        )
        return False

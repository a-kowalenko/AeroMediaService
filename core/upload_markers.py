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

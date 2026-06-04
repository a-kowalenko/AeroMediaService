"""Upload erneut aus Historie/Archiv einreihen."""
from __future__ import annotations

import logging
import os
import shutil
from typing import Any, Optional

from core.archive import find_archived_folder
from core.monitor import resolve_kunde_from_marker, should_use_dropbox_client_for_marker
from core.upload_markers import MARKER_PROCESSING, marker_paths
from core.upload_queue_registry import UploadQueueRegistry
from models.kunde import Kunde

RETRYABLE_STATUSES = frozenset({"Fehler", "Abgebrochen"})


def _kunde_from_history_fields(entry: dict[str, Any]) -> Optional[Kunde]:
    """Nutzt gespeicherte Kundendaten — ohne erneuten API-Lookup."""
    first = (entry.get("first_name") or "").strip()
    last = (entry.get("last_name") or "").strip()
    email = (entry.get("email") or "").strip()
    phone = (entry.get("phone") or "").strip() or None

    if not (first and last and email):
        return None

    return Kunde(
        first_name=first,
        last_name=last,
        email=email,
        phone=phone,
        customer_number=(entry.get("customer_number") or "").strip() or None,
        booking_number=(entry.get("booking_number") or "").strip() or None,
        type=(entry.get("type") or "").strip() or None,
    )


def _resolve_kunde_from_history_entry(config_manager, entry: dict[str, Any]) -> Kunde:
    # Gespeicherte Kundendaten bevorzugen (kein erneuter API-Call bei Retry).
    cached = _kunde_from_history_fields(entry)
    if cached:
        return cached

    marker_raw = (entry.get("marker_raw") or "").strip()
    if not marker_raw:
        raise ValueError(
            "Weder Marker (marker_raw) noch vollständige Kundendaten in der Historie. "
            "Erneuter Upload ist nicht möglich."
        )

    try:
        return resolve_kunde_from_marker(config_manager, marker_raw)
    except RuntimeError as exc:
        if "Customer-Lookup fehlgeschlagen" in str(exc):
            raise ValueError(
                f"Kundendaten konnten nicht von der API geladen werden:\n{exc}\n\n"
                "Bitte Marker-IDs prüfen oder den API-Fehler beheben. "
                "Nach einem erfolgreichen Kunden-Lookup werden Name und E-Mail "
                "in der Historie gespeichert und beim Retry ohne API verwendet."
            ) from exc
        raise ValueError(str(exc)) from exc
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Kundendaten konnten nicht ermittelt werden: {exc}") from exc


def _write_processing_marker(target_path: str, marker_raw: str, log: logging.Logger) -> None:
    _, processing_path = marker_paths(target_path)
    content = marker_raw.strip()
    if not content:
        raise ValueError("Marker-Inhalt ist leer — _in_verarbeitung.txt kann nicht geschrieben werden.")
    with open(processing_path, "w", encoding="utf-8") as marker_file:
        marker_file.write(content)
    log.info("Marker %s für Retry geschrieben.", MARKER_PROCESSING)


def retry_upload_from_history(
    config_manager,
    history_entry: dict[str, Any],
    upload_queue,
    upload_registry: UploadQueueRegistry,
    log: Optional[logging.Logger] = None,
) -> str:
    """
    Stellt einen archivierten Auftrag wieder her und reiht ihn in die Upload-Queue ein.

    Returns:
        Erfolgsmeldung für die UI.

    Raises:
        ValueError: Validierungs- oder Wiederherstellungsfehler mit UI-tauglichem Text.
    """
    logger = log or logging.getLogger(__name__)
    status = (history_entry.get("status") or "").strip()
    if status not in RETRYABLE_STATUSES:
        raise ValueError(f"Status „{status}“ unterstützt keinen erneuten Upload.")

    dir_name = (history_entry.get("dir_name") or "").strip()
    if not dir_name:
        raise ValueError("Historieneintrag ohne Verzeichnisname.")

    monitor_path = config_manager.get_setting("monitor_path")
    archive_path = config_manager.get_setting("archive_path")
    if not monitor_path:
        raise ValueError("Kein Überwachungsordner konfiguriert.")
    if not archive_path:
        raise ValueError("Kein Archiv-Ordner konfiguriert.")
    if not os.path.isdir(monitor_path):
        raise ValueError(f"Überwachungsordner existiert nicht: {monitor_path}")

    archived_hint = (history_entry.get("archived_path") or "").strip() or None
    archived_path = find_archived_folder(
        archive_path,
        dir_name,
        subfolders=("fehler", "abgebrochen"),
        archived_path_hint=archived_hint,
    )
    if not archived_path:
        raise ValueError(
            f"Ordner „{dir_name}“ wurde unter archiv/fehler oder archiv/abgebrochen nicht gefunden."
        )

    target_path = os.path.join(monitor_path, dir_name)
    if os.path.exists(target_path):
        raise ValueError(
            f"Im Überwachungsordner existiert bereits „{dir_name}“. "
            "Bitte den Konflikt manuell lösen."
        )

    if upload_registry.is_registered(target_path):
        raise ValueError(f"„{dir_name}“ ist bereits in der Upload-Warteschlange.")

    kunde = _resolve_kunde_from_history_entry(config_manager, history_entry)
    marker_raw = (history_entry.get("marker_raw") or "").strip()

    logger.info("Retry: verschiebe '%s' von '%s' nach '%s'.", dir_name, archived_path, target_path)
    shutil.move(archived_path, target_path)

    if marker_raw:
        _write_processing_marker(target_path, marker_raw, logger)

    use_dropbox_client = False
    if marker_raw:
        try:
            use_dropbox_client = should_use_dropbox_client_for_marker(config_manager, marker_raw)
        except ValueError:
            pass

    if not upload_registry.enqueue(
        upload_queue,
        {
            "dir_path": target_path,
            "kunde": kunde,
            "use_dropbox_client": use_dropbox_client,
        },
        logger,
    ):
        raise ValueError(f"„{dir_name}“ konnte nicht in die Warteschlange eingereiht werden.")

    retry_count = int(history_entry.get("retry_count") or 0) + 1
    from core.signals import signals

    signals.upload_history_update.emit({
        "dir_name": dir_name,
        "status": "Gestartet",
        "error_msg": "",
        "retry_count": retry_count,
        "first_name": kunde.first_name or "",
        "last_name": kunde.last_name or "",
        "email": kunde.email or "",
        "phone": kunde.phone or "",
        "customer_number": kunde.customer_number or "",
        "booking_number": kunde.booking_number or "",
        "type": kunde.type or "",
    })
    signals.upload_status_update.emit(f"Erneut eingereiht: {dir_name}")

    return f"„{dir_name}“ wurde in die Upload-Warteschlange eingereiht."

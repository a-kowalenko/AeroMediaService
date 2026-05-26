"""Verschieben verarbeiteter Ordner in Archiv-Unterordner (erfolg / fehler / abgebrochen)."""
import logging
import os
import shutil
import time

from core.signals import signals
from core.upload_markers import remove_upload_markers


def archive_directory(config_manager, local_dir_path, subfolder_name, log=None):
    """Verschiebt das Verzeichnis unter archive_path/<subfolder_name>."""
    logger = log or logging.getLogger(__name__)
    archive_base_path = config_manager.get_setting("archive_path")
    if not archive_base_path:
        logger.warning("Kein Archiv-Pfad konfiguriert. %s wird nicht verschoben.", local_dir_path)
        return

    target_dir = os.path.join(archive_base_path, subfolder_name)
    if not os.path.exists(target_dir):
        try:
            os.makedirs(target_dir)
        except OSError as e:
            logger.error("Konnte %s-Ordner nicht erstellen: %s", subfolder_name, e)
            return

    dir_name = os.path.basename(local_dir_path)
    destination_path = os.path.join(target_dir, dir_name)

    if os.path.exists(destination_path):
        destination_path = f"{destination_path}_{int(time.time())}"
        logger.warning("Zielpfad existiert, benenne um zu: %s", destination_path)

    try:
        shutil.move(local_dir_path, destination_path)
        remove_upload_markers(destination_path, logger)
        logger.info("Verzeichnis verschoben nach: %s", destination_path)
    except Exception as e:
        logger.error("Konnte Verzeichnis nicht nach %s verschieben: %s", destination_path, e)


def is_customer_lookup_failure(exc: BaseException) -> bool:
    """True, wenn der Customer-API-Lookup dauerhaft fehlgeschlagen ist."""
    return "Customer-Lookup fehlgeschlagen" in str(exc)


def handle_customer_lookup_failure(config_manager, local_dir_path, exc, log=None):
    """Archiviert nach fehler, meldet Status und schreibt einen Historien-Eintrag."""
    logger = log or logging.getLogger(__name__)
    dir_name = os.path.basename(local_dir_path)
    signals.upload_status_update.emit(f"Fehler: {dir_name}")
    signals.upload_history_update.emit({
        "dir_name": dir_name,
        "status": "Fehler",
        "error_msg": str(exc),
    })
    archive_directory(config_manager, local_dir_path, "fehler", logger)

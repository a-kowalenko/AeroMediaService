import logging
import os
from logging.handlers import RotatingFileHandler
from core.signals import signals


class GuiLogHandler(logging.Handler):
    """
    Ein benutzerdefinierter logging.Handler, der Log-Nachrichten
    an die GUI-Log-Anzeige über ein Qt-Signal weiterleitet.
    """

    def __init__(self):
        super().__init__()
        # Setzt das Level, auf das dieser Handler reagieren soll
        self.setLevel(logging.INFO)

    def emit(self, record):
        """Leitet die formatierte Log-Nachricht an die GUI weiter."""
        try:
            msg = self.format(record)
            signals.log_message.emit(msg)
        except Exception:
            self.handleError(record)


def setup_logging(config_manager):
    """
    Konfiguriert das Logging-System der Anwendung.

    Erstellt zwei Datei-Logger:
    1. 'debug.log': Nimmt alle Nachrichten ab DEBUG-Level auf.
    2. 'activity.log': Nimmt nur INFO-Level-Nachrichten von 'uploader' und 'email' auf.

    Und fügt den GUI-Handler hinzu.
    """
    log_dir = config_manager.get_setting("log_file_path", ".")
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError:
            print(f"Warnung: Konnte Log-Verzeichnis nicht erstellen: {log_dir}")
            log_dir = "."  # Fallback auf aktuelles Verzeichnis

    # Pfade für die Log-Dateien
    debug_log_file = os.path.join(log_dir, "debug.log")
    activity_log_file = os.path.join(log_dir, "activity.log")

    # Format für die Log-Nachrichten
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # --- Root Logger Konfiguration ---
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # --- Debug File Handler ---
    # Rotiert bei 5MB, behält 3 Backups
    debug_handler = RotatingFileHandler(debug_log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(log_format)
    root_logger.addHandler(debug_handler)

    # --- Activity File Handler ---
    class ActivityLogFilter(logging.Filter):
        """Filtert Logs, sodass nur 'uploader' und 'email' mit Level INFO durchgelassen werden."""

        def filter(self, record):
            return record.name in ['uploader', 'email'] and record.levelno == logging.INFO

    activity_handler = RotatingFileHandler(activity_log_file, maxBytes=2 * 1024 * 1024, backupCount=2, encoding='utf-8')
    activity_handler.setLevel(logging.INFO)
    activity_handler.setFormatter(log_format)
    activity_handler.addFilter(ActivityLogFilter())
    root_logger.addHandler(activity_handler)

    # --- GUI Log Handler ---
    gui_handler = GuiLogHandler()
    gui_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', datefmt='%H:%M:%S'))
    root_logger.addHandler(gui_handler)

    logging.info("Logging-System initialisiert.")

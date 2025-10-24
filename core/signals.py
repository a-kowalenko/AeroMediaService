from PySide6.QtCore import QObject, Signal


class GlobalSignals(QObject):
    """
    Ein zentrales Objekt für globale Qt-Signale, um eine Thread-sichere
    Kommunikation zwischen Worker-Threads und der GUI zu gewährleisten.
    """

    # Signal zum Senden einer Log-Nachricht an die GUI
    log_message = Signal(str)

    # Signale zur Aktualisierung der Fortschrittsanzeigen
    upload_progress_file = Signal(int, int, int)  # Fortschritt der aktuellen Datei (Prozent, aktuelle Bytes, Gesamtbytes)
    upload_progress_total = Signal(int, int, int)  # Gesamtfortschritt des Verzeichnisses (Prozent, aktuelle Bytes, Gesamtbytes)

    # Signal zur Anzeige einer Status-Nachricht (z.B. "Lade hoch...")
    upload_status_update = Signal(str)

    # Signal, das den Status des Monitorings ändert (aktiv/inaktiv)
    monitoring_status_changed = Signal(bool)

    # Signal, das den Verbindungsstatus des Clients ändert
    connection_status_changed = Signal(str)

    stop_monitoring = Signal()


# Singleton-Instanz der globalen Signale
# Diese Instanz wird in der gesamten Anwendung geteilt
signals = GlobalSignals()

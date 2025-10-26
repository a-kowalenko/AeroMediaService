import json
import logging
import os
import time
import shutil
from PySide6.QtCore import QThread, QWaitCondition, QMutex

from models.kunde import Kunde


class MonitorThread(QThread):
    """
    Dieser Thread überwacht den Eingangsordner in einem festgelegten Intervall.
    Er sucht nach Verzeichnissen mit einer '_fertig.txt'-Marker-Datei.
    Gefundene Verzeichnisse werden in die 'upload_queue' verschoben.
    """

    def __init__(self, config_manager, upload_queue):
        super().__init__()
        self.config = config_manager
        self.upload_queue = upload_queue
        self.log = logging.getLogger(__name__)

        self._is_running = False
        self._mutex = QMutex()
        self._wait_condition = QWaitCondition()  # Zum Aufwecken bei Einstellungsänderungen

    def stop(self):
        """Signalisiert dem Thread, sicher zu stoppen."""
        self.log.info("Monitor-Thread wird gestoppt...")
        self._is_running = False
        self._wait_condition.wakeAll()  # Weckt den Thread auf, damit er beendet werden kann

    def wake_up(self):
        """Weckt den Thread auf (z.B. nach Einstellungsänderungen)."""
        self._wait_condition.wakeAll()

    def run(self):
        """Die Hauptschleife des Monitor-Threads."""
        self._is_running = True
        self.log.info("Monitor-Thread gestartet.")

        while self._is_running:
            scan_path = self.config.get_setting("monitor_path")
            scan_interval = int(self.config.get_setting("scan_interval", 10))

            if not scan_path or not os.path.isdir(scan_path):
                if scan_path:
                    self.log.warning(f"Überwachungsordner '{scan_path}' existiert nicht. Pausiere.")
                else:
                    self.log.info("Kein Überwachungsordner konfiguriert. Pausiere.")

                # Warte 60 Sekunden oder bis 'stop'/'wake_up' aufgerufen wird
                self._mutex.lock()
                self._wait_condition.wait(self._mutex, 60 * 1000)
                self._mutex.unlock()
                continue  # Nächste Iteration der while-Schleife

            try:
                self.log.debug(f"Scanne Verzeichnis: {scan_path}")
                found_items = 0
                for dir_name in os.listdir(scan_path):
                    if not self._is_running:
                        break  # Sofort beenden, wenn 'stop' aufgerufen wurde

                    full_dir_path = os.path.join(scan_path, dir_name)
                    marker_file_path = os.path.join(full_dir_path, "_fertig.txt")

                    # Prüfen, ob es ein Verzeichnis ist UND die Marker-Datei existiert
                    if os.path.isdir(full_dir_path) and os.path.exists(marker_file_path):
                        self.log.info(f"Neues Verzeichnis gefunden: {dir_name}")

                        # Ziehe Kundendaten aus Marker-Datei (optional)
                        try:
                            with open(marker_file_path, 'r', encoding='utf-8') as marker_file:
                                kundendaten = marker_file.read().strip()
                                self.log.debug(f"Kundendaten für '{dir_name}': {kundendaten}")
                        except Exception as e:
                            self.log.error(f"Fehler beim Lesen der Marker-Datei für '{dir_name}': {e}")

                        # Parse Kundendaten in Kunde Object (optional)
                        kunde = None
                        if kundendaten:
                            data = json.loads(kundendaten)
                            kunde = Kunde(
                                kunde_id=int(data.get('kunde_id')),
                                email=str(data.get('email')),
                                vorname=str(data.get('vorname')),
                                nachname=str(data.get('nachname')),
                                telefon=str(data.get('telefon')),
                                foto=bool(data.get('foto')),
                                video=bool(data.get('video'))
                            )
                            self.log.info(f"Kundendaten geparst für '{dir_name}': {kunde}")
                        else:
                            self.log.warning(f"Keine Kundendaten in Marker-Datei für '{dir_name}' gefunden.")

                        # Wir fügen den Pfad direkt zur Queue hinzu.
                        # Der Uploader ist dafür verantwortlich, ihn nach dem Upload zu verschieben.
                        # Um ein doppeltes Hinzufügen zu verhindern, benennen wir die Marker-Datei um.
                        try:
                            processing_marker_path = os.path.join(full_dir_path, "_in_verarbeitung.txt")
                            os.rename(marker_file_path, processing_marker_path)

                            # Verzeichnis zur Upload-Warteschlange hinzufügen
                            self.upload_queue.put({
                                "dir_path": full_dir_path,
                                "kunde": kunde
                            })
                            self.log.info(f"'{dir_name}' zur Upload-Warteschlange hinzugefügt.")
                            found_items += 1
                        except OSError as e:
                            self.log.error(f"Fehler beim Umbenennen der Marker-Datei für '{dir_name}': {e}")

            except FileNotFoundError:
                self.log.error(f"Überwachungsordner '{scan_path}' wurde gelöscht.")
            except Exception as e:
                self.log.error(f"Fehler beim Scannen des Verzeichnisses: {e}")

            if self._is_running:
                # Warte auf das nächste Intervall oder ein Aufweck-Signal
                self.log.debug(f"Scan beendet. Warte {scan_interval} Sekunden.")
                self._mutex.lock()
                self._wait_condition.wait(self._mutex, scan_interval * 1000)
                self._mutex.unlock()

        self.log.info("Monitor-Thread beendet.")

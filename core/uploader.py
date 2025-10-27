import asyncio
import logging
import os
import shutil
import queue
import time

from PySide6.QtCore import QThread
from core.config import ConfigManager
from services.base_client import BaseClient
from services.email_client import EmailClient
from services.sms_client import SmsClient
from core.signals import signals


class UploaderThread(QThread):
    """
    Dieser Thread arbeitet die Upload-Warteschlange (upload_queue) ab.
    Er läuft kontinuierlich und wartet auf neue Einträge.
    """

    def __init__(self, config_manager: ConfigManager, upload_queue: queue.Queue, client: BaseClient,
                 email_client: EmailClient, sms_client: SmsClient):
        super().__init__()
        self.config = config_manager
        self.upload_queue = upload_queue
        self.client = client
        self.email_client = email_client
        self.sms_client = sms_client
        self.log = logging.getLogger('uploader')  # Spezieller Logger

        self._is_running = False

    def stop(self):
        """Signalisiert dem Thread, sicher zu stoppen."""
        self.log.info("Uploader-Thread wird gestoppt...")
        self._is_running = False
        # Füge ein Dummy-Item hinzu, um den blockierenden 'get'-Aufruf zu beenden
        self.upload_queue.put(None)

    def run(self):
        """Die Hauptschleife des Uploader-Threads."""
        self._is_running = True
        self.log.info("Uploader-Thread gestartet. Warte auf Upload-Aufträge.")

        while self._is_running:
            # Variablen *vor* dem try-Block initialisieren,
            # damit sie im except-Block garantiert existieren.
            local_dir_path = None
            dir_name = "unbekannt"
            kunde = None

            try:
                # Warte blockierend auf ein Item in der Queue
                current_queue_item = self.upload_queue.get()

                # Prüfung auf 'None' (Stopp-Signal) MUSS VOR dem Zugriff erfolgen!
                if current_queue_item is None or not self._is_running:
                    # 'None' ist das Signal zum Beenden
                    break

                # Ab hier ist sicher, dass current_queue_item kein None ist.
                self.log.debug(f"current_queue_item: {current_queue_item}")
                local_dir_path = current_queue_item['dir_path']
                kunde = current_queue_item['kunde']
                dir_name = os.path.basename(local_dir_path)  # dir_name hier setzen

                self.log.info(f"Beginne Verarbeitung von: {dir_name}")
                signals.upload_status_update.emit(f"Starte Upload: {dir_name}")
                signals.upload_progress_file.emit(0, 0, 0)
                signals.upload_progress_total.emit(0, 0, 0)

                # Remote-Pfad festlegen (z.B. /App-Ordner/Verzeichnisname)
                remote_path = f"/{dir_name}"

                # 1. Upload durchführen
                upload_success = self.client.upload_directory(local_dir_path, remote_path)

                if not upload_success:
                    raise Exception("Upload-Funktion des Clients meldete einen Fehler.")

                self.log.info(f"Upload für {dir_name} erfolgreich abgeschlossen.")

                # 2. Freigabelink erstellen
                share_link = self.client.get_shareable_link(remote_path)
                if not share_link:
                    # Logge den Fehler, aber fahre fort (Upload war erfolgreich)
                    self.log.error(f"Konnte Freigabelink für {dir_name} nicht erstellen.")

                # 3. Erfolgs-E-Mail senden
                if share_link and kunde and kunde.email:
                    self.email_client.send_upload_success_email(dir_name, share_link, kunde.email)
                    try:
                        asyncio.run(self.sms_client.send_upload_success_sms(share_link, kunde))
                    except Exception as sms_e:
                        self.log.error(f"SMS-Versand für {kunde.vorname} {kunde.nachname} fehlgeschlagen: {sms_e}")
                elif not kunde:
                    self.log.warning(f"Keine Kundendaten für {dir_name} gefunden. Benachrichtigungen übersprungen.")

                # 4. In Archiv-Ordner verschieben
                self.archive_directory(local_dir_path, "erfolg")

                signals.upload_status_update.emit(f"Erfolgreich: {dir_name}")

            except Exception as e:
                self.log.error(f"Fehler bei der Verarbeitung von '{dir_name}' (Pfad: {local_dir_path}): {e}")
                signals.upload_status_update.emit(f"Fehler: {dir_name}")

                # 5. Bei Fehler in Fehler-Ordner verschieben
                # Nur archivieren, wenn local_dir_path auch einen Wert hat.
                if local_dir_path:
                    self.archive_directory(local_dir_path, "fehler")

                # 6. Fehler-E-Mail senden
                self.email_client.send_upload_failure_email(dir_name, str(e))

            finally:
                if self._is_running:
                    signals.upload_progress_file.emit(0, 0, 0)
                    signals.upload_progress_total.emit(0, 0, 0)
                    self.upload_queue.task_done()
                    self.log.info("Warte auf nächsten Upload-Auftrag...")
                    signals.upload_status_update.emit("Warte auf nächsten Auftrag...")

        self.log.info("Uploader-Thread beendet.")

    def archive_directory(self, local_dir_path, subfolder_name):
        """Verschiebt das verarbeitete Verzeichnis in den Archiv- oder Fehlerordner."""
        archive_base_path = self.config.get_setting("archive_path")
        if not archive_base_path:
            self.log.warning(f"Kein Archiv-Pfad konfiguriert. {local_dir_path} wird nicht verschoben.")
            return

        target_dir = os.path.join(archive_base_path, subfolder_name)
        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir)
            except OSError as e:
                self.log.error(f"Konnte {subfolder_name}-Ordner nicht erstellen: {e}")
                return

        dir_name = os.path.basename(local_dir_path)
        destination_path = os.path.join(target_dir, dir_name)

        # Sicherstellen, dass das Ziel nicht bereits existiert
        if os.path.exists(destination_path):
            destination_path = f"{destination_path}_{int(time.time())}"
            self.log.warning(f"Zielpfad existiert, benenne um zu: {destination_path}")

        try:
            shutil.move(local_dir_path, destination_path)
            # Entferne marker files, falls vorhanden
            processing_marker_path = os.path.join(destination_path, "_in_verarbeitung.txt")
            if os.path.exists(processing_marker_path):
                os.remove(processing_marker_path)
            self.log.info(f"Verzeichnis verschoben nach: {destination_path}")
        except Exception as e:
            self.log.error(f"Konnte Verzeichnis nicht nach {destination_path} verschieben: {e}")

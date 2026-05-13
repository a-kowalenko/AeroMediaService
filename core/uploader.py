import asyncio
import logging
import os
import shutil
import queue
import time

from PySide6.QtCore import QThread
from core.config import ConfigManager
from models.kunde import Kunde
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
                kunde: Kunde = current_queue_item['kunde']
                dir_name = os.path.basename(local_dir_path)  # dir_name hier setzen

                self.log.info(f"Beginne Verarbeitung von: {dir_name}")
                signals.upload_status_update.emit(f"Starte Upload: {dir_name}")
                signals.upload_progress_file.emit(0, 0, 0)
                signals.upload_progress_total.emit(0, 0, 0)

                # History: Start
                signals.upload_history_update.emit({
                    "dir_name": dir_name,
                    "status": "Gestartet",
                    "first_name": kunde.first_name if kunde else "",
                    "last_name": kunde.last_name if kunde else "",
                    "email": kunde.email if kunde else "",
                    "phone": kunde.phone if kunde else ""
                })

                # Remote-Pfad festlegen (z.B. /App-Ordner/Verzeichnisname)
                remote_path = f"/{dir_name}"

                # 1. Upload mit mehreren Versuchen (transiente Netzwerk-/Serverfehler)
                upload_success = False
                job_delays_sec = (5, 15)
                for job_try in range(1, 4):
                    upload_success = self.client.upload_directory(local_dir_path, remote_path, kunde)
                    if upload_success:
                        break
                    if job_try < 3:
                        wait_s = job_delays_sec[job_try - 1]
                        self.log.warning(
                            "Upload '%s' meldete Fehler (Versuch %s/3). Erneuter Versuch in %ss.",
                            dir_name,
                            job_try,
                            wait_s,
                        )
                        time.sleep(wait_s)

                if not upload_success:
                    raise Exception("Upload-Funktion des Clients meldete nach 3 Versuchen weiterhin einen Fehler.")

                self.log.info(f"Upload für {dir_name} erfolgreich abgeschlossen.")

                # 2. Freigabelink erstellen (mit kurzen Retries bei spaeter Finalisierung)
                share_link = None
                for attempt in range(1, 4):
                    share_link = self.client.get_shareable_link(remote_path)
                    if share_link:
                        break
                    if attempt < 3:
                        self.log.warning(
                            f"Freigabelink noch nicht verfuegbar (Versuch {attempt}/3), warte 2s..."
                        )
                        time.sleep(2)

                if not share_link:
                    # Logge den Fehler, aber fahre fort (Upload war erfolgreich)
                    self.log.error(f"Konnte Freigabelink für {dir_name} nicht erstellen.")

                # 3. Erfolgs-E-Mail senden
                email_status = "Übersprungen"
                sms_status = "Übersprungen"
                if share_link and kunde and kunde.email:
                    try:
                        self.email_client.send_upload_success_email(dir_name, share_link, kunde.email, kunde.first_name)
                        email_status = "Gesendet"
                    except Exception as email_e:
                        email_status = f"Fehler: {email_e}"
                        self.log.error(f"E-Mail-Versand fehlgeschlagen: {email_e}")

                    sms_id_val = None
                    try:
                        sms_success, sms_id = asyncio.run(self.sms_client.send_upload_success_sms(share_link, kunde))
                        if sms_success:
                            sms_status = "Gesendet"
                            sms_id_val = sms_id
                        else:
                            if kunde.phone:
                                err_text = getattr(self.sms_client, "last_error", "") or "Fehler beim Senden"
                                sms_status = f"Fehler: {err_text}"
                    except Exception as sms_e:
                        sms_status = f"Fehler: {sms_e}"
                        self.log.error(f"SMS-Versand für {kunde.first_name} {kunde.last_name} fehlgeschlagen: {sms_e}")
                elif not kunde:
                    self.log.warning(f"Keine Kundendaten für {dir_name} gefunden. Benachrichtigungen übersprungen.")
                    sms_id_val = None

                # History: Success
                history_data = {
                    "dir_name": dir_name,
                    "status": "Erfolgreich",
                    "email_status": email_status,
                    "sms_status": sms_status
                }

                if 'sms_id_val' in locals() and sms_id_val:
                    history_data["sms_id"] = sms_id_val

                signals.upload_history_update.emit(history_data)

                # 4. In Archiv-Ordner verschieben
                self.archive_directory(local_dir_path, "erfolg")

                signals.upload_status_update.emit(f"Erfolgreich: {dir_name}")

            except Exception as e:
                self.log.error(f"Fehler bei der Verarbeitung von '{dir_name}' (Pfad: {local_dir_path}): {e}")
                signals.upload_status_update.emit(f"Fehler: {dir_name}")

                # History: Error
                signals.upload_history_update.emit({
                    "dir_name": dir_name,
                    "status": "Fehler",
                    "error_msg": str(e)
                })

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

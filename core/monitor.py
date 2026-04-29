import json
import logging
import os
import time
import shutil
import requests
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

    def _normalize_type(self, raw_type):
        """Normalisiert den Typ für den API-Call."""
        value = str(raw_type or "").strip()
        if value == "Handcam":
            return "Handycam"
        return value

    def _parse_marker_payload(self, marker_content):
        """Validiert Marker-Inhalt und liefert API-Query-Parameter."""
        if not marker_content:
            raise ValueError("Marker-Datei ist leer.")

        try:
            data = json.loads(marker_content)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Marker-Datei ist kein gültiges JSON: {exc}") from exc

        marker_type = self._normalize_type(data.get("type"))
        if not marker_type:
            raise ValueError("Pflichtfeld 'type' fehlt.")

        if "kunden_id_hash" in data and "booking_id_hash" in data:
            return {
                "customer_id": str(data["kunden_id_hash"]).strip(),
                "booking_id": str(data["booking_id_hash"]).strip(),
                "type": marker_type
            }, "hash"

        if "kunden_id" in data and "booking_id" in data:
            return {
                "id": str(data["kunden_id"]).strip(),
                "bookingid": str(data["booking_id"]).strip(),
                "type": marker_type
            }, "id"

        raise ValueError(
            "Ungültiges Marker-Format. Erwartet entweder "
            "'kunden_id_hash' + 'booking_id_hash' oder 'kunden_id' + 'booking_id'."
        )

    def _fetch_customer_data(self, query_params):
        """Lädt Kundendaten über /aero-media-customer."""
        api_base_url = self.config.get_secret("aero_customer_base_url")
        api_token = self.config.get_secret("aero_customer_api_token")

        if not api_base_url or not api_token:
            raise RuntimeError("API-Credentials fehlen (aero_customer_base_url/aero_customer_api_token).")

        response = requests.get(
            f"{api_base_url}/aero-media-customer",
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json"
            },
            params=query_params,
            timeout=15
        )

        if not response.ok:
            raise RuntimeError(f"Customer-Lookup fehlgeschlagen: HTTP {response.status_code} - {response.text[:300]}")

        payload = response.json()
        customer = payload.get("customer")
        if not customer:
            raise RuntimeError("Customer-Lookup lieferte kein 'customer'-Objekt.")

        return customer

    def _build_kunde_from_customer(self, customer):
        """Mappt API-Response auf Kunde-Modell."""
        return Kunde(
            customer_number=str(customer.get("customer_id", "")),
            booking_number=str(customer.get("booking_id", "")),
            email=str(customer.get("email", "")),
            first_name=str(customer.get("vorname", "")),
            last_name=str(customer.get("nachname", "")),
            phone=str(customer.get("telefon", "")),
            type=str(customer.get("typ", ""))
        )

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
                        try:
                            self.log.info(f"Neues Verzeichnis gefunden: {dir_name}")

                            with open(marker_file_path, 'r', encoding='utf-8') as marker_file:
                                marker_raw = marker_file.read().strip()
                            self.log.debug(f"Marker-Daten für '{dir_name}': {marker_raw}")

                            lookup_params, lookup_mode = self._parse_marker_payload(marker_raw)
                            self.log.info(f"Starte Customer-Lookup für '{dir_name}' mit Variante '{lookup_mode}'.")

                            customer = self._fetch_customer_data(lookup_params)
                            kunde = self._build_kunde_from_customer(customer)
                            self.log.info(f"Kundendaten erfolgreich geladen für '{dir_name}': {kunde}")

                            # Wir fügen den Pfad direkt zur Queue hinzu.
                            # Der Uploader ist dafür verantwortlich, ihn nach dem Upload zu verschieben.
                            # Um ein doppeltes Hinzufügen zu verhindern, benennen wir die Marker-Datei um.
                            processing_marker_path = os.path.join(full_dir_path, "_in_verarbeitung.txt")
                            os.rename(marker_file_path, processing_marker_path)

                            # Verzeichnis zur Upload-Warteschlange hinzufügen
                            self.upload_queue.put({
                                "dir_path": full_dir_path,
                                "kunde": kunde
                            })
                            self.log.info(f"'{dir_name}' zur Upload-Warteschlange hinzugefügt.")
                            found_items += 1
                        except Exception as e:
                            self.log.error(f"Fehler bei Verarbeitung von '{dir_name}': {e}")

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

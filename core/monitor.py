import json
import logging
import os
import requests
from PySide6.QtCore import QThread, QWaitCondition, QMutex

from models.kunde import Kunde


def _normalize_marker_type(raw_type):
    """Normalisiert den Typ für den API-Call."""
    value = str(raw_type or "").strip()
    if value == "Handcam":
        return "Handycam"
    return value


def parse_marker_payload(marker_content):
    """Validiert Marker-Inhalt und liefert API-Query-Parameter plus Lookup-Modus."""
    if not marker_content:
        raise ValueError("Marker-Datei ist leer.")

    try:
        data = json.loads(marker_content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Marker-Datei ist kein gültiges JSON: {exc}") from exc

    marker_type = _normalize_marker_type(data.get("type"))
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
            "customer_id": str(data["kunden_id"]).strip(),
            "booking_id": str(data["booking_id"]).strip(),
            "type": marker_type
        }, "id"

    raise ValueError(
        "Ungültiges Marker-Format. Erwartet entweder "
        "'kunden_id_hash' + 'booking_id_hash' oder 'kunden_id' + 'booking_id'."
    )


def fetch_customer_data(config_manager, query_params, lookup_mode):
    """Lädt Kundendaten über den passenden Customer-Endpoint."""
    api_base_url = config_manager.get_secret("aero_customer_base_url")
    api_token = config_manager.get_secret("aero_customer_api_token")

    if not api_base_url or not api_token:
        raise RuntimeError("API-Credentials fehlen (aero_customer_base_url/aero_customer_api_token).")

    endpoint = "/aero-media-customer"
    params = dict(query_params)
    if lookup_mode == "id":
        endpoint = "/aero-media-customer-fallback"
        params["Fallback"] = "true"

    response = requests.get(
        f"{api_base_url}{endpoint}",
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        },
        params=params,
        timeout=15
    )

    if not response.ok:
        raise RuntimeError(f"Customer-Lookup fehlgeschlagen: HTTP {response.status_code} - {response.text[:300]}")

    payload = response.json()
    customer = payload.get("customer")
    if not customer:
        raise RuntimeError("Customer-Lookup lieferte kein 'customer'-Objekt.")

    return customer


def build_kunde_from_customer(customer):
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


def recover_stalled_upload_folders(config_manager, upload_queue, log):
    """
    Legt Verzeichnisse mit _in_verarbeitung.txt (z. B. nach Absturz) erneut in die Upload-Queue.
    Marker bleibt _in_verarbeitung.txt bis der Uploader archiviert.
    """
    scan_path = config_manager.get_setting("monitor_path")
    if not scan_path or not os.path.isdir(scan_path):
        return 0

    recovered = 0
    try:
        names = os.listdir(scan_path)
    except OSError as e:
        log.warning("Recovery: Konnte Überwachungsordner nicht lesen: %s", e)
        return 0

    for dir_name in names:
        full_dir_path = os.path.join(scan_path, dir_name)
        if not os.path.isdir(full_dir_path):
            continue
        processing_marker = os.path.join(full_dir_path, "_in_verarbeitung.txt")
        if not os.path.exists(processing_marker):
            continue
        try:
            with open(processing_marker, "r", encoding="utf-8") as marker_file:
                marker_raw = marker_file.read().strip()
            lookup_params, lookup_mode = parse_marker_payload(marker_raw)
            log.info(
                "Recovery: unterbrochener Auftrag '%s', Customer-Lookup (%s).",
                dir_name,
                lookup_mode,
            )
            customer = fetch_customer_data(config_manager, lookup_params, lookup_mode)
            kunde = build_kunde_from_customer(customer)
            upload_queue.put({
                "dir_path": full_dir_path,
                "kunde": kunde,
            })
            recovered += 1
            log.info("Recovery: '%s' erneut in Upload-Warteschlange gelegt.", dir_name)
        except Exception as e:
            log.error("Recovery: Verzeichnis '%s' konnte nicht wiederaufgenommen werden: %s", dir_name, e)

    if recovered:
        log.info("Recovery: %s unterbrochene Aufträge in die Warteschlange gelegt.", recovered)
    return recovered


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
        return _normalize_marker_type(raw_type)

    def _parse_marker_payload(self, marker_content):
        return parse_marker_payload(marker_content)

    def _fetch_customer_data(self, query_params, lookup_mode):
        return fetch_customer_data(self.config, query_params, lookup_mode)

    def _build_kunde_from_customer(self, customer):
        return build_kunde_from_customer(customer)

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

                            customer = self._fetch_customer_data(lookup_params, lookup_mode)
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

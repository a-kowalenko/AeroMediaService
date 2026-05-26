import json
import logging
import os
import requests
from PySide6.QtCore import QThread, QWaitCondition, QMutex

from models.kunde import Kunde
from core.archive import handle_customer_lookup_failure, is_customer_lookup_failure
from core.upload_markers import discard_stale_fertig_marker, marker_paths
from core.upload_queue_registry import UploadQueueRegistry


def _normalize_marker_type(raw_type):
    """Normalisiert den Typ für den API-Call."""
    value = str(raw_type or "").strip()
    if value == "Handcam":
        return "Handycam"
    return value


def _load_marker_data(marker_content):
    """Parst Marker-JSON und liefert das Wurzelobjekt."""
    if not marker_content:
        raise ValueError("Marker-Datei ist leer.")

    try:
        data = json.loads(marker_content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Marker-Datei ist kein gültiges JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("Marker-JSON muss ein Objekt sein.")
    return data


def _has_api_lookup_fields(data):
    return (
        ("kunden_id_hash" in data and "booking_id_hash" in data)
        or ("kunden_id" in data and "booking_id" in data)
    )


def _has_direct_contact_fields(data):
    return "vorname" in data and "nachname" in data and "email" in data


def parse_api_marker_data(data):
    """Validiert API-Marker-Daten und liefert Query-Parameter plus Lookup-Modus."""
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


def parse_marker_payload(marker_content):
    """Validiert Marker-Inhalt und liefert API-Query-Parameter plus Lookup-Modus."""
    return parse_api_marker_data(_load_marker_data(marker_content))


def build_kunde_from_marker(data):
    """Mappt Direkt-Marker (Dropbox) auf Kunde-Modell."""
    for field in ("vorname", "nachname", "email"):
        if not str(data.get(field, "")).strip():
            raise ValueError(f"Pflichtfeld '{field}' fehlt oder ist leer.")

    phone = str(data.get("telefon", "")).strip() or None
    return Kunde(
        first_name=str(data["vorname"]).strip(),
        last_name=str(data["nachname"]).strip(),
        email=str(data["email"]).strip(),
        phone=phone,
        customer_number=None,
        booking_number=None,
        type=None,
    )


def resolve_kunde_from_marker(config_manager, marker_content):
    """
    Löst Marker-Inhalt zu einem Kunde-Objekt auf.
    API-Lookup (type + IDs) oder Direktformat (vorname/nachname/email) bei Dropbox.
    """
    data = _load_marker_data(marker_content)

    if _has_api_lookup_fields(data):
        query_params, lookup_mode = parse_api_marker_data(data)
        customer = fetch_customer_data(config_manager, query_params, lookup_mode)
        return build_kunde_from_customer(customer)

    if _has_direct_contact_fields(data):
        if config_manager.get_setting("selected_cloud_service", "dropbox") != "dropbox":
            raise ValueError(
                "Direktes Kundenformat (vorname/nachname/email) ist nur bei Dropbox gültig."
            )
        return build_kunde_from_marker(data)

    raise ValueError(
        "Ungültiges Marker-Format. Erwartet entweder "
        "'kunden_id_hash' + 'booking_id_hash', 'kunden_id' + 'booking_id' "
        "oder bei Dropbox 'vorname' + 'nachname' + 'email'."
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


def attempt_queue_upload_folder(
    config_manager,
    full_dir_path: str,
    dir_name: str,
    upload_queue,
    upload_registry: UploadQueueRegistry,
    log,
) -> bool:
    """
    Übernimmt einen Ordner (Claim) und legt ihn in die Upload-Queue.
    Returns True, wenn ein neuer Auftrag eingereiht wurde.
    """
    if not os.path.isdir(full_dir_path):
        return False

    fertig_path, processing_path = marker_paths(full_dir_path)

    if os.path.isfile(processing_path):
        discard_stale_fertig_marker(full_dir_path, log)
        return False

    if not os.path.isfile(fertig_path):
        return False

    if not upload_registry.register(full_dir_path):
        log.debug("'%s' bereits in Upload-Warteschlange vorgemerkt.", dir_name)
        return False

    try:
        with open(fertig_path, "r", encoding="utf-8") as marker_file:
            marker_raw = marker_file.read().strip()
        log.debug("Marker-Daten für '%s': %s", dir_name, marker_raw)
        kunde = resolve_kunde_from_marker(config_manager, marker_raw)
        log.info("Kundendaten erfolgreich geladen für '%s': %s", dir_name, kunde)

        try:
            os.rename(fertig_path, processing_path)
        except OSError as exc:
            raise OSError(
                f"Marker-Umbenennung fehlgeschlagen ({fertig_path} -> {processing_path}): {exc}"
            ) from exc

        upload_queue.put({
            "dir_path": full_dir_path,
            "kunde": kunde,
        })
        log.info("'%s' zur Upload-Warteschlange hinzugefügt.", dir_name)
        return True
    except Exception as exc:
        upload_registry.unregister(full_dir_path)
        if is_customer_lookup_failure(exc):
            log.error(
                "Customer-Lookup für '%s' fehlgeschlagen, verschiebe nach Archiv/fehler: %s",
                dir_name,
                exc,
            )
            handle_customer_lookup_failure(config_manager, full_dir_path, exc, log)
            return False
        raise


def recover_stalled_upload_folders(config_manager, upload_queue, upload_registry, log):
    """
    Legt Verzeichnisse mit _in_verarbeitung.txt (z. B. nach Absturz) erneut in die Upload-Queue.
    Marker bleibt _in_verarbeitung.txt bis der Uploader die Marker nach Upload entfernt/archiviert.
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
        _, processing_path = marker_paths(full_dir_path)
        if not os.path.isfile(processing_path):
            continue
        discard_stale_fertig_marker(full_dir_path, log)
        if not upload_registry.register(full_dir_path):
            log.debug("Recovery: '%s' bereits vorgemerkt, überspringe.", dir_name)
            continue
        try:
            with open(processing_path, "r", encoding="utf-8") as marker_file:
                marker_raw = marker_file.read().strip()
            kunde = resolve_kunde_from_marker(config_manager, marker_raw)
            log.info("Recovery: unterbrochener Auftrag '%s', Kundendaten geladen.", dir_name)
            upload_queue.put({
                "dir_path": full_dir_path,
                "kunde": kunde,
            })
            recovered += 1
            log.info("Recovery: '%s' erneut in Upload-Warteschlange gelegt.", dir_name)
        except Exception as e:
            upload_registry.unregister(full_dir_path)
            if is_customer_lookup_failure(e):
                log.error(
                    "Recovery: Customer-Lookup für '%s' fehlgeschlagen, verschiebe nach Archiv/fehler: %s",
                    dir_name,
                    e,
                )
                handle_customer_lookup_failure(config_manager, full_dir_path, e, log)
            else:
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

    def __init__(self, config_manager, upload_queue, upload_registry: UploadQueueRegistry):
        super().__init__()
        self.config = config_manager
        self.upload_queue = upload_queue
        self.upload_registry = upload_registry
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

    def _resolve_kunde_from_marker(self, marker_content):
        return resolve_kunde_from_marker(self.config, marker_content)

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
                    if not os.path.isdir(full_dir_path):
                        continue

                    fertig_path, processing_path = marker_paths(full_dir_path)
                    if not os.path.isfile(fertig_path) and not os.path.isfile(processing_path):
                        continue

                    try:
                        if os.path.isfile(fertig_path) and not os.path.isfile(processing_path):
                            self.log.info("Neues Verzeichnis gefunden: %s", dir_name)
                        if attempt_queue_upload_folder(
                            self.config,
                            full_dir_path,
                            dir_name,
                            self.upload_queue,
                            self.upload_registry,
                            self.log,
                        ):
                            found_items += 1
                    except Exception as e:
                        self.log.error("Fehler bei Verarbeitung von '%s': %s", dir_name, e)

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

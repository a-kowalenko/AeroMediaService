import os
import json
import requests
from requests.adapters import HTTPAdapter
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from models.kunde import Kunde
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from utils.link_shortener import LinkShortener

# Mindestabstand zwischen Fortschritts-Signalen beim Streaming-Upload (Bytes)
_PROGRESS_EMIT_INTERVAL = 256 * 1024


def _summarize_api_error_body(text: str, max_len: int = 800) -> str:
    """Kurzfassung fuer Logs/Exception: Dropbox liefert JSON mit error_summary."""
    if not text or not text.strip():
        return "(leer)"
    snippet = text.strip()
    try:
        data = json.loads(snippet)
        if isinstance(data, dict):
            summary = data.get("error_summary")
            if summary:
                return str(summary)[:max_len]
            err = data.get("error")
            if isinstance(err, dict) and ".tag" in err:
                return str(err)[:max_len]
    except json.JSONDecodeError:
        pass
    return snippet[:max_len] + ("..." if len(snippet) > max_len else "")


def _full_body_for_log(text: str, limit: int = 4000) -> str:
    if not text:
        return "(leer)"
    if len(text) <= limit:
        return text
    return text[:limit] + f"... [truncated, {len(text)} chars total]"


class _ProgressReportingReader:
    """Wrappt eine geoeffnete Binaerdatei; meldet Fortschritt periodisch beim Lesen."""

    __slots__ = ("_raw", "_file_size", "_on_progress", "_sent", "_last_threshold")

    def __init__(self, raw, file_size: int, on_progress):
        self._raw = raw
        self._file_size = max(file_size, 0)
        self._on_progress = on_progress
        self._sent = 0
        self._last_threshold = -1

    @property
    def bytes_sent(self) -> int:
        return self._sent

    def read(self, n=-1):
        chunk = self._raw.read(n)
        if chunk:
            self._sent += len(chunk)
            if self._file_size > 0:
                threshold = self._sent // _PROGRESS_EMIT_INTERVAL
                if threshold > self._last_threshold:
                    self._last_threshold = threshold
                    self._on_progress(self._sent)
        return chunk

    def __getattr__(self, name):
        return getattr(self._raw, name)


class CustomApiClient(BaseClient):
    """Implementierung des BaseClient für Custom Cloud Storage API mit Direct Upload (Presigned URLs)."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.api_base_url = None
        self.api_key = None
        self.connected = False
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)
        self._last_customer_url = None  # Speichert die letzte customer_url
        self._last_session_id = None  # Letzte Upload-Session fuer spaeteren Link-Check
        self._last_kunde = None  # Letzter Kunde fuer Customer-API-Fallback
        self.session = None  # requests.Session für Connection-Pooling
        self.progress_lock = Lock()  # Lock für Thread-sichere Progress-Updates
        self.max_parallel_uploads = 3  # Max. parallele File-Uploads

    def connect(self, auth_callback=None):
        """Verbindung zur API herstellen."""
        self.api_base_url = self.config.get_secret("custom_api_url")
        self.api_key = self.config.get_secret("custom_api_bearer_token")

        if not self.api_base_url or not self.api_key:
            self.log.warning("API Base URL oder API Key fehlen.")
            signals.connection_status_changed.emit("Fehler: API Credentials fehlen")
            return False

        # Erstelle Session für Connection-Pooling
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}"
        })

        # Connection-Pooling konfigurieren
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Test-Request zur Validierung
        try:
            response = self.session.get(
                f"{self.api_base_url}/health",
                timeout=10
            )

            if response.status_code == 200:
                self.connected = True
                self.log.info("Erfolgreich mit Custom API verbunden (Direct Upload Mode).")
                signals.connection_status_changed.emit("Verbunden")
                return True
            else:
                self.log.error(f"API Connection fehlgeschlagen: {response.status_code}")
                signals.connection_status_changed.emit(f"Fehler: HTTP {response.status_code}")
                return False

        except requests.exceptions.RequestException as e:
            self.log.error(f"Verbindungsfehler zur API: {e}")
            signals.connection_status_changed.emit(f"Verbindungsfehler: {e}")
            return False

    def disconnect(self):
        """Verbindung trennen."""
        self.log.info("Trenne Verbindung zur Custom API...")
        if self.session:
            self.session.close()
            self.session = None
        self.connected = False
        signals.connection_status_changed.emit("Nicht verbunden")
        signals.stop_monitoring.emit()

    def get_connection_status(self):
        """Verbindungsstatus zurückgeben."""
        return "Verbunden" if self.connected else "Nicht verbunden"

    def upload_directory(self, local_dir_path, remote_base_path, kunde: Kunde = None):
        """Lädt ein Verzeichnis mit Direct Upload (Presigned URLs) hoch."""
        if not self.connected:
            self.log.error("Upload fehlgeschlagen: Nicht verbunden.")
            return False

        self.log.info(f"Beginne Direct Upload von '{local_dir_path}'")
        self._last_kunde = kunde

        # 1. Dateien sammeln
        files_to_upload = []
        total_size = 0

        for root, _, files in os.walk(local_dir_path):
            for file in files:
                if file in ["_fertig.txt", "_in_verarbeitung.txt", ".DS_Store", ".apdisk", "Thumbs.db", "desktop.ini"] or file.startswith("._"):
                    continue

                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(str(local_path), str(local_dir_path))

                try:
                    file_size = os.path.getsize(local_path)
                    mime_type = self._get_mime_type(local_path)

                    files_to_upload.append({
                        "name": relative_path.replace(os.path.sep, '/'),
                        "size": file_size,
                        "type": mime_type,
                        "local_path": local_path
                    })
                    total_size += file_size
                except FileNotFoundError:
                    self.log.warning(f"Datei nicht gefunden: {local_path}")

        if not files_to_upload:
            self.log.warning("Keine Dateien zum Hochladen gefunden.")
            return True

        # 2. Direct Upload Session initialisieren
        try:
            # Extrahiere den exakten Verzeichnisnamen für den Server
            folder_name = os.path.basename(local_dir_path)
            session_data = self._initialize_direct_session(files_to_upload, folder_name, kunde)
            session_id = session_data["session_id"]
            order_id = session_data["order_id"]
            self._last_session_id = session_id

            self.log.info(f"Direct Upload Session initialisiert: {session_id}, Order: {order_id}")

        except Exception as e:
            self.log.error(f"Session-Initialisierung fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

        # 3. Dateien parallel direkt hochladen
        uploaded_counter = {'bytes': 0}  # Thread-safe counter

        try:
            # Parallele Uploads mit ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_parallel_uploads) as executor:
                futures = {}

                for file_info in files_to_upload:
                    future = executor.submit(
                        self._upload_file_direct,
                        session_id,
                        order_id,
                        file_info,
                        total_size,
                        uploaded_counter
                    )
                    futures[future] = file_info

                # Warte auf Completion und aktualisiere Progress
                for future in as_completed(futures):
                    file_info = futures[future]
                    try:
                        success = future.result()
                        if not success:
                            raise Exception(f"Upload von {file_info['name']} fehlgeschlagen")

                    except Exception as e:
                        self.log.error(f"Fehler beim Upload von {file_info['name']}: {e}")
                        raise

            # 4. Warten bis Session finalisiert ist
            self.log.info("Alle Dateien hochgeladen, warte auf Server-Finalisierung...")
            customer_url = self._wait_for_completion(session_id)

            # Speichere customer_url für get_shareable_link (nur wenn vorhanden)
            self._last_customer_url = customer_url if customer_url else None

            signals.upload_status_update.emit(f"Upload abgeschlossen.")
            if customer_url:
                self.log.info(f"Upload erfolgreich: {customer_url}")
            else:
                self.log.warning(
                    "Upload der Dateien erfolgreich, aber customer_url noch nicht verfuegbar."
                )
            return True

        except Exception as e:
            self.log.error(f"Upload fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

    def _initialize_direct_session(self, files_to_upload, base_folder_name, kunde: Kunde = None):
        """Direct Upload Session bei der API initialisieren.

        Endpoint: POST /api/upload/direct-init
        """
        # Metadata aus kunde-Parameter extrahieren
        metadata = {}
        if kunde:
            from dataclasses import asdict, is_dataclass

            # Konvertiere Kunde-Objekt zu Dictionary
            if is_dataclass(kunde):
                kunde_dict = asdict(kunde)
            elif isinstance(kunde, dict):
                kunde_dict = kunde
            else:
                kunde_dict = vars(kunde)

            metadata = kunde_dict
            self.log.info(f"Sende Metadata: {metadata}")

        payload = {
            "files": [
                {
                    "name": f["name"],
                    "size": f["size"],
                    "type": f["type"]
                }
                for f in files_to_upload
            ],
            "metadata": metadata,
            "base_folder_name": base_folder_name
        }

        response = self.session.post(
            f"{self.api_base_url}/upload/direct-init",
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            error_text = response.text[:500] if len(response.text) > 500 else response.text
            raise Exception(f"HTTP {response.status_code} - {error_text}")

        result = response.json()

        if not result.get("ok"):
            raise Exception(f"Session-Initialisierung fehlgeschlagen: {result}")

        return result

    def _get_presigned_url(self, session_id, order_id, file_name, file_type):
        """Upload-URL von der API abrufen.

        Endpoint: POST /api/upload/presigned-url
        Returns: Dict mit 'url' (Dropbox Direct Upload Link)
        """
        pathname = f"{order_id}/{file_name}"

        client_payload = json.dumps({
            "session_id": session_id,
            "order_id": order_id,
            "file_name": file_name
        })

        # Die API erwartet aktuell noch dieses Format (Vercel Blob Protocol)
        token_request = {
            "type": "blob.generate-client-token",
            "payload": {
                "pathname": pathname,
                "callbackUrl": None,
                "multipart": False,
                "clientPayload": client_payload
            }
        }

        response = self.session.post(
            f"{self.api_base_url}/upload/presigned-url",
            json=token_request,
            timeout=30
        )

        if response.status_code != 200:
            error_text = response.text[:500] if len(response.text) > 500 else response.text
            raise Exception(f"HTTP {response.status_code} - {error_text}")

        token_data = response.json()
        # Die API sendet das Feld als 'url' (Fallback auf 'directUploadUrl' für Kompatibilität)
        upload_url = token_data.get('url') or token_data.get('directUploadUrl')
        client_token = token_data.get('clientToken')

        if not upload_url:
            raise Exception(f"Keine Upload-URL in API-Antwort erhalten: {token_data}")

        return {
            'url': upload_url,
            'pathname': pathname,
            'clientToken': client_token
        }

    def _upload_file_direct(self, session_id, order_id, file_info, total_job_size, uploaded_counter):
        """Datei direkt zum Cloud Storage (Dropbox) hochladen."""
        file_name = file_info["name"]
        file_size = file_info["size"]
        file_type = file_info["type"]
        local_path = file_info["local_path"]

        try:
            # 1. Upload-URL abrufen
            presigned_data = self._get_presigned_url(
                session_id, order_id, file_name, file_type
            )

            upload_url = presigned_data.get("url")
            pathname = presigned_data.get("pathname")
            client_token = presigned_data.get("clientToken")

            self.log.info(f"✓ Upload-URL erhalten")

            # 2. Datei hochladen (Direct Upload zu Dropbox via POST)
            def emit_progress(bytes_sent_partial: int):
                if file_size <= 0:
                    return
                pct = min(100, int((bytes_sent_partial / file_size) * 100))
                with self.progress_lock:
                    base = uploaded_counter["bytes"]
                combined = base + bytes_sent_partial
                total_pct = (
                    min(100, int((combined / total_job_size) * 100))
                    if total_job_size > 0
                    else 0
                )
                signals.upload_progress_file.emit(pct, bytes_sent_partial, file_size)
                signals.upload_progress_total.emit(total_pct, combined, total_job_size)

            with open(local_path, "rb") as raw_f:
                self.log.info(f"   Direct Upload zu Dropbox: {file_name} ({file_size} bytes)")
                upload_headers = {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(file_size),
                }
                wrapped = _ProgressReportingReader(raw_f, file_size, emit_progress)
                signals.upload_progress_file.emit(0, 0, file_size)
                emit_progress(0)
                upload_response = requests.post(
                    upload_url,
                    headers=upload_headers,
                    data=wrapped,
                    timeout=600,
                )
                if file_size > 0 and wrapped.bytes_sent < file_size:
                    emit_progress(wrapped.bytes_sent)
                elif file_size > 0:
                    emit_progress(file_size)

            if upload_response.status_code not in [200, 201]:
                body = upload_response.text or ""
                summary = _summarize_api_error_body(body)
                self.log.error(
                    "Upload-Antwort: HTTP %s — %s | Body: %s",
                    upload_response.status_code,
                    summary,
                    _full_body_for_log(body),
                )
                raise Exception(
                    f"Upload failed: HTTP {upload_response.status_code} — {summary}"
                )

            # Dropbox liefert bei Erfolg Metadaten zurück, inkl. einer 'id'
            dropbox_id = None
            try:
                db_metadata = upload_response.json()
                dropbox_id = db_metadata.get('id')
                if not dropbox_id:
                    self.log.warning(f"Dropbox-Antwort enthielt keine 'id': {db_metadata}")
            except Exception as json_e:
                self.log.warning(f"Konnte Dropbox-Antwort nicht als JSON parsen: {upload_response.text[:200]}")

            # Fallback: Wenn keine ID vorhanden, erzeuge eine eindeutige ID im Dropbox-Format,
            # um 'unique constraint' Fehler in der DB zu vermeiden.
            if not dropbox_id:
                import uuid
                dropbox_id = f"id:temp_{uuid.uuid4().hex[:12]}"
                self.log.info(f"Verwende temporäre ID: {dropbox_id}")

            # 3. File beim Server registrieren
            # Wir senden alle Felder (inkl. blob_url Fallback), um Server-Fehler zu vermeiden
            register_payload = {
                'session_id': session_id,
                'order_id': order_id,
                'file_name': file_name,
                'dropbox_id': dropbox_id,
                'clientToken': client_token,
                'blob_url': f"dropbox://{order_id}/{file_name}" # Hilfs-URL für den Server
            }

            register_response = self.session.post(
                f"{self.api_base_url}/upload/register",
                json=register_payload,
                timeout=30
            )

            if not register_response.ok:
                reg_body = register_response.text or ""
                reg_summary = _summarize_api_error_body(reg_body)
                self.log.error(
                    "upload/register fehlgeschlagen: HTTP %s — %s | Body: %s",
                    register_response.status_code,
                    reg_summary,
                    _full_body_for_log(reg_body),
                )
                raise Exception(
                    f"File registration failed: HTTP {register_response.status_code} — {reg_summary}"
                )

            # 4. Progress aktualisieren
            with self.progress_lock:
                uploaded_counter['bytes'] += file_size
                current_total_bytes = uploaded_counter['bytes']
                signals.upload_progress_file.emit(100, file_size, file_size)
                total_progress = int((current_total_bytes / total_job_size) * 100)
                signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)

            self.log.info(f"✅ {file_name} erfolgreich hochgeladen")
            return True

        except Exception as e:
            self.log.error(f"Fehler beim Upload von {file_name}: {e}")
            raise

    def _wait_for_completion(self, session_id):
        """Warten bis Session vom Server finalisiert wurde.

        Der Server finalisiert die Session automatisch nachdem alle Files
        über den onUploadCompleted Callback registriert wurden.
        """
        max_wait_time = 15  # 15 Sekunden max
        poll_interval = 2  # Alle 2 Sekunden prüfen
        started_at = time.monotonic()

        self.log.info("Warte auf Server-Finalisierung...")
        signals.upload_status_update.emit("Finalisiere Upload...")

        while (time.monotonic() - started_at) < max_wait_time:
            try:
                # Session-Status abrufen
                response = self.session.get(
                    f"{self.api_base_url}/upload/status/{session_id}",
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    timeout=10
                )

                if response.ok:
                    result = response.json()
                    self.log.debug(f"Session-Status Antwort: {result}")

                    customer_url = self._extract_customer_url(result)
                    if result.get("status") == "completed" and customer_url:
                        self.log.info(f"Session finalisiert! Customer URL: {customer_url}")
                        return customer_url

                    # Manche Server liefern URL bereits vor finalem Status.
                    if customer_url:
                        self.log.info(f"Customer URL bereits verfuegbar: {customer_url}")
                        return customer_url

                    # Status-Update loggen
                    uploaded = result.get("uploaded_files", 0)
                    total = result.get("total_files", 0)
                    self.log.debug(f"Session Status: {uploaded}/{total} Files verarbeitet")
                else:
                    self.log.warning(f"Status-Check HTTP {response.status_code}: {response.text[:200]}")

            except Exception as e:
                self.log.warning(f"Fehler beim Status-Check: {e}")

            time.sleep(poll_interval)

        # Timeout - versuche trotzdem customer_url mit kurzen Retries zu holen
        self.log.warning("Timeout beim Warten auf Finalisierung, versuche URL zu holen...")

        for _ in range(3):
            try:
                response = self.session.get(
                    f"{self.api_base_url}/upload/status/{session_id}",
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    timeout=10
                )
                if response.ok:
                    result = response.json()
                    customer_url = self._extract_customer_url(result)
                    if customer_url:
                        self.log.info(f"Customer URL nach Timeout erhalten: {customer_url}")
                        return customer_url
            except Exception as e:
                self.log.warning(f"Fehler beim nachtraeglichen URL-Check: {e}")

            time.sleep(2)

        return None

    def _extract_customer_url(self, result):
        """Extrahiert den Kunden-Link robust aus unterschiedlichen API-Formaten."""
        if not isinstance(result, dict):
            return None

        # Hauefige Feldnamen im Wilden.
        direct_keys = [
            "customer_url",
            "customerUrl",
            "share_url",
            "shareUrl",
            "public_url",
            "publicUrl",
            "url"
        ]
        for key in direct_keys:
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        # Verschachtelte Varianten
        nested_candidates = [
            result.get("data"),
            result.get("result"),
            result.get("upload"),
            result.get("session")
        ]
        for candidate in nested_candidates:
            if isinstance(candidate, dict):
                for key in direct_keys:
                    value = candidate.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()

        return None

    def _extract_link_from_customer_payload(self, payload):
        """Extrahiert Link robust aus aero-media-customer Antwort."""
        if not isinstance(payload, dict):
            return None

        # Direkte Felder
        for key in ["link", "customer_url", "customerUrl", "url", "short_order_id", "shortOrderId"]:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        customer = payload.get("customer")
        if isinstance(customer, dict):
            for key in ["link", "customer_url", "customerUrl", "url", "short_order_id", "shortOrderId"]:
                value = customer.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

            # Typische verschachtelte Media-Strukturen
            for media_key in ["media", "handycam", "handcam", "files"]:
                media_obj = customer.get(media_key)
                if isinstance(media_obj, dict):
                    for key in ["link", "customer_url", "customerUrl", "url", "short_order_id", "shortOrderId"]:
                        value = media_obj.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()

        return None

    def _fetch_customer_url_from_aero_customer(self):
        """Fallback auf aero-media-customer, falls Upload-Status keinen Link liefert."""
        if not self._last_kunde:
            return None

        base_url = self.config.get_secret("aero_customer_base_url")
        token = self.config.get_secret("aero_customer_api_token")
        if not base_url or not token:
            return None

        kunde = self._last_kunde
        query_params = {
            "customer_id": str(getattr(kunde, "customer_number", "") or "").strip(),
            "booking_id": str(getattr(kunde, "booking_number", "") or "").strip(),
            "type": str(getattr(kunde, "type", "") or "").strip()
        }
        if not query_params["customer_id"] or not query_params["booking_id"] or not query_params["type"]:
            return None

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        endpoint = f"{base_url.rstrip('/')}/aero-media-customer"
        for fallback_mode in [False, True]:
            params = dict(query_params)
            if fallback_mode:
                params["Fallback"] = "true"

            try:
                response = requests.get(endpoint, headers=headers, params=params, timeout=15)
                if not response.ok:
                    self.log.warning(
                        f"aero-media-customer Lookup fehlgeschlagen (HTTP {response.status_code}) "
                        f"mit params={params}"
                    )
                    continue

                payload = response.json()
                self.log.debug(f"aero-media-customer Antwort: {payload}")
                link = self._extract_link_from_customer_payload(payload)
                if link:
                    self.log.info(f"Link ueber aero-media-customer gefunden: {link}")
                    return link
            except Exception as e:
                self.log.warning(f"aero-media-customer Lookup Fehler: {e}")

        return None

    def get_shareable_link(self, remote_path):
        """Gibt den Customer-Link zurück (bereits von Session zurückgegeben)."""
        if not self._last_customer_url:
            # Fallback: versuche URL noch einmal anhand der letzten Session zu laden.
            if self._last_session_id:
                try:
                    response = self.session.get(
                        f"{self.api_base_url}/upload/status/{self._last_session_id}",
                        headers={'Authorization': f'Bearer {self.api_key}'},
                        timeout=10
                    )
                    if response.ok:
                        result = response.json()
                        fallback_url = self._extract_customer_url(result)
                        if fallback_url:
                            self._last_customer_url = fallback_url
                            self.log.info(f"customer_url per Fallback-Check erhalten: {fallback_url}")
                    else:
                        self.log.warning(
                            f"Fallback Status-Check fehlgeschlagen (HTTP {response.status_code})"
                        )
                except Exception as e:
                    self.log.warning(f"Fallback Status-Check fuer customer_url fehlgeschlagen: {e}")

            if not self._last_customer_url:
                customer_lookup_url = self._fetch_customer_url_from_aero_customer()
                if customer_lookup_url:
                    self._last_customer_url = customer_lookup_url

            if not self._last_customer_url:
                self.log.warning("Keine customer_url verfügbar. Upload noch nicht abgeschlossen?")
                return None

        self.log.info(f"Gebe customer_url zurück: {self._last_customer_url}")

        # Optional: Link-Shortener verwenden
        try:
            shortened_link = self.link_shortener.shorten(self._last_customer_url)
            if shortened_link:
                self.log.info(f"Link gekürzt: {shortened_link}")
                return shortened_link
        except Exception as e:
            self.log.warning(f"Link-Shortener fehlgeschlagen: {e}, verwende Original-Link")

        return self._last_customer_url

    def _get_mime_type(self, file_path):
        """Ermittelt den MIME-Type einer Datei."""
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or 'application/octet-stream'

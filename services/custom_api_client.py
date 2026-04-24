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

            # Speichere customer_url für get_shareable_link
            self._last_customer_url = customer_url

            signals.upload_status_update.emit(f"Upload abgeschlossen.")
            self.log.info(f"Upload erfolgreich: {customer_url}")
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
            with open(local_path, 'rb') as f:
                self.log.info(f"   Direct Upload zu Dropbox: {file_name}")
                
                upload_headers = {
                    'Content-Type': 'application/octet-stream'
                }
                
                upload_response = requests.post(
                    upload_url,
                    headers=upload_headers,
                    data=f,
                    timeout=600
                )

            if upload_response.status_code not in [200, 201]:
                self.log.error(f"Upload-Antwort: {upload_response.status_code} - {upload_response.text}")
                raise Exception(f"Upload failed: HTTP {upload_response.status_code}")

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
                error_text = register_response.text[:200] if len(register_response.text) > 200 else register_response.text
                raise Exception(f"File registration failed: HTTP {register_response.status_code} - {error_text}")

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
        max_wait_time = 10  # 60 Sekunden max
        poll_interval = 2  # Alle 2 Sekunden prüfen
        elapsed = 0

        self.log.info("Warte auf Server-Finalisierung...")
        signals.upload_status_update.emit("Finalisiere Upload...")

        while elapsed < max_wait_time:
            try:
                # Session-Status abrufen
                response = self.session.get(
                    f"{self.api_base_url}/upload/status/{session_id}",
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    timeout=10
                )

                if response.ok:
                    result = response.json()

                    if result.get("status") == "completed":
                        customer_url = result.get("customer_url")
                        self.log.info(f"Session finalisiert! Customer URL: {customer_url}")
                        return customer_url

                    # Status-Update loggen
                    uploaded = result.get("uploaded_files", 0)
                    total = result.get("total_files", 0)
                    self.log.debug(f"Session Status: {uploaded}/{total} Files verarbeitet")

            except Exception as e:
                self.log.warning(f"Fehler beim Status-Check: {e}")

            time.sleep(poll_interval)
            elapsed += poll_interval

        # Timeout - versuche trotzdem customer_url zu holen
        self.log.warning("Timeout beim Warten auf Finalisierung, versuche URL zu holen...")

        try:
            response = self.session.get(
                f"{self.api_base_url}/upload/status/{session_id}",
                headers={'Authorization': f'Bearer {self.api_key}'},
                timeout=10
            )
            if response.ok:
                result = response.json()
                return result.get("customer_url")
        except:
            pass

        return None

    def get_shareable_link(self, remote_path):
        """Gibt den Customer-Link zurück (bereits von Session zurückgegeben)."""
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

import os
import requests
import logging
import math

from models.kunde import Kunde
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from utils.link_shortener import LinkShortener

# Chunk-Größe: 4 MB (unter dem 4.5 MB Limit)
CHUNK_SIZE = 4 * 1024 * 1024


class CustomApiClient(BaseClient):
    """Implementierung des BaseClient für Custom Cloud Storage API mit Chunked Upload."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.api_base_url = None
        self.api_key = None
        self.connected = False
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)
        self._last_customer_url = None  # Speichert die letzte customer_url

    def connect(self, auth_callback=None):
        """Verbindung zur API herstellen."""
        self.api_base_url = self.config.get_secret("custom_api_url")
        self.api_key = self.config.get_secret("custom_api_bearer_token")

        if not self.api_base_url or not self.api_key:
            self.log.warning("API Base URL oder API Key fehlen.")
            signals.connection_status_changed.emit("Fehler: API Credentials fehlen")
            return False

        # Test-Request zur Validierung
        try:
            headers = {"Authorization": f"Bearer {self.api_key}"}
            response = requests.get(
                f"{self.api_base_url}/health",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                self.connected = True
                self.log.info("Erfolgreich mit Custom API verbunden.")
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
        self.connected = False
        signals.connection_status_changed.emit("Nicht verbunden")
        signals.stop_monitoring.emit()

    def get_connection_status(self):
        """Verbindungsstatus zurückgeben."""
        return "Verbunden" if self.connected else "Nicht verbunden"

    def upload_directory(self, local_dir_path, remote_base_path, kunde: Kunde=None):
        """Lädt ein Verzeichnis mit Chunked Upload hoch."""
        if not self.connected:
            self.log.error("Upload fehlgeschlagen: Nicht verbunden.")
            return False

        self.log.info(f"Beginne Chunked Upload von '{local_dir_path}'")

        # 1. Dateien sammeln
        files_to_upload = []
        total_size = 0

        for root, _, files in os.walk(local_dir_path):
            for file in files:
                if file in ["_fertig.txt", "_in_verarbeitung.txt"]:
                    continue

                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_dir_path)

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

        # 2. Upload-Session initialisieren
        try:
            session_data = self._initialize_session(files_to_upload, kunde)
            session_id = session_data["session_id"]
            order_id = session_data["order_id"]
            chunk_size = session_data.get("chunk_size", CHUNK_SIZE)

            self.log.info(f"Session initialisiert: {session_id}, Order: {order_id}")

        except Exception as e:
            self.log.error(f"Session-Initialisierung fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

        # 3. Dateien chunked hochladen
        bytes_uploaded = 0

        try:
            for file_info in files_to_upload:
                file_name = file_info["name"]
                file_size = file_info["size"]
                local_path = file_info["local_path"]

                status_msg = f"Lade hoch: {os.path.basename(file_name)} ({file_size / 1024 ** 2:.2f} MB)"
                signals.upload_status_update.emit(status_msg)
                self.log.debug(status_msg)

                # Datei chunked hochladen
                success = self._upload_file_chunked(
                    session_id, local_path, file_name, file_size,
                    chunk_size, bytes_uploaded, total_size
                )

                if not success:
                    raise Exception(f"Upload von {file_name} fehlgeschlagen")

                bytes_uploaded += file_size

            # 4. Upload abschließen
            result = self._complete_upload(session_id)
            customer_url = result.get("customer_url")

            # Speichere customer_url für get_shareable_link
            self._last_customer_url = customer_url

            signals.upload_status_update.emit(f"Upload abgeschlossen.")
            self.log.info(f"Upload erfolgreich: {customer_url}")
            return True

        except Exception as e:
            self.log.error(f"Upload fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

    def _initialize_session(self, files_to_upload, kunde: Kunde = None):
        """Upload-Session bei der API initialisieren."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

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
                kunde_dict = vars(kunde)  # Fallback für normale Objekte

            # WICHTIG: Direkt das Dictionary verwenden, NICHT json.dumps()!
            metadata = kunde_dict  # <-- ÄNDERUNG HIER
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
            "metadata": metadata  # <-- Jetzt ein Dict, kein String!
        }

        response = requests.post(
            f"{self.api_base_url}/upload/init",
            json=payload,  # requests.post konvertiert automatisch zu JSON
            headers=headers,
            timeout=30
        )

        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code} - {response.text}")

        return response.json()

    def _upload_file_chunked(self, session_id, local_path, file_name, file_size,
                             chunk_size, base_bytes_uploaded, total_job_size):
        """Datei in Chunks hochladen."""
        total_chunks = math.ceil(file_size / chunk_size)

        headers = {"Authorization": f"Bearer {self.api_key}"}

        with open(local_path, 'rb') as f:
            for chunk_index in range(total_chunks):
                chunk_data = f.read(chunk_size)

                form_data = {
                    'session_id': session_id,
                    'file_name': file_name,
                    'chunk_index': str(chunk_index),
                    'total_chunks': str(total_chunks)
                }

                files = {
                    'chunk': (f'chunk_{chunk_index}', chunk_data, 'application/octet-stream')
                }

                response = requests.post(
                    f"{self.api_base_url}/upload/chunk",
                    data=form_data,
                    files=files,
                    headers=headers,
                    timeout=60
                )

                if response.status_code != 200:
                    raise Exception(f"Chunk {chunk_index} upload failed: HTTP {response.status_code} - {response.text}")

                # Fortschritt melden
                bytes_sent = (chunk_index + 1) * chunk_size
                if bytes_sent > file_size:
                    bytes_sent = file_size

                file_progress = int((bytes_sent / file_size) * 100)
                signals.upload_progress_file.emit(file_progress, bytes_sent, file_size)

                current_total_bytes = base_bytes_uploaded + bytes_sent
                total_progress = int((current_total_bytes / total_job_size) * 100)
                signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)

                self.log.debug(f"Chunk {chunk_index + 1}/{total_chunks} für {file_name} hochgeladen")

        return True

    def _complete_upload(self, session_id):
        """Upload-Session abschließen."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        response = requests.post(
            f"{self.api_base_url}/upload/complete",
            json={"session_id": session_id},
            headers=headers,
            timeout=120  # Längeres Timeout für Finalisierung
        )

        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code} - {response.text}")

        return response.json()

    def get_shareable_link(self, remote_path):
        """Gibt den Customer-Link zurück (bereits von complete zurückgegeben)."""
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
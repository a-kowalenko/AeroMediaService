import logging
import requests
from pathlib import Path
from typing import Optional
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from utils.link_shortener import LinkShortener
import mimetypes


class CustomApiClient(BaseClient):
    """
    Implementierung des BaseClient für cloud.kowalenko.io API.
    Ermöglicht das Hochladen von Dateien mit Bearer Token Authentifizierung.
    """

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.session: Optional[requests.Session] = None
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)
        self._is_connected = False
        self._last_order_id = None  # Speichert die letzte Order-ID für Share-Link

    def connect(self, auth_callback=None):
        """
        Stellt die Verbindung zur cloud.kowalenko.io API her.
        Validiert den API-Key durch einen Test-Request.
        """
        api_url = self.config.get_secret("custom_api_url")
        bearer_token = self.config.get_secret("custom_api_bearer_token")

        if not api_url or not bearer_token:
            self.log.warning("Cloud API URL oder Bearer Token fehlen.")
            signals.connection_status_changed.emit("Fehler: API URL/Token fehlt")
            return False

        # Validiere API-Key Format
        if not bearer_token.startswith('key_') or '.' not in bearer_token:
            self.log.warning("Ungültiges API-Key Format. Erwartet: key_xxxxx.secret")
            signals.connection_status_changed.emit("Fehler: Ungültiger API-Key")
            return False

        # Session mit Bearer Token initialisieren
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {bearer_token}",
            "Accept": "application/json",
            "User-Agent": "AeroMediaService/1.0"
        })

        # Verbindung testen mit Health-Endpoint
        try:
            self.log.info("Teste Verbindung zur cloud.kowalenko.io API...")

            # Health-Check Endpoint
            health_url = api_url.rstrip('/') + '/health'

            response = self.session.get(health_url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                status = data.get('status')

                if status == 'healthy':
                    tenant_id = data.get('tenant_id')
                    self.log.info(f"Erfolgreich mit Cloud API verbunden. Tenant: {tenant_id}")
                    self._is_connected = True
                    signals.connection_status_changed.emit("Verbunden")
                    return True
                elif status == 'unauthorized':
                    self.log.warning("API-Key ungültig oder fehlende Berechtigung")
                    signals.connection_status_changed.emit("Fehler: Ungültiger API-Key")
                    self._is_connected = False
                    return False
                else:
                    self.log.warning(f"Unerwarteter Status: {status}")
                    signals.connection_status_changed.emit(f"Fehler: {status}")
                    self._is_connected = False
                    return False
            else:
                self.log.warning(f"Cloud API antwortet mit Status {response.status_code}")
                signals.connection_status_changed.emit(f"Fehler: HTTP {response.status_code}")
                self._is_connected = False
                return False

        except requests.exceptions.ConnectionError as e:
            self.log.error(f"Verbindungsfehler zur Cloud API: {e}")
            signals.connection_status_changed.emit("Verbindungsfehler")
            self._is_connected = False
            return False
        except requests.exceptions.Timeout:
            self.log.error("Timeout bei der Verbindung zur Cloud API")
            signals.connection_status_changed.emit("Timeout")
            self._is_connected = False
            return False
        except Exception as e:
            self.log.error(f"Unerwarteter Fehler bei API Verbindung: {e}")
            signals.connection_status_changed.emit(f"Fehler: {e}")
            self._is_connected = False
            return False

    def disconnect(self):
        """Trennt die Verbindung zur cloud.kowalenko.io API."""
        if self.session:
            self.session.close()
            self.session = None

        self._is_connected = False
        self._last_order_id = None
        self.log.info("Cloud API Verbindung getrennt.")
        signals.connection_status_changed.emit("Nicht verbunden")

    def get_connection_status(self):
        """Gibt einen String zurück, der den aktuellen Verbindungsstatus beschreibt."""
        if self._is_connected and self.session:
            return "Verbunden"
        else:
            return "Nicht verbunden"

    def upload_directory(self, local_dir_path, remote_base_path):
        """
        Lädt ein komplettes Verzeichnis zur cloud.kowalenko.io API hoch.

        Die cloud.kowalenko.io API erwartet:
        - Multipart/form-data mit mehreren 'files' Einträgen
        - Dateinamen müssen den relativen Pfad enthalten (wie webkitRelativePath)
        - Endpoint: /api/upload

        Verwendet Signale aus core.signals, um den Fortschritt zu melden.
        """
        if not self._is_connected or not self.session:
            self.log.error("Nicht mit Cloud API verbunden. Upload abgebrochen.")
            signals.upload_failed.emit("Nicht verbunden")
            return False

        api_url = self.config.get_secret("custom_api_url")
        upload_url = api_url.rstrip('/') + '/upload'

        try:
            local_path = Path(local_dir_path)
            if not local_path.exists():
                self.log.error(f"Lokaler Pfad existiert nicht: {local_dir_path}")
                signals.upload_failed.emit("Pfad existiert nicht")
                return False

            # Alle Dateien im Verzeichnis sammeln
            all_files = []
            for file_path in local_path.rglob('*'):
                if file_path.is_file():
                    # Ignoriere Marker-Dateien
                    if file_path.name in ("_in_verarbeitung.txt", "_fertig.txt"):
                        continue
                    all_files.append(file_path)

            if not all_files:
                self.log.warning(f"Keine Dateien im Verzeichnis gefunden: {local_dir_path}")
                signals.upload_failed.emit("Keine Dateien gefunden")
                return False

            total_files = len(all_files)
            self.log.info(f"Starte Upload von {total_files} Datei(en) zur cloud.kowalenko.io...")
            signals.upload_started.emit(total_files)

            # Bereite alle Dateien für Multipart-Upload vor
            files_to_upload = []
            try:
                for idx, file_path in enumerate(all_files, start=1):
                    # Relativen Pfad berechnen (vom Parent des local_path)
                    # z.B. local_path = "20251031_Event" -> parent ist das Verzeichnis darüber
                    # Relativer Pfad enthält dann "20251031_Event/Outside_Foto/1.png"
                    rel_path = file_path.relative_to(local_path.parent)

                    # MIME-Type ermitteln
                    mime_type, _ = mimetypes.guess_type(str(file_path))

                    self.log.info(f"Bereite Datei vor ({idx}/{total_files}): {rel_path}")
                    signals.upload_progress.emit(f"Datei {idx}/{total_files}: {file_path.name}")

                    # Datei öffnen und zur Liste hinzufügen
                    # Format: ('files', (filename_mit_pfad, file_object, mime_type))
                    files_to_upload.append((
                        'files',
                        (str(rel_path), open(file_path, 'rb'), mime_type or 'application/octet-stream')
                    ))

                # ALLE Dateien auf einmal hochladen (cloud.kowalenko.io erwartet das so)
                self.log.info(f"Sende {total_files} Dateien zum Server...")
                signals.upload_progress.emit(f"Sende {total_files} Dateien...")

                response = self.session.post(
                    upload_url,
                    files=files_to_upload,
                    timeout=600  # 10 Minuten für große Uploads
                )

                if response.status_code not in [200, 201]:
                    error_msg = response.text
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('error', error_msg)
                    except:
                        pass

                    self.log.error(f"Upload fehlgeschlagen: HTTP {response.status_code} - {error_msg}")
                    signals.upload_failed.emit(f"HTTP {response.status_code}: {error_msg}")
                    return False

                # Erfolgreiche Antwort verarbeiten
                result = response.json()
                self._last_order_id = result.get('order_id')
                customer_url = result.get('customer_url')
                session_id = result.get('session_id')

                self.log.info(f"Upload erfolgreich abgeschlossen!")
                self.log.info(f"  Order ID: {self._last_order_id}")
                self.log.info(f"  Session ID: {session_id}")
                self.log.info(f"  Customer URL: {customer_url}")

                signals.upload_finished.emit(f"Upload erfolgreich: {total_files} Dateien")

                # Optional: Customer URL in die Zwischenablage oder zurückgeben
                if customer_url:
                    signals.upload_progress.emit(f"Customer URL: {customer_url}")

                return True

            finally:
                # Alle geöffneten Dateien schließen
                for _, file_tuple in files_to_upload:
                    try:
                        file_tuple[1].close()
                    except:
                        pass

        except Exception as e:
            self.log.error(f"Fehler beim Verzeichnis-Upload: {e}", exc_info=True)
            signals.upload_failed.emit(str(e))
            return False

    def get_shareable_link(self, remote_path):
        """
        Erstellt einen öffentlichen Freigabelink für die letzte hochgeladene Order.

        Für cloud.kowalenko.io:
        - Der "Share-Link" ist die Customer-URL, die beim Upload zurückgegeben wird
        - Format: https://cloud.kowalenko.io/content/{order_id}
        - Alternativ kann get-share-link/{order_id} verwendet werden

        Args:
            remote_path: Wird ignoriert, da cloud.kowalenko.io mit Order-IDs arbeitet

        Gibt den Link-String oder None bei einem Fehler zurück.
        """
        if not self._is_connected or not self.session:
            self.log.error("Nicht mit Cloud API verbunden. Kann keinen Link erstellen.")
            return None

        if not self._last_order_id:
            self.log.error("Keine Order-ID verfügbar. Bitte erst einen Upload durchführen.")
            return None

        api_url = self.config.get_secret("custom_api_url")

        # Versuche, den offiziellen Share-Link-Endpoint zu verwenden
        share_url = api_url.rstrip('/') + f'/get-share-link/{self._last_order_id}'

        try:
            self.log.info(f"Hole Share-Link für Order: {self._last_order_id}")

            response = self.session.get(share_url, timeout=30)

            if response.status_code in [200, 201]:
                result = response.json()

                # API gibt share_url zurück
                share_link = result.get('share_url') or result.get('customer_url') or result.get('url')

                if share_link:
                    self.log.info(f"Share-Link erhalten: {share_link}")

                    # Optional: Link kürzen, falls SkyLink konfiguriert ist
                    try:
                        shortened_link = self.link_shortener.shorten(share_link)
                        if shortened_link and shortened_link != share_link:
                            self.log.info(f"Link gekürzt: {shortened_link}")
                            return shortened_link
                    except Exception as e:
                        self.log.warning(f"Link-Kürzung fehlgeschlagen, verwende Original: {e}")

                    return share_link
                else:
                    self.log.error("API-Antwort enthält keinen Share-Link")
                    return None
            else:
                self.log.error(f"Fehler beim Abrufen des Share-Links: HTTP {response.status_code}")

                # Fallback: Erstelle die Customer-URL manuell
                base_url = api_url.replace('/api', '')  # Entferne /api aus der URL
                fallback_url = f"{base_url}/content/{self._last_order_id}"
                self.log.info(f"Verwende Fallback Customer-URL: {fallback_url}")
                return fallback_url

        except Exception as e:
            self.log.error(f"Fehler beim Abrufen des Share-Links: {e}")

            # Fallback: Erstelle die Customer-URL manuell
            try:
                base_url = api_url.replace('/api', '')
                fallback_url = f"{base_url}/content/{self._last_order_id}"
                self.log.info(f"Verwende Fallback Customer-URL: {fallback_url}")
                return fallback_url
            except:
                return None



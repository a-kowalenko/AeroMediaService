import os
import logging
import requests
from pathlib import Path
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from utils.link_shortener import LinkShortener


class CustomApiClient(BaseClient):
    """
    Implementierung des BaseClient für eine benutzerdefinierte API mit Bearer Token Authentifizierung.
    Ermöglicht das Hochladen von Dateien in eine beliebige Cloud über eine eigene API.
    """

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.session: requests.Session = None
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)
        self._is_connected = False

    def connect(self, auth_callback=None):
        """
        Stellt die Verbindung zur Custom API her.
        Validiert die Verbindung durch einen Test-Request an den Health-/Status-Endpoint.
        """
        api_url = self.config.get_secret("custom_api_url")
        bearer_token = self.config.get_secret("custom_api_bearer_token")

        if not api_url or not bearer_token:
            self.log.warning("Custom API URL oder Bearer Token fehlen.")
            signals.connection_status_changed.emit("Fehler: API URL/Token fehlt")
            return False

        # Session mit Bearer Token initialisieren
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "AeroMediaService/1.0"
        })

        # Verbindung testen
        try:
            self.log.info("Teste Verbindung zur Custom API...")

            # Verwende den konfigurierten Health-Check Endpoint (falls vorhanden)
            health_endpoint = self.config.get_setting("custom_api_health_endpoint", "/health")
            test_url = api_url.rstrip('/') + '/' + health_endpoint.lstrip('/')

            response = self.session.get(test_url, timeout=10)

            if response.status_code == 200:
                self.log.info("Erfolgreich mit Custom API verbunden.")
                self._is_connected = True
                signals.connection_status_changed.emit("Verbunden")
                return True
            else:
                self.log.warning(f"Custom API antwortet mit Status {response.status_code}")
                signals.connection_status_changed.emit(f"Fehler: HTTP {response.status_code}")
                self._is_connected = False
                return False

        except requests.exceptions.ConnectionError as e:
            self.log.error(f"Verbindungsfehler zur Custom API: {e}")
            signals.connection_status_changed.emit("Verbindungsfehler")
            self._is_connected = False
            return False
        except requests.exceptions.Timeout:
            self.log.error("Timeout bei der Verbindung zur Custom API")
            signals.connection_status_changed.emit("Timeout")
            self._is_connected = False
            return False
        except Exception as e:
            self.log.error(f"Unerwarteter Fehler bei Custom API Verbindung: {e}")
            signals.connection_status_changed.emit(f"Fehler: {e}")
            self._is_connected = False
            return False

    def disconnect(self):
        """Trennt die Verbindung zur Custom API."""
        if self.session:
            self.session.close()
            self.session = None

        self._is_connected = False
        self.log.info("Custom API Verbindung getrennt.")
        signals.connection_status_changed.emit("Nicht verbunden")

    def get_connection_status(self):
        """Gibt einen String zurück, der den aktuellen Verbindungsstatus beschreibt."""
        if self._is_connected and self.session:
            return "Verbunden"
        else:
            return "Nicht verbunden"

    def upload_directory(self, local_dir_path, remote_base_path):
        """
        Lädt ein komplettes Verzeichnis zur Custom API hoch.
        Verwendet Signale aus core.signals, um den Fortschritt zu melden.
        """
        if not self._is_connected or not self.session:
            self.log.error("Nicht mit Custom API verbunden. Upload abgebrochen.")
            signals.upload_failed.emit("Nicht verbunden")
            return False

        api_url = self.config.get_secret("custom_api_url")
        upload_endpoint = self.config.get_setting("custom_api_upload_endpoint", "/upload")
        upload_url = api_url.rstrip('/') + '/' + upload_endpoint.lstrip('/')

        try:
            local_path = Path(local_dir_path)
            if not local_path.exists():
                self.log.error(f"Lokaler Pfad existiert nicht: {local_dir_path}")
                signals.upload_failed.emit("Pfad existiert nicht")
                return False

            # Alle Dateien im Verzeichnis sammeln
            all_files = []
            for root, dirs, files in os.walk(local_dir_path):
                for file in files:
                    file_path = Path(root) / file
                    all_files.append(file_path)

            if not all_files:
                self.log.warning(f"Keine Dateien im Verzeichnis gefunden: {local_dir_path}")
                signals.upload_failed.emit("Keine Dateien gefunden")
                return False

            total_files = len(all_files)
            self.log.info(f"Starte Upload von {total_files} Datei(en) zur Custom API...")
            signals.upload_started.emit(total_files)

            # Jede Datei hochladen
            for idx, file_path in enumerate(all_files, start=1):
                # Relativen Pfad berechnen
                rel_path = file_path.relative_to(local_path)
                remote_path = f"{remote_base_path}/{rel_path.as_posix()}"

                self.log.info(f"Lade Datei hoch ({idx}/{total_files}): {file_path.name}")
                signals.upload_progress.emit(f"Datei {idx}/{total_files}: {file_path.name}")

                # Datei hochladen
                try:
                    with open(file_path, 'rb') as f:
                        # Multipart-Upload
                        files = {'file': (file_path.name, f, 'application/octet-stream')}
                        data = {
                            'remote_path': remote_path,
                            'base_path': remote_base_path
                        }

                        response = self.session.post(upload_url, files=files, data=data, timeout=300)

                        if response.status_code not in [200, 201]:
                            self.log.error(f"Upload fehlgeschlagen für {file_path.name}: HTTP {response.status_code}")
                            signals.upload_failed.emit(f"HTTP {response.status_code} für {file_path.name}")
                            return False

                        self.log.debug(f"Datei erfolgreich hochgeladen: {file_path.name}")

                except Exception as e:
                    self.log.error(f"Fehler beim Upload von {file_path.name}: {e}")
                    signals.upload_failed.emit(f"Fehler bei {file_path.name}")
                    return False

            self.log.info(f"Upload von {total_files} Datei(en) erfolgreich abgeschlossen.")
            signals.upload_finished.emit(f"Upload abgeschlossen: {total_files} Dateien")
            return True

        except Exception as e:
            self.log.error(f"Fehler beim Verzeichnis-Upload: {e}")
            signals.upload_failed.emit(str(e))
            return False

    def get_shareable_link(self, remote_path):
        """
        Erstellt einen öffentlichen Freigabelink für einen Pfad über die Custom API.
        Gibt den Link-String oder None bei einem Fehler zurück.
        """
        if not self._is_connected or not self.session:
            self.log.error("Nicht mit Custom API verbunden. Kann keinen Link erstellen.")
            return None

        api_url = self.config.get_secret("custom_api_url")
        share_endpoint = self.config.get_setting("custom_api_share_endpoint", "/share")
        share_url = api_url.rstrip('/') + '/' + share_endpoint.lstrip('/')

        try:
            self.log.info(f"Erstelle Freigabelink für: {remote_path}")

            data = {'remote_path': remote_path}
            response = self.session.post(share_url, json=data, timeout=30)

            if response.status_code in [200, 201]:
                result = response.json()

                # Je nach API-Struktur kann der Link unterschiedlich heißen
                share_link = result.get('share_link') or result.get('url') or result.get('link')

                if share_link:
                    self.log.info(f"Freigabelink erstellt: {share_link}")

                    # Optional: Link kürzen, falls SkyLink konfiguriert ist
                    try:
                        shortened_link = self.link_shortener.shorten_link(share_link)
                        if shortened_link:
                            self.log.info(f"Link gekürzt: {shortened_link}")
                            return shortened_link
                    except Exception as e:
                        self.log.warning(f"Link-Kürzung fehlgeschlagen, verwende Original: {e}")

                    return share_link
                else:
                    self.log.error("API-Antwort enthält keinen Share-Link")
                    return None
            else:
                self.log.error(f"Fehler beim Erstellen des Share-Links: HTTP {response.status_code}")
                return None

        except Exception as e:
            self.log.error(f"Fehler beim Erstellen des Freigabelinks: {e}")
            return None


import dropbox
import os
import logging
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from utils.link_shortener import LinkShortener

# Maximale Chunk-Größe für Dropbox-Uploads (8 MB)
CHUNK_SIZE = 8 * 1024 * 1024


class DropboxClient(BaseClient):
    """Implementierung des BaseClient für Dropbox."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.dbx: dropbox.Dropbox = None
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)

    def connect(self, auth_callback=None):
        """
        Versucht, sich mit Dropbox zu verbinden.
        1. Versucht, einen gespeicherten Refresh-Token zu verwenden.
        2. Wenn nicht vorhanden, startet der OAuth-Flow und verwendet das auth_callback.
        """
        app_key = self.config.get_secret("db_app_key")
        app_secret = self.config.get_secret("db_app_secret")
        refresh_token = self.config.get_secret("db_refresh_token")

        if not app_key or not app_secret:
            self.log.warning("App Key oder App Secret für Dropbox fehlen.")
            signals.connection_status_changed.emit("Fehler: App Key/Secret fehlt")
            return False

        # 1. Versuchen, sich mit Refresh-Token zu verbinden
        if refresh_token:
            self.log.info("Versuche Verbindung mit Refresh-Token...")
            try:
                self.dbx = dropbox.Dropbox(
                    app_key=app_key,
                    app_secret=app_secret,
                    oauth2_refresh_token=refresh_token
                )
                self.dbx.users_get_current_account()  # Testet die Verbindung
                self.log.info("Erfolgreich mit Dropbox verbunden (via Refresh-Token).")
                signals.connection_status_changed.emit("Verbunden")
                return True
            except dropbox.exceptions.AuthError as e:
                self.log.warning(f"Refresh-Token ungültig, starte OAuth-Flow: {e}")
                self.config.delete_secret("db_refresh_token")  # Ungültigen Token löschen
                self.dbx = None
            except Exception as e:
                self.log.error(f"Verbindungsfehler mit Refresh-Token: {e}")
                signals.connection_status_changed.emit(f"Verbindungsfehler: {e}")
                self.dbx = None
                return False

        # 2. OAuth-Flow starten, wenn kein (gültiger) Token vorhanden ist
        self.log.info("Kein Refresh-Token, starte OAuth-Flow...")
        if not auth_callback:
            self.log.error("Auth-Callback fehlt, um OAuth-Flow abzuschließen.")
            signals.connection_status_changed.emit("Fehler: Auth-Callback fehlt")
            return False

        try:
            auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(app_key, app_secret, use_pkce=True, token_access_type='offline')
            authorize_url = auth_flow.start()

            # Das auth_callback (aus dem Settings-Dialog) muss nun den Benutzer
            # zu dieser URL schicken und den resultierenden Code abfragen.
            auth_code = auth_callback(authorize_url)  # Blockierender Aufruf

            if not auth_code:
                self.log.warning("OAuth-Flow vom Benutzer abgebrochen.")
                signals.connection_status_changed.emit("Nicht verbunden (Abbruch)")
                return False

            # Auth-Code gegen Token tauschen
            oauth_result = auth_flow.finish(auth_code)

            # Refresh-Token sicher speichern
            self.config.save_secret("db_refresh_token", oauth_result.refresh_token)

            # Dropbox-Client initialisieren
            self.dbx = dropbox.Dropbox(oauth2_access_token=oauth_result.access_token)
            account_info = self.dbx.users_get_current_account()
            self.log.info(f"Erfolgreich mit Dropbox-Konto verbunden: {account_info.name.display_name}")

            # WICHTIG: Erneutes Initialisieren mit Refresh-Token für Langzeit-Zugriff
            self.dbx = dropbox.Dropbox(
                app_key=app_key,
                app_secret=app_secret,
                oauth2_refresh_token=oauth_result.refresh_token
            )
            signals.connection_status_changed.emit("Verbunden")
            return True

        except Exception as e:
            self.log.error(f"Fehler während des OAuth-Flows: {e}")
            signals.connection_status_changed.emit(f"OAuth-Fehler: {e}")
            return False

    def disconnect(self):
        """Trennt die Verbindung und löscht den Refresh-Token."""
        self.log.info("Trenne Verbindung zu Dropbox...")
        self.config.delete_secret("db_refresh_token")
        if self.dbx:
            try:
                self.dbx.auth_token_revoke()
            except Exception as e:
                self.log.warning(f"Fehler beim Revoken des Tokens (Token evtl. schon ungültig): {e}")
        self.dbx = None
        signals.connection_status_changed.emit("Nicht verbunden")
        self.log.info("Verbindung getrennt.")

        # beende Monitoring
        signals.stop_monitoring.emit()

    def get_connection_status(self):
        """Prüft die Verbindung und gibt den Status zurück."""
        if not self.dbx:
            return "Nicht verbunden"
        try:
            self.dbx.users_get_current_account()
            return "Verbunden"
        except Exception as e:
            self.log.error(f"Verbindungsprüfung fehlgeschlagen: {e}")
            return "Verbindungsfehler"

    def upload_directory(self, local_dir_path, remote_base_path):
        """Lädt ein Verzeichnis rekursiv hoch und meldet den Fortschritt."""
        if not self.dbx:
            self.log.error("Upload fehlgeschlagen: Nicht mit Dropbox verbunden.")
            return False

        self.log.info(f"Beginne Upload von '{local_dir_path}' nach '{remote_base_path}'")

        # 1. Alle Dateien sammeln und Gesamtgröße berechnen
        files_to_upload = []
        total_size = 0
        for root, _, files in os.walk(local_dir_path):
            for file in files:
                local_path = os.path.join(root, file)
                # Ignoriere die Marker-Datei
                if file == "_fertig.txt" or file == "_in_verarbeitung.txt":
                    continue

                # Relativen Pfad für Dropbox berechnen
                relative_path = os.path.relpath(local_path, local_dir_path)
                dropbox_path = f"{remote_base_path}/{relative_path}".replace(os.path.sep, '/')

                try:
                    file_size = os.path.getsize(local_path)
                    files_to_upload.append((local_path, dropbox_path, file_size))
                    total_size += file_size
                except FileNotFoundError:
                    self.log.warning(f"Datei nicht gefunden, überspringe: {local_path}")

        if total_size == 0:
            self.log.warning("Keine Dateien (oder nur leere Dateien) zum Hochladen gefunden.")
            signals.upload_progress_total.emit(100, 0, 0)
            return True  # Technisch gesehen erfolgreich, da nichts zu tun war

        # 2. Dateien hochladen und Fortschritt melden
        bytes_uploaded = 0
        try:
            for local_path, dropbox_path, file_size in files_to_upload:
                status_msg = f"Lade hoch: {os.path.basename(local_path)} ({file_size / 1024 ** 2:.2f} MB)"
                signals.upload_status_update.emit(status_msg)
                self.log.debug(status_msg)

                file_size_in_mb = file_size / 1024 ** 2

                signals.upload_progress_file.emit(0, 0, file_size_in_mb)

                with open(local_path, 'rb') as f:
                    if file_size <= CHUNK_SIZE:
                        # Einfacher Upload für kleine Dateien
                        self.dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
                        signals.upload_progress_file.emit(100, file_size_in_mb, file_size_in_mb)
                        total_progress = int((bytes_uploaded / total_size) * 100)
                        signals.upload_progress_total.emit(total_progress, bytes_uploaded, total_size)
                    else:
                        # Session-Upload für große Dateien
                        self._upload_large_file(f, dropbox_path, file_size, bytes_uploaded, total_size)
                    bytes_uploaded += file_size



            signals.upload_status_update.emit(f"Upload für '{remote_base_path}' abgeschlossen.")
            self.log.info(f"Upload für '{remote_base_path}' abgeschlossen.")
            return True

        except Exception as e:
            self.log.error(f"Fehler beim Upload von {local_path}: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

    def _upload_large_file(self, f, dropbox_path, file_size, base_bytes_uploaded, total_job_size):
        """Privater Helfer für große Datei-Uploads mit Fortschritt."""
        session_start_result = self.dbx.files_upload_session_start(f.read(CHUNK_SIZE))
        session_id = session_start_result.session_id
        cursor = dropbox.files.UploadSessionCursor(session_id=session_id, offset=f.tell())

        bytes_sent = f.tell()
        file_progress = int((bytes_sent / file_size) * 100)
        signals.upload_progress_file.emit(file_progress, bytes_sent, total_job_size)

        current_total_bytes = base_bytes_uploaded + bytes_sent
        total_progress = int((current_total_bytes / total_job_size) * 100)
        signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)

        while (file_size - f.tell()) > CHUNK_SIZE:
            chunk = f.read(CHUNK_SIZE)
            self.dbx.files_upload_session_append_v2(chunk, cursor)
            cursor.offset = f.tell()

            bytes_sent = f.tell()
            file_progress = int((bytes_sent / file_size) * 100)
            signals.upload_progress_file.emit(file_progress, bytes_sent, file_size)

            current_total_bytes = base_bytes_uploaded + bytes_sent
            total_progress = int((current_total_bytes / total_job_size) * 100)
            signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)

        # Letztes Stück hochladen
        chunk = f.read()
        commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        self.dbx.files_upload_session_finish(chunk, cursor, commit)
        signals.upload_progress_file.emit(100, file_size, file_size)

        current_total_bytes = base_bytes_uploaded + file_size
        total_progress = int((current_total_bytes / total_job_size) * 100)
        signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)

    def get_shareable_link(self, remote_path):
        """Erstellt einen Freigabelink für das hochgeladene Verzeichnis."""
        if not self.dbx:
            self.log.error("Link-Erstellung fehlgeschlagen: Nicht verbunden.")
            return None

        try:
            self.log.info(f"Erstelle Freigabelink für: {remote_path}")
            # Prüfen, ob bereits ein Link existiert
            links = self.dbx.sharing_list_shared_links(path=remote_path).links
            if links:
                self.log.debug("Link existiert bereits, verwende existierenden Link.")
                # Link kürzen und zurückgeben
                return self.link_shortener.shorten(links[0].url)

            # Neuen Link erstellen
            settings = dropbox.sharing.SharedLinkSettings(
                requested_visibility=dropbox.sharing.RequestedVisibility.public)
            link = self.dbx.sharing_create_shared_link_with_settings(remote_path, settings=settings)
            self.log.info(f"Link erfolgreich erstellt: {link.url}")
            # Link kürzen und zurückgeben
            return self.link_shortener.shorten(link.url)

        except dropbox.exceptions.ApiError as e:
            error_message = str(e)
            if "shared_link_already_exists" in error_message:
                self.log.warning("API-Fehler 'Link existiert bereits', versuche Abruf...")
                try:
                    # Workaround: Manchmal schlägt der erste Check fehl
                    links = self.dbx.sharing_list_shared_links(path=remote_path).links
                    if links:
                        # Link kürzen und zurückgeben
                        return self.link_shortener.shorten(links[0].url)
                except Exception as e2:
                    self.log.error(f"Fehler beim Abrufen des existierenden Links: {e2}")
                    return None # Bei Fehler den Kürzer nicht aufrufen
            self.log.error(f"Dropbox API Fehler bei Link-Erstellung: {e}")
            return None
        except Exception as e:
            self.log.error(f"Allgemeiner Fehler bei Link-Erstellung: {e}")
            return None

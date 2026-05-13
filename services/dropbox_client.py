import dropbox
import os
import logging
import time

import requests

from models.kunde import Kunde
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from core.upload_control import UploadCancelled
from utils.link_shortener import LinkShortener
from utils.upload_checkpoint import (
    clear_checkpoint,
    load_checkpoint,
    manifest_fingerprint,
    save_checkpoint,
    should_skip_upload_file,
)

# Maximale Chunk-Größe für Dropbox-Uploads (8 MB)
CHUNK_SIZE = 8 * 1024 * 1024


class DropboxClient(BaseClient):
    """Implementierung des BaseClient für Dropbox."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.dbx: dropbox.Dropbox = None
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)
        self._upload_control = None

    def _upload_coop_tick(self):
        ctl = getattr(self, "_upload_control", None)
        if ctl:
            ctl.wait_if_paused()

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

    def _should_retry_dropbox_error(self, exc):
        """True bei typischen transienten Fehlern (Rate-Limit, Netz, 5xx)."""
        if isinstance(
            exc,
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError,
            ),
        ):
            return True
        http_err_cls = getattr(dropbox.exceptions, "HttpError", None)
        if http_err_cls is not None and isinstance(exc, http_err_cls):
            return True
        if isinstance(exc, dropbox.exceptions.ApiError):
            err = getattr(exc, "error", None)
            if err is not None:
                for name in ("is_rate_limit", "is_internal_error"):
                    pred = getattr(err, name, None)
                    if callable(pred):
                        try:
                            if pred():
                                return True
                        except Exception:
                            pass
            lowered = str(exc).lower()
            if any(
                s in lowered
                for s in ("too_many_requests", "rate_limit", "internal_server", "503", "502", "504")
            ):
                return True
        return False

    def _with_dropbox_retry(self, tag, operation, max_attempts=5):
        """Führt eine Dropbox-API-Operation mit begrenzten Wiederholungen aus."""
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            try:
                return operation()
            except dropbox.exceptions.AuthError:
                raise
            except Exception as e:
                last_exc = e
                if attempt >= max_attempts or not self._should_retry_dropbox_error(e):
                    raise
                delay = min(60.0, 2.0 ** attempt)
                self.log.warning(
                    "%s: Versuch %s/%s fehlgeschlagen, warte %.1fs — %s",
                    tag,
                    attempt,
                    max_attempts,
                    delay,
                    e,
                )
                time.sleep(delay)
        raise last_exc

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

    def upload_directory(self, local_dir_path, remote_base_path, kunde: Kunde = None, control=None):
        """Lädt ein Verzeichnis rekursiv hoch und meldet den Fortschritt."""
        self._upload_control = control
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
                if should_skip_upload_file(file):
                    continue

                # Relativen Pfad für Dropbox berechnen
                relative_path = os.path.relpath(local_path, local_dir_path)
                dropbox_path = f"{remote_base_path}/{relative_path}".replace(os.path.sep, '/')

                try:
                    file_size = os.path.getsize(local_path)
                    rel_norm = relative_path.replace(os.path.sep, '/')
                    files_to_upload.append((local_path, dropbox_path, file_size, rel_norm))
                    total_size += file_size
                except FileNotFoundError:
                    self.log.warning(f"Datei nicht gefunden, überspringe: {local_path}")

        if total_size == 0:
            self.log.warning("Keine Dateien (oder nur leere Dateien) zum Hochladen gefunden.")
            signals.upload_progress_total.emit(100, 0, 0)
            return True  # Technisch gesehen erfolgreich, da nichts zu tun war

        files_to_upload.sort(key=lambda t: t[3])
        manifest = [{"name": t[3], "size": t[2]} for t in files_to_upload]
        manifest_fp = manifest_fingerprint(manifest)

        raw_ck = load_checkpoint(local_dir_path)
        resume_ck = None
        if (
            raw_ck
            and raw_ck.get("kind") == "dropbox_native"
            and raw_ck.get("manifest_fp") == manifest_fp
            and raw_ck.get("remote_base_path") == remote_base_path
        ):
            resume_ck = raw_ck
        elif raw_ck:
            self.log.warning("Dropbox-Native-Checkpoint verworfen.")
            clear_checkpoint(local_dir_path)

        start_idx = 0
        resume_db = None
        if resume_ck:
            start_idx = min(int(resume_ck.get("next_file_index", 0)), len(files_to_upload))
            da = resume_ck.get("db_active") or {}
            if start_idx < len(files_to_upload) and da.get("rel_path") == files_to_upload[start_idx][3]:
                if int(da.get("offset", 0)) > 0 and da.get("session_id"):
                    resume_db = {
                        "session_id": str(da["session_id"]),
                        "offset": int(da["offset"]),
                        "rel_path": da["rel_path"],
                        "dropbox_path": da.get("dropbox_path") or files_to_upload[start_idx][1],
                    }
            self.log.info(
                "Dropbox-Upload fortsetzen (next_file_index=%s).",
                start_idx,
            )

        bytes_uploaded = (
            sum(t[2] for t in files_to_upload[:start_idx]) if start_idx else 0
        )

        def save_native_ck(**kwargs):
            payload = {
                "kind": "dropbox_native",
                "manifest_fp": manifest_fp,
                "remote_base_path": remote_base_path,
                "total_size": total_size,
                "phase": "uploading",
            }
            payload.update(kwargs)
            save_checkpoint(local_dir_path, payload)

        if resume_ck is None:
            save_native_ck(
                next_file_index=0,
                db_active=None,
            )

        outer_resume = resume_db

        # 2. Dateien hochladen und Fortschritt melden
        local_path = ""
        try:
            for i in range(start_idx, len(files_to_upload)):
                local_path, dropbox_path, file_size, rel_norm = files_to_upload[i]
                self._upload_coop_tick()
                ro = outer_resume if i == start_idx else None

                status_msg = f"Lade hoch: {os.path.basename(local_path)} ({file_size / 1024 ** 2:.2f} MB)"
                signals.upload_status_update.emit(status_msg)
                self.log.debug(status_msg)

                file_size_in_mb = file_size / 1024 ** 2

                signals.upload_progress_file.emit(0, 0, file_size_in_mb)

                with open(local_path, 'rb') as f:
                    if file_size <= CHUNK_SIZE:
                        self._upload_coop_tick()
                        data = f.read()

                        def _do_small_upload(data_blob=data, path=dropbox_path):
                            return self.dbx.files_upload(
                                data_blob, path, mode=dropbox.files.WriteMode.overwrite
                            )

                        self._with_dropbox_retry(
                            f"files_upload:{os.path.basename(local_path)}",
                            _do_small_upload,
                        )
                        signals.upload_progress_file.emit(100, file_size_in_mb, file_size_in_mb)
                        total_progress = int((bytes_uploaded / total_size) * 100)
                        signals.upload_progress_total.emit(total_progress, bytes_uploaded, total_size)
                    else:
                        def on_db_progress(cursor, _rel=rel_norm, _dp=dropbox_path, _i=i):
                            if cursor is None:
                                save_native_ck(next_file_index=_i, db_active=None)
                            else:
                                save_native_ck(
                                    next_file_index=_i,
                                    db_active={
                                        "rel_path": _rel,
                                        "session_id": str(cursor.session_id),
                                        "offset": int(cursor.offset),
                                        "dropbox_path": _dp,
                                    },
                                )

                        self._upload_large_file(
                            f,
                            dropbox_path,
                            file_size,
                            bytes_uploaded,
                            total_size,
                            resume=ro,
                            rel_path=rel_norm,
                            on_progress_save=on_db_progress,
                        )
                    bytes_uploaded += file_size

                save_native_ck(
                    next_file_index=i + 1,
                    db_active=None,
                )

            clear_checkpoint(local_dir_path)
            signals.upload_status_update.emit(f"Upload für '{remote_base_path}' abgeschlossen.")
            self.log.info(f"Upload für '{remote_base_path}' abgeschlossen.")
            return True

        except UploadCancelled:
            raise
        except Exception as e:
            self.log.error(f"Fehler beim Upload von {local_path}: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

    def _upload_large_file(
        self,
        f,
        dropbox_path,
        file_size,
        base_bytes_uploaded,
        total_job_size,
        *,
        resume: dict | None = None,
        rel_path: str = "",
        on_progress_save=None,
    ):
        """Große Datei per Session-Upload; optional Fortsetzung mit resume (session_id, offset)."""

        cursor = None

        if (
            resume
            and int(resume.get("offset", 0)) > 0
            and str(resume.get("rel_path", "")) == rel_path
        ):
            off = int(resume["offset"])
            if off > file_size:
                raise RuntimeError(f"{rel_path}: Resume-Offset {off} > Dateigröße {file_size}")
            f.seek(off)
            cursor = dropbox.files.UploadSessionCursor(
                session_id=str(resume["session_id"]),
                offset=off,
            )
            self.log.info(
                "Dropbox-Session wird bei Byte %s fortgesetzt (session_id=%r).",
                off,
                resume.get("session_id"),
            )
            if on_progress_save:
                on_progress_save(cursor)
        else:
            self._upload_coop_tick()

            def _session_start():
                f.seek(0)
                return self.dbx.files_upload_session_start(f.read(CHUNK_SIZE))

            session_start_result = self._with_dropbox_retry("files_upload_session_start", _session_start)
            session_id = session_start_result.session_id
            cursor = dropbox.files.UploadSessionCursor(session_id=session_id, offset=f.tell())
            if on_progress_save:
                on_progress_save(cursor)

        self._upload_coop_tick()
        bytes_sent = f.tell()
        file_progress = int((bytes_sent / file_size) * 100)
        signals.upload_progress_file.emit(file_progress, bytes_sent, total_job_size)

        current_total_bytes = base_bytes_uploaded + bytes_sent
        total_progress = int((current_total_bytes / total_job_size) * 100)
        signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)

        self._upload_coop_tick()
        while (file_size - f.tell()) > CHUNK_SIZE:
            self._upload_coop_tick()
            chunk_pos = f.tell()

            def _append():
                f.seek(chunk_pos)
                chunk = f.read(CHUNK_SIZE)
                return self.dbx.files_upload_session_append_v2(chunk, cursor)

            self._with_dropbox_retry("files_upload_session_append_v2", _append)
            cursor.offset = f.tell()

            bytes_sent = f.tell()
            file_progress = int((bytes_sent / file_size) * 100)
            signals.upload_progress_file.emit(file_progress, bytes_sent, file_size)

            current_total_bytes = base_bytes_uploaded + bytes_sent
            total_progress = int((current_total_bytes / total_job_size) * 100)
            signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)
            if on_progress_save:
                on_progress_save(cursor)

        self._upload_coop_tick()
        finish_pos = f.tell()

        def _finish():
            f.seek(finish_pos)
            chunk = f.read()
            commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            return self.dbx.files_upload_session_finish(chunk, cursor, commit)

        self._with_dropbox_retry("files_upload_session_finish", _finish)
        if on_progress_save:
            on_progress_save(None)
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
            links_result = self._with_dropbox_retry(
                "sharing_list_shared_links",
                lambda: self.dbx.sharing_list_shared_links(path=remote_path),
            )
            links = links_result.links
            if links:
                self.log.debug("Link existiert bereits, verwende existierenden Link.")
                # Link kürzen und zurückgeben
                return self.link_shortener.shorten(links[0].url)

            # Neuen Link erstellen
            settings = dropbox.sharing.SharedLinkSettings(
                requested_visibility=dropbox.sharing.RequestedVisibility.public)
            link = self._with_dropbox_retry(
                "sharing_create_shared_link_with_settings",
                lambda: self.dbx.sharing_create_shared_link_with_settings(remote_path, settings=settings),
            )
            self.log.info(f"Link erfolgreich erstellt: {link.url}")
            # Link kürzen und zurückgeben
            return self.link_shortener.shorten(link.url)

        except dropbox.exceptions.ApiError as e:
            error_message = str(e)
            if "shared_link_already_exists" in error_message:
                self.log.warning("API-Fehler 'Link existiert bereits', versuche Abruf...")
                try:
                    # Workaround: Manchmal schlägt der erste Check fehl
                    links_result = self._with_dropbox_retry(
                        "sharing_list_shared_links_retry",
                        lambda: self.dbx.sharing_list_shared_links(path=remote_path),
                    )
                    links = links_result.links
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

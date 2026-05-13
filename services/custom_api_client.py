import os
import json
import random
import requests
import dropbox
from requests.adapters import HTTPAdapter
import logging
import time
from threading import Lock

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

# Entspricht Server PROXIED_UPLOAD_CHUNK_BYTES; Vercel-Multipart-Limit ~4.5 MiB Request-Body.
CHUNK_BYTES = 4 * 1024 * 1024
# Direkter Dropbox-Upload soll wie der DropboxClient mit 8 MiB arbeiten.
DROPBOX_CHUNK_BYTES = 8 * 1024 * 1024


class ApiAuthError(Exception):
    """401/403: ungueltiger Key oder fehlende 'upload'-Permission — kein Retry."""


def _body_suggests_invocation_timeout(text: str) -> bool:
    if not text:
        return False
    return "FUNCTION_INVOCATION_TIMEOUT" in text.upper()


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


class CustomApiClient(BaseClient):
    """Custom Cloud API: direct-init, chunked Session-Upload (4 MB), finalize."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.api_base_url = None
        self.api_key = None
        self.connected = False
        self.log = logging.getLogger(__name__)
        self.link_shortener = LinkShortener(config_manager)
        self._last_customer_url = None  # Speichert die letzte customer_url
        self._last_session_id = None  # Letzte Upload-Session fuer spaeteren Link-Check
        self._last_order_id = None  # order_id aus direct-init / finalize
        self._last_kunde = None  # Letzter Kunde fuer Customer-API-Fallback
        self.session = None  # requests.Session für Connection-Pooling
        self.progress_lock = Lock()  # Lock für Thread-sichere Progress-Updates
        self.dbx: dropbox.Dropbox | None = None
        self._upload_control = None

    def _upload_coop_tick(self):
        ctl = getattr(self, "_upload_control", None)
        if ctl:
            ctl.wait_if_paused()

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
                self.log.info("Erfolgreich mit Custom API verbunden (Session-Chunk-Upload).")
                signals.connection_status_changed.emit("Verbunden")
                return True
            if response.status_code in (401, 403):
                msg = "Bearer-Token ungueltig oder ohne 'upload'-Permission"
                snippet = (response.text or "")[:200]
                self.log.error("API Connection: %s — %s", msg, snippet)
                signals.connection_status_changed.emit(f"Fehler: {msg}")
                return False
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
        self.dbx = None
        self.connected = False
        signals.connection_status_changed.emit("Nicht verbunden")
        signals.stop_monitoring.emit()

    def get_connection_status(self):
        """Verbindungsstatus zurückgeben."""
        return "Verbunden" if self.connected else "Nicht verbunden"

    def _api_origin(self) -> str:
        """Host-Basis ohne doppeltes /api (custom_api_url z. B. https://host oder https://host/api)."""
        b = str(self.api_base_url or "").rstrip("/")
        if b.endswith("/api"):
            return b[:-4]
        return b

    def _upload_api_root(self) -> str:
        """Basis-URL fuer Upload-Routen (z. B. https://host/api/upload)."""
        return f"{self._api_origin()}/api/upload"

    @staticmethod
    def _session_ctx(session_id: str, file_name: str, offset=None) -> str:
        if offset is not None:
            return f"session={session_id!r} file={file_name!r} off={offset}"
        return f"session={session_id!r} file={file_name!r}"

    @staticmethod
    def _http_transient(status: int, body: str) -> bool:
        if status in (408, 429, 502, 503, 504):
            return True
        return _body_suggests_invocation_timeout(body or "")

    def _backoff_delay(self, attempt: int) -> float:
        """attempt 1-basiert: 2–30 s + Jitter."""
        return min(30.0, 2.0 ** (attempt - 1)) + random.uniform(0.0, 1.5)

    def _post_json_upload(
        self,
        path_suffix: str,
        json_body: dict,
        *,
        timeout: int,
        tag: str,
        soft_fail_statuses: frozenset | None = None,
    ):
        """POST JSON an /api/upload/... mit Retry bei transienten Fehlern."""
        url = f"{self._upload_api_root()}{path_suffix}"
        max_attempts = 6
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.post(url, json=json_body, timeout=timeout)
            except requests.exceptions.RequestException as e:
                if attempt >= max_attempts:
                    self.log.error("%s: %s nach %s Versuchen", tag, e, max_attempts)
                    raise
                d = self._backoff_delay(attempt)
                self.log.warning(
                    "%s: Netzwerk-Fehler, Versuch %s/%s, warte %.1fs — %s",
                    tag,
                    attempt,
                    max_attempts,
                    d,
                    e,
                )
                time.sleep(d)
                continue
            body = r.text or ""
            if soft_fail_statuses and r.status_code in soft_fail_statuses:
                return r
            if r.status_code in (401, 403):
                raise ApiAuthError(
                    f"API-Key fehlt oder hat keine 'upload'-Permission (HTTP {r.status_code}). "
                    f"{(body or '')[:200]}"
                )
            if r.ok:
                if attempt > 1:
                    self.log.info("%s: HTTP %s nach Versuch %s", tag, r.status_code, attempt)
                return r
            if self._http_transient(r.status_code, body):
                if attempt >= max_attempts:
                    summary = _summarize_api_error_body(body)
                    self.log.error(
                        "%s: HTTP %s nach %s Versuchen — %s",
                        tag,
                        r.status_code,
                        max_attempts,
                        summary,
                    )
                    raise Exception(f"{tag}: HTTP {r.status_code} — {summary}")
                d = self._backoff_delay(attempt)
                self.log.warning(
                    "%s: HTTP %s, Versuch %s/%s, warte %.1fs — %s",
                    tag,
                    r.status_code,
                    attempt,
                    max_attempts,
                    d,
                    _summarize_api_error_body(body)[:200],
                )
                time.sleep(d)
                continue
            summary = _summarize_api_error_body(body)
            self.log.error(
                "%s: HTTP %s — %s | Body: %s",
                tag,
                r.status_code,
                summary,
                _full_body_for_log(body),
            )
            raise Exception(f"{tag}: HTTP {r.status_code} — {summary}")

    def _post_session_multipart_with_retry(
        self,
        subpath: str,
        fields: dict,
        *,
        session_id: str,
        file_name: str,
        offset_for_log,
        per_request_timeout: int = 600,
    ):
        """POST multipart an session/start|append|finish mit Retry (503/504/Netz)."""
        url = f"{self._upload_api_root()}{subpath}"
        max_attempts = 6
        ctx = self._session_ctx(session_id, file_name, offset_for_log)
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.post(url, files=fields, timeout=per_request_timeout)
            except requests.exceptions.RequestException as e:
                if attempt >= max_attempts:
                    self.log.error(
                        "Session %s [%s]: %s nach %s Versuchen",
                        subpath,
                        ctx,
                        e,
                        max_attempts,
                    )
                    raise
                d = self._backoff_delay(attempt)
                self.log.warning(
                    "Session %s [%s]: Netzwerk-Fehler, Versuch %s/%s, warte %.1fs — %s",
                    subpath,
                    ctx,
                    attempt,
                    max_attempts,
                    d,
                    e,
                )
                time.sleep(d)
                continue
            body = r.text or ""
            if r.status_code in (401, 403):
                raise ApiAuthError(
                    f"API-Key fehlt oder hat keine 'upload'-Permission (HTTP {r.status_code}). "
                    f"{(body or '')[:200]}"
                )
            if r.ok:
                self.log.info(
                    "Session %s [%s]: HTTP %s%s",
                    subpath,
                    ctx,
                    r.status_code,
                    f" (Versuch {attempt})" if attempt > 1 else "",
                )
                return r
            if self._http_transient(r.status_code, body):
                if attempt >= max_attempts:
                    summary = _summarize_api_error_body(body)
                    self.log.error(
                        "Session %s [%s]: HTTP %s nach %s Versuchen — %s",
                        subpath,
                        ctx,
                        r.status_code,
                        max_attempts,
                        summary,
                    )
                    raise Exception(f"Session {subpath}: HTTP {r.status_code} — {summary}")
                d = self._backoff_delay(attempt)
                self.log.warning(
                    "Session %s [%s]: HTTP %s, Versuch %s/%s, warte %.1fs — %s",
                    subpath,
                    ctx,
                    r.status_code,
                    attempt,
                    max_attempts,
                    d,
                    _summarize_api_error_body(body)[:200],
                )
                time.sleep(d)
                continue
            summary = _summarize_api_error_body(body)
            self.log.error(
                "Session %s [%s]: HTTP %s — %s | Body: %s",
                subpath,
                ctx,
                r.status_code,
                summary,
                _full_body_for_log(body),
            )
            raise Exception(f"Session upload {subpath}: HTTP {r.status_code} — {summary}")

    def _parse_next_offset(
        self,
        r: requests.Response,
        expected_next: int,
        session_id: str,
        file_name: str,
        step: str,
    ) -> int:
        try:
            j = r.json()
        except (json.JSONDecodeError, TypeError):
            self.log.warning(
                "[%s] %s: keine JSON-Antwort, verwende next_offset=%s",
                self._session_ctx(session_id, file_name),
                step,
                expected_next,
            )
            return expected_next
        if isinstance(j, dict) and "next_offset" in j:
            try:
                no = int(j["next_offset"])
            except (ValueError, TypeError):
                self.log.warning(
                    "[%s] %s: next_offset unparseable %r, verwende %s",
                    self._session_ctx(session_id, file_name),
                    step,
                    j.get("next_offset"),
                    expected_next,
                )
                return expected_next
            if no != expected_next:
                self.log.warning(
                    "[%s] %s: next_offset Drift server=%s erwartet=%s — Server-Wert",
                    self._session_ctx(session_id, file_name),
                    step,
                    no,
                    expected_next,
                )
            return no
        self.log.warning(
            "[%s] %s: keine next_offset in Antwort, verwende %s",
            self._session_ctx(session_id, file_name),
            step,
            expected_next,
        )
        return expected_next

    def _session_start(self, session_id: str, file_name: str, expected_size: int, chunk: bytes):
        return self._post_session_multipart_with_retry(
            "/session/start",
            {
                "session_id": (None, session_id),
                "file_name": (None, file_name),
                "expected_size": (None, str(expected_size)),
                "chunk": ("chunk", chunk, "application/octet-stream"),
            },
            session_id=session_id,
            file_name=file_name,
            offset_for_log=0,
        )

    def _session_append(self, session_id: str, file_name: str, offset: int, chunk: bytes):
        return self._post_session_multipart_with_retry(
            "/session/append",
            {
                "session_id": (None, session_id),
                "file_name": (None, file_name),
                "offset": (None, str(offset)),
                "chunk": ("chunk", chunk, "application/octet-stream"),
            },
            session_id=session_id,
            file_name=file_name,
            offset_for_log=offset,
        )

    def _session_finish(
        self, session_id: str, file_name: str, offset: int, chunk: bytes, mime_type: str
    ):
        fields = {
            "session_id": (None, session_id),
            "file_name": (None, file_name),
            "offset": (None, str(offset)),
            "chunk": ("chunk", chunk, "application/octet-stream"),
            "mime_type": (None, mime_type or "application/octet-stream"),
        }
        return self._post_session_multipart_with_retry(
            "/session/finish",
            fields,
            session_id=session_id,
            file_name=file_name,
            offset_for_log=offset,
        )

    def upload_directory(self, local_dir_path, remote_base_path, kunde: Kunde = None, control=None):
        """Lädt ein Verzeichnis per direct-init, 4-MB-Session-Chunks und finalize hoch."""
        self._upload_control = control
        if not self.connected:
            self.log.error("Upload fehlgeschlagen: Nicht verbunden.")
            return False

        self.log.info(f"Beginne Session-Upload von '{local_dir_path}'")
        self._last_kunde = kunde
        upload_mode = str(
            self.config.get_setting("custom_api_upload_mode", "proxied_session")
            or "proxied_session"
        ).strip()
        if upload_mode == "direct_dropbox_complete":
            self.log.info("Custom API Upload-Modus: direct_dropbox_complete")
            return self._upload_directory_direct_dropbox_complete(local_dir_path, kunde)
        self.log.info("Custom API Upload-Modus: proxied_session")

        # 1. Dateien sammeln
        files_to_upload = []
        total_size = 0

        for root, _, files in os.walk(local_dir_path):
            for file in files:
                if should_skip_upload_file(file):
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

        files_to_upload.sort(key=lambda x: x["name"])
        manifest = [{"name": f["name"], "size": f["size"], "type": f["type"]} for f in files_to_upload]
        manifest_fp = manifest_fingerprint(manifest)
        folder_name = os.path.basename(local_dir_path)

        raw_ck = load_checkpoint(local_dir_path)
        resume_ck = None
        if (
            raw_ck
            and raw_ck.get("kind") == "custom_api_proxied"
            and raw_ck.get("manifest_fp") == manifest_fp
        ):
            resume_ck = raw_ck
        elif raw_ck:
            self.log.warning("Upload-Checkpoint verworfen (Manifest/Typ passt nicht).")
            clear_checkpoint(local_dir_path)

        session_id = None
        order_id = None

        if resume_ck and resume_ck.get("phase") == "finalizing" and resume_ck.get("api_session_id"):
            self.log.info("Checkpoint: Finalisierung der Upload-Session wird fortgesetzt.")
            try:
                sid = resume_ck["api_session_id"]
                self._last_session_id = sid
                if resume_ck.get("order_id") is not None:
                    self._last_order_id = str(resume_ck["order_id"])
                signals.upload_status_update.emit("Finalisiere Upload...")
                self._upload_coop_tick()
                customer_url = self._finalize_session(sid)
                if not customer_url:
                    customer_url = self._wait_for_completion_legacy(sid)
                self._last_customer_url = customer_url if customer_url else None
                clear_checkpoint(local_dir_path)
                signals.upload_status_update.emit("Upload abgeschlossen.")
                if customer_url:
                    self.log.info("Upload erfolgreich: %s", customer_url)
                else:
                    self.log.warning(
                        "Upload finalisiert, aber customer_url noch nicht verfuegbar."
                    )
                return True
            except ApiAuthError as e:
                self.log.error("Finalize (Recovery): %s", e)
                signals.upload_status_update.emit(str(e))
                return False
            except UploadCancelled:
                raise
            except Exception as e:
                self.log.error("Finalize (Recovery) fehlgeschlagen: %s", e)
                signals.upload_status_update.emit(f"Fehler: {e}")
                return False

        if resume_ck and resume_ck.get("api_session_id"):
            session_id = resume_ck["api_session_id"]
            order_id = resume_ck.get("order_id")
            self._last_session_id = session_id
            if order_id is not None:
                self._last_order_id = str(order_id)
            self.log.info(
                "Proxied-Session wird fortgesetzt (session_id=%s, next_file_index=%s).",
                session_id,
                resume_ck.get("next_file_index", 0),
            )

        # 2. Direct Upload Session initialisieren (falls kein Resume)
        try:
            if not session_id:
                session_data = self._initialize_direct_session(files_to_upload, folder_name, kunde)
                session_id = session_data["session_id"]
                order_id = session_data.get("order_id")
                self._last_session_id = session_id
                if order_id is not None:
                    self._last_order_id = str(order_id)
                self.log.info(
                    "Upload-Session initialisiert: session_id=%s%s",
                    session_id,
                    f", order_id={order_id}" if order_id else "",
                )
                save_checkpoint(
                    local_dir_path,
                    {
                        "kind": "custom_api_proxied",
                        "manifest_fp": manifest_fp,
                        "folder_name": folder_name,
                        "api_session_id": session_id,
                        "order_id": order_id,
                        "total_size": total_size,
                        "completed_bytes": 0,
                        "next_file_index": 0,
                        "custom_active": None,
                        "phase": "uploading",
                    },
                )

        except ApiAuthError as e:
            self.log.error("Session-Initialisierung: %s", e)
            signals.upload_status_update.emit(str(e))
            return False
        except UploadCancelled:
            raise
        except Exception as e:
            self.log.error(f"Session-Initialisierung fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

        start_idx = 0
        resume_server_offset = None
        if resume_ck:
            start_idx = min(int(resume_ck.get("next_file_index", 0)), len(files_to_upload))
            ca = resume_ck.get("custom_active") or {}
            if start_idx < len(files_to_upload) and ca.get("file_name") == files_to_upload[start_idx]["name"]:
                ro = int(ca.get("server_offset", 0))
                if ro > 0:
                    resume_server_offset = ro

        uploaded_counter = {
            "bytes": int(resume_ck.get("completed_bytes", 0)) if resume_ck else 0
        }

        def save_proxied_ck(**kwargs):
            payload = {
                "kind": "custom_api_proxied",
                "manifest_fp": manifest_fp,
                "folder_name": folder_name,
                "api_session_id": session_id,
                "order_id": order_id,
                "total_size": total_size,
                "phase": "uploading",
            }
            payload.update(kwargs)
            save_checkpoint(local_dir_path, payload)

        # 3. Dateien nacheinander (Session-Offsets pro Datei seriell)
        try:
            for i in range(start_idx, len(files_to_upload)):
                self._upload_coop_tick()
                file_info = files_to_upload[i]
                roff = resume_server_offset if i == start_idx else None
                resume_server_offset = None

                def make_handlers():
                    def on_chunk_committed(fi, server_off):
                        save_proxied_ck(
                            completed_bytes=sum(f["size"] for f in files_to_upload[:fi]),
                            next_file_index=fi,
                            custom_active={
                                "file_name": files_to_upload[fi]["name"],
                                "server_offset": server_off,
                            },
                        )

                    def on_file_completed(fi):
                        save_proxied_ck(
                            completed_bytes=sum(f["size"] for f in files_to_upload[: fi + 1]),
                            next_file_index=fi + 1,
                            custom_active=None,
                        )

                    return on_chunk_committed, on_file_completed

                on_chunk, on_done = make_handlers()
                self._upload_file_via_session(
                    session_id,
                    file_info,
                    total_size,
                    uploaded_counter,
                    file_index=i,
                    resume_server_offset=roff,
                    on_chunk_committed=on_chunk,
                    on_file_completed=on_done,
                )

            # 4. Session abschliessen (Kunden-URL)
            self.log.info("Alle Dateien hochgeladen, finalisiere Session...")
            self._upload_coop_tick()
            save_proxied_ck(
                completed_bytes=total_size,
                next_file_index=len(files_to_upload),
                custom_active=None,
                phase="finalizing",
            )
            signals.upload_status_update.emit("Finalisiere Upload...")
            customer_url = self._finalize_session(session_id)
            if not customer_url:
                customer_url = self._wait_for_completion_legacy(session_id)

            # Speichere customer_url für get_shareable_link (nur wenn vorhanden)
            self._last_customer_url = customer_url if customer_url else None

            clear_checkpoint(local_dir_path)
            signals.upload_status_update.emit(f"Upload abgeschlossen.")
            if customer_url:
                self.log.info(f"Upload erfolgreich: {customer_url}")
            else:
                self.log.warning(
                    "Upload der Dateien erfolgreich, aber customer_url noch nicht verfuegbar."
                )
            return True

        except ApiAuthError as e:
            self.log.error("Upload: %s", e)
            signals.upload_status_update.emit(str(e))
            return False
        except UploadCancelled:
            raise
        except Exception as e:
            self.log.error(f"Upload fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

    def _initialize_direct_session(self, files_to_upload, base_folder_name, kunde: Kunde = None):
        """Direct Upload Session bei der API initialisieren.

        Endpoint: POST /api/upload/direct-init
        """
        metadata = {}
        if kunde:
            from dataclasses import asdict, is_dataclass

            if is_dataclass(kunde):
                kunde_dict = asdict(kunde)
            elif isinstance(kunde, dict):
                kunde_dict = dict(kunde)
            else:
                kunde_dict = dict(vars(kunde))

            metadata = dict(kunde_dict)

        metadata["base_folder_name"] = base_folder_name
        self.log.info("direct-init metadata: %s", metadata)

        payload = {
            "files": [
                {
                    "name": f["name"],
                    "size": f["size"],
                    "type": f["type"],
                }
                for f in files_to_upload
            ],
            "metadata": metadata,
            "base_folder_name": base_folder_name,
        }

        response = self._post_json_upload("/direct-init", payload, timeout=60, tag="direct-init")
        result = response.json()
        if not result.get("session_id"):
            raise Exception(f"Session-Initialisierung fehlgeschlagen (keine session_id): {result}")
        if result.get("ok") is False:
            raise Exception(f"Session-Initialisierung fehlgeschlagen: {result}")

        oid = result.get("order_id")
        if oid is not None:
            self._last_order_id = str(oid)

        return result

    def _upload_file_via_session(
        self,
        session_id: str,
        file_info: dict,
        total_job_size: int,
        uploaded_counter: dict,
        *,
        file_index: int = 0,
        resume_server_offset: int | None = None,
        on_chunk_committed=None,
        on_file_completed=None,
    ):
        """Eine Datei: session/start → optional session/append* → session/finish (CHUNK_BYTES).

        resume_server_offset: Server-Offset zum Fortsetzen (kein erneuter session/start).
        on_chunk_committed(file_index, server_offset): nach jedem bestätigten Offset.
        on_file_completed(file_index): nach erfolgreichem Abschluss der Datei.
        """
        file_name = file_info["name"]
        file_size = file_info["size"]
        local_path = file_info["local_path"]
        mime_type = file_info["type"]

        def emit_progress(bytes_sent_partial: int):
            if file_size <= 0:
                pct = 100
                sent = 0
            else:
                sent = bytes_sent_partial
                pct = min(100, int((sent / file_size) * 100))
            with self.progress_lock:
                base = uploaded_counter["bytes"]
            combined = base + sent
            total_pct = (
                min(100, int((combined / total_job_size) * 100))
                if total_job_size > 0
                else 0
            )
            total_denom = file_size if file_size > 0 else 1
            signals.upload_progress_file.emit(pct, sent, total_denom)
            signals.upload_progress_total.emit(total_pct, combined, total_job_size)

        self.log.info(
            "[%s] Session-Upload: %s bytes%s",
            self._session_ctx(session_id, file_name),
            file_size,
            f", resume_off={resume_server_offset}" if resume_server_offset is not None else "",
        )
        signals.upload_status_update.emit(f"Lade hoch: {file_name}")
        emit_progress(0)
        denom = file_size if file_size > 0 else 1
        signals.upload_progress_file.emit(0, 0, denom)

        self._upload_coop_tick()
        with open(local_path, "rb") as f:
            off = 0
            if resume_server_offset is not None and resume_server_offset > 0:
                off = int(resume_server_offset)
                if off > file_size:
                    raise RuntimeError(f"{file_name}: Checkpoint-Offset {off} > Dateigröße {file_size}")
                f.seek(off)
                self.log.info(
                    "[%s] Setze Upload bei Byte %s fort.",
                    self._session_ctx(session_id, file_name),
                    off,
                )
            else:
                self._upload_coop_tick()
                first_len = min(CHUNK_BYTES, file_size) if file_size > 0 else 0
                first = f.read(first_len)
                r_start = self._session_start(session_id, file_name, file_size, first)
                off = self._parse_next_offset(
                    r_start, len(first), session_id, file_name, "start"
                )
                emit_progress(off)
                if on_chunk_committed:
                    on_chunk_committed(file_index, off)

            while file_size - off > CHUNK_BYTES:
                self._upload_coop_tick()
                buf = f.read(CHUNK_BYTES)
                if len(buf) != CHUNK_BYTES:
                    raise RuntimeError(
                        f"{file_name}: append erwartete {CHUNK_BYTES} Bytes, erhalten {len(buf)}"
                    )
                if off + len(buf) >= file_size:
                    raise RuntimeError(
                        f"{file_name}: append wuerde letzten Block senden — stattdessen finish"
                    )
                r_app = self._session_append(session_id, file_name, off, buf)
                off = self._parse_next_offset(
                    r_app, off + len(buf), session_id, file_name, "append"
                )
                emit_progress(off)
                if on_chunk_committed:
                    on_chunk_committed(file_index, off)

            self._upload_coop_tick()
            last = f.read()
            if off + len(last) != file_size:
                raise RuntimeError(
                    f"{file_name}: finish-Invariante verletzt off={off} "
                    f"len(last)={len(last)} expected_size={file_size}"
                )
            self._session_finish(session_id, file_name, off, last, mime_type)

        emit_progress(file_size)

        with self.progress_lock:
            uploaded_counter["bytes"] += file_size
            current_total_bytes = uploaded_counter["bytes"]
        fd = file_size if file_size > 0 else 1
        signals.upload_progress_file.emit(100, file_size, fd)
        total_progress = (
            int((current_total_bytes / total_job_size) * 100) if total_job_size > 0 else 100
        )
        signals.upload_progress_total.emit(
            total_progress, current_total_bytes, total_job_size
        )
        self.log.info("[%s] Fertig", self._session_ctx(session_id, file_name))
        if on_file_completed:
            on_file_completed(file_index)

    def _finalize_session(self, session_id: str):
        """POST /api/upload/finalize — liefert u. a. customer_url."""
        r = self._post_json_upload(
            "/finalize",
            {"session_id": session_id},
            timeout=120,
            tag="finalize",
            soft_fail_statuses=frozenset({404, 405, 501}),
        )
        if r.status_code in (404, 405, 501):
            self.log.warning(
                "finalize nicht unterstuetzt (HTTP %s), nutze Status-Poll-Fallback [session=%r]",
                r.status_code,
                session_id,
            )
            return None

        try:
            data = r.json()
        except json.JSONDecodeError:
            self.log.warning("finalize: keine JSON-Antwort [session=%r]", session_id)
            return None

        for key in ("archive_url", "media_url", "order_id"):
            val = data.get(key)
            if val:
                self.log.info(
                    "finalize %s=%s HTTP %s [session=%r]",
                    key,
                    val,
                    r.status_code,
                    session_id,
                )
        oid = data.get("order_id")
        if oid is not None:
            self._last_order_id = str(oid)

        return self._extract_customer_url(data)

    def _wait_for_completion_legacy(self, session_id):
        """Fallback: Status per GET pollen (alte APIs ohne finalize-URL in Antwort)."""
        max_wait_time = 30
        poll_interval = 2
        started_at = time.monotonic()
        status_urls = [
            f"{self._upload_api_root()}/status/{session_id}",
            f"{self._api_origin()}/upload/status/{session_id}",
        ]

        self.log.info("Warte auf Server-Finalisierung (Status-Poll)...")

        while (time.monotonic() - started_at) < max_wait_time:
            for status_url in status_urls:
                try:
                    response = self.session.get(status_url, timeout=15)
                    if not response.ok:
                        continue
                    result = response.json()
                    customer_url = self._extract_customer_url(result)
                    if result.get("status") == "completed" and customer_url:
                        self.log.info("Session finalisiert (Poll): %s", customer_url)
                        return customer_url
                    if customer_url:
                        self.log.info("Customer URL (Poll): %s", customer_url)
                        return customer_url
                except Exception as e:
                    self.log.debug("Status-Poll %s: %s", status_url, e)
            time.sleep(poll_interval)
            self._upload_coop_tick()

        self.log.warning("Timeout beim Status-Poll, letzte Versuche...")
        for status_url in status_urls:
            try:
                response = self.session.get(status_url, timeout=15)
                if response.ok:
                    customer_url = self._extract_customer_url(response.json())
                    if customer_url:
                        return customer_url
            except Exception as e:
                self.log.warning("Status-Poll Fehler: %s", e)
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
            if self._last_session_id and self.session and self.api_base_url:
                for status_url in (
                    f"{self._upload_api_root()}/status/{self._last_session_id}",
                    f"{self._api_origin()}/upload/status/{self._last_session_id}",
                ):
                    try:
                        response = self.session.get(status_url, timeout=15)
                        if response.ok:
                            fallback_url = self._extract_customer_url(response.json())
                            if fallback_url:
                                self._last_customer_url = fallback_url
                                self.log.info(
                                    "customer_url per Fallback-Check erhalten: %s", fallback_url
                                )
                                break
                    except Exception as e:
                        self.log.warning("Fallback Status-Check (%s): %s", status_url, e)
                else:
                    self.log.warning("Fallback Status-Check: keine customer_url")

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

    def _upload_directory_direct_dropbox_complete(self, local_dir_path, kunde: Kunde = None):
        """Neuer Modus: direct-init -> direkter Dropbox-Upload -> client-complete."""
        files_to_upload = []
        total_size = 0
        folder_name = os.path.basename(local_dir_path)
        remote_base_path = f"/{folder_name}"

        for root, _, files in os.walk(local_dir_path):
            for file in files:
                if should_skip_upload_file(file):
                    continue

                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(str(local_path), str(local_dir_path))
                try:
                    file_size = os.path.getsize(local_path)
                    mime_type = self._get_mime_type(local_path)
                except FileNotFoundError:
                    self.log.warning(f"Datei nicht gefunden: {local_path}")
                    continue

                rel_norm = relative_path.replace(os.path.sep, "/")
                files_to_upload.append(
                    {
                        "name": rel_norm,
                        "size": file_size,
                        "type": mime_type,
                        "local_path": local_path,
                        "dropbox_path": f"{remote_base_path}/{rel_norm}".replace("//", "/"),
                    }
                )
                total_size += file_size

        if not files_to_upload:
            self.log.warning("Keine Dateien zum Hochladen gefunden.")
            return True

        files_to_upload.sort(key=lambda x: x["name"])
        manifest = [{"name": f["name"], "size": f["size"], "type": f["type"]} for f in files_to_upload]
        manifest_fp = manifest_fingerprint(manifest)

        raw_ck = load_checkpoint(local_dir_path)
        resume_ck = None
        if (
            raw_ck
            and raw_ck.get("kind") == "custom_api_direct_dropbox"
            and raw_ck.get("manifest_fp") == manifest_fp
        ):
            resume_ck = raw_ck
        elif raw_ck:
            self.log.warning("Direct-Dropbox-Checkpoint verworfen.")
            clear_checkpoint(local_dir_path)

        session_id = None
        order_id = None
        uploaded_files: list = []

        if resume_ck and resume_ck.get("phase") == "client_complete_pending" and resume_ck.get("api_session_id"):
            session_id = resume_ck["api_session_id"]
            uploaded_files = list(resume_ck.get("uploaded_files") or [])
            self._last_session_id = session_id
            if resume_ck.get("order_id") is not None:
                self._last_order_id = str(resume_ck["order_id"])
            self.log.info("Checkpoint: client-complete wird fortgesetzt (session_id=%s).", session_id)
            self._connect_dropbox_for_direct_mode()
            done_resp = self._post_json_upload(
                "/client-complete",
                {"session_id": session_id, "files": uploaded_files},
                timeout=120,
                tag="client-complete",
            )
            done_data = done_resp.json() if done_resp.content else {}
            self._last_customer_url = self._extract_customer_url(done_data)
            if not self._last_customer_url:
                self._last_customer_url = (
                    done_data.get("media_url")
                    or done_data.get("archive_url")
                    or done_data.get("url")
                )
            for key in ("media_url", "customer_url", "archive_url", "order_id"):
                val = done_data.get(key) if isinstance(done_data, dict) else None
                if val:
                    self.log.info("client-complete %s=%s [session=%r]", key, val, session_id)
            if isinstance(done_data, dict) and done_data.get("order_id") is not None:
                self._last_order_id = str(done_data["order_id"])
            clear_checkpoint(local_dir_path)
            signals.upload_status_update.emit("Upload abgeschlossen.")
            return True

        if resume_ck and resume_ck.get("api_session_id"):
            session_id = resume_ck["api_session_id"]
            order_id = resume_ck.get("order_id")
            uploaded_files = list(resume_ck.get("uploaded_files") or [])
            self._last_session_id = session_id
            if order_id is not None:
                self._last_order_id = str(order_id)
            self.log.info(
                "Direct-Dropbox-Upload fortsetzen (session_id=%s, next_file_index=%s).",
                session_id,
                resume_ck.get("next_file_index", 0),
            )

        if not session_id:
            session_data = self._initialize_direct_session(files_to_upload, folder_name, kunde)
            session_id = session_data["session_id"]
            order_id = session_data.get("order_id")
            self._last_session_id = session_id
            if order_id is not None:
                self._last_order_id = str(order_id)
            self.log.info(
                "Direct-Dropbox Session initialisiert: session_id=%s%s",
                session_id,
                f", order_id={order_id}" if order_id else "",
            )
            save_checkpoint(
                local_dir_path,
                {
                    "kind": "custom_api_direct_dropbox",
                    "manifest_fp": manifest_fp,
                    "api_session_id": session_id,
                    "order_id": order_id,
                    "uploaded_files": [],
                    "next_file_index": 0,
                    "dd_active": None,
                    "phase": "uploading",
                },
            )

        self._connect_dropbox_for_direct_mode()

        start_idx = 0
        resume_dd = None
        if resume_ck:
            start_idx = min(int(resume_ck.get("next_file_index", 0)), len(files_to_upload))
            dd = resume_ck.get("dd_active") or {}
            if start_idx < len(files_to_upload) and dd.get("file_name") == files_to_upload[start_idx]["name"]:
                if int(dd.get("offset", 0)) > 0 and dd.get("session_id"):
                    resume_dd = {
                        "session_id": str(dd["session_id"]),
                        "offset": int(dd["offset"]),
                        "file_name": dd["file_name"],
                        "dropbox_path": dd.get("dropbox_path") or files_to_upload[start_idx]["dropbox_path"],
                    }

        def names_done() -> set:
            return {row["file_name"] for row in uploaded_files}

        uploaded_counter = {
            "bytes": sum(f["size"] for f in files_to_upload if f["name"] in names_done())
        }

        def save_dd_ck(**kwargs):
            payload = {
                "kind": "custom_api_direct_dropbox",
                "manifest_fp": manifest_fp,
                "api_session_id": session_id,
                "order_id": order_id,
                "uploaded_files": uploaded_files,
                "phase": "uploading",
            }
            payload.update(kwargs)
            save_checkpoint(local_dir_path, payload)

        outer_resume_dd = resume_dd
        for i in range(start_idx, len(files_to_upload)):
            self._upload_coop_tick()
            file_info = files_to_upload[i]
            ro = outer_resume_dd if i == start_idx else None

            def on_dd_progress(active: dict | None, _i=i):
                if active:
                    save_dd_ck(
                        next_file_index=_i,
                        dd_active={
                            "session_id": active["session_id"],
                            "offset": active["offset"],
                            "file_name": active["file_name"],
                            "dropbox_path": active["dropbox_path"],
                        },
                    )
                else:
                    save_dd_ck(next_file_index=_i, dd_active=None)

            md = self._upload_file_direct_to_dropbox(
                file_info,
                total_size,
                uploaded_counter,
                resume_dd=ro,
                on_dd_progress=on_dd_progress,
            )
            row = {
                "file_name": file_info["name"],
                "file_size": int(md.size if getattr(md, "size", None) is not None else file_info["size"]),
                "dropbox_id": getattr(md, "id", "") or "",
                "dropbox_path": (
                    getattr(md, "path_lower", None)
                    or getattr(md, "path_display", None)
                    or file_info["dropbox_path"]
                ),
            }
            uploaded_files.append(row)
            save_dd_ck(
                uploaded_files=list(uploaded_files),
                next_file_index=i + 1,
                dd_active=None,
            )

        save_dd_ck(
            uploaded_files=list(uploaded_files),
            next_file_index=len(files_to_upload),
            dd_active=None,
            phase="client_complete_pending",
        )

        self._upload_coop_tick()
        done_resp = self._post_json_upload(
            "/client-complete",
            {"session_id": session_id, "files": uploaded_files},
            timeout=120,
            tag="client-complete",
        )
        done_data = done_resp.json() if done_resp.content else {}
        self._last_customer_url = self._extract_customer_url(done_data)
        if not self._last_customer_url:
            self._last_customer_url = (
                done_data.get("media_url")
                or done_data.get("archive_url")
                or done_data.get("url")
            )

        for key in ("media_url", "customer_url", "archive_url", "order_id"):
            val = done_data.get(key) if isinstance(done_data, dict) else None
            if val:
                self.log.info("client-complete %s=%s [session=%r]", key, val, session_id)
        if isinstance(done_data, dict) and done_data.get("order_id") is not None:
            self._last_order_id = str(done_data["order_id"])

        clear_checkpoint(local_dir_path)
        signals.upload_status_update.emit("Upload abgeschlossen.")
        return True

    def _connect_dropbox_for_direct_mode(self):
        """Initialisiert Dropbox-Client mit lokalen Credentials (wie DropboxClient)."""
        if self.dbx is not None:
            return
        app_key = self.config.get_secret("db_app_key")
        app_secret = self.config.get_secret("db_app_secret")
        refresh_token = self.config.get_secret("db_refresh_token")
        if not app_key or not app_secret or not refresh_token:
            raise Exception(
                "Dropbox-Credentials fehlen für direct_dropbox_complete "
                "(db_app_key/db_app_secret/db_refresh_token)."
            )
        self.dbx = dropbox.Dropbox(
            app_key=app_key,
            app_secret=app_secret,
            oauth2_refresh_token=refresh_token,
        )
        self.dbx.users_get_current_account()

    def _upload_file_direct_to_dropbox(
        self,
        file_info: dict,
        total_job_size: int,
        uploaded_counter: dict,
        *,
        resume_dd: dict | None = None,
        on_dd_progress=None,
    ):
        file_name = file_info["name"]
        file_size = file_info["size"]
        local_path = file_info["local_path"]
        dropbox_path = file_info["dropbox_path"]

        def emit_progress(bytes_sent_partial: int):
            sent = max(0, int(bytes_sent_partial))
            pct = min(100, int((sent / file_size) * 100)) if file_size > 0 else 100
            with self.progress_lock:
                base = uploaded_counter["bytes"]
            combined = base + sent
            total_pct = min(100, int((combined / total_job_size) * 100)) if total_job_size > 0 else 100
            denom = file_size if file_size > 0 else 1
            signals.upload_progress_file.emit(pct, sent, denom)
            signals.upload_progress_total.emit(total_pct, combined, total_job_size)

        self.log.info(
            "[direct_dropbox session=%r file=%r] Upload nach %s (%s bytes)%s",
            self._last_session_id,
            file_name,
            dropbox_path,
            file_size,
            ", resume" if resume_dd else "",
        )
        signals.upload_status_update.emit(f"Lade hoch: {file_name}")
        emit_progress(0)

        def _active_state(cursor_obj):
            return {
                "session_id": str(cursor_obj.session_id),
                "offset": int(cursor_obj.offset),
                "file_name": file_name,
                "dropbox_path": dropbox_path,
            }

        self._upload_coop_tick()
        with open(local_path, "rb") as f:
            if file_size <= DROPBOX_CHUNK_BYTES:
                self._upload_coop_tick()
                md = self.dbx.files_upload(
                    f.read(),
                    dropbox_path,
                    mode=dropbox.files.WriteMode.overwrite,
                )
                emit_progress(file_size)
                if on_dd_progress:
                    on_dd_progress(None)
            else:
                cursor = None
                if (
                    resume_dd
                    and int(resume_dd.get("offset", 0)) > 0
                    and str(resume_dd.get("file_name", "")) == file_name
                ):
                    off = int(resume_dd["offset"])
                    if off > file_size:
                        raise RuntimeError(f"{file_name}: Resume-Offset {off} > Dateigröße {file_size}")
                    f.seek(off)
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=str(resume_dd["session_id"]),
                        offset=off,
                    )
                    self.log.info(
                        "[direct_dropbox] Setze Dropbox-Session bei Byte %s fort (session_id=%r).",
                        off,
                        resume_dd.get("session_id"),
                    )
                    emit_progress(off)
                    if on_dd_progress:
                        on_dd_progress(_active_state(cursor))
                else:
                    self._upload_coop_tick()
                    start = self.dbx.files_upload_session_start(f.read(DROPBOX_CHUNK_BYTES))
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=start.session_id, offset=f.tell()
                    )
                    emit_progress(cursor.offset)
                    if on_dd_progress:
                        on_dd_progress(_active_state(cursor))

                while (file_size - f.tell()) > DROPBOX_CHUNK_BYTES:
                    self._upload_coop_tick()
                    chunk = f.read(DROPBOX_CHUNK_BYTES)
                    self.dbx.files_upload_session_append_v2(chunk, cursor)
                    cursor.offset = f.tell()
                    emit_progress(cursor.offset)
                    if on_dd_progress:
                        on_dd_progress(_active_state(cursor))

                self._upload_coop_tick()
                final_chunk = f.read()
                commit = dropbox.files.CommitInfo(
                    path=dropbox_path,
                    mode=dropbox.files.WriteMode.overwrite,
                )
                md = self.dbx.files_upload_session_finish(final_chunk, cursor, commit)
                emit_progress(file_size)
                if on_dd_progress:
                    on_dd_progress(None)

        with self.progress_lock:
            uploaded_counter["bytes"] += file_size
            current_total_bytes = uploaded_counter["bytes"]
        signals.upload_progress_file.emit(100, file_size, file_size if file_size > 0 else 1)
        total_progress = int((current_total_bytes / total_job_size) * 100) if total_job_size > 0 else 100
        signals.upload_progress_total.emit(total_progress, current_total_bytes, total_job_size)
        return md

    def _get_mime_type(self, file_path):
        """Ermittelt den MIME-Type einer Datei."""
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or 'application/octet-stream'

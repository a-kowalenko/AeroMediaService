import os
import json
import random
import requests
from requests.adapters import HTTPAdapter
import logging
import time
from threading import Lock

from models.kunde import Kunde
from services.base_client import BaseClient
from core.config import ConfigManager
from core.signals import signals
from utils.link_shortener import LinkShortener

# Entspricht Server PROXIED_UPLOAD_CHUNK_BYTES; Vercel-Multipart-Limit ~4.5 MiB Request-Body.
CHUNK_BYTES = 4 * 1024 * 1024


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

    def upload_directory(self, local_dir_path, remote_base_path, kunde: Kunde = None):
        """Lädt ein Verzeichnis per direct-init, 4-MB-Session-Chunks und finalize hoch."""
        if not self.connected:
            self.log.error("Upload fehlgeschlagen: Nicht verbunden.")
            return False

        self.log.info(f"Beginne Session-Upload von '{local_dir_path}'")
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
            order_id = session_data.get("order_id")
            self._last_session_id = session_id

            self.log.info(
                "Upload-Session initialisiert: session_id=%s%s",
                session_id,
                f", order_id={order_id}" if order_id else "",
            )

        except ApiAuthError as e:
            self.log.error("Session-Initialisierung: %s", e)
            signals.upload_status_update.emit(str(e))
            return False
        except Exception as e:
            self.log.error(f"Session-Initialisierung fehlgeschlagen: {e}")
            signals.upload_status_update.emit(f"Fehler: {e}")
            return False

        # 3. Dateien nacheinander (Session-Offsets pro Datei seriell)
        uploaded_counter = {"bytes": 0}

        try:
            for file_info in files_to_upload:
                self._upload_file_via_session(
                    session_id, file_info, total_size, uploaded_counter
                )

            # 4. Session abschliessen (Kunden-URL)
            self.log.info("Alle Dateien hochgeladen, finalisiere Session...")
            signals.upload_status_update.emit("Finalisiere Upload...")
            customer_url = self._finalize_session(session_id)
            if not customer_url:
                customer_url = self._wait_for_completion_legacy(session_id)

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

        except ApiAuthError as e:
            self.log.error("Upload: %s", e)
            signals.upload_status_update.emit(str(e))
            return False
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
        self, session_id: str, file_info: dict, total_job_size: int, uploaded_counter: dict
    ):
        """Eine Datei: session/start → optional session/append* → session/finish (CHUNK_BYTES)."""
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
            "[%s] Session-Upload: %s bytes",
            self._session_ctx(session_id, file_name),
            file_size,
        )
        signals.upload_status_update.emit(f"Lade hoch: {file_name}")
        emit_progress(0)
        denom = file_size if file_size > 0 else 1
        signals.upload_progress_file.emit(0, 0, denom)

        with open(local_path, "rb") as f:
            first_len = min(CHUNK_BYTES, file_size) if file_size > 0 else 0
            first = f.read(first_len)
            r_start = self._session_start(session_id, file_name, file_size, first)
            off = self._parse_next_offset(
                r_start, len(first), session_id, file_name, "start"
            )
            emit_progress(off)

            while file_size - off > CHUNK_BYTES:
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

    def _get_mime_type(self, file_path):
        """Ermittelt den MIME-Type einer Datei."""
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or 'application/octet-stream'

import imaplib
import logging
import re
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr, formatdate, make_msgid
from core.config import ConfigManager


class EmailClient:
    """Verwaltet den Versand von E-Mails über SMTP und optional IMAP-Ablage."""

    _SENT_FOLDER_HINTS = (
        "Sent",
        "Sent Items",
        "Gesendet",
        "Gesendete Objekte",
        "INBOX.Sent",
        "INBOX.Gesendet",
    )

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.log = logging.getLogger('email')  # Spezieller Logger
        self._cached_sent_folder = None

    def send_email(self, to_recipient, subject, body):
        """Stellt die Verbindung zum SMTP-Server her und versendet die E-Mail."""
        original_recipient = to_recipient
        sandbox_str = self.config.get_setting("smtp_sandbox_mode", "false")
        if sandbox_str.lower() == "true":
            fallback = self.config.get_setting("smtp_fallback_recipient")
            if not fallback:
                self.log.error("E-Mail-Versand fehlgeschlagen: Sandbox-Modus aktiv, aber kein Fallback-Empfänger konfiguriert.")
                return False
            if original_recipient != fallback:
                self.log.info(
                    f"Sandbox-Modus: E-Mail für {original_recipient} wird an Fallback {fallback} gesendet."
                )
            to_recipient = fallback

        # Lade SMTP-Einstellungen
        host = self.config.get_setting("smtp_host")
        try:
            port = int(self.config.get_setting("smtp_port", 587))
        except ValueError:
            self.log.error("Ungültiger SMTP-Port, verwende 587.")
            port = 587

        user = self.config.get_secret("smtp_user")
        password = self.config.get_secret("smtp_pass")
        sender_addr = self.config.get_setting("smtp_sender_addr")
        sender_name = self.config.get_setting("smtp_sender_name", "Dropbox Uploader")

        if not all([host, user, password, sender_addr]):
            self.log.error("E-Mail-Versand fehlgeschlagen: SMTP-Einstellungen unvollständig.")
            return False

        try:
            self.log.info(f"Versende E-Mail an {to_recipient}...")

            # E-Mail-Nachricht erstellen
            msg = MIMEMultipart()
            msg['From'] = formataddr((sender_name, sender_addr))
            msg['To'] = to_recipient
            msg['Subject'] = subject
            msg['Date'] = formatdate(localtime=True)
            msg['Message-ID'] = make_msgid()
            msg.attach(MIMEText(body, 'html'))

            raw_message = msg.as_string()

            # Verbindung zum Server
            # Wir gehen von STARTTLS aus, was der Standard für Port 587 ist
            server = smtplib.SMTP(host, port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(user, password)

            # E-Mail senden
            server.sendmail(sender_addr, [to_recipient], raw_message)
            server.quit()

            self.log.info(f"E-Mail an {to_recipient} erfolgreich versendet.")
            self._save_to_sent_folder(msg)
            return True

        except smtplib.SMTPException as e:
            self.log.error(f"SMTP-Fehler beim Senden der E-Mail: {e}")
        except Exception as e:
            self.log.error(f"Allgemeiner Fehler beim E-Mail-Versand: {e}")

        return False

    def _resolve_imap_credentials(self):
        host = (self.config.get_setting("imap_host") or "").strip()
        if not host:
            host = (self.config.get_setting("smtp_host") or "").strip()
        try:
            port = int(self.config.get_setting("imap_port", 993))
        except (TypeError, ValueError):
            port = 993
        same_credentials = self.config.get_setting("imap_same_credentials", "true")
        if str(same_credentials).lower() == "true":
            user = self.config.get_secret("smtp_user")
            password = self.config.get_secret("smtp_pass")
        else:
            user = self.config.get_secret("imap_user") or self.config.get_secret("smtp_user")
            password = self.config.get_secret("imap_pass") or self.config.get_secret("smtp_pass")
        folder = (self.config.get_setting("imap_sent_folder") or "").strip()
        return host, port, user, password, folder

    @staticmethod
    def _format_imap_data(data) -> str:
        if not data:
            return ""
        parts = []
        for item in data:
            if item is None:
                continue
            if isinstance(item, bytes):
                parts.append(item.decode("utf-8", errors="replace"))
            else:
                parts.append(str(item))
        return " ".join(parts)

    @staticmethod
    def _is_valid_mailbox_path(path: str, delimiter: str = "/") -> bool:
        if not path or not path.strip():
            return False
        if path == delimiter:
            return False
        if path.startswith(delimiter):
            return False
        return True

    @staticmethod
    def _parse_list_entry(list_entry) -> dict | None:
        text = (
            list_entry.decode("utf-8", errors="replace")
            if isinstance(list_entry, bytes)
            else str(list_entry)
        ).strip()

        flags: list[str] = []
        flags_match = re.match(r"\(([^)]*)\)", text)
        if flags_match:
            flags = [flag for flag in flags_match.group(1).split() if flag]

        # RFC 3501: (flags) "delimiter" "mailbox" — delimiter ist quoted[0], mailbox quoted[1]
        quoted = re.findall(r'"([^"]*)"', text)
        delimiter = quoted[0] if quoted else "/"
        path = quoted[1] if len(quoted) >= 2 else ""

        if not path:
            unquoted_match = re.match(r'\([^)]*\)\s+"[^"]*"\s+(\S+)\s*$', text)
            if unquoted_match:
                path = unquoted_match.group(1)

        if not EmailClient._is_valid_mailbox_path(path, delimiter):
            return None

        name = path.split(delimiter)[-1] if delimiter in path else path
        return {"flags": flags, "path": path, "name": name, "delimiter": delimiter}

    def _list_mail_folders(self, mail) -> list[dict]:
        typ, data = mail.list()
        if typ != "OK" or not data:
            return []

        folders: list[dict] = []
        for item in data:
            if not item:
                continue
            folder = self._parse_list_entry(item)
            if folder and folder.get("path"):
                folders.append(folder)
        return folders

    @staticmethod
    def _folder_has_sent_flag(folder: dict) -> bool:
        return any(flag.upper() == "\\SENT" for flag in folder.get("flags", []))

    @staticmethod
    def _folder_matches_sent_hint(folder: dict) -> bool:
        path = folder.get("path") or ""
        name = folder.get("name") or path
        path_lower = path.lower()

        for hint in EmailClient._SENT_FOLDER_HINTS:
            if path == hint or name == hint:
                return True
        return "gesendet" in path_lower or "sent" in path_lower

    def _resolve_sent_folder_path(self, mail, configured_folder: str) -> tuple[str | None, str | None]:
        if self._cached_sent_folder and not self._is_valid_mailbox_path(self._cached_sent_folder):
            self._cached_sent_folder = None

        folders = self._list_mail_folders(mail)
        if not folders:
            return None, None

        folder_paths = {folder["path"] for folder in folders}

        if self._cached_sent_folder and self._cached_sent_folder in folder_paths:
            return self._cached_sent_folder, "cache"

        sent_by_flag = next(
            (
                folder
                for folder in folders
                if self._folder_has_sent_flag(folder) and self._is_valid_mailbox_path(folder["path"])
            ),
            None,
        )
        if sent_by_flag:
            return sent_by_flag["path"], "\\Sent"

        sent_by_name = next(
            (
                folder
                for folder in folders
                if self._folder_matches_sent_hint(folder) and self._is_valid_mailbox_path(folder["path"])
            ),
            None,
        )
        if sent_by_name:
            return sent_by_name["path"], "name"

        if configured_folder and configured_folder in folder_paths:
            return configured_folder, "configured"

        return None, None

    def _append_to_folder(self, mail, folder: str, msg) -> tuple[str, str]:
        typ, data = mail.append(
            folder,
            "\\Seen",
            imaplib.Time2Internaldate(time.time()),
            msg.as_bytes(),
        )
        return typ, self._format_imap_data(data)

    def _save_to_sent_folder(self, msg) -> None:
        enabled = self.config.get_setting("imap_save_sent_enabled", "true")
        if str(enabled).lower() != "true":
            return

        host, port, user, password, configured_folder = self._resolve_imap_credentials()
        if not all([host, user, password]):
            self.log.warning("IMAP-Ablage übersprungen: Zugangsdaten unvollständig.")
            return

        mail = None
        try:
            mail = imaplib.IMAP4_SSL(host, port)
            mail.login(user, password)
            sent_folder, source = self._resolve_sent_folder_path(mail, configured_folder)
            if not sent_folder:
                available = ", ".join(
                    folder["path"] for folder in self._list_mail_folders(mail)
                ) or "(keine)"
                self.log.warning(
                    "IMAP-Ablage übersprungen: Kein Gesendet-Ordner auf dem Server gefunden. "
                    f"Verfügbare Ordner: {available}"
                )
                return

            if not self._is_valid_mailbox_path(sent_folder):
                self.log.warning(
                    f"IMAP-Ablage übersprungen: Ungültiger Gesendet-Ordner '{sent_folder}'."
                )
                return

            typ, error_text = self._append_to_folder(mail, sent_folder, msg)
            if typ != "OK":
                self.log.warning(
                    f"IMAP-Ablage fehlgeschlagen (SMTP war OK) für '{sent_folder}': {error_text}"
                )
                return

            self._cached_sent_folder = sent_folder
            self.log.info(
                f"E-Mail in IMAP-Ordner '{sent_folder}' abgelegt "
                f"(Erkennung: {source})."
            )
        except Exception as e:
            self.log.warning(f"IMAP-Ablage fehlgeschlagen (SMTP war OK): {e}")
        finally:
            if mail is not None:
                try:
                    mail.logout()
                except Exception:
                    pass

    def send_upload_success_email(self, directory_name, share_link, email, vorname):
        """Sendet eine Erfolgs-E-Mail mit dem Freigabelink. Gibt True bei Erfolg zurück."""
        recipient = email
        if not recipient:
            recipient = self.config.get_setting("smtp_fallback_recipient")
        if not recipient:
            self.log.warning("Kein Fallback-Empfänger für Erfolgs-Mail konfiguriert.")
            return False

        subject = f"Upload erfolgreich: {directory_name}"
        body = f"""
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                        .container {{ width: 90%; margin: auto; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }}
                        .button {{ 
                            background-color: #007bff; color: #ffffff; padding: 10px 15px; 
                            text-decoration: none; border-radius: 5px; display: inline-block;
                        }}
                        .button:hover {{ background-color: #0056b3; }}
                        .link {{ color: #007bff; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2>Hallo {vorname},</h2>
                        <p>vielen Dank für deinen Besuch.</p>

                        <p>Die Medien zu deinem Sprung wurden erfolgreich hochgeladen und sind jetzt verfügbar.</p>
                        <p>Du kannst sie über den folgenden Link herunterladen:</p>
                        <p>
                            <a href="{share_link}" class="button">Zum Download</a>
                        </p>
                        <p>
                            Falls der Button nicht funktioniert, kopiere bitte diesen Link in deinen Browser:<br>
                            <a href="{share_link}" class="link">{share_link}</a>
                        </p>

                        <p>Bei Fragen ruf einfach bei uns an unter 05674-99930, montags, dienstags, donnerstags und freitags 9:30 - 13 Uhr.</p>
                        <p>Der Link bleibt ca. 14 Tage aktiv.</p>
                        <p>Dein AERO Fallschirmsport Team</p>
                    </div>
                </body>
                </html>
                """
        return self.send_email(recipient, subject, body)

    def send_upload_failure_email(self, directory_name, error):
        """Sendet eine Fehler-E-Mail."""
        recipient = self.config.get_setting("smtp_fallback_recipient")
        if not recipient:
            self.log.warning("Kein Fallback-Empfänger für Fehler-Mail konfiguriert.")
            return

        subject = f"Upload FEHLGESCHLAGEN: {directory_name}"
        body = (
            f"Hallo,\n\n"
            f"Das Verzeichnis '{directory_name}' konnte NICHT hochgeladen werden.\n\n"
            f"Fehlerdetails:\n"
            f"{error}\n\n"
            f"Das Verzeichnis wurde in den Fehler-Ordner verschoben (falls konfiguriert)."
        )
        self.send_email(recipient, subject, body)

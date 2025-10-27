import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from core.config import ConfigManager


class EmailClient:
    """Verwaltet den Versand von E-Mails über SMTP."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.log = logging.getLogger('email')  # Spezieller Logger

    def send_email(self, to_recipient, subject, body):
        """Stellt die Verbindung zum SMTP-Server her und versendet die E-Mail."""

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
            msg.attach(MIMEText(body, 'html'))

            # Verbindung zum Server
            # Wir gehen von STARTTLS aus, was der Standard für Port 587 ist
            server = smtplib.SMTP(host, port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(user, password)

            # E-Mail senden
            server.sendmail(sender_addr, [to_recipient], msg.as_string())
            server.quit()

            self.log.info(f"E-Mail an {to_recipient} erfolgreich versendet.")
            return True

        except smtplib.SMTPException as e:
            self.log.error(f"SMTP-Fehler beim Senden der E-Mail: {e}")
        except Exception as e:
            self.log.error(f"Allgemeiner Fehler beim E-Mail-Versand: {e}")

        return False

    def send_upload_success_email(self, directory_name, share_link, email):
        """Sendet eine Erfolgs-E-Mail mit dem Freigabelink."""
        recipient = email
        if not recipient:
            recipient = self.config.get_setting("smtp_fallback_recipient")
        if not recipient:
            self.log.warning("Kein Fallback-Empfänger für Erfolgs-Mail konfiguriert.")
            return

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
                        <h2>Hallo,</h2>
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
        self.send_email(recipient, subject, body)

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

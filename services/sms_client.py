import logging
import aiohttp  # Erforderlich für asynchrone HTTP-Anfragen

from core.config import ConfigManager
from models.kunde import Kunde


class SmsClient:
    """Verwaltet den Versand von SMS-Nachrichten über die Seven.io API."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.log = logging.getLogger('sms')  # Spezieller Logger
        # Der API-Endpunkt ist normalerweise statisch
        self.endpoint = "https://gateway.seven.io/api/sms"

    async def send_sms(self, to_recipient, text_body):
        """
        Stellt die Verbindung zur Seven.io API her und versendet die SMS.
        (Analog zu send_email)
        """

        # Lade API-Einstellungen
        try:
            # Sandbox-Modus aus Config lesen (standardmäßig 'false')
            sandbox_str = self.config.get_setting("seven_sandbox_mode", "false")
            sandbox = 1 if sandbox_str.lower() == 'true' else 0
            mode = "SANDBOX" if sandbox == 1 else "LIVE"

            if sandbox == 1:
                # Verwende den Sandbox-Key
                api_key = self.config.get_secret("seven_sandbox_api_key")
                self.log.debug("Verwende Seven.io Sandbox API Key.")
            else:
                # Verwende den Produktions-Key
                api_key = self.config.get_secret("seven_api_key")
                self.log.debug("Verwende Seven.io Production API Key.")

            sender = self.config.get_setting("seven_sender")

        except Exception as e:
            self.log.error(f"Fehler beim Laden der SMS-Konfiguration: {e}")
            return False

        if not all([api_key, sender]):
            # Logik angepasst, um den spezifischen Key-Namen anzuzeigen
            key_name = "seven_sandbox_api_key" if sandbox == 1 else "seven_api_key"
            self.log.error(f"SMS-Versand fehlgeschlagen: '{key_name}' oder 'seven_sender' unvollständig.")
            return False

        if not to_recipient:
            self.log.error("SMS-Versand fehlgeschlagen: Kein Empfänger (to_recipient) angegeben.")
            return False

        try:
            self.log.info(f"({mode}) Versende SMS an {to_recipient}...")

            # Daten für die POST-Anfrage
            payload = {
                "to": to_recipient,
                "text": text_body,
                "from": sender,
                "sandbox": sandbox
            }
            headers = {"X-Api-Key": api_key}

            # Setze ein Timeout für die Anfrage
            timeout = aiohttp.ClientTimeout(total=10)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.endpoint, headers=headers, data=payload) as resp:

                    response_text = await resp.text()

                    if resp.status == 200:
                        self.log.info(
                            f"({mode}) SMS an {to_recipient} erfolgreich verarbeitet. Response: {response_text[:80]}{'...' if len(response_text) > 80 else ''}")
                        return True
                    else:
                        # Logge den API-Fehler
                        self.log.error(
                            f"({mode}) SMS-API-Fehler bei Versand an {to_recipient}. Status: {resp.status}, Response: {response_text}")
                        return False

        except aiohttp.ClientError as e:
            # Spezifischer Fehler für Netzwerk/HTTP-Probleme
            self.log.error(f"AIOHTTP-Fehler beim Senden der SMS an {to_recipient}: {e}")
        except Exception as e:
            # Allgemeiner Fehler (z.B. Timeout, DNS-Fehler)
            self.log.error(f"Allgemeiner Fehler beim SMS-Versand an {to_recipient}: {e}")

        return False

    async def send_upload_success_sms(self, share_link, kunde: Kunde):
        """
        Sendet eine Erfolgs-SMS mit dem Freigabelink.
        (Analog zu send_upload_success_email)
        """

        phone_number = kunde.phone
        if not phone_number:
            self.log.warning(
                f"Keine Telefonnummer für Erfolgs-SMS (Gast: {kunde.first_name} {kunde.last_name}) angegeben. Versand wird übersprungen.")
            return False  # Nicht senden, wenn keine Nummer da ist

        # Text für die SMS formatieren (kürzer als E-Mail)
        text = (
            f"Hallo {kunde.first_name},\n"
            f"dein Medien-Download ist fertig.\n"
            f"Link (14 Tage gültig): {share_link}\n\n"
            f"Dein AERO Fallschirmsport Team\n"
            f"(Bei Fragen: 05674-99930)"
        )

        # Rufe die zentrale Versandmethode auf
        return await self.send_sms(phone_number, text)

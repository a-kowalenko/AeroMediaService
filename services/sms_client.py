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
        self.last_error = ""

    async def get_balance(self, api_key):
        """Ruft die aktuelle Balance von Seven.io ab."""
        url = "https://gateway.seven.io/api/balance"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("amount")
        except Exception as e:
            self.log.error(f"Fehler beim Abrufen der Seven.io Balance: {e}")
        return None

    async def send_sms(self, to_recipient, text_body):
        """
        Stellt die Verbindung zur Seven.io API her und versendet die SMS.
        Gibt ein Tupel (Erfolg_boolean, sms_id_string_oder_None) zurück.
        """

        self.last_error = ""

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
            self.last_error = f"Konfigurationsfehler: {e}"
            return False, None

        if not all([api_key, sender]):
            # Logik angepasst, um den spezifischen Key-Namen anzuzeigen
            key_name = "seven_sandbox_api_key" if sandbox == 1 else "seven_api_key"
            self.log.error(f"SMS-Versand fehlgeschlagen: '{key_name}' oder 'seven_sender' unvollständig.")
            self.last_error = f"Konfigurationsfehler: {key_name}/seven_sender unvollständig"
            return False, None

        if not to_recipient:
            self.log.error("SMS-Versand fehlgeschlagen: Kein Empfänger (to_recipient) angegeben.")
            self.last_error = "Kein Empfänger angegeben"
            return False, None

        try:
            self.log.info(f"({mode}) Versende SMS an {to_recipient}...")

            # Daten für die POST-Anfrage
            payload = {
                "to": to_recipient,
                "text": text_body,
                "from": sender,
                "sandbox": sandbox,
                "json": 1
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

                        sms_id = None
                        try:
                            import json
                            data = json.loads(response_text)
                            messages = data.get("messages", [])
                            if messages:
                                message = messages[0] or {}
                                message_success = message.get("success")
                                message_error_text = message.get("error_text")
                                message_error_code = message.get("error")
                                message_id = message.get("id")

                                if message_id is not None:
                                    sms_id = str(message_id)

                                # Seven kann HTTP 200 liefern, obwohl die Nachricht fehlschlug.
                                if message_success in (False, "false", 0, "0"):
                                    self.last_error = (
                                        f"{message_error_text or 'SMS abgelehnt'}"
                                        + (f" (Code {message_error_code})" if message_error_code is not None else "")
                                    )
                                    self.log.error(
                                        f"({mode}) SMS abgelehnt für {to_recipient}: {self.last_error}. "
                                        f"Response: {response_text}"
                                    )
                                    return False, sms_id
                        except Exception as parse_e:
                            self.log.error(f"Konnte SMS-ID nicht auslesen: {parse_e}")
                            self.last_error = f"Antwort konnte nicht verarbeitet werden: {parse_e}"

                        # Balance prüfen
                        if mode == "LIVE":
                            balance = await self.get_balance(api_key)
                            if balance is not None and balance < 1.0:
                                self.log.error(f"ACHTUNG: Seven.io Balance ist unter 1€ (Aktueller Stand: {balance}€)")

                        return True, sms_id
                    else:
                        # Logge den API-Fehler
                        self.log.error(
                            f"({mode}) SMS-API-Fehler bei Versand an {to_recipient}. Status: {resp.status}, Response: {response_text}")
                        self.last_error = f"HTTP {resp.status}: {response_text}"
                        return False, None

        except aiohttp.ClientError as e:
            # Spezifischer Fehler für Netzwerk/HTTP-Probleme
            self.log.error(f"AIOHTTP-Fehler beim Senden der SMS an {to_recipient}: {e}")
            self.last_error = f"Netzwerkfehler: {e}"
        except Exception as e:
            # Allgemeiner Fehler (z.B. Timeout, DNS-Fehler)
            self.log.error(f"Allgemeiner Fehler beim SMS-Versand an {to_recipient}: {e}")
            self.last_error = f"Allgemeiner Fehler: {e}"

        return False, None

    async def send_upload_success_sms(self, share_link, kunde: Kunde):
        """
        Sendet eine Erfolgs-SMS mit dem Freigabelink.
        Gibt ein Tupel (Erfolg_boolean, sms_id_string_oder_None) zurück.
        """

        phone_number = kunde.phone
        if not phone_number:
            self.log.warning(
                f"Keine Telefonnummer für Erfolgs-SMS (Gast: {kunde.first_name} {kunde.last_name}) angegeben. Versand wird übersprungen.")
            return False, None  # Nicht senden, wenn keine Nummer da ist

        # Text für die SMS formatieren (kürzer als E-Mail)
        text = (
            f"Hallo {kunde.first_name},\n"
            f"dein Medien-Download ist fertig.\n"
            f"Link (14 Tage gültig): {share_link}\n\n"
            f"Dein AERO Team\n"
            f"(Bei Fragen: 05674-99930)"
        )

        # Rufe die zentrale Versandmethode auf
        return await self.send_sms(phone_number, text)

    async def get_sms_journal(self, limit=100):
        """
        Ruft das Logbuch (Outbound Journal) von Seven.io ab.
        """
        sandbox_str = self.config.get_setting("seven_sandbox_mode", "false")
        sandbox = 1 if sandbox_str.lower() == 'true' else 0
        if sandbox == 1:
            api_key = self.config.get_secret("seven_sandbox_api_key")
        else:
            api_key = self.config.get_secret("seven_api_key")

        if not api_key:
            return None

        url = f"https://gateway.seven.io/api/journal/outbound?limit={limit}"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data
                    else:
                        self.log.error(f"Fehler beim Abrufen des SMS-Journals: HTTP {resp.status}")
                        return None
        except Exception as e:
            self.log.error(f"Ausnahme beim Abrufen des SMS-Journals: {e}")
            return None

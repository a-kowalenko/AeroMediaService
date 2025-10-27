import requests
import logging

class LinkShortener:
    """Utility-Klasse zum Kürzen von URLs mit dem SkyLink-Shortener."""

    def __init__(self, config_manager):
        self.config = config_manager
        self.log = logging.getLogger(__name__)

    def shorten(self, long_url):
        """Kürzt eine URL mit dem SkyLink-Shortener."""
        api_url = self.config.get_secret("skylink_api_url")
        api_key = self.config.get_secret("skylink_api_key")

        if not api_url or not api_key:
            self.log.error("SkyLink API URL oder Key fehlt in der Konfiguration!")
            return long_url

        headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json'
        }
        data = {'url': long_url}

        try:
            response = requests.post(api_url, json=data, headers=headers, timeout=5)

            if response.status_code == 201:
                short_url = response.json().get('short_url')
                self.log.info(f"Link erfolgreich gekürzt: {short_url}")
                return short_url
            else:
                self.log.warning(
                    f"Kürzen des Links fehlgeschlagen (Status {response.status_code}), "
                    f"Content-Type: {response.headers.get('content-type')}"
                )
                if response.status_code == 401:
                    self.log.error("API-Key für SkyLink-Shortener ist ungültig oder fehlt!")
                return long_url

        except requests.exceptions.Timeout:
            self.log.error("Fehler bei der Verbindung zum SkyLink-Shortener: Timeout")
            return long_url
        except requests.RequestException as e:
            self.log.error(f"Fehler bei der Verbindung zum SkyLink-Shortener: {e}")
            return long_url

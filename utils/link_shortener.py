import requests
import logging

class LinkShortener:
    """Utility-Klasse zum Kürzen von URLs mit dem SkyLink-Shortener."""

    def __init__(self, config_manager):
        self.config = config_manager
        self.log = logging.getLogger(__name__)

    def shorten(self, long_url):
        """Kürzt eine URL mit dem SkyLink-Shortener."""

        self.log.info(f"Versuche, URL zu kürzen: {long_url}")

        api_url = self.config.get_secret("skylink_api_url")
        api_key = self.config.get_secret("skylink_api_key")

        if not api_url:
            self.log.error("SkyLink API URL oder Key fehlt in der Konfiguration!")
            return long_url

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'

        data = {'long_url': long_url}

        try:
            response = requests.post(api_url, json=data, headers=headers, timeout=5)

            if response.status_code == 201 or response.status_code == 200:
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

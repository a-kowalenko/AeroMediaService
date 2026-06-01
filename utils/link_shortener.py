import calendar
import logging
from datetime import datetime, timedelta, timezone

import requests

# Preset-Keys für shortener_expires_preset (Settings-ComboBox)
EXPIRES_PRESET_PERMANENT = "permanent"
EXPIRES_PRESET_14D = "14d"
EXPIRES_PRESET_1M = "1m"
EXPIRES_PRESET_3M = "3m"
EXPIRES_PRESET_6M = "6m"
EXPIRES_PRESET_1Y = "1y"

EXPIRES_PRESET_KEYS = (
    EXPIRES_PRESET_PERMANENT,
    EXPIRES_PRESET_14D,
    EXPIRES_PRESET_1M,
    EXPIRES_PRESET_3M,
    EXPIRES_PRESET_6M,
    EXPIRES_PRESET_1Y,
)


def expires_at_from_preset(preset: str) -> str | None:
    """
    Berechnet expires_at (ISO-8601 UTC) ab jetzt.
    None = permanent (Feld wird nicht gesendet).
    """
    key = (preset or EXPIRES_PRESET_PERMANENT).strip().lower()
    if key == EXPIRES_PRESET_PERMANENT or key not in EXPIRES_PRESET_KEYS:
        return None

    now = datetime.now(timezone.utc)
    if key == EXPIRES_PRESET_14D:
        exp = now + timedelta(days=14)
    else:
        months = {
            EXPIRES_PRESET_1M: 1,
            EXPIRES_PRESET_3M: 3,
            EXPIRES_PRESET_6M: 6,
            EXPIRES_PRESET_1Y: 12,
        }[key]
        exp = _add_calendar_months(now, months)

    return exp.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _add_calendar_months(dt: datetime, months: int) -> datetime:
    """Addiert Kalendermonate (z. B. 31. Jan. + 1 Monat → 28./29. Feb.)."""
    m = dt.month - 1 + months
    year = dt.year + m // 12
    month = m % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


class LinkShortener:
    """Kürzt Freigabe-URLs über POST /api/shorten (skydive-media.de o. ä.)."""

    def __init__(self, config_manager):
        self.config = config_manager
        self.log = logging.getLogger(__name__)

    def _is_enabled(self, override_enabled=None):
        if override_enabled is not None:
            return bool(override_enabled)
        raw = self.config.get_setting("link_shortener_enabled", "false")
        return str(raw).lower() == "true"

    def _resolve_preset(self, override_preset=None) -> str:
        if override_preset is not None:
            return str(override_preset).strip().lower() or EXPIRES_PRESET_PERMANENT
        return (
            self.config.get_setting("shortener_expires_preset", EXPIRES_PRESET_PERMANENT)
            or EXPIRES_PRESET_PERMANENT
        ).strip().lower()

    def _resolve_credentials(self, override_base=None, override_key=None):
        base = (override_base or "").strip() or self.config.get_secret("shortener_base_url")
        api_key = (override_key or "").strip() or self.config.get_secret("shortener_api_key")

        if not base or not api_key:
            legacy_url = self.config.get_secret("skylink_api_url")
            legacy_key = self.config.get_secret("skylink_api_key")
            if legacy_key and not api_key:
                api_key = legacy_key
            if legacy_url and not base:
                base = self._legacy_url_to_base(legacy_url)

        return base, api_key

    @staticmethod
    def _legacy_url_to_base(api_url: str) -> str:
        """Alte SkyLink-URL (voller Endpoint) in Basis-URL umwandeln."""
        url = api_url.strip().rstrip("/")
        for suffix in ("/api/shorten", "/api/create"):
            if url.endswith(suffix):
                return url[: -len(suffix)]
        return url

    def shorten(
        self,
        long_url,
        *,
        override_base=None,
        override_key=None,
        override_enabled=None,
        override_preset=None,
    ):
        """Kürzt eine URL; bei Fehler oder deaktiviertem Shortener die Original-URL."""

        if not self._is_enabled(override_enabled):
            self.log.debug("Link-Shortener deaktiviert, überspringe Kürzen.")
            return long_url

        base, api_key = self._resolve_credentials(override_base, override_key)
        if not base or not api_key:
            self.log.debug("Shortener Basis-URL oder API-Key fehlt, überspringe Kürzen.")
            return long_url

        preset = self._resolve_preset(override_preset)
        expires_at = expires_at_from_preset(preset)

        endpoint = f"{base.rstrip('/')}/api/shorten"
        self.log.info("Versuche, URL zu kürzen: %s", long_url)

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        body = {"url": long_url}
        if expires_at:
            body["expires_at"] = expires_at

        try:
            response = requests.post(endpoint, json=body, headers=headers, timeout=30)

            if response.status_code == 201:
                short_url = response.json().get("short_url")
                if short_url:
                    self.log.info("Link erfolgreich gekürzt: %s", short_url)
                    return short_url
                self.log.warning("Shortener-Antwort ohne short_url")
                return long_url

            error_msg = self._parse_error(response)
            self.log.warning(
                "Kürzen fehlgeschlagen (Status %s): %s",
                response.status_code,
                error_msg,
            )
            if response.status_code == 401:
                self.log.error("API-Key ungültig, abgelaufen oder Rate-Limit überschritten.")
            return long_url

        except requests.exceptions.Timeout:
            self.log.error("Verbindung zum Link-Shortener: Timeout")
            return long_url
        except requests.RequestException as e:
            self.log.error("Verbindung zum Link-Shortener: %s", e)
            return long_url

    @staticmethod
    def _parse_error(response):
        try:
            data = response.json()
            if isinstance(data, dict) and data.get("error"):
                return data["error"]
        except ValueError:
            pass
        text = (response.text or "").strip()
        if len(text) > 300:
            return text[:300] + "..."
        return text or f"HTTP {response.status_code}"

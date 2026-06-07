import re

_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def is_valid_email(value: str) -> bool:
    """Einfache E-Mail-Formatprüfung (nicht RFC-komplett)."""
    if not value or not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate or " " in candidate:
        return False
    return bool(_EMAIL_RE.match(candidate))


def is_valid_share_link(value: str) -> bool:
    """Prüft, ob ein String wie eine HTTP(S)-URL aussieht."""
    if not value or not isinstance(value, str):
        return False
    candidate = value.strip()
    return candidate.startswith("http://") or candidate.startswith("https://")

"""Gesamtstatus der Upload-Historie (Versendet / Komplett / …)."""
from __future__ import annotations

from datetime import datetime
import re

# Ohne Zustellbestätigung nach dieser Zeit gilt SMS als zugestellt (Seven-DLR fehlt oft).
SMS_DELIVERY_STALE_HOURS = 72


def translate_sms_dlr_status(status: str | None) -> str:
    lower_status = (status or "").lower()
    if "notdelivered" in lower_status or lower_status in {"undeliv", "rejectd", "expired"}:
        return "Fehlgeschlagen"
    if "failed" in lower_status:
        return "Fehlgeschlagen"
    if "delivered" in lower_status or lower_status == "delivrd":
        return "Zugestellt"
    if "buffered" in lower_status:
        return "Gepuffert"
    if "transmitted" in lower_status:
        return "Übertragen"
    if "accepted" in lower_status or lower_status == "acceptd":
        return "Akzeptiert"
    if "rejected" in lower_status:
        return "Abgelehnt"
    return status or ""


def _parse_iso_timestamp(ts_str: str | None) -> float | None:
    if not ts_str:
        return None
    text = str(ts_str).strip().replace("Z", "")
    if not text:
        return None
    text = text.split("+")[0]
    if "." in text:
        main, frac = text.split(".", 1)
        text = f"{main}.{frac[:6]}"
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            length = 26 if ".%f" in fmt else 19
            return datetime.strptime(text[:length], fmt).timestamp()
        except ValueError:
            continue
    return None


def sms_sent_reference_timestamp(item_data: dict) -> float | None:
    """Zeitpunkt des letzten SMS-Versands für Stale-Logik."""
    for key in ("last_sms_resent_at", "last_updated", "created_at"):
        ts = _parse_iso_timestamp(item_data.get(key))
        if ts is not None:
            return ts
    return None


def hours_since_sms_sent(item_data: dict) -> float | None:
    ref = sms_sent_reference_timestamp(item_data)
    if ref is None:
        return None
    return max(0.0, (datetime.now().timestamp() - ref) / 3600.0)


def is_sms_sent_status(status_value: str | None) -> bool:
    s = (status_value or "").strip().lower()
    if not s:
        return False
    if s == "übersprungen":
        return True
    return any(
        token in s
        for token in (
            "gesendet",
            "zugestellt",
            "erfolgreich",
            "übertragen",
            "gepuffert",
            "akzeptiert",
        )
    )


def is_sms_delivered_status(status_value: str | None, item_data: dict | None = None) -> bool:
    s = (status_value or "").strip().lower()
    if not s:
        return False
    if s == "übersprungen":
        return True
    if ("zugestellt" in s) or ("erfolgreich" in s):
        return True
    if item_data and is_sms_sent_status(status_value):
        age = hours_since_sms_sent(item_data)
        if age is not None and age >= SMS_DELIVERY_STALE_HOURS:
            return True
    return False


def build_overall_status(item_data: dict) -> str:
    """Erstellt den Gesamtstatus für das Main Grid."""
    upload_status = (item_data.get("status") or "").strip()
    email_status = (item_data.get("email_status") or "").strip()
    sms_status = (item_data.get("sms_status") or "").strip()
    email_value = (item_data.get("email") or "").strip()
    phone_value = (item_data.get("phone") or "").strip()

    def is_problem(status_value: str | None) -> bool:
        s = (status_value or "").strip().lower()
        if not s:
            return False
        return ("fehler" in s) or ("fehlgeschlagen" in s) or ("abgelehnt" in s)

    def is_in_progress(status_value: str | None) -> bool:
        s = (status_value or "").strip().lower()
        if not s:
            return False
        return ("gestartet" in s) or ("übertragen" in s) or ("gepuffert" in s) or ("akzeptiert" in s)

    def is_best_upload(status_value: str | None) -> bool:
        return "erfolgreich" in (status_value or "").strip().lower()

    def is_best_email(status_value: str | None) -> bool:
        s = (status_value or "").strip().lower()
        return ("gesendet" in s) or ("zugestellt" in s) or ("erfolgreich" in s)

    upload_problem = is_problem(upload_status)
    email_problem = bool(email_value) and is_problem(email_status)
    sms_problem = bool(phone_value) and is_problem(sms_status)
    if upload_problem or email_problem or sms_problem:
        return "Problem"

    # Nur Upload/E-Mail-Laufstatus blockiert; SMS-Zwischenstände (Übertragen …) = Versendet.
    if is_in_progress(upload_status) or is_in_progress(email_status):
        return "In Bearbeitung"

    upload_is_best = is_best_upload(upload_status)
    email_is_best = (not email_value) or is_best_email(email_status)
    sms_is_sent = (not phone_value) or is_sms_sent_status(sms_status)
    sms_is_delivered = (not phone_value) or is_sms_delivered_status(sms_status, item_data)

    if upload_is_best and email_is_best and sms_is_sent:
        if sms_is_delivered:
            return "Komplett"
        return "Versendet"

    if upload_status or email_status or sms_status:
        return "Teilweise"

    return "Unbekannt"


def history_entry_needs_sms_journal_check(item: dict) -> bool:
    """True, wenn ein Journal-Abgleich den SMS-Status noch verbessern könnte."""
    phone = (item.get("phone") or "").strip()
    if not phone:
        return False
    sms_status = (item.get("sms_status") or "").strip()
    if not sms_status or sms_status == "Übersprungen":
        return False
    if sms_status in {"Zugestellt", "Fehlgeschlagen"}:
        return False
    if is_problem_status(sms_status):
        return False
    if is_sms_delivered_status(sms_status, item):
        return False
    return True


def is_problem_status(status_value: str | None) -> bool:
    s = (status_value or "").strip().lower()
    if not s:
        return False
    return ("fehler" in s) or ("fehlgeschlagen" in s) or ("abgelehnt" in s)


def normalize_phone_digits(phone: str | None) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = "49" + digits[1:]
    elif len(digits) <= 11 and not digits.startswith("49"):
        digits = "49" + digits.lstrip("0")
    return digits


def phones_match(phone_a: str | None, phone_b: str | None) -> bool:
    a = normalize_phone_digits(phone_a)
    b = normalize_phone_digits(phone_b)
    if not a or not b:
        return False
    if a == b:
        return True
    tail_len = min(len(a), len(b), 10)
    if tail_len >= 8 and a[-tail_len:] == b[-tail_len:]:
        return True
    return False

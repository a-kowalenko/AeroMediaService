"""Abgleich der Upload-Historie mit dem Seven.io Outbound-Journal."""
from __future__ import annotations

from datetime import datetime

from core.history_status import (
    history_entry_needs_sms_journal_check,
    phones_match,
    translate_sms_dlr_status,
    _parse_iso_timestamp,
)


JOURNAL_MATCH_WINDOW_SEC = 86_400  # 24 h um den Versandzeitpunkt


def _journal_message_timestamp(msg: dict) -> float | None:
    for key in ("timestamp", "dlr_timestamp", "time", "status_time"):
        ts = _parse_iso_timestamp(msg.get(key))
        if ts is not None:
            return ts
    return None


def _history_sms_reference_timestamp(item: dict) -> float | None:
    for key in ("last_sms_resent_at", "last_updated", "created_at"):
        ts = _parse_iso_timestamp(item.get(key))
        if ts is not None:
            return ts
    return None


def _journal_recipient(msg: dict) -> str:
    for key in ("to", "recipient", "system"):
        value = msg.get(key)
        if value:
            return str(value)
    return ""


def match_history_entry_to_journal(item: dict, journal_data: list[dict]) -> dict | None:
    """Findet die passende Journal-Nachricht per sms_id oder Telefon/Zeit."""
    sms_id = str(item.get("sms_id") or "").strip()
    if sms_id.lower() in {"none", "null", "nan"}:
        sms_id = ""

    journal_by_id = {str(msg.get("id")): msg for msg in journal_data if msg.get("id")}
    if sms_id and sms_id in journal_by_id:
        return journal_by_id[sms_id]

    phone = (item.get("phone") or "").strip()
    if not phone:
        return None

    ref_ts = _history_sms_reference_timestamp(item)
    candidates: list[tuple[float, dict]] = []
    for msg in journal_data:
        if not phones_match(phone, _journal_recipient(msg)):
            continue
        msg_ts = _journal_message_timestamp(msg)
        if ref_ts is not None and msg_ts is not None:
            delta = abs(msg_ts - ref_ts)
            if delta > JOURNAL_MATCH_WINDOW_SEC:
                continue
            candidates.append((delta, msg))
        else:
            candidates.append((0.0, msg))

    if not candidates:
        return None

    candidates.sort(key=lambda pair: pair[0])
    return candidates[0][1]


def apply_journal_message_to_item(item: dict, matched_msg: dict) -> bool:
    """Schreibt DLR-Status, Preis und sms_id in den Historieneintrag."""
    status_raw = matched_msg.get("dlr") or matched_msg.get("state") or matched_msg.get("status") or ""
    translated_status = translate_sms_dlr_status(status_raw)
    price = matched_msg.get("price")

    changed = False
    if translated_status and translated_status != item.get("sms_status"):
        item["sms_status"] = translated_status
        changed = True

    if price and price != item.get("sms_price"):
        item["sms_price"] = price
        changed = True

    msg_id = matched_msg.get("id")
    if msg_id and not item.get("sms_id"):
        item["sms_id"] = str(msg_id)
        changed = True

    if changed:
        item["last_updated"] = datetime.now().isoformat()
    return changed


def update_history_from_journal(history: list[dict], journal_data: list[dict]) -> list[dict]:
    """Aktualisiert offene SMS-Einträge; liefert die geänderten Einträge."""
    updated_items: list[dict] = []
    for item in history:
        if not history_entry_needs_sms_journal_check(item):
            continue
        matched_msg = match_history_entry_to_journal(item, journal_data)
        if matched_msg and apply_journal_message_to_item(item, matched_msg):
            updated_items.append(dict(item))
    return updated_items

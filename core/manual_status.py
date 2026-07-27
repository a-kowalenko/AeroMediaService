"""Manuelle Schnellaktionen für den Gesamtstatus in der Upload-Historie."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from core.history_status import build_overall_status, is_problem_status

ACTION_MARK_COMPLETE = "Komplett"
ACTION_MARK_SENT = "Versendet"
ACTION_RESOLVE_PROBLEM = "Problem auflösen"

MANUAL_STATUS_ACTIONS = (
    ACTION_MARK_COMPLETE,
    ACTION_MARK_SENT,
    ACTION_RESOLVE_PROBLEM,
)


def _snapshot_channels(entry: dict[str, Any]) -> dict[str, str]:
    return {
        "status": (entry.get("status") or "").strip(),
        "email_status": (entry.get("email_status") or "").strip(),
        "sms_status": (entry.get("sms_status") or "").strip(),
        "error_msg": (entry.get("error_msg") or "").strip(),
    }


def _has_email(entry: dict[str, Any]) -> bool:
    return bool((entry.get("email") or "").strip())


def _has_phone(entry: dict[str, Any]) -> bool:
    return bool((entry.get("phone") or "").strip())


def collect_manual_status_warnings(entry: dict[str, Any], action: str) -> list[str]:
    """Hinweise vor dem manuellen Setzen (blockieren nicht)."""
    warnings: list[str] = []
    upload_status = (entry.get("status") or "").strip()

    if action in {ACTION_MARK_COMPLETE, ACTION_MARK_SENT}:
        if upload_status not in {"", "Erfolgreich", "Gestartet"}:
            warnings.append(
                f"Upload-Status ist „{upload_status}“ — wird auf „Erfolgreich“ gesetzt."
            )
        if not (entry.get("share_link") or "").strip():
            warnings.append("Kein Download-Link gespeichert — erneuter Versand ist ggf. nicht möglich.")

    if action == ACTION_RESOLVE_PROBLEM:
        if build_overall_status(entry) != "Problem":
            warnings.append("Der aktuelle Gesamtstatus ist nicht „Problem“.")

    if action == ACTION_MARK_COMPLETE and not _has_email(entry) and not _has_phone(entry):
        warnings.append("Weder E-Mail noch Telefon hinterlegt.")

    return warnings


def _target_email_status(entry: dict[str, Any], *, delivered: bool) -> str | None:
    if not _has_email(entry):
        return None
    current = (entry.get("email_status") or "").strip()
    if delivered or is_problem_status(current) or not current:
        return "Gesendet"
    return None


def _target_sms_status(entry: dict[str, Any], *, delivered: bool) -> str | None:
    if not _has_phone(entry):
        return None
    current = (entry.get("sms_status") or "").strip()
    if delivered:
        if current == "Zugestellt":
            return None
        return "Zugestellt"
    if is_problem_status(current) or not current:
        return "Gesendet"
    lower = current.lower()
    if any(
        token in lower
        for token in ("gesendet", "zugestellt", "übertragen", "gepuffert", "akzeptiert", "übersprungen")
    ):
        return None
    return "Gesendet"


def _apply_resolve_problem(entry: dict[str, Any], updates: dict[str, Any]) -> None:
    upload_status = (entry.get("status") or "").strip()
    if is_problem_status(upload_status) or upload_status in {"Fehler", "Abgebrochen"}:
        updates["status"] = "Erfolgreich"
        updates["error_msg"] = ""

    email_status = (entry.get("email_status") or "").strip()
    if _has_email(entry) and is_problem_status(email_status):
        updates["email_status"] = "Gesendet"

    sms_status = (entry.get("sms_status") or "").strip()
    if _has_phone(entry) and is_problem_status(sms_status):
        updates["sms_status"] = "Zugestellt"


def build_manual_status_update(
    entry: dict[str, Any],
    action: str,
    *,
    reason: str = "",
) -> dict[str, Any]:
    """
    Erstellt das Update-Payload für HistoryManager.add_or_update().

    Raises:
        ValueError: Unbekannte Aktion oder fehlender dir_name.
    """
    if action not in MANUAL_STATUS_ACTIONS:
        raise ValueError(f"Unbekannte Aktion: {action}")

    dir_name = (entry.get("dir_name") or "").strip()
    if not dir_name:
        raise ValueError("Historieneintrag ohne dir_name.")

    before = _snapshot_channels(entry)
    updates: dict[str, Any] = {"dir_name": dir_name}

    if action == ACTION_MARK_COMPLETE:
        updates["status"] = "Erfolgreich"
        email_target = _target_email_status(entry, delivered=True)
        if email_target:
            updates["email_status"] = email_target
        sms_target = _target_sms_status(entry, delivered=True)
        if sms_target:
            updates["sms_status"] = sms_target
        if is_problem_status(before["status"]) or before["status"] in {"Fehler", "Abgebrochen"}:
            updates["error_msg"] = ""

    elif action == ACTION_MARK_SENT:
        updates["status"] = "Erfolgreich"
        email_target = _target_email_status(entry, delivered=False)
        if email_target:
            updates["email_status"] = email_target
        sms_target = _target_sms_status(entry, delivered=False)
        if sms_target:
            updates["sms_status"] = sms_target
        if is_problem_status(before["status"]) or before["status"] in {"Fehler", "Abgebrochen"}:
            updates["error_msg"] = ""

    elif action == ACTION_RESOLVE_PROBLEM:
        _apply_resolve_problem(entry, updates)
        if not any(k in updates for k in ("status", "email_status", "sms_status", "error_msg")):
            raise ValueError("Kein Problem-Status zum Auflösen vorhanden.")

    after_preview = deepcopy(entry)
    after_preview.update(updates)
    after = _snapshot_channels(after_preview)

    if before == after and action != ACTION_RESOLVE_PROBLEM:
        raise ValueError("Status ist bereits auf dem Zielzustand — keine Änderung nötig.")

    now = datetime.now().isoformat()
    log_entry = {
        "at": now,
        "action": action,
        "from": before,
        "to": after,
        "reason": (reason or "").strip(),
        "triggered_by": "manual",
    }

    status_change_log = list(entry.get("status_change_log") or [])
    status_change_log.insert(0, log_entry)

    updates["manual_status_override"] = True
    updates["manual_status_at"] = now
    updates["manual_status_action"] = action
    if reason.strip():
        updates["manual_status_note"] = reason.strip()

    if _has_phone(entry) and "sms_status" in updates:
        updates["sms_status_locked"] = True

    updates["status_change_log"] = status_change_log
    return updates


def format_manual_status_summary(entry: dict[str, Any]) -> str:
    """Kurztext für die Detailansicht."""
    if not entry.get("manual_status_override"):
        return "—"
    action = (entry.get("manual_status_action") or "").strip() or "Manuell"
    at_raw = (entry.get("manual_status_at") or "").strip()
    at_display = at_raw.replace("T", " ")[:16] if at_raw else "—"
    note = (entry.get("manual_status_note") or "").strip()
    if note:
        return f"{action} ({at_display}) — {note}"
    return f"{action} ({at_display})"

"""E-Mail/SMS erneut aus der Upload-Historie versenden."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from core.config import ConfigManager
from models.kunde import Kunde, normalize_phone
from services.custom_api_client import CustomApiClient
from utils.validation import is_valid_email, is_valid_share_link

RESENDABLE_UPLOAD_STATUS = "Erfolgreich"


@dataclass
class ChannelResult:
    channel: str
    status: str
    success: bool
    sms_id: Optional[str] = None


@dataclass
class ResendResult:
    email_result: Optional[ChannelResult]
    sms_result: Optional[ChannelResult]
    share_link: str
    history_updates: dict[str, Any]


def is_sandbox_email(config_manager: ConfigManager) -> bool:
    return str(config_manager.get_setting("smtp_sandbox_mode", "false")).lower() == "true"


def is_sandbox_sms(config_manager: ConfigManager) -> bool:
    return str(config_manager.get_setting("seven_sandbox_mode", "false")).lower() == "true"


def get_sandbox_warnings(config_manager: ConfigManager) -> list[str]:
    warnings: list[str] = []
    if is_sandbox_email(config_manager):
        fallback = (config_manager.get_setting("smtp_fallback_recipient") or "").strip()
        if fallback:
            warnings.append(f"E-Mail-Sandbox aktiv — Versand geht an {fallback}.")
        else:
            warnings.append("E-Mail-Sandbox aktiv — kein Fallback-Empfänger konfiguriert.")
    if is_sandbox_sms(config_manager):
        warnings.append("SMS-Sandbox aktiv — keine echte Zustellung.")
    return warnings


def normalize_contact(email: str, phone: str) -> tuple[str, Optional[str]]:
    return (email or "").strip(), normalize_phone(phone)


def validate_contact_for_channels(
    email: str,
    phone: Optional[str],
    send_email: bool,
    send_sms: bool,
) -> None:
    if not send_email and not send_sms:
        raise ValueError("Bitte mindestens einen Kanal auswählen.")
    if send_email:
        if not email:
            raise ValueError("E-Mail-Adresse fehlt.")
        if not is_valid_email(email):
            raise ValueError("E-Mail-Adresse ist ungültig.")
    if send_sms:
        if not phone:
            raise ValueError("Telefonnummer fehlt.")


def _is_delivered_email_status(status: str) -> bool:
    s = (status or "").strip().lower()
    return ("gesendet" in s) or ("zugestellt" in s) or ("erfolgreich" in s)


def _is_delivered_sms_status(status: str) -> bool:
    s = (status or "").strip().lower()
    return ("zugestellt" in s) or ("erfolgreich" in s)


def channels_already_delivered(entry: dict[str, Any], send_email: bool, send_sms: bool) -> list[str]:
    delivered: list[str] = []
    if send_email and _is_delivered_email_status(entry.get("email_status") or ""):
        delivered.append("email")
    if send_sms and _is_delivered_sms_status(entry.get("sms_status") or ""):
        delivered.append("sms")
    return delivered


def can_resend_notifications(entry: dict[str, Any]) -> bool:
    return (entry.get("status") or "").strip() == RESENDABLE_UPLOAD_STATUS


def _remote_path_for_entry(entry: dict[str, Any]) -> str:
    remote = (entry.get("remote_path") or "").strip()
    if remote:
        return remote
    dir_name = (entry.get("dir_name") or "").strip()
    if dir_name:
        return f"/{dir_name}"
    return ""


def _lookup_link_from_cloud(entry: dict[str, Any], cloud_client) -> Optional[str]:
    if cloud_client is None:
        return None
    if cloud_client.get_connection_status() != "Verbunden":
        return None

    if isinstance(cloud_client, CustomApiClient):
        link = cloud_client.lookup_customer_url(
            (entry.get("customer_number") or "").strip(),
            (entry.get("booking_number") or "").strip(),
            (entry.get("type") or "").strip(),
        )
        if not link:
            return None
        try:
            shortened = cloud_client.link_shortener.shorten(link)
            return shortened or link
        except Exception:
            return link

    remote_path = _remote_path_for_entry(entry)
    if not remote_path:
        return None
    return cloud_client.get_shareable_link(remote_path)


def lookup_share_link_from_cloud(entry: dict[str, Any], cloud_client) -> str:
    """Lädt einen Link ausschließlich über die Cloud (ohne gespeicherten/manuellen Link)."""
    link = _lookup_link_from_cloud(entry, cloud_client)
    if link:
        return link.strip()
    raise ValueError("Download-Link konnte nicht aus der Cloud geladen werden.")


def resolve_share_link(
    entry: dict[str, Any],
    cloud_client=None,
    manual_link: Optional[str] = None,
) -> str:
    stored = (entry.get("share_link") or "").strip()
    if stored:
        return stored

    manual = (manual_link or "").strip()
    if manual:
        if not is_valid_share_link(manual):
            raise ValueError("Download-Link muss mit http:// oder https:// beginnen.")
        return manual

    link = _lookup_link_from_cloud(entry, cloud_client)
    if link:
        return link.strip()

    raise ValueError(
        "Kein Download-Link verfügbar. Bitte Link manuell eingeben oder Cloud verbinden."
    )


def build_contact_update_payload(entry: dict[str, Any], email: str, phone: Optional[str]) -> dict[str, Any]:
    return {
        "dir_name": entry.get("dir_name"),
        "email": email,
        "phone": phone or "",
    }


def build_resend_history_updates(
    entry: dict[str, Any],
    email: str,
    phone: Optional[str],
    share_link: str,
    email_result: Optional[ChannelResult],
    sms_result: Optional[ChannelResult],
    channels: list[str],
    sandbox_email: bool,
    sandbox_sms: bool,
) -> dict[str, Any]:
    now = datetime.now().isoformat()
    log_entry = {
        "at": now,
        "channels": list(channels),
        "email": email,
        "phone": phone or "",
        "share_link": share_link,
        "email_status": email_result.status if email_result else None,
        "sms_status": sms_result.status if sms_result else None,
        "sms_id": sms_result.sms_id if sms_result else None,
        "sandbox_email": sandbox_email,
        "sandbox_sms": sandbox_sms,
        "triggered_by": "manual_resend",
    }

    resend_log = list(entry.get("resend_log") or [])
    resend_log.insert(0, log_entry)

    updates: dict[str, Any] = {
        "dir_name": entry.get("dir_name"),
        "email": email,
        "phone": phone or "",
        "share_link": share_link,
        "resend_log": resend_log,
        "last_updated": now,
    }

    if email_result is not None:
        updates["email_status"] = email_result.status
        if email_result.success:
            updates["email_resend_count"] = int(entry.get("email_resend_count") or 0) + 1
            updates["last_email_resent_at"] = now

    if sms_result is not None:
        updates["sms_status"] = sms_result.status
        if sms_result.sms_id:
            updates["sms_id"] = sms_result.sms_id
        if sms_result.success:
            updates["sms_resend_count"] = int(entry.get("sms_resend_count") or 0) + 1
            updates["last_sms_resent_at"] = now

    return updates


def format_resend_result_message(result: ResendResult) -> str:
    lines: list[str] = []
    email_to = (result.history_updates.get("email") or "").strip()
    phone_to = (result.history_updates.get("phone") or "").strip()

    if result.email_result is not None:
        prefix = "✓" if result.email_result.success else "✗"
        target = f" an {email_to}" if email_to else ""
        lines.append(f"{prefix} E-Mail{target}: {result.email_result.status}")
    if result.sms_result is not None:
        prefix = "✓" if result.sms_result.success else "✗"
        target = f" an {phone_to}" if phone_to else ""
        lines.append(f"{prefix} SMS{target}: {result.sms_result.status}")
    return "\n".join(lines) if lines else "Kein Versand durchgeführt."


def resend_had_failures(result: ResendResult) -> bool:
    for channel_result in (result.email_result, result.sms_result):
        if channel_result is not None and not channel_result.success:
            return True
    return False


def format_resend_history_summary(entry: dict[str, Any]) -> str:
    email_count = int(entry.get("email_resend_count") or 0)
    sms_count = int(entry.get("sms_resend_count") or 0)
    parts: list[str] = []
    if email_count:
        parts.append(f"E-Mail {email_count}× erneut")
    if sms_count:
        parts.append(f"SMS {sms_count}× erneut")
    if not parts:
        return "Keine Wiederversände"
    return " | ".join(parts)


def resend_notifications(
    entry: dict[str, Any],
    email: str,
    phone: Optional[str],
    share_link: str,
    send_email: bool,
    send_sms: bool,
    email_client,
    sms_client,
    config_manager: ConfigManager,
    log: Optional[logging.Logger] = None,
) -> ResendResult:
    logger = log or logging.getLogger(__name__)

    if not can_resend_notifications(entry):
        raise ValueError("Nur erfolgreiche Uploads unterstützen einen erneuten Versand.")

    email, phone = normalize_contact(email, phone)
    validate_contact_for_channels(email, phone, send_email, send_sms)

    if not is_valid_share_link(share_link):
        raise ValueError("Ungültiger Download-Link.")

    dir_name = (entry.get("dir_name") or "").strip()
    first_name = (entry.get("first_name") or "").strip() or "Gast"

    kunde = Kunde(
        first_name=first_name,
        last_name=(entry.get("last_name") or "").strip() or None,
        email=email,
        phone=phone,
        customer_number=(entry.get("customer_number") or "").strip() or None,
        booking_number=(entry.get("booking_number") or "").strip() or None,
        type=(entry.get("type") or "").strip() or None,
    )

    email_result: Optional[ChannelResult] = None
    sms_result: Optional[ChannelResult] = None
    channels: list[str] = []

    if send_email:
        channels.append("email")
        try:
            success = email_client.send_upload_success_email(dir_name, share_link, email, first_name)
            if success:
                email_result = ChannelResult("email", "Gesendet", True)
            else:
                email_result = ChannelResult("email", "Fehler: Versand fehlgeschlagen", False)
        except Exception as exc:
            logger.error("E-Mail-Resend fehlgeschlagen: %s", exc)
            email_result = ChannelResult("email", f"Fehler: {exc}", False)

    if send_sms:
        channels.append("sms")
        try:
            sms_success, sms_id = asyncio.run(sms_client.send_upload_success_sms(share_link, kunde))
            if sms_success:
                sms_result = ChannelResult("sms", "Gesendet", True, sms_id=sms_id)
            else:
                err_text = getattr(sms_client, "last_error", "") or "Fehler beim Senden"
                sms_result = ChannelResult("sms", f"Fehler: {err_text}", False, sms_id=sms_id)
        except Exception as exc:
            logger.error("SMS-Resend fehlgeschlagen: %s", exc)
            sms_result = ChannelResult("sms", f"Fehler: {exc}", False)

    history_updates = build_resend_history_updates(
        entry,
        email,
        phone,
        share_link,
        email_result,
        sms_result,
        channels,
        is_sandbox_email(config_manager),
        is_sandbox_sms(config_manager),
    )

    return ResendResult(
        email_result=email_result,
        sms_result=sms_result,
        share_link=share_link,
        history_updates=history_updates,
    )


def migrate_share_links_for_history(
    history: list[dict[str, Any]],
    cloud_client,
    log: Optional[logging.Logger] = None,
) -> int:
    """Versucht fehlende share_links für erfolgreiche Einträge nachzuladen."""
    logger = log or logging.getLogger(__name__)
    if cloud_client is None or cloud_client.get_connection_status() != "Verbunden":
        return 0

    updated = 0
    for entry in history:
        if (entry.get("status") or "").strip() != RESENDABLE_UPLOAD_STATUS:
            continue
        if (entry.get("share_link") or "").strip():
            continue
        try:
            link = _lookup_link_from_cloud(entry, cloud_client)
        except Exception as exc:
            logger.debug("Share-Link-Migration für %s übersprungen: %s", entry.get("dir_name"), exc)
            continue
        if not link:
            continue
        entry["share_link"] = link.strip()
        remote_path = _remote_path_for_entry(entry)
        if remote_path:
            entry["remote_path"] = remote_path
        entry["last_updated"] = datetime.now().isoformat()
        updated += 1
    return updated

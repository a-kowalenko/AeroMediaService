"""Dropbox Manifest v1.1 (paths_only) builder for Cloud /api/orders/create."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from models.kunde import Kunde

STANDARD_CATEGORIES = frozenset({
    "Outside_Foto",
    "Handcam_Foto",
    "Preview_Foto",
    "Outside_Video",
    "Handcam_Video",
    "Preview_Video",
})

log = logging.getLogger(__name__)


def normalize_customer_type(raw_type: str | None) -> str:
    value = str(raw_type or "").strip().lower()
    if value in ("handycam", "handcam"):
        return "handcam"
    if value == "outside":
        return "outside"
    return value or "outside"


def _client_hints(category_names: set[str]) -> dict:
    return {
        "has_previews": any("Preview_" in name for name in category_names),
        "has_videos": any("_Video" in name for name in category_names),
        "has_photos": any("_Foto" in name for name in category_names),
    }


def build_manifest_v11(
    *,
    base_dir: str,
    kunde: Kunde | None,
    uploaded_files: list[dict],
    root_share_link: str | None,
    uploader_version: str,
) -> dict:
    """
    Build Manifest v1.1 (paths_only) from local upload results.

    Each entry in uploaded_files must have: name, rel_path, size, mime;
    dropbox_id is optional.
    """
    categories_map: dict[str, list[dict]] = {}

    for file_row in uploaded_files:
        rel_path = str(file_row.get("rel_path") or file_row.get("file_name") or "").replace("\\", "/")
        if not rel_path:
            log.warning("Datei ohne rel_path übersprungen: %r", file_row)
            continue

        parts = rel_path.split("/")
        if len(parts) < 2:
            log.warning("Datei ohne Kategorie-Unterordner übersprungen: %s", rel_path)
            continue

        category_name = parts[0]
        if category_name not in STANDARD_CATEGORIES:
            log.warning(
                "Unbekannte Kategorie '%s' für %s — wird übersprungen.",
                category_name,
                rel_path,
            )
            continue

        entry = {
            "name": str(file_row.get("name") or os.path.basename(rel_path)),
            "rel_path": rel_path,
            "size": int(file_row.get("size") or file_row.get("file_size") or 0),
            "mime": str(file_row.get("mime") or file_row.get("type") or "application/octet-stream"),
        }
        dropbox_id = file_row.get("dropbox_id")
        if dropbox_id:
            entry["dropbox_id"] = str(dropbox_id)

        categories_map.setdefault(category_name, []).append(entry)

    categories = []
    files_count = 0
    bytes_total = 0
    for category_name in sorted(categories_map.keys()):
        files = sorted(categories_map[category_name], key=lambda f: f["rel_path"])
        files_count += len(files)
        bytes_total += sum(f["size"] for f in files)
        categories.append({
            "name": category_name,
            "folder_path": f"/{base_dir}/{category_name}",
            "files": files,
        })

    customer = {
        "customer_number": str(getattr(kunde, "customer_number", None) or ""),
        "booking_number": str(getattr(kunde, "booking_number", None) or ""),
        "type": normalize_customer_type(getattr(kunde, "type", None)),
        "first_name": str(getattr(kunde, "first_name", None) or ""),
        "last_name": str(getattr(kunde, "last_name", None) or ""),
        "email": str(getattr(kunde, "email", None) or ""),
        "phone": str(getattr(kunde, "phone", None) or ""),
        "handcam_foto": bool(getattr(kunde, "handcam_foto", False)),
        "handcam_video": bool(getattr(kunde, "handcam_video", False)),
        "outside_foto": bool(getattr(kunde, "outside_foto", False)),
        "outside_video": bool(getattr(kunde, "outside_video", False)),
        "ist_bezahlt_handcam_foto": bool(getattr(kunde, "ist_bezahlt_handcam_foto", False)),
        "ist_bezahlt_handcam_video": bool(getattr(kunde, "ist_bezahlt_handcam_video", False)),
        "ist_bezahlt_outside_foto": bool(getattr(kunde, "ist_bezahlt_outside_foto", False)),
        "ist_bezahlt_outside_video": bool(getattr(kunde, "ist_bezahlt_outside_video", False)),
    }

    root_folder: dict = {"path": f"/{base_dir}"}
    if root_share_link:
        root_folder["share_link"] = root_share_link

    return {
        "meta": {
            "version": "1.1",
            "link_mode": "paths_only",
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "uploader_version": uploader_version,
        },
        "customer": customer,
        "base_dir": base_dir,
        "root_folder": root_folder,
        "categories": categories,
        "totals": {
            "files_count": files_count,
            "bytes_total": bytes_total,
        },
        "client_hints": _client_hints(set(categories_map.keys())),
    }

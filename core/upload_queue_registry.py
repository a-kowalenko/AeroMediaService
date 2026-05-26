"""Verhindert doppelte Upload-Queue-Einträge und hält eine geordnete Warteschlangen-Übersicht."""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Literal, Optional

from core.signals import signals

log = logging.getLogger(__name__)

QueueState = Literal["waiting", "active"]


@dataclass
class QueueEntry:
    dir_path: str
    dir_name: str
    customer_label: str
    enqueued_at: float
    state: QueueState


def format_customer_label(kunde: Any) -> str:
    """Anzeigename aus Kunde-Objekt oder None."""
    if not kunde:
        return "—"
    first = (getattr(kunde, "first_name", None) or "").strip()
    last = (getattr(kunde, "last_name", None) or "").strip()
    name = f"{first} {last}".strip()
    if name:
        return name
    email = (getattr(kunde, "email", None) or "").strip()
    if email:
        return email
    return "—"


class UploadQueueRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: set[str] = set()
        self._entries: list[QueueEntry] = []

    def _key(self, dir_path: str) -> str:
        return os.path.normcase(os.path.abspath(dir_path))

    def _find_entry_index(self, dir_path: str) -> int | None:
        key = self._key(dir_path)
        for i, entry in enumerate(self._entries):
            if self._key(entry.dir_path) == key:
                return i
        return None

    def _emit_changed(self) -> None:
        signals.upload_queue_changed.emit(self.snapshot_dicts())

    def snapshot(self) -> list[QueueEntry]:
        with self._lock:
            return list(self._entries)

    def snapshot_dicts(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        with self._lock:
            result = []
            for position, entry in enumerate(self._entries, start=1):
                wait_seconds = max(0.0, now - entry.enqueued_at)
                result.append({
                    "position": position,
                    "dir_name": entry.dir_name,
                    "customer_label": entry.customer_label,
                    "state": entry.state,
                    "wait_seconds": wait_seconds,
                })
            return result

    def register(self, dir_path: str) -> bool:
        key = self._key(dir_path)
        with self._lock:
            if key in self._pending:
                return False
            self._pending.add(key)
            return True

    def unregister(self, dir_path: Optional[str]) -> None:
        if not dir_path:
            return
        key = self._key(dir_path)
        changed = False
        with self._lock:
            if key in self._pending:
                self._pending.discard(key)
                changed = True
            idx = self._find_entry_index(dir_path)
            if idx is not None:
                self._entries.pop(idx)
                changed = True
        if changed:
            self._emit_changed()

    def is_registered(self, dir_path: str) -> bool:
        return self._key(dir_path) in self._pending

    def _append_entry(self, item: dict[str, Any], state: QueueState = "waiting") -> None:
        dir_path = item["dir_path"]
        self._entries.append(
            QueueEntry(
                dir_path=dir_path,
                dir_name=os.path.basename(dir_path),
                customer_label=format_customer_label(item.get("kunde")),
                enqueued_at=time.monotonic(),
                state=state,
            )
        )

    def mark_active(self, dir_path: str) -> None:
        with self._lock:
            idx = self._find_entry_index(dir_path)
            if idx is None:
                return
            entry = self._entries[idx]
            self._entries[idx] = QueueEntry(
                dir_path=entry.dir_path,
                dir_name=entry.dir_name,
                customer_label=entry.customer_label,
                enqueued_at=entry.enqueued_at,
                state="active",
            )
        self._emit_changed()

    def enqueue(
        self,
        upload_queue: queue.Queue,
        item: dict[str, Any],
        logger: Optional[logging.Logger] = None,
        *,
        already_registered: bool = False,
    ) -> bool:
        dir_path = item.get("dir_path")
        if not dir_path:
            return False
        lg = logger or log
        with self._lock:
            key = self._key(dir_path)
            if not already_registered:
                if key in self._pending:
                    lg.info(
                        "Upload bereits vorgemerkt, überspringe Queue: %s",
                        os.path.basename(dir_path),
                    )
                    return False
                self._pending.add(key)
            elif key not in self._pending:
                return False
            upload_queue.put(item)
            self._append_entry(item)
        self._emit_changed()
        return True

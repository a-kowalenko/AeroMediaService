"""Verhindert doppelte Upload-Queue-Einträge für denselben Ordner."""
from __future__ import annotations

import logging
import os
import queue
import threading
from typing import Any, Optional

log = logging.getLogger(__name__)


class UploadQueueRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: set[str] = set()

    def _key(self, dir_path: str) -> str:
        return os.path.normcase(os.path.abspath(dir_path))

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
        with self._lock:
            self._pending.discard(key)

    def is_registered(self, dir_path: str) -> bool:
        return self._key(dir_path) in self._pending

    def enqueue(
        self,
        upload_queue: queue.Queue,
        item: dict[str, Any],
        logger: Optional[logging.Logger] = None,
    ) -> bool:
        dir_path = item.get("dir_path")
        if not dir_path:
            return False
        if not self.register(dir_path):
            lg = logger or log
            lg.info(
                "Upload bereits vorgemerkt, überspringe Queue: %s",
                os.path.basename(dir_path),
            )
            return False
        upload_queue.put(item)
        return True

"""Kooperative Steuerung laufender Uploads (Pause / Abbrechen) im Uploader-Thread."""

import threading
import time


class UploadCancelled(Exception):
    """Der Benutzer hat den aktuellen Upload-Job abgebrochen."""

    pass


class UploadControl:
    """Thread-sicheres Steuerobjekt; wird vom GUI-Thread gesetzt, vom Upload-Worker gelesen."""

    def __init__(self):
        self._pause = threading.Event()
        self._cancel = threading.Event()

    def reset_for_new_job(self):
        self._pause.clear()
        self._cancel.clear()

    def request_pause(self):
        self._pause.set()

    def request_resume(self):
        self._pause.clear()

    def request_cancel(self):
        self._cancel.set()
        self._pause.clear()

    def wait_if_paused(self):
        while self._pause.is_set():
            if self._cancel.is_set():
                raise UploadCancelled()
            time.sleep(0.15)
        if self._cancel.is_set():
            raise UploadCancelled()

    def check_cancelled(self):
        if self._cancel.is_set():
            raise UploadCancelled()

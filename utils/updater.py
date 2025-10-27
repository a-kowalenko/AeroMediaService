import sys
import os
import requests
import threading
import queue
import tempfile
import subprocess
import logging
from packaging import version

from PySide6.QtCore import QObject, Signal, Slot, QThread, QTimer, Qt
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QTextEdit,
    QCheckBox, QPushButton, QProgressBar, QMessageBox
)

if sys.platform == "win32":
    import ctypes

GITHUB_API_URL = "https://api.github.com/repos/a-kowalenko/AeroMediaService/releases/latest"
DOWNLOAD_NAME = "setup_update.exe"

log = logging.getLogger(__name__)


# --- Benutzerdefinierte Exceptions für saubere Fehlerbehandlung ---

class UACError(Exception):
    """Wird ausgelöst, wenn der UAC-Dialog (Admin-Anfrage) fehlschlägt oder abgebrochen wird."""
    pass


class UpdateCancelledError(Exception):
    """Wird ausgelöst, wenn der Benutzer den Download aktiv abbricht."""
    pass


# --- Ende Exceptions ---


class UpdateCheckWorker(QObject):
    """
    Prüft in einem separaten Thread auf Updates, um die GUI nicht zu blockieren.
    """
    updateAvailable = Signal(str, str, str)
    noUpdateAvailable = Signal(str)
    error = Signal(str)
    finished = Signal()

    def __init__(self, app_version, config, show_no_update_message=False):
        super().__init__()
        self.app_version = app_version
        self.config = config
        self.show_no_update_message = show_no_update_message

    @Slot()
    def run(self):
        """Führt die Update-Prüfung durch."""
        try:
            log.info("Update-Prüfung wird gestartet...")
            response = requests.get(GITHUB_API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            latest_version_str = data.get("tag_name", "0.0.0").lstrip('v')
            latest_version = version.parse(latest_version_str)
            current_version = version.parse(self.app_version)

            ignored_version = self.config.get_setting("updater_ignore_version", "")

            log.debug(
                f"Aktuelle Version: {current_version}, Neueste Version: {latest_version}, Ignoriert: {ignored_version}")

            is_new_version = latest_version > current_version
            is_ignored = (latest_version_str == ignored_version)
            is_manual_check = self.show_no_update_message

            # Zeige Update, wenn es neu ist UND (es nicht ignoriert ist ODER es ein manueller Check ist)
            if is_new_version and (not is_ignored or is_manual_check):
                release_notes = data.get("body", "Keine Details verfügbar.")
                assets = data.get("assets", [])
                installer_url = ""
                for asset in assets:
                    if asset.get("name", "").lower().endswith(".exe"):
                        installer_url = asset.get("browser_download_url")
                        break

                if not installer_url:
                    log.warning("Update gefunden, aber keine .exe-Datei im Release.")
                    # Signal immer senden. Der Empfänger (app.py)
                    # entscheidet, ob eine Message-Box angezeigt wird.
                    self.error.emit(f"Update {latest_version_str} verfügbar, aber kein Installer (.exe) gefunden.")
                else:
                    self.updateAvailable.emit(latest_version_str, release_notes, installer_url)

            else:
                # Kein Update verfügbar ODER es ist ein Auto-Check einer ignorierten Version
                message_text = ""
                if is_new_version and is_ignored and not is_manual_check:
                    message_text = f"Version {latest_version_str} ist verfügbar, wurde aber ignoriert."
                else:
                    message_text = f"Sie haben bereits die neueste Version ({self.app_version})."

                self.noUpdateAvailable.emit(message_text)

        except requests.RequestException as e:
            error_msg = f"Netzwerkfehler bei Update-Prüfung: {e}"
            log.error(error_msg)
            self.error.emit("Update-Prüfung fehlgeschlagen: Netzwerkfehler.")
        except Exception as e:
            error_msg = f"Fehler bei Update-Prüfung: {e}"
            log.error(error_msg)
            self.error.emit(f"Update-Prüfung fehlgeschlagen: {e}")

        finally:
            self.finished.emit()


class AskUpdateDialog(QDialog):
    """
    Fragt den Benutzer, ob ein Update durchgeführt werden soll.
    """

    def __init__(self, parent, app_version, latest_version, release_notes, config):
        super().__init__(parent)
        self.setWindowTitle("Update verfügbar!")
        self.setMinimumWidth(500)
        self.config = config
        self.version_to_ignore = latest_version

        layout = QVBoxLayout(self)

        info_text = (
            f"Eine neue Version (<b>{latest_version}</b>) ist verfügbar.<br>"
            f"Ihre installierte Version: <b>{app_version}</b><br><br>"
            f"<b>Änderungen:</b>"
        )
        layout.addWidget(QLabel(info_text))

        self.release_notes_display = QTextEdit()
        self.release_notes_display.setReadOnly(True)

        self.release_notes_display.setMarkdown(release_notes)

        self.release_notes_display.setMaximumHeight(200)
        layout.addWidget(self.release_notes_display)

        self.ignore_checkbox = QCheckBox("Diese Version nicht mehr anzeigen")
        layout.addWidget(self.ignore_checkbox)

        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.later_button = QPushButton("Später")
        self.later_button.clicked.connect(self.reject)
        button_layout.addWidget(self.later_button)

        self.update_button = QPushButton("Jetzt aktualisieren")
        self.update_button.setDefault(True)
        self.update_button.clicked.connect(self.accept)
        button_layout.addWidget(self.update_button)

        layout.addLayout(button_layout)

    def check_and_save_settings(self):
        """Speichert die 'Ignorieren'-Einstellung, falls ausgewählt."""
        if self.ignore_checkbox.isChecked():
            self.config.save_setting("updater_ignore_version", self.version_to_ignore)
        else:
            # Lösche die Ignorieren-Einstellung, wenn die Checkbox nicht gesetzt ist
            # und die aktuell ignorierte Version diese ist.
            if self.config.get_setting("updater_ignore_version") == self.version_to_ignore:
                self.config.save_setting("updater_ignore_version", "")

    def accept(self):
        """Wird bei "Jetzt aktualisieren" aufgerufen."""
        self.check_and_save_settings()
        super().accept()

    def reject(self):
        """Wird bei "Später" oder Schließen aufgerufen."""
        self.check_and_save_settings()
        super().reject()


class UpdateProgressDialog(QDialog):
    """
    Zeigt den Download-Fortschritt an.
    """

    def __init__(self, parent, installer_url):
        super().__init__(parent)
        self.setWindowTitle("Update wird heruntergeladen...")
        self.setMinimumWidth(400)
        self.setModal(True)

        self.installer_url = installer_url
        self.progress_queue = queue.Queue()
        self.cancel_event = threading.Event()
        self.download_thread = None

        layout = QVBoxLayout(self)
        self.status_label = QLabel("Lade Update herunter...")
        layout.addWidget(self.status_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Unbestimmter Modus
        layout.addWidget(self.progress_bar)

        self.cancel_button = QPushButton("Abbrechen")
        self.cancel_button.clicked.connect(self.on_cancel)
        layout.addWidget(self.cancel_button, alignment=Qt.AlignmentFlag.AlignRight)

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.check_queue)
        self.timer.start(100)  # Alle 100ms

        self.start_download()

    def start_download(self):
        """Startet den Download-Thread."""
        self.download_thread = threading.Thread(
            target=download_and_install_thread,
            args=(self.installer_url, self.progress_queue, self.cancel_event),
            daemon=True
        )
        self.download_thread.start()

    def on_cancel(self):
        """Wird aufgerufen, wenn der Benutzer auf "Abbrechen" klickt."""
        reply = QMessageBox.question(
            self,
            "Abbrechen?",
            "Möchten Sie das Update wirklich abbrechen?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            self.cancel_event.set()
            self.status_label.setText("Breche ab...")
            self.cancel_button.setEnabled(False)

    def check_queue(self):
        """Verarbeitet Nachrichten vom Download-Thread."""
        msg = None
        try:
            while True:
                msg = self.progress_queue.get_nowait()
        except queue.Empty:
            pass

        if msg:
            if isinstance(msg, tuple):
                # (downloaded, total)
                downloaded, total = msg
                if total > 0:
                    if self.progress_bar.maximum() == 0:
                        self.progress_bar.setRange(0, 100)

                    progress_percent = int((downloaded / total) * 100)
                    self.progress_bar.setValue(progress_percent)

                    downloaded_mb = downloaded / (1024 * 1024)
                    total_mb = total / (1024 * 1024)
                    self.status_label.setText(
                        f"Lade herunter... {progress_percent}% ({downloaded_mb:.1f} / {total_mb:.1f} MB)")
                else:
                    downloaded_mb = downloaded / (1024 * 1024)
                    self.status_label.setText(f"Lade herunter... ({downloaded_mb:.1f} MB)")

            elif msg == "DOWNLOAD_COMPLETE":
                self.status_label.setText("Download abgeschlossen. Starte Installation...")
                self.cancel_button.setVisible(False)
                self.progress_bar.setRange(0, 0)

            elif msg == "EXIT_APP":
                self.status_label.setText("Update wird ausgeführt. App wird beendet.")
                self.timer.stop()
                self.parent().close()  # Löst closeEvent im Hauptfenster aus
                self.accept()

            elif msg == "CANCELLED":
                log.info("Update vom Benutzer abgebrochen.")
                self.timer.stop()
                self.reject()

            elif isinstance(msg, str) and msg.startswith("Fehler:"):
                log.error(f"Fehler im Update-Prozess: {msg}")
                self.timer.stop()
                QMessageBox.critical(self, "Update-Fehler", msg)
                self.reject()

    def closeEvent(self, event):
        """Verhindert das Schließen des Dialogs, während der Download läuft."""
        if self.download_thread and self.download_thread.is_alive():
            self.on_cancel()
            event.ignore()
        else:
            event.accept()


def initialize_updater(main_window, app_version, config, show_no_update_message=False):
    """
    Startet die Update-Prüfung.
    """
    if main_window.update_thread and main_window.update_thread.isRunning():
        log.warning("Update-Prüfung läuft bereits.")
        return

    main_window.update_thread = QThread()
    main_window.update_worker = UpdateCheckWorker(app_version, config, show_no_update_message)
    main_window.update_worker.moveToThread(main_window.update_thread)

    # Signale verbinden
    main_window.update_worker.updateAvailable.connect(main_window.on_update_available)
    main_window.update_worker.noUpdateAvailable.connect(main_window.on_no_update)
    main_window.update_worker.error.connect(main_window.on_update_error)

    # Thread-Management und Aufräumen
    main_window.update_thread.started.connect(main_window.update_worker.run)
    main_window.update_worker.finished.connect(main_window.update_thread.quit)
    main_window.update_thread.finished.connect(main_window.update_worker.deleteLater)
    main_window.update_thread.finished.connect(main_window.update_thread.deleteLater)

    # Referenzen im Hauptfenster löschen, um "RuntimeError" zu verhindern
    main_window.update_thread.finished.connect(lambda: setattr(main_window, 'update_thread', None))
    main_window.update_thread.finished.connect(lambda: setattr(main_window, 'update_worker', None))

    main_window.update_thread.start()


# --- Download-Thread-Funktion ---

def download_and_install_thread(installer_url, progress_queue, cancel_event):
    """Lädt Installer herunter und startet ihn mit Admin-Rechten."""
    temp_dir = tempfile.gettempdir()
    installer_path = os.path.join(temp_dir, DOWNLOAD_NAME)

    try:
        log.info(f"Starte Download von: {installer_url}")
        headers = {'User-Agent': 'AeroMediaService-Updater (requests)'}

        with requests.get(installer_url, stream=True, allow_redirects=True,
                          headers=headers, timeout=(10, 60)) as r:
            r.raise_for_status()

            total_size = int(r.headers.get('content-length', 0))
            downloaded_size = 0

            with open(installer_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=512 * 1024):
                    if cancel_event.is_set():
                        raise UpdateCancelledError("Download vom Benutzer abgebrochen.")

                    f.write(chunk)
                    downloaded_size += len(chunk)
                    progress_queue.put((downloaded_size, total_size))

        if cancel_event.is_set():
            raise UpdateCancelledError("Download vom Benutzer nach Download abgebrochen.")

        progress_queue.put("DOWNLOAD_COMPLETE")
        log.info(f"Download abgeschlossen. Datei: {installer_path}")

        try:
            if sys.platform == "win32":
                log.info("Starte Installer mit Admin-Rechten (runas)...")
                # /S für Silent-Installation (gemäß NSIS-Skript)
                ret = ctypes.windll.shell32.ShellExecuteW(
                    None, "runas", installer_path, "/S", None, 1
                )
                if ret <= 32:
                    log.error(f"ShellExecuteW fehlgeschlagen, Code: {ret}")
                    raise UACError(f"ShellExecuteW fehlgeschlagen mit Code: {ret}")
            else:
                log.info("Starte Installer (Non-Windows)...")
                subprocess.Popen([installer_path, "/S"], start_new_session=True)

            progress_queue.put("EXIT_APP")

        except UACError as e:
            log.warning(f"Update fehlgeschlagen: Administratorrechte wurden verweigert. {e}")
            progress_queue.put("Fehler: Administratorrechte wurden verweigert.")
        except Exception as e:
            log.error(f"Fehler beim Starten des Installers: {e}")
            progress_queue.put(f"Fehler: {e}")

    except UpdateCancelledError:
        progress_queue.put("CANCELLED")
    except requests.Timeout as e:
        log.error(f"Update-Download Timeout: {e}")
        progress_queue.put("Fehler: Download-Timeout")
    except Exception as e:
        log.error(f"Fehler beim Download/Install: {e}")
        progress_queue.put(f"Fehler: {e}")

        try:
            if os.path.exists(installer_path):
                log.info(f"Lösche unvollständige Datei: {installer_path}")
                os.remove(installer_path)
        except Exception as cleanup_e:
            log.error(f"Fehler beim Löschen der unvollständigen Datei: {cleanup_e}")


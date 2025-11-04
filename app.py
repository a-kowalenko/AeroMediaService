import sys
import logging
import queue

from PySide6.QtGui import QIcon, QPainter, QColor
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QProgressBar, QLabel, QStatusBar,
    QMessageBox
)

from PySide6.QtCore import QCoreApplication, QProcess, Slot, Qt, Signal

# Importiere alle Komponenten des Projekts
from core import APP_VERSION
from core.config import ConfigManager
from core.logger import setup_logging
from core.signals import signals
from core.monitor import MonitorThread
from core.uploader import UploaderThread
from services.dropbox_client import DropboxClient
from services.custom_api_client import CustomApiClient
from services.email_client import EmailClient
from services.sms_client import SmsClient
from settings import SettingsDialog
from utils.constants import ICON_PATH
from utils.updater import initialize_updater, AskUpdateDialog, UpdateProgressDialog


class StatusLight(QWidget):
    """
    Ein einfacher runder Indikator (Ampellicht), der die Farbe ändern kann.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(16, 16)  # Feste Größe für das Licht
        self._color = QColor(Qt.GlobalColor.red)  # Standardmäßig rot (keine Verbindung)

    def paintEvent(self, event):
        """Zeichnet den farbigen Kreis."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)  # Kantenglättung
        painter.setBrush(self._color)
        painter.setPen(Qt.GlobalColor.transparent)  # Kein Rand

        # Berechne den Radius und das Zentrum
        radius = min(self.width(), self.height()) / 2.0
        center = self.rect().center()

        painter.drawEllipse(center, radius, radius)

    def setColor(self, color):
        """Setzt die Farbe des Lichts und zeichnet es neu."""
        if isinstance(color, str):
            self._color = QColor(color)
        elif isinstance(color, QColor):
            self._color = color
        else:
            self._color = QColor(Qt.GlobalColor.gray)  # Fallback

        self.update()  # Löst ein paintEvent aus


class MainWindow(QMainWindow):
    """
    Das Hauptfenster der Anwendung.
    Es enthält die Log-Anzeige, Fortschrittsbalken und Steuer-Buttons.
    Es verwaltet die Lebenszyklen der Worker-Threads.
    """

    update_check_finished_signal = Signal(str)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Aero Media Service")
        self.setGeometry(100, 100, 800, 600)

        # App-Icon laden
        icon_path = ICON_PATH
        self.app_icon = QIcon(icon_path)
        self.setWindowIcon(self.app_icon)

        # --- Initialisierung der Kernkomponenten ---
        self.config = ConfigManager()

        # Logging MUSS nach ConfigManager initialisiert werden
        setup_logging(self.config)
        self.log = logging.getLogger(__name__)
        self.log.info("Anwendung wird gestartet...")

        self.upload_queue = queue.Queue()

        # --- Dienste (Clients) ---
        # (Clients benötigen ConfigManager)
        self.db_client = DropboxClient(self.config)
        self.custom_api_client = CustomApiClient(self.config)
        self.email_client = EmailClient(self.config)
        self.sms_client = SmsClient(self.config)

        # Aktueller aktiver Cloud-Client (wird von get_active_cloud_client() bestimmt)
        self.active_cloud_client = self.get_active_cloud_client()

        # --- Worker-Threads ---
        # (Threads benötigen Config, Queue und Clients)
        self.monitor_thread = None  # Wird bei Bedarf gestartet
        self.uploader_thread = UploaderThread(self.config,
                                              self.upload_queue,
                                              self.active_cloud_client,
                                              self.email_client,
                                              self.sms_client)

        # --- Update-Worker ---
        self.update_thread = None
        self.update_worker = None

        # Member-Variable für den letzten Update-Status
        self.latest_version_info = "Noch nicht geprüft."

        # --- GUI-Komponenten (Platzhalter) ---
        self.status_light = None
        self.monitor_button = None

        # --- GUI Erstellen ---
        self.init_ui()

        # --- Signale verbinden ---
        self.connect_signals()

        # --- Threads starten ---
        self.uploader_thread.start()

        # --- Automatische Verbindung und Start ---
        self.auto_connect_and_start()

    def init_ui(self):
        """Erstellt die Benutzeroberfläche."""

        # Zentrales Widget
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        self.setCentralWidget(main_widget)

        # 1. Button-Leiste (oben)
        button_layout = QHBoxLayout()

        self.monitor_button = QPushButton("Monitoring starten")
        self.monitor_button.setCheckable(True)  # Macht ihn zu einem Toggle-Button
        self.monitor_button.clicked.connect(self.toggle_monitoring)

        self.status_light = StatusLight(self)
        self.status_light.setToolTip(
            "Status:\nRot: Nicht verbunden\nGelb: Verbunden, Monitoring aus\nGrün: Verbunden, Monitoring aktiv")

        self.settings_button = QPushButton("Einstellungen")
        self.settings_button.clicked.connect(self.open_settings)

        self.restart_button = QPushButton("Neustart")
        self.restart_button.clicked.connect(self.restart_app)

        button_layout.addWidget(self.monitor_button)
        button_layout.addWidget(self.status_light)
        button_layout.addStretch()
        button_layout.addWidget(self.settings_button)
        button_layout.addWidget(self.restart_button)
        main_layout.addLayout(button_layout)

        # 2. Log-Anzeige
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        main_layout.addWidget(self.log_display)

        # 3. Fortschrittsanzeigen
        progress_layout = QVBoxLayout()

        # Status-Text (z.B. "Lade hoch...")
        self.status_label = QLabel("Bereit.")
        progress_layout.addWidget(self.status_label)

        # Gesamtfortschritt (Verzeichnis)
        total_progress_layout = QHBoxLayout()
        total_progress_layout.addWidget(QLabel("Gesamtfortschritt (Verzeichnis):"))
        self.total_progress_label = QLabel("0% (0.0 MB / 0.0 MB)")
        total_progress_layout.addStretch()
        total_progress_layout.addWidget(self.total_progress_label)
        progress_layout.addLayout(total_progress_layout)

        self.total_progress_bar = QProgressBar(textVisible=False)
        progress_layout.addWidget(self.total_progress_bar)

        # Dateifortschritt (Aktuelle Datei)
        file_progress_layout = QHBoxLayout()
        file_progress_layout.addWidget(QLabel("Dateifortschritt (Aktuell):"))
        self.file_progress_label = QLabel("0% (0.0 MB / 0.0 MB)")
        file_progress_layout.addStretch()
        file_progress_layout.addWidget(self.file_progress_label)
        progress_layout.addLayout(file_progress_layout)

        self.file_progress_bar = QProgressBar(textVisible=False)
        progress_layout.addWidget(self.file_progress_bar)

        main_layout.addLayout(progress_layout)

        # 4. Statusleiste (unten)
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        version = APP_VERSION
        if not version:
            version = "0.0.0"

        self.app_info_label = QLabel(f"Aero Media Service v{version} - by Andreas Kowalenko")
        self.status_bar.addWidget(self.app_info_label)

        self.connection_status_label = QLabel("Verbindung: Nicht verbunden")
        self.status_bar.addPermanentWidget(self.connection_status_label)

    def connect_signals(self):
        """Verbindet die globalen Signale mit den GUI-Slots."""
        signals.log_message.connect(self.add_log_message)
        signals.upload_progress_file.connect(self.update_file_progress)
        signals.upload_progress_total.connect(self.update_total_progress)
        signals.upload_status_update.connect(self.status_label.setText)
        signals.monitoring_status_changed.connect(self.update_monitoring_status)

        signals.connection_status_changed.connect(self.connection_status_label.setText)
        signals.connection_status_changed.connect(self.update_status_light)

        signals.stop_monitoring.connect(self.stop_monitoring)

        # Einstellungsänderungen abfangen
        self.config.settings_changed.connect(self.on_settings_changed)

    def get_active_cloud_client(self):
        """
        Gibt den aktuell ausgewählten Cloud-Client zurück.
        """
        selected_cloud = self.config.get_setting("selected_cloud_service", "dropbox")
        if selected_cloud == "custom_api":
            return self.custom_api_client
        else:
            return self.db_client

    def auto_connect_and_start(self):
        """
        Versucht beim Start, automatisch eine Verbindung herzustellen,
        das Monitoring zu starten und auf Updates zu prüfen.
        """
        self.log.info("Prüfe auf Auto-Verbindung...")

        # Aktuellen Cloud-Client ermitteln
        self.active_cloud_client = self.get_active_cloud_client()
        selected_cloud = self.config.get_setting("selected_cloud_service", "dropbox")

        # Auto-Verbindung je nach Service
        if selected_cloud == "dropbox":
            if self.config.get_secret("db_refresh_token"):
                self.status_label.setText("Stelle automatische Verbindung her (Dropbox)...")
                if self.db_client.connect():
                    self.log.info("Automatische Dropbox-Verbindung erfolgreich.")
                    self.start_monitoring()  # Startet automatisch das Monitoring
                else:
                    self.log.warning("Automatische Dropbox-Verbindung fehlgeschlagen.")
                    self.status_label.setText("Automatische Verbindung fehlgeschlagen.")
            else:
                self.log.info("Keine gespeicherten Dropbox-Tokens, kein Auto-Start.")
        elif selected_cloud == "custom_api":
            if self.config.get_secret("custom_api_url") and self.config.get_secret("custom_api_bearer_token"):
                self.status_label.setText("Stelle automatische Verbindung her (Custom API)...")
                if self.custom_api_client.connect():
                    self.log.info("Automatische Custom API-Verbindung erfolgreich.")
                    self.start_monitoring()  # Startet automatisch das Monitoring
                else:
                    self.log.warning("Automatische Custom API-Verbindung fehlgeschlagen.")
                    self.status_label.setText("Automatische Verbindung fehlgeschlagen.")
            else:
                self.log.info("Keine Custom API Konfiguration, kein Auto-Start.")

        self.log.info("Starte automatische Update-Prüfung im Hintergrund...")
        initialize_updater(self, APP_VERSION, self.config, show_no_update_message=False)

        self.update_status_light()

    # --- Slot-Funktionen (Reaktionen auf Events) ---

    @Slot(str)
    def add_log_message(self, message):
        """Fügt eine Nachricht zur Log-Anzeige hinzu."""
        self.log_display.append(message)
        # Auto-Scroll
        self.log_display.verticalScrollBar().setValue(self.log_display.verticalScrollBar().maximum())

    @Slot(bool)
    def toggle_monitoring(self, checked):
        """Startet oder stoppt den Monitor-Thread."""
        if checked:
            self.start_monitoring()
        else:
            self.stop_monitoring()

    @Slot(bool)
    def update_monitoring_status(self, is_active):
        """Aktualisiert den Monitor-Button-Status (wird von Signalen aufgerufen)."""
        if is_active:
            self.monitor_button.setChecked(True)
            self.monitor_button.setText("Monitoring stoppen")
            self.status_label.setText("Überwachung aktiv...")
        else:
            self.monitor_button.setChecked(False)
            self.monitor_button.setText("Monitoring starten")
            self.status_label.setText("Überwachung gestoppt.")

        # NEU: Ampelstatus nach jeder Monitoring-Änderung aktualisieren
        self.update_status_light()

    @Slot()
    def update_status_light(self):
        """
        Aktualisiert die Farbe der Status-Ampel basierend auf
        Verbindungs- und Monitoring-Status.
        """
        if not self.status_light:  # Sicherstellen, dass die UI initialisiert ist
            return

        # Status des aktiven Cloud-Clients abfragen
        active_client = self.get_active_cloud_client()
        is_connected = (active_client.get_connection_status() == "Verbunden")
        is_monitoring = (self.monitor_button and self.monitor_button.isChecked())

        # Logik für die Ampel
        if not is_connected:
            self.status_light.setColor("red")  # Rot: Keine Verbindung
        elif is_connected and not is_monitoring:
            self.status_light.setColor("yellow")  # Gelb: Verbunden, aber Monitoring aus
        elif is_connected and is_monitoring:
            self.status_light.setColor("green")  # Grün: Verbunden und Monitoring aktiv


        # Aktualisierten aktiven Cloud-Client ermitteln
        self.active_cloud_client = self.get_active_cloud_client()

    @Slot()
    def on_settings_changed(self):
        """Wird aufgerufen, wenn Einstellungen gespeichert wurden."""
        self.log.info("Einstellungen wurden geändert. Wecke Threads auf...")

        # Prüfe, ob der Cloud-Service gewechselt wurde
        new_active_client = self.get_active_cloud_client()
        if new_active_client != self.active_cloud_client:
            self.log.info("Cloud-Service wurde gewechselt. Aktualisiere UploaderThread...")
            self.active_cloud_client = new_active_client
            # UploaderThread muss mit neuem Client aktualisiert werden
            self.uploader_thread.client = self.active_cloud_client

        # Die Threads lesen die Konfiguration bei Bedarf neu.
        # Wir müssen den Monitor-Thread aufwecken, falls er auf das Intervall wartet,
        # damit er den neuen Pfad oder das Intervall sofort übernimmt.
        if self.monitor_thread:
            self.monitor_thread.wake_up()

        # Logging neu konfigurieren, falls sich der Pfad geändert hat
        # (Einfacher Ansatz: Logging beim Start konfigurieren.
        # Für dynamische Änderung wäre mehr Aufwand nötig, z.B. Handler entfernen/hinzufügen)
        self.log.warning("Log-Pfad-Änderungen erfordern einen Neustart.")

    @Slot()
    def open_settings(self):
        """Öffnet den Einstellungsdialog."""
        self.log.debug("Öffne Einstellungsdialog...")

        dialog = SettingsDialog(self.config,
                                self.db_client,
                                APP_VERSION,
                                self.latest_version_info,
                                self)

        # Verbinde das Signal, um Update-Status zu empfangen
        self.update_check_finished_signal.connect(dialog.on_update_check_finished)

        dialog.exec()  # Öffnet den Dialog modal (blockierend)

        # Trennt das Signal wieder, wenn der Dialog geschlossen wird,
        # um Speicherlecks zu vermeiden.
        self.update_check_finished_signal.disconnect(dialog.on_update_check_finished)

        # Nach dem Schließen des Dialogs den Status aktualisieren
        self.log.debug("Einstellungsdialog geschlossen.")

        # Aktualisierten aktiven Cloud-Client ermitteln
        self.active_cloud_client = self.get_active_cloud_client()

        # Status des aktiven Cloud-Clients im Hauptfenster aktualisieren
        signals.connection_status_changed.emit(self.active_cloud_client.get_connection_status())

    @Slot()
    def restart_app(self):
        """Startet die Anwendung neu."""
        self.log.info("Starte Anwendung neu...")

        # Worker-Threads sauber beenden
        self.stop_monitoring()
        self.uploader_thread.stop()
        self.uploader_thread.wait(5000)  # 5 Sek. warten

        # Neustart-Prozess
        QCoreApplication.quit()
        QProcess.startDetached(sys.executable, sys.argv)

    def format_bytes(self, b):
        """Hilfsfunktion zur Formatierung von Bytes in MB."""
        mb = b / (1024 * 1024)
        return f"{mb:.1f} MB"

    @Slot(int, int, int)
    def update_file_progress(self, percent, current_bytes, total_bytes):
        """Aktualisiert den Fortschritt der einzelnen Datei (Bar + Text)."""
        self.file_progress_bar.setValue(percent)
        text = "0% (0.0 MB / 0.0 MB)"
        if total_bytes > 0 or current_bytes > 0:  # Verhindert "0.0 MB / 0.0 MB" bei Start
            text = f"{percent}% ({self.format_bytes(current_bytes)} / {self.format_bytes(total_bytes)})"
        self.file_progress_label.setText(text)

    @Slot(int, int, int)
    def update_total_progress(self, percent, current_bytes, total_bytes):
        """Aktualisiert den Gesamtfortschritt (Bar + Text)."""
        self.total_progress_bar.setValue(percent)
        text = "0% (0.0 MB / 0.0 MB)"
        if total_bytes > 0 or current_bytes > 0:  # Verhindert "0.0 MB / 0.0 MB" bei Start
            text = f"{percent}% ({self.format_bytes(current_bytes)} / {self.format_bytes(total_bytes)})"
        self.total_progress_label.setText(text)

    # --- Thread-Management ---

    def start_monitoring(self):
        """Startet den Monitor-Thread, falls er nicht bereits läuft."""
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.log.warning("Monitor-Thread läuft bereits.")
            return

        if not self.config.get_setting("monitor_path"):
            self.log.error("Monitoring nicht gestartet: Kein Überwachungsordner konfiguriert.")
            self.update_monitoring_status(False)
            return

        if self.db_client.get_connection_status() != "Verbunden":
            self.log.error("Monitoring nicht gestartet: Keine Cloud-Verbindung.")
            self.update_monitoring_status(False)
            return

        self.log.info("Starte Monitor-Thread...")
        self.monitor_thread = MonitorThread(self.config, self.upload_queue)
        self.monitor_thread.start()
        self.update_monitoring_status(True)

    def stop_monitoring(self):
        """Stoppt den Monitor-Thread."""
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.log.info("Stoppe Monitor-Thread...")
            self.monitor_thread.stop()
            self.monitor_thread.wait(5000)  # 5 Sek. auf Beendigung warten
            self.log.info("Monitor-Thread gestoppt.")
        self.monitor_thread = None
        self.update_monitoring_status(False)

    def closeEvent(self, event):
        """Wird aufgerufen, wenn das Fenster geschlossen wird."""
        self.log.info("Anwendung wird beendet...")

        # Alle Threads sauber herunterfahren
        self.stop_monitoring()

        self.uploader_thread.stop()
        self.uploader_thread.wait(5000)  # Warte max 5 Sek.

        # Sicherstellen, dass der Update-Thread auch beendet wird
        if self.update_thread and self.update_thread.isRunning():
            self.log.debug("Stoppe Update-Thread...")
            self.update_thread.quit()
            self.update_thread.wait(1000)

        self.log.info("Auf Wiedersehen!")
        event.accept()

    @Slot()
    def check_for_updates_manual(self):
        """Startet die manuelle Update-Prüfung (mit Feedback)."""
        self.status_label.setText("Suche nach Updates...")
        initialize_updater(self, APP_VERSION, self.config, show_no_update_message=True)

    @Slot(str, str, str)
    def on_update_available(self, latest_version, release_notes, installer_url):
        """Slot, der aufgerufen wird, wenn ein Update gefunden wurde."""
        self.log.info(f"Update verfügbar: {latest_version}")
        self.status_label.setText(f"Update auf Version {latest_version} verfügbar!")

        # Status für Einstellungsdialog speichern
        self.latest_version_info = f"Neue Version {latest_version} verfügbar!"

        # Signal an den (evtl. offenen) Einstellungsdialog senden
        self.update_check_finished_signal.emit(self.latest_version_info)

        # Stoppe Monitoring, um Konflikte zu vermeiden
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.log.warning("Update gefunden. Stoppe Monitoring temporär.")
            self.stop_monitoring()

        dialog = AskUpdateDialog(self, APP_VERSION, latest_version, release_notes, self.config)

        if dialog.exec():
            # Benutzer hat "Jetzt aktualisieren" gewählt
            self.log.info("Starte Download des Updates...")
            progress_dialog = UpdateProgressDialog(self, installer_url)
            progress_dialog.exec()
            # Die App wird durch den Progress-Dialog beendet, wenn 'EXIT_APP' kommt
        else:
            # Benutzer hat "Später" gewählt
            self.log.info("Update auf 'Später' verschoben.")
            self.status_label.setText("Bereit.")

    @Slot(str)
    def on_no_update(self, message):
        """Slot, der aufgerufen wird, wenn kein Update verfügbar ist."""
        self.log.info(f"Update-Prüfung: {message}")
        self.status_label.setText("Bereit.")

        # Status für Einstellungsdialog speichern
        self.latest_version_info = message  # z.B. "Sie sind auf dem neuesten Stand."

        # Signal an den (evtl. offenen) Einstellungsdialog senden
        self.update_check_finished_signal.emit(self.latest_version_info)

        # Zeige Feedback nur bei manueller Prüfung
        if self.sender() and self.sender().show_no_update_message:
            QMessageBox.information(self, "Update-Prüfung", message)

    @Slot(str)
    def on_update_error(self, message):
        """Slot, der bei einem Fehler der Update-Prüfung aufgerufen wird."""
        self.log.error(f"Fehler bei Update-Prüfung: {message}")
        self.status_label.setText("Update-Prüfung fehlgeschlagen.")

        # Status für Einstellungsdialog speichern
        self.latest_version_info = message  # z.B. "Fehler: Netzwerk nicht erreichbar."

        # Signal an den (evtl. offenen) Einstellungsdialog senden
        self.update_check_finished_signal.emit(self.latest_version_info)

        # Zeige Feedback nur bei manueller Prüfung
        if self.sender() and self.sender().show_no_update_message:
            QMessageBox.critical(self, "Update-Fehler", message)

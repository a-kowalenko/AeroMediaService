import os
import sys
import logging
import queue

from PySide6.QtGui import QIcon, QPainter, QColor, QPixmap, QPalette, QPen, QFont
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QProgressBar, QLabel, QStatusBar,
    QMessageBox, QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView, QLineEdit, QAbstractItemView, QSplitter,
    QDialog, QDialogButtonBox, QCheckBox, QFormLayout, QGroupBox,
)

from PySide6.QtCore import QCoreApplication, QProcess, QThread, QTimer, QRectF, Slot, Qt, Signal, QObject

# Importiere alle Komponenten des Projekts
from core import APP_VERSION
from core.config import ConfigManager
from core.logger import setup_logging
from core.signals import signals
from core.monitor import MonitorThread, recover_stalled_upload_folders
from core.uploader import UploaderThread
from core.upload_queue_registry import UploadQueueRegistry
from core.retry_upload import retry_upload_from_history, RETRYABLE_STATUSES
from core.history_status import build_overall_status, history_entry_needs_sms_journal_check
from core.sms_history_sync import update_history_from_journal
from core.resend_notifications import (
    can_resend_notifications,
    resolve_share_link,
    lookup_share_link_from_cloud,
    validate_contact_for_channels,
    normalize_contact,
    channels_already_delivered,
    get_sandbox_warnings,
    build_contact_update_payload,
    resend_notifications,
    format_resend_result_message,
    format_resend_history_summary,
    migrate_share_links_for_history,
    resend_had_failures,
)
from models.kunde import normalize_phone
from utils.validation import is_valid_email, is_valid_share_link
from services.dropbox_client import DropboxClient
from services.custom_api_client import CustomApiClient
from services.email_client import EmailClient
from services.sms_client import SmsClient
from settings import SettingsDialog
from utils.constants import ICON_PATH
from utils.loading_overlay import LoadingOverlay
from utils.updater import initialize_updater, AskUpdateDialog, UpdateProgressDialog
from utils.history_manager import HistoryManager


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


class HistoryRefreshOverlay(LoadingOverlay):
    """Dezente halbtransparente Schicht mit zentriertem Lade-Indikator über dem Historien-Panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)


class _HistoryPanelHost(QWidget):
    """Stackt ein Panel-Widget und das Lade-Overlay gleich groß übereinander."""

    def __init__(self, content: QWidget):
        super().__init__()
        self._content = content
        self._overlay = HistoryRefreshOverlay(self)
        self._content.setParent(self)
        self._overlay.hide()

    @property
    def overlay(self) -> HistoryRefreshOverlay:
        return self._overlay

    def resizeEvent(self, event):
        super().resizeEvent(event)
        r = self.rect()
        self._content.setGeometry(r)
        self._overlay.setGeometry(r)


class HistoryFileLoadWorker(QObject):
    """Lädt upload_history.json im Hintergrund."""

    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, file_path: str):
        super().__init__()
        self.file_path = file_path

    def run(self):
        import json
        import os

        try:
            if not os.path.exists(self.file_path):
                self.finished.emit([])
                return
            with open(self.file_path, "r", encoding="utf-8") as handle:
                self.finished.emit(json.load(handle))
        except Exception as exc:
            self.failed.emit(str(exc))


class StartupConnectWorker(QObject):
    """Stellt beim Start die Cloud-Verbindung im Hintergrund her."""

    finished = Signal(bool, str)

    def __init__(self, config, db_client, custom_api_client):
        super().__init__()
        self.config = config
        self.db_client = db_client
        self.custom_api_client = custom_api_client
        self.log = logging.getLogger(__name__)

    @Slot()
    def run(self):
        try:
            selected_cloud = self.config.get_setting("selected_cloud_service", "dropbox")
            if selected_cloud == "dropbox":
                if not self.config.get_secret("db_refresh_token"):
                    self.log.info("Keine gespeicherten Dropbox-Tokens, kein Auto-Start.")
                    self.finished.emit(False, "")
                    return
                self.log.info("Stelle automatische Verbindung her (Dropbox)...")
                if self.db_client.connect():
                    self.log.info("Automatische Dropbox-Verbindung erfolgreich.")
                    self.finished.emit(True, "Bereit.")
                else:
                    self.log.warning("Automatische Dropbox-Verbindung fehlgeschlagen.")
                    self.finished.emit(False, "Automatische Verbindung fehlgeschlagen.")
            elif selected_cloud == "custom_api":
                if (
                    not self.config.get_secret("custom_api_url")
                    or not self.config.get_secret("custom_api_bearer_token")
                ):
                    self.log.info("Keine Custom API Konfiguration, kein Auto-Start.")
                    self.finished.emit(False, "")
                    return
                self.log.info("Stelle automatische Verbindung her (Custom API)...")
                if self.custom_api_client.connect():
                    self.log.info("Automatische Custom API-Verbindung erfolgreich.")
                    self._connect_dropbox_for_contact_markers()
                    self.finished.emit(True, "Bereit.")
                else:
                    self.log.warning("Automatische Custom API-Verbindung fehlgeschlagen.")
                    self.finished.emit(False, "Automatische Verbindung fehlgeschlagen.")
            else:
                self.finished.emit(False, "")
        except Exception as exc:
            self.log.error("Fehler bei automatischer Verbindung: %s", exc)
            self.finished.emit(False, f"Verbindungsfehler: {exc}")

    def _connect_dropbox_for_contact_markers(self):
        if not self.config.get_secret("db_refresh_token"):
            self.log.info(
                "Kein Dropbox Refresh-Token — reine Kontakt-Marker können nicht über Dropbox hochgeladen werden."
            )
            return
        if self.db_client.dbx is not None:
            return
        if self.db_client.connect():
            self.log.info("Dropbox parallel verbunden (für reine Kontakt-Marker).")
        else:
            self.log.warning(
                "Dropbox-Verbindung für reine Kontakt-Marker fehlgeschlagen — "
                "diese Uploads werden scheitern, bis Dropbox verbunden ist."
            )


class ResendNotificationsDialog(QDialog):
    """Dialog zum erneuten Versenden von E-Mail und/oder SMS."""

    def __init__(self, entry, email, phone, share_link, cloud_client, config_manager, parent=None):
        super().__init__(parent)
        self.entry = dict(entry)
        self.cloud_client = cloud_client
        self.config_manager = config_manager

        self.setWindowTitle("Benachrichtigung erneut senden")
        self.setMinimumWidth(520)

        layout = QVBoxLayout(self)

        dir_name = (entry.get("dir_name") or "").strip()
        first = (entry.get("first_name") or "").strip()
        last = (entry.get("last_name") or "").strip()
        customer = f"{first} {last}".strip() or "—"

        layout.addWidget(QLabel(f"<b>Auftrag:</b> {dir_name}"))
        layout.addWidget(QLabel(f"<b>Kunde:</b> {customer}"))

        for warning in get_sandbox_warnings(config_manager):
            warn_label = QLabel(f"⚠ {warning}")
            warn_label.setStyleSheet("color: #b45309; font-weight: 500;")
            warn_label.setWordWrap(True)
            layout.addWidget(warn_label)

        layout.addWidget(QLabel("<b>Download-Link</b>"))
        link_row = QHBoxLayout()
        self.link_input = QLineEdit()
        self.link_input.setPlaceholderText("https://…")
        self.link_input.setText((share_link or "").strip())
        link_row.addWidget(self.link_input)

        self.load_link_btn = QPushButton("Aus Cloud laden")
        self.load_link_btn.setToolTip("Link über die verbundene Cloud ermitteln")
        self.load_link_btn.clicked.connect(self._on_load_link_clicked)
        link_row.addWidget(self.load_link_btn)
        layout.addLayout(link_row)

        self._update_load_link_button_state()
        self.email_checkbox = QCheckBox("E-Mail senden")
        self.sms_checkbox = QCheckBox("SMS senden")
        self.email_checkbox.setChecked(bool((email or "").strip()))
        self.sms_checkbox.setChecked(bool(normalize_phone(phone)))
        layout.addWidget(self.email_checkbox)
        layout.addWidget(self.sms_checkbox)

        sms_hint = QLabel("SMS: Jeder Versand verursacht Kosten bei Seven.io.")
        sms_hint.setStyleSheet("color: gray;")
        sms_hint.setWordWrap(True)
        layout.addWidget(sms_hint)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Cancel | QDialogButtonBox.StandardButton.Ok
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("Jetzt senden")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _update_load_link_button_state(self):
        connected = (
            self.cloud_client is not None
            and self.cloud_client.get_connection_status() == "Verbunden"
        )
        self.load_link_btn.setEnabled(connected)

    def _on_load_link_clicked(self):
        try:
            link = lookup_share_link_from_cloud(self.entry, self.cloud_client)
        except ValueError as exc:
            QMessageBox.warning(self, "Link laden", str(exc))
            return
        except Exception as exc:
            QMessageBox.warning(self, "Link laden", f"Link konnte nicht geladen werden:\n{exc}")
            return
        self.link_input.setText(link)

    def get_send_email(self) -> bool:
        return self.email_checkbox.isChecked()

    def get_send_sms(self) -> bool:
        return self.sms_checkbox.isChecked()

    def get_share_link(self) -> str:
        return self.link_input.text().strip()


class ResendNotificationsWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, entry, email, phone, share_link, send_email, send_sms, email_client, sms_client, config_manager):
        super().__init__()
        self.entry = entry
        self.email = email
        self.phone = phone
        self.share_link = share_link
        self.send_email = send_email
        self.send_sms = send_sms
        self.email_client = email_client
        self.sms_client = sms_client
        self.config_manager = config_manager

    def run(self):
        try:
            result = resend_notifications(
                self.entry,
                self.email,
                self.phone,
                self.share_link,
                self.send_email,
                self.send_sms,
                self.email_client,
                self.sms_client,
                self.config_manager,
            )
            self.finished.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class ShareLinkMigrationWorker(QObject):
    finished = Signal(int)

    def __init__(self, history_manager, cloud_client):
        super().__init__()
        self.history_manager = history_manager
        self.cloud_client = cloud_client

    def run(self):
        try:
            count = migrate_share_links_for_history(
                self.history_manager.history,
                self.cloud_client,
            )
            if count:
                self.history_manager.save_history()
            self.finished.emit(count)
        except Exception:
            self.finished.emit(0)


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
        self._last_log_file_path = os.path.normpath(
            self.config.get_setting("log_file_path", ".") or "."
        )
        self.log.info("Anwendung wird gestartet...")

        self.upload_queue = queue.Queue()
        self.upload_registry = UploadQueueRegistry()

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
        self.uploader_thread = UploaderThread(
            self.config,
            self.upload_queue,
            self.active_cloud_client,
            self.email_client,
            self.sms_client,
            self.upload_registry,
            dropbox_client=self.db_client,
        )

        # --- Update-Worker ---
        self.update_thread = None
        self.update_worker = None
        self._startup_thread = None
        self._startup_worker = None

        self.history_manager = HistoryManager()
        self._upload_recovery_done = False
        self._history_pending_by_dir = {}
        self._history_debounce_timer = QTimer(self)
        self._history_debounce_timer.setSingleShot(True)
        self._history_debounce_timer.setInterval(400)
        self._history_debounce_timer.timeout.connect(self._flush_debounced_history_updates)
        self._sms_check_schedule_timer = QTimer(self)
        self._sms_check_schedule_timer.setSingleShot(True)
        self._sms_check_schedule_timer.setInterval(1500)
        self._sms_check_schedule_timer.timeout.connect(self._run_scheduled_sms_status_check)

        self.current_history_page = 0
        self.history_items_per_page = 25
        self._history_refresh_overlay_refs = 0
        self._contact_populating = False
        self._contact_dirty = False
        self._resend_worker_thread = None
        self._resend_worker = None
        self._resend_btn_default_text = "Benachrichtigung erneut senden…"
        self._share_link_migration_thread = None
        self._history_file_load_thread = None
        self._history_loaded_mtime = self.history_manager.get_file_mtime()
        self._history_ui_initialized = False
        self._status_icon_cache: dict[str, QIcon] = {}

        # Member-Variable für den letzten Update-Status
        self.latest_version_info = "Noch nicht geprüft."

        # --- GUI-Komponenten (Platzhalter) ---
        self.status_light = None
        self.monitor_button = None

        # --- GUI Erstellen ---
        self.init_ui()

        # --- Signale verbinden ---
        self.connect_signals()
        self._refresh_upload_queue_table(self.upload_registry.snapshot_dicts())

        # --- Threads starten ---
        self.uploader_thread.start()

    def init_ui(self):
        """Erstellt die Benutzeroberfläche."""

        # Zentrales Widget
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        self.setCentralWidget(main_widget)
        self._startup_overlay = LoadingOverlay(main_widget, message="Verbindung wird hergestellt…")
        self._startup_overlay.hide()

        # Tab Widget
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # --- Tab 1: Monitor ---
        self.monitor_tab = QWidget()
        monitor_layout = QVBoxLayout(self.monitor_tab)

        # 1. Button-Leiste (oben)
        button_layout = QHBoxLayout()

        self.monitor_button = QPushButton("Monitoring starten")
        self.monitor_button.setCheckable(True)  # Macht ihn zu einem Toggle-Button
        self.monitor_button.clicked.connect(self.toggle_monitoring)

        self.status_light = StatusLight(self)
        self.status_light.setToolTip(
            "Status:\nRot: Nicht verbunden\nGelb: Verbunden, Monitoring aus\nGrün: Verbunden, Monitoring aktiv")

        self.autoscroll_check = QCheckBox("Auto-Scroll")
        self.autoscroll_check.setToolTip(
            "Log-Anzeige bei neuen Einträgen automatisch nach unten scrollen")
        autoscroll_enabled = str(self.config.get_setting("monitor_autoscroll", "true")).lower() != "false"
        self.autoscroll_check.setChecked(autoscroll_enabled)
        self.autoscroll_check.toggled.connect(self._on_autoscroll_toggled)

        self.settings_button = QPushButton("Einstellungen")
        self.settings_button.clicked.connect(self.open_settings)

        self.restart_button = QPushButton("Neustart")
        self.restart_button.clicked.connect(self.restart_app)

        button_layout.addWidget(self.monitor_button)
        button_layout.addWidget(self.status_light)
        button_layout.addWidget(self.autoscroll_check)
        button_layout.addStretch()
        button_layout.addWidget(self.settings_button)
        button_layout.addWidget(self.restart_button)
        monitor_layout.addLayout(button_layout)

        # 2. Log-Anzeige
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        monitor_layout.addWidget(self.log_display)

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

        upload_ctl_layout = QHBoxLayout()
        self.upload_pause_toggle_btn = QPushButton("Pause")
        self.upload_pause_toggle_btn.setEnabled(False)
        self.upload_pause_toggle_btn.setToolTip(
            "Upload nach dem aktuellen Datenblock anhalten bzw. pausierten Upload fortsetzen"
        )
        self.upload_pause_toggle_btn.clicked.connect(self._on_upload_pause_toggle_clicked)
        self._upload_toggle_shows_resume = False
        self.upload_cancel_btn = QPushButton("Abbrechen")
        self.upload_cancel_btn.setEnabled(False)
        self.upload_cancel_btn.setToolTip(
            "Aktuellen Upload abbrechen; Stand bleibt im Checkpoint fuer spaeteres Fortsetzen erhalten"
        )
        self.upload_cancel_btn.clicked.connect(self._on_upload_cancel_clicked)
        upload_ctl_layout.addWidget(self.upload_pause_toggle_btn)
        upload_ctl_layout.addWidget(self.upload_cancel_btn)
        upload_ctl_layout.addStretch()
        progress_layout.addLayout(upload_ctl_layout)

        monitor_layout.addLayout(progress_layout)

        self.tabs.addTab(self.monitor_tab, "Monitor")

        # --- Tab 2: Warteschlange ---
        self.queue_tab = QWidget()
        queue_tab_layout = QVBoxLayout(self.queue_tab)

        self.upload_queue_summary_label = QLabel("Keine ausstehenden Uploads.")
        queue_tab_layout.addWidget(self.upload_queue_summary_label)

        self.upload_queue_table = QTableWidget()
        self.upload_queue_table.setColumnCount(4)
        self.upload_queue_table.setHorizontalHeaderLabels(["#", "Ordner", "Kunde", "Status"])
        self.upload_queue_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.upload_queue_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.upload_queue_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.upload_queue_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.upload_queue_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.upload_queue_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.upload_queue_table.verticalHeader().setVisible(False)
        queue_tab_layout.addWidget(self.upload_queue_table)

        self._queue_tab_index = self.tabs.addTab(self.queue_tab, "Warteschlange")

        # --- Tab 3: Upload-Historie ---
        self.history_tab = QWidget()
        history_layout = QVBoxLayout(self.history_tab)

        # Suche & Buttons
        hist_top_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Suchen...")
        self.search_input.textChanged.connect(self.on_search_changed)
        hist_top_layout.addWidget(self.search_input)

        self.history_manual_refresh_btn = QPushButton("\u21bb")
        self.history_manual_refresh_btn.setToolTip("Historie von Festplatte laden und SMS-Status prüfen")
        self.history_manual_refresh_btn.setFixedWidth(36)
        self.history_manual_refresh_btn.clicked.connect(self.on_history_manual_refresh_clicked)
        hist_top_layout.addWidget(self.history_manual_refresh_btn)

        self.history_refresh_countdown_label = QLabel()
        self.history_refresh_countdown_label.setMinimumWidth(180)
        hist_top_layout.addWidget(self.history_refresh_countdown_label)

        self._history_refresh_interval_sec = 60
        self._history_refresh_seconds_left = self._history_refresh_interval_sec
        self._history_refresh_timer = QTimer(self)
        self._history_refresh_timer.timeout.connect(self._on_history_refresh_timer_tick)
        self._history_refresh_timer.start(1000)
        self._update_history_refresh_countdown_label()

        self.retry_upload_btn = QPushButton("Erneut hochladen")
        self.retry_upload_btn.setEnabled(False)
        self.retry_upload_btn.setToolTip(
            "Fehlgeschlagenen oder abgebrochenen Upload aus dem Archiv erneut einreihen"
        )
        self.retry_upload_btn.clicked.connect(self.on_retry_upload_clicked)
        hist_top_layout.addWidget(self.retry_upload_btn)

        self.resend_notifications_btn = QPushButton("Benachrichtigung erneut senden…")
        self.resend_notifications_btn.setEnabled(False)
        self.resend_notifications_btn.setToolTip(
            "E-Mail und/oder SMS für einen erfolgreichen Upload erneut versenden"
        )
        self.resend_notifications_btn.clicked.connect(self.on_resend_notifications_clicked)
        hist_top_layout.addWidget(self.resend_notifications_btn)

        self.delete_selected_btn = QPushButton("Ausgewählte löschen")
        self.delete_selected_btn.clicked.connect(self.delete_selected_history)
        hist_top_layout.addWidget(self.delete_selected_btn)

        self.delete_all_btn = QPushButton("Alle löschen")
        self.delete_all_btn.clicked.connect(self.delete_all_history)
        hist_top_layout.addWidget(self.delete_all_btn)

        history_layout.addLayout(hist_top_layout)

        # Splitter: oben Historie, unten Eigenschaften + Kontakt nebeneinander
        self.history_splitter = QSplitter(Qt.Orientation.Vertical)

        self.history_table = QTableWidget()
        self.history_table.setColumnCount(4)
        self.history_table.setHorizontalHeaderLabels(["Datum", "Name", "Status", "Fehler"])
        self.history_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.history_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.history_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.history_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.history_table.itemSelectionChanged.connect(self.on_history_selection_changed)
        self.history_table.itemDoubleClicked.connect(self.on_history_cell_clicked)
        self.history_splitter.addWidget(self.history_table)

        self.history_details_splitter = QSplitter(Qt.Orientation.Horizontal)

        self.detail_table = QTableWidget()
        self.detail_table.setColumnCount(2)
        self.detail_table.setHorizontalHeaderLabels(["Eigenschaft", "Wert"])
        self.detail_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.detail_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.detail_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.detail_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.history_details_splitter.addWidget(self.detail_table)

        self.history_contact_group = QGroupBox("Kontakt")
        self.history_contact_group.setStyleSheet(
            "QGroupBox { font-weight: 600; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 4px; }"
        )
        contact_outer = QVBoxLayout(self.history_contact_group)

        contact_form = QFormLayout()
        contact_form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        self.contact_email_input = QLineEdit()
        self.contact_email_input.setPlaceholderText("name@beispiel.de")
        self.contact_email_input.textChanged.connect(self._on_contact_fields_changed)
        email_field_box = QVBoxLayout()
        email_field_box.setSpacing(2)
        email_field_box.addWidget(self.contact_email_input)
        self.contact_email_status_label = QLabel("—")
        self.contact_email_status_label.setStyleSheet("color: gray; font-size: 11px;")
        email_field_box.addWidget(self.contact_email_status_label)
        email_widget = QWidget()
        email_widget.setLayout(email_field_box)
        contact_form.addRow("E-Mail", email_widget)

        self.contact_phone_input = QLineEdit()
        self.contact_phone_input.setPlaceholderText("+49 …")
        self.contact_phone_input.textChanged.connect(self._on_contact_fields_changed)
        phone_field_box = QVBoxLayout()
        phone_field_box.setSpacing(2)
        phone_field_box.addWidget(self.contact_phone_input)
        self.contact_phone_status_label = QLabel("—")
        self.contact_phone_status_label.setStyleSheet("color: gray; font-size: 11px;")
        phone_field_box.addWidget(self.contact_phone_status_label)
        phone_widget = QWidget()
        phone_widget.setLayout(phone_field_box)
        contact_form.addRow("Telefon", phone_widget)

        contact_outer.addLayout(contact_form)

        contact_btn_row = QHBoxLayout()
        self.contact_save_btn = QPushButton("Änderungen speichern")
        self.contact_save_btn.setEnabled(False)
        self.contact_save_btn.clicked.connect(self.on_contact_save_clicked)
        contact_btn_row.addWidget(self.contact_save_btn)
        contact_btn_row.addStretch()
        contact_outer.addLayout(contact_btn_row)

        self.history_details_splitter.addWidget(self.history_contact_group)
        self.history_details_splitter.setStretchFactor(0, 3)
        self.history_details_splitter.setStretchFactor(1, 2)

        self.history_splitter.addWidget(self.history_details_splitter)
        self.history_splitter.setStretchFactor(0, 3)
        self.history_splitter.setStretchFactor(1, 2)

        self._history_panel_host = _HistoryPanelHost(self.history_splitter)
        self._history_refresh_overlay = self._history_panel_host.overlay
        history_layout.addWidget(self._history_panel_host)

        # Pagination
        pag_layout = QHBoxLayout()
        self.prev_page_btn = QPushButton("< Vorherige")
        self.prev_page_btn.clicked.connect(self.prev_history_page)
        self.page_label = QLabel("Seite 1")
        self.next_page_btn = QPushButton("Nächste >")
        self.next_page_btn.clicked.connect(self.next_history_page)

        pag_layout.addStretch()
        pag_layout.addWidget(self.prev_page_btn)
        pag_layout.addWidget(self.page_label)
        pag_layout.addWidget(self.next_page_btn)
        pag_layout.addStretch()

        history_layout.addLayout(pag_layout)

        self.tabs.addTab(self.history_tab, "Upload-Historie")

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
        self.tabs.currentChanged.connect(self.on_tab_changed)
        
        signals.log_message.connect(self.add_log_message)
        signals.upload_history_update.connect(self.on_history_update)
        signals.upload_progress_file.connect(self.update_file_progress)
        signals.upload_progress_total.connect(self.update_total_progress)
        signals.upload_status_update.connect(self.status_label.setText)
        signals.upload_job_active.connect(self._on_upload_job_active_changed)
        signals.upload_queue_changed.connect(self._refresh_upload_queue_table)
        signals.monitoring_status_changed.connect(self.update_monitoring_status)

        # Upload-Fortschritt-Signale
        signals.upload_started.connect(lambda count: self.add_log_message(logging.INFO, f"Upload gestartet: {count} Datei(en)"))
        signals.upload_progress.connect(lambda msg: self.add_log_message(logging.INFO, msg))
        signals.upload_finished.connect(lambda msg: self.add_log_message(logging.INFO, msg))
        signals.upload_failed.connect(lambda msg: self.add_log_message(logging.ERROR, f"❌ Upload fehlgeschlagen: {msg}"))

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

    def _sync_startup_overlay_geometry(self):
        central = self.centralWidget()
        if central and hasattr(self, "_startup_overlay"):
            self._startup_overlay.setGeometry(central.rect())

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._sync_startup_overlay_geometry()

    @Slot()
    def deferred_startup(self):
        """Verzögerter Start: Cloud-Verbindung und Update-Prüfung nach dem ersten UI-Frame."""
        self.log.info("Prüfe auf Auto-Verbindung...")
        if self._should_auto_connect():
            self.status_label.setText("Verbindung wird hergestellt…")
            self._sync_startup_overlay_geometry()
            self._startup_overlay.show_loading()
            self._start_startup_connect_worker()
        else:
            self.status_label.setText("Bereit.")
            self.update_status_light()
            QTimer.singleShot(2500, self._start_share_link_migration)
        self.log.info("Starte automatische Update-Prüfung im Hintergrund...")
        initialize_updater(self, APP_VERSION, self.config, show_no_update_message=False)

    def _should_auto_connect(self) -> bool:
        selected_cloud = self.config.get_setting("selected_cloud_service", "dropbox")
        if selected_cloud == "dropbox":
            return bool(self.config.get_secret("db_refresh_token"))
        if selected_cloud == "custom_api":
            return bool(
                self.config.get_secret("custom_api_url")
                and self.config.get_secret("custom_api_bearer_token")
            )
        return False

    def _start_startup_connect_worker(self):
        if self._startup_thread and self._startup_thread.isRunning():
            return

        self._startup_thread = QThread()
        self._startup_worker = StartupConnectWorker(
            self.config, self.db_client, self.custom_api_client
        )
        self._startup_worker.moveToThread(self._startup_thread)
        self._startup_thread.started.connect(self._startup_worker.run)
        self._startup_worker.finished.connect(self._on_startup_connect_finished)
        self._startup_worker.finished.connect(self._startup_thread.quit)
        self._startup_thread.finished.connect(self._startup_worker.deleteLater)
        self._startup_thread.finished.connect(self._startup_thread.deleteLater)
        self._startup_thread.finished.connect(lambda: setattr(self, "_startup_thread", None))
        self._startup_thread.finished.connect(lambda: setattr(self, "_startup_worker", None))
        self._startup_thread.start()

    @Slot(bool, str)
    def _on_startup_connect_finished(self, success, message):
        self._startup_overlay.hide_loading()
        self.active_cloud_client = self.get_active_cloud_client()
        if success:
            self.start_monitoring(skip_status_check=True)
            self.status_label.setText(message or "Bereit.")
        elif message:
            self.status_label.setText(message)
        else:
            self.status_label.setText("Bereit.")
        self.update_status_light()
        QTimer.singleShot(2500, self._start_share_link_migration)

    def _auto_connect_dropbox_for_pure_contact_markers(self):
        """Dropbox parallel verbinden, falls reine Kontakt-Marker über DropboxClient laufen."""
        if not self.config.get_secret("db_refresh_token"):
            self.log.info(
                "Kein Dropbox Refresh-Token — reine Kontakt-Marker können nicht über Dropbox hochgeladen werden."
            )
            return
        if self.db_client.dbx is not None:
            return
        if self.db_client.connect():
            self.log.info("Dropbox parallel verbunden (für reine Kontakt-Marker).")
        else:
            self.log.warning(
                "Dropbox-Verbindung für reine Kontakt-Marker fehlgeschlagen — "
                "diese Uploads werden scheitern, bis Dropbox verbunden ist."
            )

    # --- Slot-Funktionen (Reaktionen auf Events) ---

    @Slot(int, str)
    def add_log_message(self, level, message):
        """Fügt eine Nachricht zur Log-Anzeige hinzu und färbt sie ein."""
        # Standardtext: Systempalette (Light/Dark), nicht fest „white“ (sonst unsichtbar auf hellem Hintergrund).
        color = self.log_display.palette().color(QPalette.ColorRole.Text).name()
        if level == logging.ERROR:
            color = "red"
        elif level == logging.WARNING:
            color = "orange"
        elif level == logging.DEBUG:
            color = "gray"

        # Verwende HTML für die Farbe, ersetze \n durch <br> um Formatierung beizubehalten
        formatted_msg = message.replace('\n', '<br>')
        html_message = f"<span style='color:{color};'>{formatted_msg}</span>"
        self.log_display.append(html_message)
        if self.autoscroll_check.isChecked():
            scrollbar = self.log_display.verticalScrollBar()
            scrollbar.setValue(scrollbar.maximum())

    @Slot(bool)
    def _on_autoscroll_toggled(self, checked):
        self.config.save_setting("monitor_autoscroll", "true" if checked else "false")

    @Slot(dict)
    def on_history_update(self, data):
        """Wird aufgerufen, wenn ein Upload aktualisiert wird."""
        updated_dir = data.get("dir_name")
        if not updated_dir:
            selected_id = self.get_selected_history_id()
            self.history_manager.add_or_update(data)
            self._sync_history_loaded_mtime()
            self.refresh_history_table(maintain_page=True)
            self._refresh_history_detail_if_needed(data, selected_id)
            self._schedule_sms_status_check_if_needed()
            return

        existing = self._history_pending_by_dir.get(updated_dir)
        if existing:
            existing.update(data)
        else:
            self._history_pending_by_dir[updated_dir] = dict(data)
        self._history_debounce_timer.start()

    @Slot()
    def _flush_debounced_history_updates(self):
        batch = self._history_pending_by_dir
        self._history_pending_by_dir = {}
        if not batch:
            return
        selected_id = self.get_selected_history_id()
        for merged in batch.values():
            self.history_manager.add_or_update(merged)
        self._sync_history_loaded_mtime()
        self.refresh_history_table(maintain_page=True)
        if selected_id:
            for merged in batch.values():
                self._refresh_history_detail_if_needed(merged, selected_id)
        self._schedule_sms_status_check_if_needed()

    def _schedule_sms_status_check_if_needed(self):
        if not any(history_entry_needs_sms_journal_check(item) for item in self.history_manager.history):
            return
        self._sms_check_schedule_timer.start()

    @Slot()
    def _run_scheduled_sms_status_check(self):
        if any(history_entry_needs_sms_journal_check(item) for item in self.history_manager.history):
            self.check_sms_status()

    def _refresh_history_detail_if_needed(self, data, selected_id):
        """Detail-Grid aktualisieren, wenn der selektierte Eintrag zu diesem Update passt."""
        if not selected_id:
            return
        updated_id = data.get("id")
        updated_dir = data.get("dir_name")
        if (updated_id and updated_id == selected_id) or (
            not updated_id and updated_dir and self._selected_entry_matches_dir(selected_id, updated_dir)
        ):
            item_data = self.get_history_entry_by_id(selected_id)
            if item_data:
                self.populate_detail_table(item_data)
                self._populate_contact_card(item_data)
                self._update_resend_notifications_button_state(item_data)

    def _selected_entry_matches_dir(self, selected_id, dir_name):
        """Hilfsfunktion: Prüft, ob die selektierte ID zu einem Verzeichnisnamen gehört."""
        if not selected_id or not dir_name:
            return False
        for entry in self.history_manager.history:
            if entry.get("id") == selected_id and entry.get("dir_name") == dir_name:
                return True
        return False

    def _sms_status_worker_busy(self):
        """True, wenn der SMS-Worker-Thread noch (oder scheinbar noch) existiert und läuft."""
        t = getattr(self, "_sms_worker_thread", None)
        if t is None:
            return False
        try:
            return t.isRunning()
        except RuntimeError:
            # C++-Objekt bereits von deleteLater entfernt, Python-Referenz war noch gesetzt
            self._sms_worker_thread = None
            return False

    @Slot()
    def _on_sms_worker_thread_destroyed(self):
        try:
            dead = self.sender()
        except RuntimeError:
            dead = None
        if dead is not None and dead is getattr(self, "_sms_worker_thread", None):
            self._sms_worker_thread = None

    def _begin_history_refresh_overlay(self):
        self._history_refresh_overlay_refs += 1
        if self._history_refresh_overlay_refs == 1:
            self._history_refresh_overlay.show_loading()

    def _end_history_refresh_overlay(self):
        if self._history_refresh_overlay_refs <= 0:
            return
        self._history_refresh_overlay_refs -= 1
        if self._history_refresh_overlay_refs == 0:
            self._history_refresh_overlay.hide_loading()

    def _sync_history_loaded_mtime(self):
        self._history_loaded_mtime = self.history_manager.get_file_mtime()

    def _history_file_load_busy(self) -> bool:
        thread = getattr(self, "_history_file_load_thread", None)
        if thread is None:
            return False
        try:
            return thread.isRunning()
        except RuntimeError:
            self._history_file_load_thread = None
            return False

    def _history_refresh_busy(self) -> bool:
        return self._history_file_load_busy() or self._sms_status_worker_busy()

    def _history_file_changed_on_disk(self) -> bool:
        current_mtime = self.history_manager.get_file_mtime()
        return current_mtime != self._history_loaded_mtime

    def _ensure_history_ui_rendered(self):
        """Zeigt die in-memory-Historie an, ohne von Platte neu zu laden."""
        if self._history_ui_initialized:
            return
        QTimer.singleShot(0, self._render_history_ui_once)

    @Slot()
    def _render_history_ui_once(self):
        if self._history_ui_initialized:
            return
        self.refresh_history_table(maintain_page=True)
        self._history_ui_initialized = True

    def reload_history_from_disk_and_refresh(self, *, force=False, check_sms=True):
        """Lädt Historie bei Bedarf asynchron von Platte und prüft optional SMS-Status."""
        if self._history_file_load_busy():
            return

        file_changed = force or self._history_file_changed_on_disk()
        if not file_changed:
            self._ensure_history_ui_rendered()
            return

        self._begin_history_refresh_overlay()
        self.status_label.setText("Historie wird geladen…")

        self._history_file_load_thread = QThread()
        worker = HistoryFileLoadWorker(self.history_manager.file_path)
        worker.moveToThread(self._history_file_load_thread)
        self._history_file_load_thread.started.connect(worker.run)
        worker.finished.connect(self._on_history_file_loaded)
        worker.failed.connect(self._on_history_file_load_failed)
        worker.finished.connect(self._history_file_load_thread.quit)
        worker.failed.connect(self._history_file_load_thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        self._history_file_load_thread.finished.connect(self._history_file_load_thread.deleteLater)
        self._history_file_load_thread.start()

        self._history_file_load_check_sms = check_sms

    @Slot(object)
    def _on_history_file_loaded(self, data):
        self.history_manager.history = data if isinstance(data, list) else []
        self._sync_history_loaded_mtime()
        self.refresh_history_table(maintain_page=True)
        self._history_ui_initialized = True
        self._history_file_load_thread = None

        check_sms = getattr(self, "_history_file_load_check_sms", True)
        if check_sms:
            self.status_label.setText("SMS-Status wird geprüft…")
            sms_started = self.check_sms_status()
            if not sms_started:
                QTimer.singleShot(320, self._end_history_refresh_overlay)
                self.status_label.setText("Historie geladen.")
        else:
            QTimer.singleShot(120, self._end_history_refresh_overlay)
            self.status_label.setText("Historie geladen.")

    @Slot(str)
    def _on_history_file_load_failed(self, message):
        self.log.error("Historie-Datei konnte nicht geladen werden: %s", message)
        self._history_file_load_thread = None
        self._end_history_refresh_overlay()
        self.status_label.setText("Historie laden fehlgeschlagen.")
        QMessageBox.warning(self, "Upload-Historie", f"Historie konnte nicht geladen werden:\n{message}")

    def _update_history_refresh_countdown_label(self):
        if hasattr(self, "history_refresh_countdown_label"):
            self.history_refresh_countdown_label.setText(
                f"Nächstes Laden in {self._history_refresh_seconds_left} s"
            )

    @Slot()
    def _on_history_refresh_timer_tick(self):
        self._history_refresh_seconds_left -= 1
        if self._history_refresh_seconds_left <= 0:
            self.reload_history_from_disk_and_refresh(force=False, check_sms=True)
            self._history_refresh_seconds_left = self._history_refresh_interval_sec
        self._update_history_refresh_countdown_label()

    @Slot()
    def on_history_manual_refresh_clicked(self):
        self._history_refresh_seconds_left = self._history_refresh_interval_sec
        self.reload_history_from_disk_and_refresh(force=True, check_sms=True)
        self._update_history_refresh_countdown_label()

    def refresh_history_table(self, maintain_page=False):
        # Aktuell selektierte ID merken, um Auswahl beizubehalten
        selected_id = None
        selected_items = self.history_table.selectedItems()
        if selected_items:
            row = selected_items[0].row()
            first_col_item = self.history_table.item(row, 0)
            if first_col_item:
                selected_id = first_col_item.data(Qt.ItemDataRole.UserRole)

        search_text = self.search_input.text()
        filtered_data = self.history_manager.get_filtered_history(search_text)

        total_items = len(filtered_data)
        max_page = max(0, (total_items - 1) // self.history_items_per_page)

        if not maintain_page or self.current_history_page > max_page:
            self.current_history_page = 0

        start_idx = self.current_history_page * self.history_items_per_page
        end_idx = start_idx + self.history_items_per_page
        page_data = filtered_data[start_idx:end_idx]

        self.history_table.blockSignals(True)
        self.history_table.setRowCount(0)
        for row_idx, item in enumerate(page_data):
            self.history_table.insertRow(row_idx)

            # Store ID in the first item's UserRole mapping for deletion
            raw_date = item.get("last_updated", "")
            formatted_date = ""
            if "T" in raw_date:
                try:
                    d_part, t_part = raw_date.split("T")
                    y, m, d = d_part.split("-")
                    formatted_date = f"{d}.{m}.{y} {t_part[:8]}"
                except Exception:
                    formatted_date = raw_date[:19].replace("T", " ")
            else:
                formatted_date = raw_date[:19].replace("T", " ")

            date_item = QTableWidgetItem(formatted_date)
            date_item.setToolTip(formatted_date)
            date_item.setData(Qt.ItemDataRole.UserRole, item.get("id"))
            # Store full item data for detail view
            date_item.setData(Qt.ItemDataRole.UserRole + 1, item)
            date_item.setFlags(date_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            # Name generieren
            first_name = item.get("first_name", "")
            last_name = item.get("last_name", "")
            if first_name or last_name:
                name_text = f"{first_name} {last_name}".strip()
            else:
                name_text = item.get("dir_name", "Unbekannt")

            name_item = QTableWidgetItem(name_text)
            name_item.setToolTip(name_text)
            name_item.setFlags(name_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            status_val = self.build_overall_status(item)
            status_item = QTableWidgetItem(status_val)
            status_item.setToolTip(status_val)
            status_item.setIcon(self.get_status_icon(status_val))
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            error_text = self.build_combined_error_text(item)
            err_item = QTableWidgetItem(error_text)
            err_item.setToolTip(error_text)
            err_item.setFlags(err_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            self.history_table.setItem(row_idx, 0, date_item)
            self.history_table.setItem(row_idx, 1, name_item)
            self.history_table.setItem(row_idx, 2, status_item)
            self.history_table.setItem(row_idx, 3, err_item)

        self.history_table.blockSignals(False)
        
        # Selektion wiederherstellen
        if selected_id:
            for row in range(self.history_table.rowCount()):
                item = self.history_table.item(row, 0)
                if item and item.data(Qt.ItemDataRole.UserRole) == selected_id:
                    self.history_table.selectRow(row)
                    break
        else:
            self.on_history_selection_changed()

        self.page_label.setText(f"Seite {self.current_history_page + 1} von {max_page + 1}")
        self.prev_page_btn.setEnabled(self.current_history_page > 0)
        self.next_page_btn.setEnabled(self.current_history_page < max_page)

    def build_combined_error_text(self, item_data):
        """Sammelt alle Fehlermeldungen (Upload/E-Mail/SMS) in einem Text."""
        errors = []

        upload_error = (item_data.get("error_msg") or "").strip()
        if upload_error:
            errors.append(f"Upload: {upload_error}")

        email_status = (item_data.get("email_status") or "").strip()
        if email_status and (
            "fehler" in email_status.lower()
            or "fehlgeschlagen" in email_status.lower()
            or "abgelehnt" in email_status.lower()
        ):
            errors.append(f"E-Mail: {email_status}")

        sms_status = (item_data.get("sms_status") or "").strip()
        if sms_status and (
            "fehler" in sms_status.lower()
            or "fehlgeschlagen" in sms_status.lower()
            or "abgelehnt" in sms_status.lower()
        ):
            errors.append(f"SMS: {sms_status}")

        return " | ".join(errors)

    def build_overall_status(self, item_data):
        """Erstellt den Gesamtstatus für das Main Grid."""
        return build_overall_status(item_data)

    def get_status_icon(self, status_text, context_key=""):
        """Erzeugt ein farbiges Kreis-Icon basierend auf dem Status-Text (mit Cache)."""
        color = "gray"
        lower_status = status_text.lower()
        lower_context = (context_key or "").lower()

        # E-Mail "Gesendet" gilt als erfolgreicher Endzustand.
        if "email" in lower_context and "gesendet" in lower_status:
            color = "green"
        elif "sms" in lower_context and "gesendet" in lower_status:
            # SMS "Gesendet" ist noch nicht zwingend zugestellt.
            color = "orange"
        elif "problem" in lower_status:
            color = "red"
        elif "in bearbeitung" in lower_status:
            color = "blue"
        elif "komplett" in lower_status:
            color = "#16A34A"
        elif "versendet" in lower_status:
            color = "#86EFAC"
        elif "erfolgreich" in lower_status:
            color = "green"
        elif "zugestellt" in lower_status:
            color = "green"
        elif "gesendet" in lower_status:
            color = "orange"
        elif "fehler" in lower_status or "fehlgeschlagen" in lower_status or "abgelehnt" in lower_status:
            color = "red"
        elif "gestartet" in lower_status or "übertragen" in lower_status or "gepuffert" in lower_status or "akzeptiert" in lower_status:
            color = "blue"
        elif "übersprungen" in lower_status:
            color = "gray"

        cache_key = f"{color}|{lower_context}"
        cached = self._status_icon_cache.get(cache_key)
        if cached is not None:
            return cached

        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setBrush(QColor(color))
        painter.setPen(Qt.GlobalColor.transparent)
        painter.drawEllipse(2, 2, 12, 12)
        painter.end()
        icon = QIcon(pixmap)
        self._status_icon_cache[cache_key] = icon
        return icon

    @Slot(int)
    def on_tab_changed(self, index):
        """Wird aufgerufen, wenn der Tab gewechselt wird."""
        if hasattr(self, 'history_tab') and hasattr(self, 'tabs') and self.tabs.widget(index) == self.history_tab:
            QTimer.singleShot(0, self._on_history_tab_activated)

    @Slot()
    def _on_history_tab_activated(self):
        """Verzögertes Laden: Tab wird zuerst gezeichnet, dann Historie aktualisiert."""
        if self._history_refresh_busy():
            return
        self.reload_history_from_disk_and_refresh(force=False, check_sms=True)

    @Slot()
    def on_history_selection_changed(self):
        """Aktualisiert das Detail Grid basierend auf der Auswahl im Main Grid."""
        selected_items = self.history_table.selectedItems()
        if not selected_items:
            self.detail_table.setRowCount(0)
            self._populate_contact_card(None)
            self._update_retry_upload_button_state()
            self._update_resend_notifications_button_state()
            return

        row = selected_items[0].row()
        date_item = self.history_table.item(row, 0)
        if not date_item:
            self.detail_table.setRowCount(0)
            self._populate_contact_card(None)
            self._update_retry_upload_button_state()
            self._update_resend_notifications_button_state()
            return

        item_data = date_item.data(Qt.ItemDataRole.UserRole + 1)
        if not item_data:
            self.detail_table.setRowCount(0)
            self._populate_contact_card(None)
            self._update_retry_upload_button_state()
            self._update_resend_notifications_button_state()
            return

        self.populate_detail_table(item_data)
        self._populate_contact_card(item_data)
        self._update_retry_upload_button_state(item_data)
        self._update_resend_notifications_button_state(item_data)

    def _update_retry_upload_button_state(self, item_data=None):
        if item_data is None:
            entry_id = self.get_selected_history_id()
            item_data = self.get_history_entry_by_id(entry_id) if entry_id else None
        can_retry = bool(
            item_data
            and (item_data.get("status") or "").strip() in RETRYABLE_STATUSES
        )
        self.retry_upload_btn.setEnabled(can_retry)

    @Slot()
    def on_retry_upload_clicked(self):
        entry_id = self.get_selected_history_id()
        entry = self.get_history_entry_by_id(entry_id)
        if not entry:
            return

        status = (entry.get("status") or "").strip()
        if status not in RETRYABLE_STATUSES:
            QMessageBox.warning(
                self,
                "Erneut hochladen",
                f"Status „{status}“ unterstützt keinen erneuten Upload.",
            )
            return

        active_client = self.get_active_cloud_client()
        if active_client.get_connection_status() != "Verbunden":
            QMessageBox.warning(
                self,
                "Erneut hochladen",
                "Keine Cloud-Verbindung. Bitte zuerst verbinden.",
            )
            return

        dir_name = entry.get("dir_name", "")
        error_hint = (entry.get("error_msg") or "").strip()
        confirm_lines = [f"Upload für „{dir_name}“ erneut starten?"]
        if error_hint:
            confirm_lines.append("")
            confirm_lines.append(f"Letzter Fehler: {error_hint}")

        reply = QMessageBox.question(
            self,
            "Erneut hochladen",
            "\n".join(confirm_lines),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            message = retry_upload_from_history(
                self.config,
                entry,
                self.upload_queue,
                self.upload_registry,
                self.log,
            )
        except ValueError as exc:
            QMessageBox.warning(self, "Erneut hochladen", str(exc))
            return
        except Exception as exc:
            self.log.exception("Retry-Upload fehlgeschlagen: %s", exc)
            QMessageBox.critical(
                self,
                "Erneut hochladen",
                f"Upload konnte nicht erneut gestartet werden:\n{exc}",
            )
            return

        QMessageBox.information(self, "Erneut hochladen", message)
        self.refresh_history_table(maintain_page=True)
        self._update_retry_upload_button_state()

    def _populate_contact_card(self, item_data):
        """Befüllt die Kontakt-Karte aus einem Historieneintrag."""
        self._contact_populating = True
        try:
            if not item_data:
                self.contact_email_input.clear()
                self.contact_phone_input.clear()
                self.contact_email_status_label.setText("—")
                self.contact_phone_status_label.setText("—")
                self.history_contact_group.setEnabled(False)
                self._contact_dirty = False
                self.contact_save_btn.setEnabled(False)
                return

            self.history_contact_group.setEnabled(True)
            self.contact_email_input.setText((item_data.get("email") or "").strip())
            self.contact_phone_input.setText((item_data.get("phone") or "").strip())

            self._apply_contact_status_labels(
                item_data.get("email_status", ""),
                item_data.get("sms_status", ""),
            )
            self._contact_dirty = False
            self.contact_save_btn.setEnabled(False)
        finally:
            self._contact_populating = False

    def _on_contact_fields_changed(self):
        if self._contact_populating:
            return
        self._contact_dirty = True
        self.contact_save_btn.setEnabled(True)

    def _get_contact_values_from_ui(self) -> tuple[str, str | None]:
        email = self.contact_email_input.text().strip()
        phone = normalize_phone(self.contact_phone_input.text())
        return email, phone

    def _validate_contact_values(self, email: str, phone: str | None, require_email: bool, require_phone: bool):
        if require_email:
            if not email:
                raise ValueError("E-Mail-Adresse fehlt.")
            if not is_valid_email(email):
                raise ValueError("E-Mail-Adresse ist ungültig.")
        elif email and not is_valid_email(email):
            raise ValueError("E-Mail-Adresse ist ungültig.")
        if require_phone and not phone:
            raise ValueError("Telefonnummer fehlt.")

    def on_contact_save_clicked(self):
        entry_id = self.get_selected_history_id()
        entry = self.get_history_entry_by_id(entry_id)
        if not entry:
            return

        email, phone = self._get_contact_values_from_ui()
        try:
            self._validate_contact_values(email, phone, require_email=False, require_phone=False)
        except ValueError as exc:
            QMessageBox.warning(self, "Kontakt speichern", str(exc))
            return

        payload = build_contact_update_payload(entry, email, phone)
        self.history_manager.add_or_update(payload)
        self._sync_history_loaded_mtime()
        if phone:
            self.contact_phone_input.setText(phone)
        self._contact_dirty = False
        self.contact_save_btn.setEnabled(False)

        refreshed = self.get_history_entry_by_id(entry_id)
        if refreshed:
            self.populate_detail_table(refreshed)
            self._populate_contact_card(refreshed)
        self.refresh_history_table(maintain_page=True)
        self.status_label.setText("Kontaktdaten gespeichert.")

    def _update_resend_notifications_button_state(self, item_data=None):
        if item_data is None:
            entry_id = self.get_selected_history_id()
            item_data = self.get_history_entry_by_id(entry_id) if entry_id else None
        can_resend = bool(item_data and can_resend_notifications(item_data))
        self.resend_notifications_btn.setEnabled(can_resend)

    def _resend_worker_busy(self) -> bool:
        thread = getattr(self, "_resend_worker_thread", None)
        if thread is None:
            return False
        try:
            return thread.isRunning()
        except RuntimeError:
            self._resend_worker_thread = None
            self._resend_worker = None
            return False

    @Slot()
    def _on_resend_worker_thread_destroyed(self):
        try:
            dead = self.sender()
        except RuntimeError:
            dead = None
        if dead is not None and dead is getattr(self, "_resend_worker_thread", None):
            self._resend_worker_thread = None
            self._resend_worker = None

    def _contact_status_label_style(self, status: str) -> str:
        s = (status or "").strip().lower()
        if "fehler" in s or "fehlgeschlagen" in s or "abgelehnt" in s:
            return "color: #dc2626; font-size: 11px; font-weight: 500;"
        if "gesendet" in s or "zugestellt" in s or "erfolgreich" in s:
            return "color: #16a34a; font-size: 11px; font-weight: 500;"
        return "color: gray; font-size: 11px;"

    def _apply_contact_status_labels(self, email_status: str, sms_status: str):
        email_text = (email_status or "").strip() or "—"
        sms_text = (sms_status or "").strip() or "—"
        self.contact_email_status_label.setText(f"Status: {email_text}")
        self.contact_email_status_label.setStyleSheet(self._contact_status_label_style(email_text))
        self.contact_phone_status_label.setText(f"Status: {sms_text}")
        self.contact_phone_status_label.setStyleSheet(self._contact_status_label_style(sms_text))

    def _set_resend_ui_busy(self, busy: bool):
        if busy:
            self._begin_history_refresh_overlay()
            self.resend_notifications_btn.setText("Wird gesendet…")
            self.resend_notifications_btn.setEnabled(False)
            self.history_contact_group.setEnabled(False)
            self.contact_save_btn.setEnabled(False)
            self.status_label.setText("Benachrichtigung wird gesendet…")
            return

        self._end_history_refresh_overlay()
        self.resend_notifications_btn.setText(self._resend_btn_default_text)
        self.history_contact_group.setEnabled(True)
        self._update_resend_notifications_button_state()

    @Slot()
    def on_resend_notifications_clicked(self):
        entry_id = self.get_selected_history_id()
        entry = self.get_history_entry_by_id(entry_id)
        if not entry:
            return
        if not can_resend_notifications(entry):
            QMessageBox.warning(
                self,
                "Benachrichtigung erneut senden",
                "Nur erfolgreiche Uploads unterstützen einen erneuten Versand.",
            )
            return
        if self._resend_worker_busy():
            QMessageBox.information(self, "Benachrichtigung erneut senden", "Ein Versand läuft bereits.")
            return

        email, phone = self._get_contact_values_from_ui()
        initial_link = (entry.get("share_link") or "").strip()
        cloud_client = self.get_active_cloud_client()

        dialog = ResendNotificationsDialog(
            entry,
            email,
            phone,
            initial_link,
            cloud_client,
            self.config,
            self,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        send_email = dialog.get_send_email()
        send_sms = dialog.get_send_sms()
        share_link = dialog.get_share_link()

        try:
            email, phone = normalize_contact(email, phone)
            validate_contact_for_channels(email, phone, send_email, send_sms)
            if not is_valid_share_link(share_link):
                raise ValueError("Bitte einen gültigen Download-Link eingeben.")
        except ValueError as exc:
            QMessageBox.warning(self, "Benachrichtigung erneut senden", str(exc))
            return

        delivered = channels_already_delivered(entry, send_email, send_sms)
        if delivered:
            parts = []
            if "email" in delivered:
                parts.append("E-Mail wurde bereits als gesendet markiert")
            if "sms" in delivered:
                parts.append("SMS wurde bereits als zugestellt markiert")
            reply = QMessageBox.question(
                self,
                "Benachrichtigung erneut senden",
                f"{'. '.join(parts)}.\n\nTrotzdem erneut senden?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        self._persist_contact_before_resend(entry, email, phone, share_link)
        entry = self.get_history_entry_by_id(entry_id) or entry

        self._set_resend_ui_busy(True)

        self._resend_worker_thread = QThread()
        self._resend_worker = ResendNotificationsWorker(
            dict(entry),
            email,
            phone,
            share_link,
            send_email,
            send_sms,
            self.email_client,
            self.sms_client,
            self.config,
        )
        self._resend_worker.moveToThread(self._resend_worker_thread)
        self._resend_worker_thread.destroyed.connect(self._on_resend_worker_thread_destroyed)
        self._resend_worker_thread.started.connect(self._resend_worker.run)
        self._resend_worker.finished.connect(self._on_resend_notifications_finished)
        self._resend_worker.failed.connect(self._on_resend_notifications_failed)
        self._resend_worker.finished.connect(self._resend_worker_thread.quit)
        self._resend_worker.failed.connect(self._resend_worker_thread.quit)
        self._resend_worker.finished.connect(self._resend_worker.deleteLater)
        self._resend_worker.failed.connect(self._resend_worker.deleteLater)
        self._resend_worker_thread.finished.connect(self._resend_worker_thread.deleteLater)
        self._resend_worker_thread.start()

    def _persist_contact_before_resend(self, entry, email, phone, share_link):
        payload = build_contact_update_payload(entry, email, phone)
        payload["share_link"] = share_link
        self.history_manager.add_or_update(payload)
        self._sync_history_loaded_mtime()
        entry.update(payload)
        self._contact_dirty = False
        self.contact_save_btn.setEnabled(False)
        if phone:
            self.contact_phone_input.setText(phone)

    @Slot(object)
    def _on_resend_notifications_finished(self, result):
        try:
            self.history_manager.add_or_update(result.history_updates)
            self._sync_history_loaded_mtime()
            signals.upload_history_update.emit(dict(result.history_updates))
            self.check_sms_status()
            self.refresh_history_table(maintain_page=True)

            entry_id = self.get_selected_history_id()
            refreshed = self.get_history_entry_by_id(entry_id)
            if refreshed:
                self.populate_detail_table(refreshed)
                self._populate_contact_card(refreshed)
                self._apply_contact_status_labels(
                    refreshed.get("email_status", ""),
                    refreshed.get("sms_status", ""),
                )

            message = format_resend_result_message(result)
            self.log.info("Resend abgeschlossen: %s", message.replace("\n", " | "))

            if resend_had_failures(result):
                self.status_label.setText("Benachrichtigung teilweise fehlgeschlagen.")
                QMessageBox.warning(self, "Benachrichtigung erneut senden", message)
            else:
                self.status_label.setText("Benachrichtigung erfolgreich versendet.")
                QMessageBox.information(self, "Benachrichtigung erneut senden", message)
        finally:
            self._set_resend_ui_busy(False)

    @Slot(str)
    def _on_resend_notifications_failed(self, message):
        try:
            self.log.error("Resend fehlgeschlagen: %s", message)
            self.status_label.setText("Benachrichtigung fehlgeschlagen.")
            QMessageBox.warning(self, "Benachrichtigung erneut senden", message)
        finally:
            self._set_resend_ui_busy(False)

    def _start_share_link_migration(self):
        if self._share_link_migration_thread is not None:
            try:
                if self._share_link_migration_thread.isRunning():
                    return
            except RuntimeError:
                self._share_link_migration_thread = None

        cloud_client = self.get_active_cloud_client()
        if cloud_client.get_connection_status() != "Verbunden":
            return

        needs_migration = any(
            (item.get("status") or "").strip() == "Erfolgreich"
            and not (item.get("share_link") or "").strip()
            for item in self.history_manager.history
        )
        if not needs_migration:
            return

        self._share_link_migration_thread = QThread()
        worker = ShareLinkMigrationWorker(self.history_manager, cloud_client)
        worker.moveToThread(self._share_link_migration_thread)
        self._share_link_migration_thread.started.connect(worker.run)
        worker.finished.connect(self._on_share_link_migration_finished)
        worker.finished.connect(self._share_link_migration_thread.quit)
        worker.finished.connect(worker.deleteLater)
        self._share_link_migration_thread.finished.connect(self._share_link_migration_thread.deleteLater)
        self._share_link_migration_thread.start()

    @Slot(int)
    def _on_share_link_migration_finished(self, count):
        if count:
            self.log.info("%s Download-Link(s) für Alteinträge nachgeladen.", count)
            self._sync_history_loaded_mtime()
            self.refresh_history_table(maintain_page=True)
            entry_id = self.get_selected_history_id()
            refreshed = self.get_history_entry_by_id(entry_id)
            if refreshed:
                self.populate_detail_table(refreshed)
                self._populate_contact_card(refreshed)
            self.status_label.setText(f"{count} Download-Link(s) für Alteinträge nachgeladen.")

    def populate_detail_table(self, item_data):
        """Befüllt das Detail Grid aus einem Historien-Eintrag."""
        self.detail_table.setRowCount(0)

        details = []
        details.append(("Verzeichnis", item_data.get("dir_name", "")))
        details.append(("Upload-Status", item_data.get("status", "")))
        details.append(("Download-Link", item_data.get("share_link", "") or "—"))
        details.append(("Wiederversände", format_resend_history_summary(item_data)))

        sms_price = item_data.get("sms_price", "")
        price_text = f"{sms_price}€" if sms_price else ""
        details.append(("SMS Kosten", price_text))
        details.append(("Fehlertext", self.build_combined_error_text(item_data)))

        resend_log = item_data.get("resend_log") or []
        for idx, log_entry in enumerate(resend_log[:5]):
            at_raw = (log_entry.get("at") or "").strip()
            at_display = at_raw.replace("T", " ")[:16] if at_raw else "—"
            channels = ", ".join(log_entry.get("channels") or [])
            email_log = (log_entry.get("email") or "").strip()
            email_st = log_entry.get("email_status") or "—"
            sms_st = log_entry.get("sms_status") or "—"
            if channels == "email":
                summary = f"{at_display} — E-Mail an {email_log} → {email_st}"
            elif channels == "sms":
                summary = f"{at_display} — SMS → {sms_st}"
            else:
                summary = f"{at_display} — {channels} → E-Mail: {email_st}, SMS: {sms_st}"
            details.append((f"Resend {idx + 1}", summary))

        self.detail_table.setRowCount(len(details))
        for i, (key, value) in enumerate(details):
            key_item = QTableWidgetItem(key)
            val_item = QTableWidgetItem(value)
            key_item.setFlags(key_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            val_item.setFlags(val_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            val_item.setToolTip(value)

            if "Status" in key:
                val_item.setIcon(self.get_status_icon(value, key))

            self.detail_table.setItem(i, 0, key_item)
            self.detail_table.setItem(i, 1, val_item)

    def get_selected_history_id(self):
        """Liest die ID des aktuell selektierten Historien-Eintrags aus."""
        selected_items = self.history_table.selectedItems()
        if not selected_items:
            return None
        selected_row = selected_items[0].row()
        selected_date_item = self.history_table.item(selected_row, 0)
        if not selected_date_item:
            return None
        return selected_date_item.data(Qt.ItemDataRole.UserRole)

    def get_history_entry_by_id(self, entry_id):
        """Liefert den Historien-Eintrag zu einer ID."""
        if not entry_id:
            return None
        for entry in self.history_manager.history:
            if entry.get("id") == entry_id:
                return entry
        return None

    @Slot(QTableWidgetItem)
    def on_history_cell_clicked(self, item):
        """Zeigt den kompletten Zelltext in einem kopierbaren, nicht editierbaren Dialog."""
        if not item:
            return

        full_text = item.text() or ""
        if not full_text:
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Zellinhalt")
        dialog.resize(700, 280)

        layout = QVBoxLayout(dialog)
        text_view = QTextEdit(dialog)
        text_view.setReadOnly(True)
        text_view.setPlainText(full_text)
        text_view.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse |
            Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        layout.addWidget(text_view)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=dialog)
        buttons.rejected.connect(dialog.reject)
        buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)

        dialog.exec()

    @Slot()
    def on_search_changed(self):
        self.current_history_page = 0
        self.refresh_history_table()

    @Slot()
    def prev_history_page(self):
        if self.current_history_page > 0:
            self.current_history_page -= 1
            self.refresh_history_table(maintain_page=True)

    @Slot()
    def next_history_page(self):
        self.current_history_page += 1
        self.refresh_history_table(maintain_page=True)

    @Slot()
    def delete_selected_history(self):
        selected_rows = set(item.row() for item in self.history_table.selectedItems())
        if not selected_rows:
            return

        reply = QMessageBox.question(self, "Löschen bestätigen", f"Möchten Sie {len(selected_rows)} Eintrag/Einträge löschen?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            ids_to_delete = []
            for row in selected_rows:
                item = self.history_table.item(row, 0)
                if item:
                    item_id = item.data(Qt.ItemDataRole.UserRole)
                    if item_id:
                        ids_to_delete.append(item_id)
            self.history_manager.delete_items(ids_to_delete)
            self._sync_history_loaded_mtime()
            self.refresh_history_table(maintain_page=True)

    @Slot()
    def delete_all_history(self):
        reply = QMessageBox.question(self, "Löschen bestätigen", "Möchten Sie wirklich die gesamte Historie löschen?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self.history_manager.clear_all()
            self._sync_history_loaded_mtime()
            self.refresh_history_table()

    def check_sms_status(self):
        """Startet die asynchrone Abfrage des SMS-Status via Seven.io API (kein paralleler Lauf).

        Returns:
            True, wenn ein Worker gestartet wurde; False, wenn bereits einer läuft.
        """
        if self._sms_status_worker_busy():
            return False

        # Starte den Worker-Thread
        self._sms_worker_thread = QThread()
        self._sms_worker_thread.destroyed.connect(self._on_sms_worker_thread_destroyed)
        self._sms_worker = SmsStatusWorker(self.sms_client, self.history_manager)
        self._sms_worker.moveToThread(self._sms_worker_thread)

        self._sms_worker_thread.started.connect(self._sms_worker.run)
        self._sms_worker.finished.connect(self._sms_worker_thread.quit)
        self._sms_worker.finished.connect(self._sms_worker.deleteLater)
        self._sms_worker_thread.finished.connect(self._sms_worker_thread.deleteLater)
        self._sms_worker.finished.connect(self.on_sms_status_checked)
        self._sms_worker.item_updated.connect(self.on_history_update)

        self._sms_worker_thread.start()
        return True

    @Slot()
    def on_sms_status_checked(self):
        """Wird aufgerufen, wenn die SMS-Status-Prüfung abgeschlossen ist."""
        self._sync_history_loaded_mtime()
        self.refresh_history_table(maintain_page=True)
        self._end_history_refresh_overlay()
        self.status_label.setText("Historie aktualisiert.")

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

        if self.config.get_setting("selected_cloud_service", "dropbox") == "custom_api":
            self._auto_connect_dropbox_for_pure_contact_markers()

        # Die Threads lesen die Konfiguration bei Bedarf neu.
        # Wir müssen den Monitor-Thread aufwecken, falls er auf das Intervall wartet,
        # damit er den neuen Pfad oder das Intervall sofort übernimmt.
        if self.monitor_thread:
            self.monitor_thread.wake_up()

        new_log_path = os.path.normpath(self.config.get_setting("log_file_path", ".") or ".")
        if new_log_path != self._last_log_file_path:
            self.log.warning("Log-Pfad-Änderungen erfordern einen Neustart.")
            self._last_log_file_path = new_log_path

    @Slot()
    def open_settings(self):
        """Öffnet den Einstellungsdialog."""
        self.log.debug("Öffne Einstellungsdialog...")

        dialog = SettingsDialog(
            self.config,
            self.db_client,
            APP_VERSION,
            self.latest_version_info,
            self,
            custom_api_client=self.custom_api_client,
        )

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

    @staticmethod
    def _format_queue_wait_seconds(seconds: float) -> str:
        total = int(seconds)
        if total < 60:
            return f"{total} Sek."
        minutes, secs = divmod(total, 60)
        if minutes < 60:
            return f"{minutes} Min." if secs == 0 else f"{minutes} Min. {secs} Sek."
        hours, rem = divmod(minutes, 60)
        return f"{hours} Std. {rem} Min."

    @staticmethod
    def _format_queue_status(entry: dict) -> str:
        state = entry.get("state", "waiting")
        wait = MainWindow._format_queue_wait_seconds(entry.get("wait_seconds", 0))
        if state == "active":
            return f"Aktiv · {wait}"
        return f"Wartend · {wait}"

    @Slot(object)
    def _refresh_upload_queue_table(self, entries):
        if entries is None:
            entries = []
        active_count = sum(1 for e in entries if e.get("state") == "active")
        waiting_count = len(entries) - active_count
        if not entries:
            tab_title = "Warteschlange"
            summary = "Keine ausstehenden Uploads."
        elif active_count and waiting_count:
            tab_title = f"Warteschlange ({len(entries)})"
            summary = f"{active_count} aktiv, {waiting_count} wartend"
        elif active_count:
            tab_title = f"Warteschlange ({len(entries)})"
            summary = f"{active_count} aktiv"
        else:
            tab_title = f"Warteschlange ({len(entries)})"
            summary = f"{waiting_count} wartend"
        self.tabs.setTabText(self._queue_tab_index, tab_title)
        self.upload_queue_summary_label.setText(summary)

        self.upload_queue_table.setRowCount(0)
        self.upload_queue_table.clearSpans()
        if not entries:
            return

        bold_font = QFont()
        bold_font.setBold(True)
        for row, entry in enumerate(entries):
            self.upload_queue_table.insertRow(row)
            is_active = entry.get("state") == "active"
            cells = [
                str(entry.get("position", row + 1)),
                entry.get("dir_name", ""),
                entry.get("customer_label", "—"),
                self._format_queue_status(entry),
            ]
            for col, text in enumerate(cells):
                item = QTableWidgetItem(text)
                if is_active:
                    item.setFont(bold_font)
                self.upload_queue_table.setItem(row, col, item)

    @Slot(bool)
    def _on_upload_job_active_changed(self, active: bool):
        self.upload_cancel_btn.setEnabled(active)
        self._upload_toggle_shows_resume = False
        self.upload_pause_toggle_btn.setText("Pause")
        self.upload_pause_toggle_btn.setEnabled(active)

    @Slot()
    def _on_upload_pause_toggle_clicked(self):
        if not self._upload_toggle_shows_resume:
            self.uploader_thread.request_upload_pause()
            self._upload_toggle_shows_resume = True
            self.upload_pause_toggle_btn.setText("Weiter")
            self.status_label.setText("Upload pausiert …")
        else:
            self.uploader_thread.request_upload_resume()
            self._upload_toggle_shows_resume = False
            self.upload_pause_toggle_btn.setText("Pause")
            self.status_label.setText("Upload wird fortgesetzt …")

    @Slot()
    def _on_upload_cancel_clicked(self):
        self.uploader_thread.request_upload_cancel()
        self.upload_pause_toggle_btn.setEnabled(False)
        self.upload_cancel_btn.setEnabled(False)

    @Slot(int, object, object)
    def update_file_progress(self, percent, current_bytes, total_bytes):
        """Aktualisiert den Fortschritt der einzelnen Datei (Bar + Text)."""
        current_bytes = int(current_bytes or 0)
        total_bytes = int(total_bytes or 0)
        self.file_progress_bar.setValue(percent)
        text = "0% (0.0 MB / 0.0 MB)"
        if total_bytes > 0 or current_bytes > 0:  # Verhindert "0.0 MB / 0.0 MB" bei Start
            text = f"{percent}% ({self.format_bytes(current_bytes)} / {self.format_bytes(total_bytes)})"
        self.file_progress_label.setText(text)

    @Slot(int, object, object)
    def update_total_progress(self, percent, current_bytes, total_bytes):
        """Aktualisiert den Gesamtfortschritt (Bar + Text)."""
        current_bytes = int(current_bytes or 0)
        total_bytes = int(total_bytes or 0)
        self.total_progress_bar.setValue(percent)
        text = "0% (0.0 MB / 0.0 MB)"
        if total_bytes > 0 or current_bytes > 0:  # Verhindert "0.0 MB / 0.0 MB" bei Start
            text = f"{percent}% ({self.format_bytes(current_bytes)} / {self.format_bytes(total_bytes)})"
        self.total_progress_label.setText(text)

    # --- Thread-Management ---

    def start_monitoring(self, *, skip_status_check=False):
        """Startet den Monitor-Thread, falls er nicht bereits läuft."""
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.log.warning("Monitor-Thread läuft bereits.")
            return

        if not self.config.get_setting("monitor_path"):
            self.log.error("Monitoring nicht gestartet: Kein Überwachungsordner konfiguriert.")
            self.update_monitoring_status(False)
            return

        # Verwende den aktuell aktiven Cloud-Client
        active_client = self.get_active_cloud_client()
        if not skip_status_check and active_client.get_connection_status() != "Verbunden":
            self.log.error("Monitoring nicht gestartet: Keine Cloud-Verbindung.")
            self.update_monitoring_status(False)
            return

        if not self._upload_recovery_done:
            recover_stalled_upload_folders(
                self.config, self.upload_queue, self.upload_registry, self.log
            )
            self._upload_recovery_done = True

        self.log.info("Starte Monitor-Thread...")
        self.monitor_thread = MonitorThread(
            self.config, self.upload_queue, self.upload_registry
        )
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

        if getattr(self, "_history_debounce_timer", None) is not None:
            self._history_debounce_timer.stop()
        if getattr(self, "_history_pending_by_dir", None) and self._history_pending_by_dir:
            self._flush_debounced_history_updates()

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


import asyncio
from datetime import datetime

class SmsStatusWorker(QObject):
    """Worker-Klasse für die asynchrone Abfrage des SMS-Status."""
    finished = Signal()
    item_updated = Signal(dict)

    def __init__(self, sms_client: SmsClient, history_manager: HistoryManager):
        super().__init__()
        self.sms_client = sms_client
        self.history_manager = history_manager

    def run(self):
        """Führt die API-Abfrage aus und aktualisiert die Historie."""
        try:
            journal_data = asyncio.run(self.sms_client.get_sms_journal(limit=200))
            entries = []
            if isinstance(journal_data, list):
                entries = journal_data
            elif isinstance(journal_data, dict):
                for key in ("messages", "items", "data", "entries"):
                    candidate = journal_data.get(key)
                    if isinstance(candidate, list):
                        entries = candidate
                        break

            if entries:
                updated_items = update_history_from_journal(self.history_manager.history, entries)
                if updated_items:
                    self.history_manager.save_history()
                    for item in updated_items:
                        self.item_updated.emit(dict(item))
        except Exception as e:
            logging.getLogger(__name__).error(f"Fehler bei der SMS-Status-Prüfung im Worker: {e}")
        finally:
            self.finished.emit()

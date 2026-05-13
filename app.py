import sys
import logging
import queue

from PySide6.QtGui import QIcon, QPainter, QColor, QPixmap, QPalette, QPen
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QProgressBar, QLabel, QStatusBar,
    QMessageBox, QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView, QLineEdit, QAbstractItemView, QSplitter,
    QDialog, QDialogButtonBox
)

from PySide6.QtCore import QCoreApplication, QProcess, QThread, QTimer, QRectF, Slot, Qt, Signal

# Importiere alle Komponenten des Projekts
from core import APP_VERSION
from core.config import ConfigManager
from core.logger import setup_logging
from core.signals import signals
from core.monitor import MonitorThread, recover_stalled_upload_folders
from core.uploader import UploaderThread
from services.dropbox_client import DropboxClient
from services.custom_api_client import CustomApiClient
from services.email_client import EmailClient
from services.sms_client import SmsClient
from settings import SettingsDialog
from utils.constants import ICON_PATH
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


class HistoryRefreshOverlay(QWidget):
    """Dezente halbtransparente Schicht mit zentriertem Lade-Indikator über der Historien-Tabelle."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._angle = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._advance_angle)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)

    def _advance_angle(self):
        self._angle = (self._angle + 11) % 360
        self.update()

    def show_loading(self):
        self._angle = 0
        self._timer.start(40)
        self.show()
        self.raise_()

    def hide_loading(self):
        self._timer.stop()
        self.hide()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        scrim = QColor(self.palette().color(QPalette.ColorRole.Window))
        scrim.setAlpha(120)
        painter.fillRect(self.rect(), scrim)

        cx = self.width() / 2.0
        cy = self.height() / 2.0
        card = QRectF(cx - 40, cy - 40, 80, 80)
        base = QColor(self.palette().color(QPalette.ColorRole.Base))
        base.setAlpha(242)
        painter.setBrush(base)
        painter.setPen(QPen(self.palette().color(QPalette.ColorRole.Mid), 1))
        painter.drawRoundedRect(card, 14, 14)

        ring = QRectF(cx - 22, cy - 22, 44, 44)
        track = QPen(self.palette().color(QPalette.ColorRole.Mid))
        track.setWidth(5)
        track.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(track)
        painter.drawArc(ring, 0, 360 * 16)

        accent = QPen(self.palette().color(QPalette.ColorRole.Highlight))
        accent.setWidth(5)
        accent.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(accent)
        painter.drawArc(ring, self._angle * 16, 280 * 16)


class _HistoryTableHost(QWidget):
    """Stackt die Tabelle und das Lade-Overlay gleich groß übereinander."""

    def __init__(self, table: QTableWidget):
        super().__init__()
        self._table = table
        self._overlay = HistoryRefreshOverlay(self)
        self._table.setParent(self)
        self._overlay.hide()

    @property
    def overlay(self) -> HistoryRefreshOverlay:
        return self._overlay

    def resizeEvent(self, event):
        super().resizeEvent(event)
        r = self.rect()
        self._table.setGeometry(r)
        self._overlay.setGeometry(r)


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

        self.history_manager = HistoryManager()
        self._upload_recovery_done = False
        self._history_pending_by_dir = {}
        self._history_debounce_timer = QTimer(self)
        self._history_debounce_timer.setSingleShot(True)
        self._history_debounce_timer.setInterval(400)
        self._history_debounce_timer.timeout.connect(self._flush_debounced_history_updates)

        self.current_history_page = 0
        self.history_items_per_page = 25
        self._history_refresh_overlay_refs = 0

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

        self.settings_button = QPushButton("Einstellungen")
        self.settings_button.clicked.connect(self.open_settings)

        self.restart_button = QPushButton("Neustart")
        self.restart_button.clicked.connect(self.restart_app)

        button_layout.addWidget(self.monitor_button)
        button_layout.addWidget(self.status_light)
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

        # --- Tab 2: Upload-Historie ---
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

        self.delete_selected_btn = QPushButton("Ausgewählte löschen")
        self.delete_selected_btn.clicked.connect(self.delete_selected_history)
        hist_top_layout.addWidget(self.delete_selected_btn)

        self.delete_all_btn = QPushButton("Alle löschen")
        self.delete_all_btn.clicked.connect(self.delete_all_history)
        hist_top_layout.addWidget(self.delete_all_btn)

        history_layout.addLayout(hist_top_layout)

        # Splitter für Main und Detail Grid
        self.history_splitter = QSplitter(Qt.Orientation.Vertical)

        # Tabelle (Main Grid)
        self.history_table = QTableWidget()
        self.history_table.setColumnCount(4)
        self.history_table.setHorizontalHeaderLabels(["Datum", "Name", "Status", "Fehler"])
        self.history_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.history_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.history_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.history_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.history_table.itemSelectionChanged.connect(self.on_history_selection_changed)
        self.history_table.itemDoubleClicked.connect(self.on_history_cell_clicked)
        self._history_table_host = _HistoryTableHost(self.history_table)
        self._history_refresh_overlay = self._history_table_host.overlay
        self.history_splitter.addWidget(self._history_table_host)

        # Detail Grid
        self.detail_table = QTableWidget()
        self.detail_table.setColumnCount(2)
        self.detail_table.setHorizontalHeaderLabels(["Eigenschaft", "Wert"])
        self.detail_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.detail_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.detail_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.detail_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.history_splitter.addWidget(self.detail_table)

        history_layout.addWidget(self.history_splitter)

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
        self.refresh_history_table()

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
        # Auto-Scroll
        self.log_display.verticalScrollBar().setValue(self.log_display.verticalScrollBar().maximum())

    @Slot(dict)
    def on_history_update(self, data):
        """Wird aufgerufen, wenn ein Upload aktualisiert wird."""
        updated_dir = data.get("dir_name")
        if not updated_dir:
            selected_id = self.get_selected_history_id()
            self.history_manager.add_or_update(data)
            self.refresh_history_table(maintain_page=True)
            self._refresh_history_detail_if_needed(data, selected_id)
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
        self.refresh_history_table(maintain_page=True)
        if selected_id:
            for merged in batch.values():
                self._refresh_history_detail_if_needed(merged, selected_id)

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

    def reload_history_from_disk_and_refresh(self):
        """Liest upload_history.json neu ein, zeichnet die Tabelle neu, startet SMS-Status-Prüfung."""
        self._begin_history_refresh_overlay()
        sms_started = False
        try:
            self.history_manager.reload_from_file()
            self.refresh_history_table(maintain_page=True)
            sms_started = self.check_sms_status()
        except Exception:
            self.log.exception("Historie-Refresh fehlgeschlagen")
            self._end_history_refresh_overlay()
            return
        if not sms_started:
            QTimer.singleShot(320, self._end_history_refresh_overlay)

    def _update_history_refresh_countdown_label(self):
        if hasattr(self, "history_refresh_countdown_label"):
            self.history_refresh_countdown_label.setText(
                f"Nächstes Laden in {self._history_refresh_seconds_left} s"
            )

    @Slot()
    def _on_history_refresh_timer_tick(self):
        self._history_refresh_seconds_left -= 1
        if self._history_refresh_seconds_left <= 0:
            self.reload_history_from_disk_and_refresh()
            self._history_refresh_seconds_left = self._history_refresh_interval_sec
        self._update_history_refresh_countdown_label()

    @Slot()
    def on_history_manual_refresh_clicked(self):
        self._history_refresh_seconds_left = self._history_refresh_interval_sec
        self.reload_history_from_disk_and_refresh()
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
        upload_status = (item_data.get("status") or "").strip()
        email_status = (item_data.get("email_status") or "").strip()
        sms_status = (item_data.get("sms_status") or "").strip()
        email_value = (item_data.get("email") or "").strip()
        phone_value = (item_data.get("phone") or "").strip()

        def is_problem(status_value):
            s = (status_value or "").strip().lower()
            if not s:
                return False
            return ("fehler" in s) or ("fehlgeschlagen" in s) or ("abgelehnt" in s)

        def is_in_progress(status_value):
            s = (status_value or "").strip().lower()
            if not s:
                return False
            return ("gestartet" in s) or ("übertragen" in s) or ("gepuffert" in s) or ("akzeptiert" in s)

        def is_best_upload(status_value):
            s = (status_value or "").strip().lower()
            return "erfolgreich" in s

        def is_best_email(status_value):
            s = (status_value or "").strip().lower()
            # E-Mail kennt i.d.R. keinen "zugestellt"-Rückkanal.
            return ("gesendet" in s) or ("zugestellt" in s) or ("erfolgreich" in s)

        def is_best_sms(status_value):
            s = (status_value or "").strip().lower()
            # Für SMS gilt erst "zugestellt" als bester Endzustand.
            return ("zugestellt" in s) or ("erfolgreich" in s)

        upload_problem = is_problem(upload_status)
        email_problem = bool(email_value) and is_problem(email_status)
        sms_problem = bool(phone_value) and is_problem(sms_status)
        has_problem = upload_problem or email_problem or sms_problem
        if has_problem:
            return "Problem"

        sms_sent_waiting_delivery = bool(phone_value) and ("gesendet" in sms_status.lower()) and not is_best_sms(sms_status)
        if sms_sent_waiting_delivery:
            return "In Bearbeitung"

        if any(is_in_progress(s) for s in (upload_status, email_status, sms_status)):
            return "In Bearbeitung"

        upload_is_best = is_best_upload(upload_status)
        email_is_best = (not email_value) or is_best_email(email_status)
        sms_is_best = (not phone_value) or is_best_sms(sms_status)
        if upload_is_best and email_is_best and sms_is_best:
            return "Erfolgreich"

        # Abgeschlossen, aber nicht auf Bestwert in allen Unter-Status.
        if upload_status or email_status or sms_status:
            return "Teilweise"

        return "Unbekannt"

    def get_status_icon(self, status_text, context_key=""):
        """Erzeugt ein farbiges Kreis-Icon basierend auf dem Status-Text."""
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

        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setBrush(QColor(color))
        painter.setPen(Qt.GlobalColor.transparent)
        painter.drawEllipse(2, 2, 12, 12)
        painter.end()
        return QIcon(pixmap)

    @Slot(int)
    def on_tab_changed(self, index):
        """Wird aufgerufen, wenn der Tab gewechselt wird."""
        if hasattr(self, 'history_tab') and hasattr(self, 'tabs') and self.tabs.widget(index) == self.history_tab:
            # Historie von Platte + Tabelle + SMS-Status wie beim periodischen Refresh
            self.reload_history_from_disk_and_refresh()

    @Slot()
    def on_history_selection_changed(self):
        """Aktualisiert das Detail Grid basierend auf der Auswahl im Main Grid."""
        selected_items = self.history_table.selectedItems()
        if not selected_items:
            self.detail_table.setRowCount(0)
            return

        row = selected_items[0].row()
        date_item = self.history_table.item(row, 0)
        if not date_item:
            self.detail_table.setRowCount(0)
            return

        item_data = date_item.data(Qt.ItemDataRole.UserRole + 1)
        if not item_data:
            self.detail_table.setRowCount(0)
            return

        self.populate_detail_table(item_data)

    def populate_detail_table(self, item_data):
        """Befüllt das Detail Grid aus einem Historien-Eintrag."""
        self.detail_table.setRowCount(0)

        details = []
        details.append(("Verzeichnis", item_data.get("dir_name", "")))
        details.append(("Upload-Status", item_data.get("status", "")))

        email_val = item_data.get("email", "")
        email_status = item_data.get("email_status", "")
        email_text = f"{email_val} ({email_status})" if email_val else email_status
        details.append(("Email mit Status", email_text))

        phone_val = item_data.get("phone", "")
        sms_status = item_data.get("sms_status", "")
        phone_text = f"{phone_val} ({sms_status})" if phone_val else sms_status
        details.append(("Telefonnummer mit SMS-Status", phone_text))

        sms_price = item_data.get("sms_price", "")
        price_text = f"{sms_price}€" if sms_price else ""
        details.append(("SMS Kosten", price_text))
        details.append(("Fehlertext", self.build_combined_error_text(item_data)))

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
            self.refresh_history_table(maintain_page=True)

    @Slot()
    def delete_all_history(self):
        reply = QMessageBox.question(self, "Löschen bestätigen", "Möchten Sie wirklich die gesamte Historie löschen?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self.history_manager.clear_all()
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
        self.refresh_history_table(maintain_page=True)
        self._end_history_refresh_overlay()

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

        # Verwende den aktuell aktiven Cloud-Client
        active_client = self.get_active_cloud_client()
        if active_client.get_connection_status() != "Verbunden":
            self.log.error("Monitoring nicht gestartet: Keine Cloud-Verbindung.")
            self.update_monitoring_status(False)
            return

        if not self._upload_recovery_done:
            recover_stalled_upload_folders(self.config, self.upload_queue, self.log)
            self._upload_recovery_done = True

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


from PySide6.QtCore import QObject
import asyncio
from datetime import datetime, timezone

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
            journal_data = asyncio.run(self.sms_client.get_sms_journal(limit=100))
            entries = []
            if isinstance(journal_data, list):
                entries = journal_data
            elif isinstance(journal_data, dict):
                # API kann je nach Endpoint/Version unterschiedliche Wrapper liefern.
                for key in ("messages", "items", "data", "entries"):
                    candidate = journal_data.get(key)
                    if isinstance(candidate, list):
                        entries = candidate
                        break

            if entries:
                self._update_history_with_journal(entries)
        except Exception as e:
            logging.getLogger(__name__).error(f"Fehler bei der SMS-Status-Prüfung im Worker: {e}")
        finally:
            self.finished.emit()

    def _translate_status(self, status):
        lower_status = (status or "").lower()
        if "delivered" in lower_status:
            return "Zugestellt"
        elif "notdelivered" in lower_status or "failed" in lower_status:
            return "Fehlgeschlagen"
        elif "buffered" in lower_status:
            return "Gepuffert"
        elif "transmitted" in lower_status:
            return "Übertragen"
        elif "accepted" in lower_status:
            return "Akzeptiert"
        elif "rejected" in lower_status:
            return "Abgelehnt"
        return status

    def _update_history_with_journal(self, journal_data):
        history = self.history_manager.history
        updated = False

        # Erstelle Lookups für effizienteres Matching
        journal_by_id = {str(msg.get("id")): msg for msg in journal_data if msg.get("id")}
        
        from datetime import datetime
        import time

        def parse_iso(ts_str):
            try:
                # Einfacher Parser, ignoriert Zeitzonen für Differenzberechnung
                clean_str = ts_str.replace('Z', '').split('+')[0].split('.')[0]
                return time.mktime(time.strptime(clean_str, "%Y-%m-%dT%H:%M:%S"))
            except Exception:
                # Versuch alternatives Format z.B. "2024-02-13 05:50:58"
                try:
                    clean_str = ts_str.split('.')[0]
                    return time.mktime(time.strptime(clean_str, "%Y-%m-%d %H:%M:%S"))
                except Exception:
                    return 0

        for item in history:
            # Nur aktualisieren, wenn nicht schon zugestellt/fehlgeschlagen
            if item.get("sms_status") in ["Zugestellt", "Fehlgeschlagen"]:
                pass
                # Update still allows price to change or ID to be filled if missing
            matched_msg = None
            sms_id = str(item.get("sms_id") or "").strip()
            if sms_id.lower() in {"none", "null", "nan"}:
                sms_id = ""
            
            if sms_id and str(sms_id) in journal_by_id:
                matched_msg = journal_by_id[str(sms_id)]

            if matched_msg:
                status_raw = matched_msg.get("dlr") or matched_msg.get("state", "")
                translated_status = self._translate_status(status_raw)
                price = matched_msg.get("price")
                
                changed = False
                if translated_status and translated_status != item.get("sms_status"):
                    item["sms_status"] = translated_status
                    changed = True
                
                if price and price != item.get("sms_price"):
                    item["sms_price"] = price
                    changed = True
                    
                if not item.get("sms_id") and matched_msg.get("id"):
                    item["sms_id"] = str(matched_msg.get("id"))
                    changed = True
                    
                if changed:
                    item["last_updated"] = datetime.now().isoformat()
                    updated = True
                    # Live-Update in die UI senden (thread-sicher via Qt-Signal).
                    self.item_updated.emit(dict(item))

        if updated:
            self.history_manager.save_history()

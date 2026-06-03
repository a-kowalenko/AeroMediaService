import logging
import requests
from packaging import version
from PySide6.QtWidgets import (
    QDialog, QTabWidget, QWidget, QVBoxLayout, QFormLayout, QHBoxLayout,
    QLineEdit, QPushButton, QFileDialog, QSpinBox, QLabel,
    QRadioButton, QButtonGroup, QGroupBox, QMessageBox, QInputDialog,
    QCheckBox, QComboBox, QTextEdit
)
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl, Slot
from core.config import ConfigManager
from services.base_client import BaseClient
from services.custom_api_client import CUSTOM_DB_APP_KEY, CUSTOM_DB_APP_SECRET
from utils.link_shortener import (
    EXPIRES_PRESET_14D,
    EXPIRES_PRESET_1M,
    EXPIRES_PRESET_1Y,
    EXPIRES_PRESET_3M,
    EXPIRES_PRESET_6M,
    EXPIRES_PRESET_PERMANENT,
    LinkShortener,
)
from utils.updater import UpdateProgressDialog, initialize_version_list_loader


class SettingsDialog(QDialog):
    """
    Der Einstellungsdialog, der dem Benutzer die Konfiguration
    der Anwendung in Tabs (Allgemein, Cloud, E-Mail, SMS, Link-Shortener, Extras) ermöglicht.
    """

    def __init__(self, config_manager: ConfigManager, client: BaseClient,
                 app_version: str, latest_version_info: str, parent=None,
                 custom_api_client=None):
        super().__init__(parent)
        self.setWindowTitle("Einstellungen")
        self.setMinimumWidth(700)

        self.config = config_manager
        self.client = client
        self.custom_api_client = custom_api_client
        if self.custom_api_client is None and parent is not None:
            self.custom_api_client = getattr(parent, "custom_api_client", None)
        self.log = logging.getLogger(__name__)

        # Versionsinformationen speichern
        self.app_version = app_version
        self.latest_version_info = latest_version_info or "Noch nicht geprüft."

        # Hauptlayout
        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Tabs erstellen
        self.tabs.addTab(self.create_general_tab(), "Allgemein")
        self.tabs.addTab(self.create_cloud_tab(), "Cloud-Dienst")
        self.tabs.addTab(self.create_email_tab(), "E-Mail (SMTP/IMAP)")
        self.tabs.addTab(self.create_sms_tab(), "SMS-Dienst")
        self.tabs.addTab(self.create_shortener_tab(), "Link-Shortener")
        self.tabs.addTab(self.create_extras_tab(), "Extras")

        # Standard-Buttons (Speichern, Abbrechen)
        button_layout = QVBoxLayout()  # Eigener Layout-Container für Buttons
        self.save_button = QPushButton("Speichern & Übernehmen")
        self.save_button.clicked.connect(self.save_settings)
        button_layout.addWidget(self.save_button)
        main_layout.addLayout(button_layout)

        # 1. Einstellungen laden (blockiert Signale)
        self.load_settings()

        # 2. Initialen Status setzen (fragt API 1x ab)
        self.update_dropbox_status()
        self.update_custom_dropbox_status()

        # 3. Initiale Sichtbarkeit der Cloud-Gruppen setzen
        self.on_cloud_service_changed()

        # 4. Initialen SMS-Guthaben-Stand abrufen
        import PySide6.QtCore as QtCore
        QtCore.QTimer.singleShot(0, self.refresh_seven_balance)

    # --- Tab-Erstellung ---

    def create_general_tab(self):
        """Erstellt den Tab 'Allgemein'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # --- Gruppe 1: Überwachungs-Einstellungen ---
        monitor_group = QGroupBox("Überwachungs-Einstellungen")
        monitor_layout = QFormLayout(monitor_group)
        monitor_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        # Überwachungsordner
        self.monitor_path_edit = QLineEdit()
        self.monitor_path_button = QPushButton("Durchsuchen...")
        self.monitor_path_button.clicked.connect(lambda: self.select_directory(self.monitor_path_edit))
        monitor_layout.addRow("Zu überwachender Ordner:",
                              self.create_path_widget(self.monitor_path_edit, self.monitor_path_button))

        # Archivordner
        self.archive_path_edit = QLineEdit()
        self.archive_path_button = QPushButton("Durchsuchen...")
        self.archive_path_button.clicked.connect(lambda: self.select_directory(self.archive_path_edit))
        monitor_layout.addRow("Archiv-Ordner (erfolg / fehler / abgebrochen):",
                              self.create_path_widget(self.archive_path_edit, self.archive_path_button))

        # Log-Ordner
        self.log_path_edit = QLineEdit()
        self.log_path_button = QPushButton("Durchsuchen...")
        self.log_path_button.clicked.connect(lambda: self.select_directory(self.log_path_edit))
        monitor_layout.addRow("Log-Datei-Ordner:", self.create_path_widget(self.log_path_edit, self.log_path_button))

        # Scan-Intervall
        self.scan_interval_spin = QSpinBox()
        self.scan_interval_spin.setRange(5, 3600)
        self.scan_interval_spin.setSuffix(" Sekunden")
        monitor_layout.addRow("Scan-Intervall:", self.scan_interval_spin)

        self.folder_stability_check = QCheckBox(
            "Ordner-Stabilität vor Upload (wartet auf unveränderten Inhalt)"
        )
        monitor_layout.addRow("", self.folder_stability_check)

        self.folder_stability_seconds_spin = QSpinBox()
        self.folder_stability_seconds_spin.setRange(5, 300)
        self.folder_stability_seconds_spin.setSuffix(" Sekunden")
        monitor_layout.addRow("Stabilitätsdauer:", self.folder_stability_seconds_spin)

        layout.addWidget(monitor_group)

        layout.addStretch(1)  # Schiebt alles nach oben

        return widget

    def create_cloud_tab(self):
        """Erstellt den Tab 'Cloud-Dienst'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Auswahl des Dienstes (Dropbox oder Custom API)
        service_group = QGroupBox("Cloud-Dienst auswählen")
        service_layout = QVBoxLayout()
        self.radio_dropbox = QRadioButton("Dropbox")
        self.radio_custom_api = QRadioButton("Custom API (Bearer Token)")
        self.radio_dropbox.setChecked(True)  # Standard
        self.radio_group = QButtonGroup()
        self.radio_group.setExclusive(True)  # Nur ein Button kann ausgewählt sein
        self.radio_group.addButton(self.radio_dropbox)
        self.radio_group.addButton(self.radio_custom_api)
        service_layout.addWidget(self.radio_dropbox)
        service_layout.addWidget(self.radio_custom_api)

        # Verbinde Radiobutton-Änderungen mit Handler
        self.radio_dropbox.toggled.connect(self.on_cloud_service_changed)
        self.radio_custom_api.toggled.connect(self.on_cloud_service_changed)

        service_group.setLayout(service_layout)
        layout.addWidget(service_group)

        # Dropbox-Einstellungen
        self.dropbox_group = QGroupBox("Dropbox-Einstellungen")
        db_layout = QFormLayout()

        self.db_app_key_edit = QLineEdit()

        # Lambda stellt sicher, dass update_connect_button_state ohne Parameter (None) aufgerufen wird
        self.db_app_key_edit.textChanged.connect(lambda: self.update_connect_button_state(is_connected=None))
        db_layout.addRow("App Key:", self.db_app_key_edit)

        self.db_app_secret_edit = QLineEdit()
        self.db_app_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)

        # Lambda stellt sicher, dass update_connect_button_state ohne Parameter (None) aufgerufen wird
        self.db_app_secret_edit.textChanged.connect(lambda: self.update_connect_button_state(is_connected=None))
        db_layout.addRow("App Secret:", self.db_app_secret_edit)

        dev_console_link = QLabel(
            '<a href="https://www.dropbox.com/developers/apps">Dropbox Developer Console öffnen</a>')
        dev_console_link.setOpenExternalLinks(True)
        db_layout.addRow("", dev_console_link)

        # Verbindungs-Steuerung
        self.db_connect_button = QPushButton("Mit Dropbox verbinden")
        self.db_connect_button.clicked.connect(self.toggle_dropbox_connection)
        self.db_status_label = QLabel("Status: Unbekannt")
        db_layout.addRow(self.db_connect_button, self.db_status_label)

        self.dropbox_group.setLayout(db_layout)
        layout.addWidget(self.dropbox_group)

        # --- Custom API Einstellungen ---
        self.custom_api_group = QGroupBox("Custom API-Einstellungen")
        custom_api_layout = QFormLayout()

        self.custom_api_url_edit = QLineEdit()
        self.custom_api_url_edit.setPlaceholderText("z.B. https://api.meine-cloud.de")
        custom_api_layout.addRow("API Base URL:", self.custom_api_url_edit)

        self.custom_api_token_edit = QLineEdit()
        self.custom_api_token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.custom_api_token_edit.setPlaceholderText("Bearer Token für Authentifizierung")
        custom_api_layout.addRow("Bearer Token:", self.custom_api_token_edit)

        self.aero_customer_base_url_edit = QLineEdit()
        self.aero_customer_base_url_edit.setPlaceholderText("z.B. https://api.example.com/functions/v1")
        custom_api_layout.addRow("Aero Customer Base URL:", self.aero_customer_base_url_edit)

        self.aero_customer_api_token_edit = QLineEdit()
        self.aero_customer_api_token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.aero_customer_api_token_edit.setPlaceholderText("Token für /aero-media-customer Lookup")
        custom_api_layout.addRow("Aero Customer API Token:", self.aero_customer_api_token_edit)

        # Erweiterte Einstellungen
        self.custom_api_upload_endpoint_edit = QLineEdit()
        self.custom_api_upload_endpoint_edit.setPlaceholderText("/upload")
        custom_api_layout.addRow("Upload Endpoint:", self.custom_api_upload_endpoint_edit)

        self.custom_api_share_endpoint_edit = QLineEdit()
        self.custom_api_share_endpoint_edit.setPlaceholderText("/share")
        custom_api_layout.addRow("Share Endpoint:", self.custom_api_share_endpoint_edit)

        self.custom_api_health_endpoint_edit = QLineEdit()
        self.custom_api_health_endpoint_edit.setPlaceholderText("/health")
        custom_api_layout.addRow("Health Check Endpoint:", self.custom_api_health_endpoint_edit)

        self.custom_api_mode_combo = QComboBox()
        self.custom_api_mode_combo.addItem("Proxy Session Upload (bestehend)", "proxied_session")
        self.custom_api_mode_combo.addItem("Dropbox Upload + Manifest v1.1", "direct_dropbox_complete")
        custom_api_layout.addRow("Upload-Modus:", self.custom_api_mode_combo)

        self.custom_dropbox_group = QGroupBox("Dropbox für Upload (Manifest v1.1, separates Konto)")
        custom_db_layout = QFormLayout()

        self.custom_db_app_key_edit = QLineEdit()
        self.custom_db_app_key_edit.textChanged.connect(self.update_custom_dropbox_connect_button)
        custom_db_layout.addRow("App Key:", self.custom_db_app_key_edit)

        self.custom_db_app_secret_edit = QLineEdit()
        self.custom_db_app_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.custom_db_app_secret_edit.textChanged.connect(self.update_custom_dropbox_connect_button)
        custom_db_layout.addRow("App Secret:", self.custom_db_app_secret_edit)

        custom_db_dev_link = QLabel(
            '<a href="https://www.dropbox.com/developers/apps">Dropbox Developer Console öffnen</a>'
        )
        custom_db_dev_link.setOpenExternalLinks(True)
        custom_db_layout.addRow("", custom_db_dev_link)

        self.custom_db_connect_button = QPushButton("Mit Dropbox verbinden (Upload-Konto)")
        self.custom_db_connect_button.clicked.connect(self.toggle_custom_dropbox_connection)
        self.custom_db_status_label = QLabel("Status: Unbekannt")
        custom_db_layout.addRow(self.custom_db_connect_button, self.custom_db_status_label)

        self.custom_dropbox_group.setLayout(custom_db_layout)
        custom_api_layout.addRow(self.custom_dropbox_group)

        # Verbindungs-Steuerung für Custom API
        self.custom_api_connect_button = QPushButton("Mit Custom API verbinden")
        self.custom_api_connect_button.clicked.connect(self.toggle_custom_api_connection)
        self.custom_api_status_label = QLabel("Status: Unbekannt")
        custom_api_layout.addRow(self.custom_api_connect_button, self.custom_api_status_label)

        self.custom_api_group.setLayout(custom_api_layout)
        layout.addWidget(self.custom_api_group)

        layout.addStretch(1)  # Schiebt alles nach oben

        return widget

    def _populate_shortener_expires_combo(self):
        """Füllt die Gültigkeits-Auswahl für den Link-Shortener."""
        self.shortener_expires_combo.clear()
        options = [
            ("Permanent", EXPIRES_PRESET_PERMANENT),
            ("14 Tage", EXPIRES_PRESET_14D),
            ("1 Monat", EXPIRES_PRESET_1M),
            ("3 Monate", EXPIRES_PRESET_3M),
            ("6 Monate", EXPIRES_PRESET_6M),
            ("1 Jahr", EXPIRES_PRESET_1Y),
        ]
        for label, key in options:
            self.shortener_expires_combo.addItem(label, key)

    def create_shortener_tab(self):
        """Erstellt den Tab 'Link-Shortener'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        shortener_group = QGroupBox("Link-Shortener (vor E-Mail/SMS)")
        form = QFormLayout(shortener_group)

        self.shortener_enabled_check = QCheckBox(
            "Freigabe-Links vor dem Versand kürzen"
        )
        form.addRow("", self.shortener_enabled_check)

        self.shortener_base_edit = QLineEdit()
        self.shortener_base_edit.setPlaceholderText("z.B. https://skydive-media.de")
        form.addRow("Basis-URL:", self.shortener_base_edit)

        self.shortener_api_key_edit = QLineEdit()
        self.shortener_api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.shortener_api_key_edit.setPlaceholderText("key_<id>.<secret>")
        form.addRow("API-Key:", self.shortener_api_key_edit)

        self.shortener_expires_combo = QComboBox()
        self._populate_shortener_expires_combo()
        form.addRow("Gültigkeit ab Erstellung:", self.shortener_expires_combo)

        hint = QLabel(
            "API-Key mit Permission <b>shorten</b> (Bearer <code>key_…secret</code>). "
            "Endpoint: <code>POST {Basis-URL}/api/shorten</code>"
        )
        hint.setWordWrap(True)
        form.addRow("", hint)

        test_layout = QHBoxLayout()
        self.shortener_test_button = QPushButton("Verbindung testen")
        self.shortener_test_button.clicked.connect(self.test_shortener_connection)
        test_layout.addWidget(self.shortener_test_button)
        test_layout.addStretch(1)
        form.addRow("", test_layout)

        layout.addWidget(shortener_group)
        layout.addStretch(1)
        return widget

    def create_email_tab(self):
        """Erstellt den Tab 'E-Mail (SMTP/IMAP)'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # --- Gruppe 1: SMTP-Server-Verbindung ---
        conn_group = QGroupBox("SMTP-Server-Verbindung")
        conn_layout = QFormLayout(conn_group)

        self.smtp_host_edit = QLineEdit()
        conn_layout.addRow("SMTP-Host:", self.smtp_host_edit)

        self.smtp_port_edit = QSpinBox()
        self.smtp_port_edit.setRange(1, 65535)
        self.smtp_port_edit.setValue(587)
        conn_layout.addRow("SMTP-Port:", self.smtp_port_edit)

        self.smtp_user_edit = QLineEdit()
        conn_layout.addRow("Benutzername:", self.smtp_user_edit)

        self.smtp_pass_edit = QLineEdit()
        self.smtp_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        conn_layout.addRow("Passwort:", self.smtp_pass_edit)

        layout.addWidget(conn_group)

        # --- Gruppe 2: Absender-Konfiguration ---
        sender_group = QGroupBox("Absender-Konfiguration")
        sender_layout = QFormLayout(sender_group)

        self.smtp_sender_addr_edit = QLineEdit()
        sender_layout.addRow("Absender-Adresse:", self.smtp_sender_addr_edit)

        self.smtp_sender_name_edit = QLineEdit()
        sender_layout.addRow("Absender-Name:", self.smtp_sender_name_edit)

        self.smtp_fallback_recipient_edit = QLineEdit()
        sender_layout.addRow("Fallback-Empfänger (für Status-Mails):", self.smtp_fallback_recipient_edit)

        self.smtp_sandbox_check = QCheckBox(
            "Sandbox-Modus aktivieren (alle E-Mails gehen an den Fallback-Empfänger)"
        )
        sender_layout.addRow("Test-Modus:", self.smtp_sandbox_check)

        layout.addWidget(sender_group)

        # --- Gruppe 3: IMAP-Ablage in Gesendet ---
        imap_group = QGroupBox("IMAP (Kopie in Gesendet)")
        imap_layout = QFormLayout(imap_group)

        self.imap_save_sent_check = QCheckBox(
            "Versendete E-Mails im Postfach ablegen (Ordner Gesendet)"
        )
        self.imap_save_sent_check.setChecked(True)
        imap_layout.addRow("Archiv:", self.imap_save_sent_check)

        self.imap_host_edit = QLineEdit()
        self.imap_host_edit.setPlaceholderText("Leer = SMTP-Host")
        imap_layout.addRow("IMAP-Host:", self.imap_host_edit)

        self.imap_port_edit = QSpinBox()
        self.imap_port_edit.setRange(1, 65535)
        self.imap_port_edit.setValue(993)
        imap_layout.addRow("IMAP-Port:", self.imap_port_edit)

        self.imap_sent_folder_edit = QLineEdit()
        self.imap_sent_folder_edit.setPlaceholderText("Leer = Auto-Erkennung (\\Sent / Gesendet)")
        imap_layout.addRow("Gesendet-Ordner:", self.imap_sent_folder_edit)

        self.imap_same_credentials_check = QCheckBox(
            "Gleiche Zugangsdaten wie SMTP (empfohlen)"
        )
        self.imap_same_credentials_check.setChecked(True)
        self.imap_same_credentials_check.toggled.connect(self._toggle_imap_credentials_fields)
        imap_layout.addRow("Zugangsdaten:", self.imap_same_credentials_check)

        self.imap_user_edit = QLineEdit()
        imap_layout.addRow("IMAP-Benutzername:", self.imap_user_edit)

        self.imap_pass_edit = QLineEdit()
        self.imap_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        imap_layout.addRow("IMAP-Passwort:", self.imap_pass_edit)

        layout.addWidget(imap_group)
        self._toggle_imap_credentials_fields(self.imap_same_credentials_check.isChecked())

        layout.addStretch(1)  # Schiebt alles nach oben

        return widget

    def _toggle_imap_credentials_fields(self, use_smtp_credentials: bool):
        """Blendet IMAP-Zugangsdaten aus, wenn SMTP-Daten wiederverwendet werden."""
        self.imap_user_edit.setEnabled(not use_smtp_credentials)
        self.imap_pass_edit.setEnabled(not use_smtp_credentials)

    def create_sms_tab(self):
        """Erstellt den Tab 'SMS (Seven.io)'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # --- Gruppe 1: SMS-Dienst auswählen ---
        service_group = QGroupBox("SMS-Dienst auswählen")
        service_layout = QVBoxLayout()

        self.radio_seven = QRadioButton("Seven.io")
        self.radio_seven.setChecked(True)  # Standard

        self.sms_radio_group = QButtonGroup()
        self.sms_radio_group.addButton(self.radio_seven)

        service_layout.addWidget(self.radio_seven)
        service_group.setLayout(service_layout)
        layout.addWidget(service_group)

        # --- Gruppe 2: Seven.io-Einstellungen ---
        self.seven_group = QGroupBox("Seven.io-Einstellungen")
        seven_layout = QFormLayout()

        # API Key (Produktion)
        self.sms_api_key_edit = QLineEdit()
        self.sms_api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        seven_layout.addRow("Production API Key:", self.sms_api_key_edit)

        # API Key (Sandbox)
        self.sms_sandbox_api_key_edit = QLineEdit()
        self.sms_sandbox_api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        seven_layout.addRow("Sandbox API Key:", self.sms_sandbox_api_key_edit)

        # Absender
        self.sms_sender_edit = QLineEdit()
        self.sms_sender_edit.setPlaceholderText("z.B. AERO oder +49...")
        seven_layout.addRow("Absender (max. 11 Zeichen):", self.sms_sender_edit)

        # Sandbox-Modus
        self.sms_sandbox_check = QCheckBox("Sandbox-Modus aktivieren (simuliert Versand, keine Kosten)")
        seven_layout.addRow("Test-Modus:", self.sms_sandbox_check)

        # Link
        api_link = QLabel('<a href="https://www.seven.io">seven.io Website (API-Keys)</a>')
        api_link.setOpenExternalLinks(True)
        seven_layout.addRow("", api_link)

        # --- Aktuelle Balance ---
        balance_layout = QHBoxLayout()
        self.sms_balance_label = QLabel("Aktuelle Balance: Unbekannt")
        self.sms_balance_refresh_btn = QPushButton("Aktualisieren")
        self.sms_balance_refresh_btn.clicked.connect(self.refresh_seven_balance)
        balance_layout.addWidget(self.sms_balance_label)
        balance_layout.addWidget(self.sms_balance_refresh_btn)
        seven_layout.addRow("Guthaben:", balance_layout)

        self.seven_group.setLayout(seven_layout)
        layout.addWidget(self.seven_group)

        layout.addStretch(1)  # Füllt den restlichen Platz nach unten auf

        return widget

    def create_extras_tab(self):
        """Erstellt den Tab 'Extras'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # --- Gruppe 1: Software-Update ---
        update_group = QGroupBox("Software-Update")
        update_layout = QFormLayout(update_group)

        self.current_version_label = QLabel(f"Aktuell installierte Version: <b>{self.app_version}</b>")
        update_layout.addRow(self.current_version_label)

        self.update_status_label = QLabel(f"Update-Status: <b>{self.latest_version_info}</b>")
        update_layout.addRow(self.update_status_label)

        self.update_check_button = QPushButton("Jetzt auf Updates prüfen")
        if self.parent() and hasattr(self.parent(), 'check_for_updates_manual'):
            self.update_check_button.clicked.connect(self.on_check_for_updates_clicked)
        else:
            self.update_check_button.setEnabled(False)
            self.update_check_button.setToolTip("Konnte keine Update-Funktion im Hauptfenster finden.")
        update_layout.addRow(self.update_check_button)
        layout.addWidget(update_group)

        # --- Gruppe 2: Version wechseln ---
        switch_group = QGroupBox("Version wechseln")
        switch_layout = QFormLayout(switch_group)

        self.version_hint_label = QLabel(
            "Verfügbare stabile Versionen"
        )
        switch_layout.addRow(self.version_hint_label)

        self.show_prereleases_check = QCheckBox("Prereleases anzeigen")
        self.show_prereleases_check.toggled.connect(self.on_show_prereleases_toggled)
        switch_layout.addRow(self.show_prereleases_check)

        self.version_combo = QComboBox()
        self.version_combo.currentIndexChanged.connect(self.on_version_selected)
        switch_layout.addRow("Ziel-Version:", self.version_combo)

        self.refresh_versions_button = QPushButton("Liste neu laden")
        self.refresh_versions_button.clicked.connect(self.load_switchable_versions)
        switch_layout.addRow(self.refresh_versions_button)

        self.version_notes_edit = QTextEdit()
        self.version_notes_edit.setReadOnly(True)
        self.version_notes_edit.setMaximumHeight(220)
        switch_layout.addRow("Release-Notes:", self.version_notes_edit)

        self.version_switch_button = QPushButton("Auf diese Version wechseln")
        self.version_switch_button.clicked.connect(self.on_switch_version_clicked)
        switch_layout.addRow(self.version_switch_button)

        layout.addWidget(switch_group)
        layout.addStretch(1)

        self.version_list_thread = None
        self.version_list_worker = None
        self._switchable_versions = []
        self.load_switchable_versions()

        return widget

    @Slot()
    def test_shortener_connection(self):
        """Testet den Link-Shortener mit den aktuellen Formularwerten."""
        base = self.shortener_base_edit.text().strip()
        api_key = self.shortener_api_key_edit.text().strip()
        if not base or not api_key:
            QMessageBox.warning(
                self,
                "Link-Shortener",
                "Bitte Basis-URL und API-Key eintragen.",
            )
            return

        self.shortener_test_button.setEnabled(False)
        try:
            shortener = LinkShortener(self.config)
            test_url = "https://example.com/aero-media-shortener-test"
            result = shortener.shorten(
                test_url,
                override_base=base,
                override_key=api_key,
                override_enabled=True,
                override_preset=self.shortener_expires_combo.currentData(),
            )
            if result != test_url:
                QMessageBox.information(
                    self,
                    "Link-Shortener",
                    f"Test erfolgreich.\n\nKurzlink:\n{result}",
                )
            else:
                QMessageBox.warning(
                    self,
                    "Link-Shortener",
                    "Kürzen fehlgeschlagen. Details stehen im Log.",
                )
        finally:
            self.shortener_test_button.setEnabled(True)

    def refresh_seven_balance(self):
        """Ruft die aktuelle Balance von Seven.io ab."""
        import requests
        
        is_sandbox = self.sms_sandbox_check.isChecked()
        if is_sandbox:
            api_key = self.sms_sandbox_api_key_edit.text()
        else:
            api_key = self.sms_api_key_edit.text()
            
        if not api_key:
            self.sms_balance_label.setText("Aktuelle Balance: Fehlender API-Key")
            return
            
        self.sms_balance_label.setText("Aktuelle Balance: Lade...")
        self.sms_balance_refresh_btn.setEnabled(False)
        
        def fetch_balance():
            url = "https://gateway.seven.io/api/balance"
            headers = {"X-Api-Key": api_key, "Accept": "application/json"}
            try:
                response = requests.get(url, headers=headers, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    amount = data.get("amount")
                    # amount = f"{amount:.2f}"
                    currency = data.get("currency", "€")
                    if currency == "EUR":
                         currency = "€"
                    return f"{amount} {currency}"
                else:
                    return f"Fehler ({response.status_code})"
            except requests.exceptions.RequestException as e:
                return "Netzwerkfehler"

        balance_str = fetch_balance()
        self.sms_balance_label.setText(f"Aktuelle Balance: {balance_str}")
        self.sms_balance_refresh_btn.setEnabled(True)

    # --------------------

    # --- Hilfsfunktionen ---

    def create_path_widget(self, line_edit, button):
        """Erstellt ein kombiniertes Widget aus QLineEdit und QPushButton."""

        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(line_edit)
        layout.addWidget(button)
        return widget

    def select_directory(self, line_edit):
        """Öffnet einen Dialog zur Ordnerauswahl."""
        directory = QFileDialog.getExistingDirectory(self, "Ordner auswählen", line_edit.text())
        if directory:
            line_edit.setText(directory)

    # --- Einstellungs-Management ---

    def load_settings(self):
        """Lädt alle Einstellungen aus dem ConfigManager in die GUI-Felder."""
        self.log.debug("Lade Einstellungen in den Dialog...")

        # Signale der Cloud-Textfelder blockieren,
        # um textChanged-Spam beim Laden zu verhindern.
        try:
            self.db_app_key_edit.blockSignals(True)
            self.db_app_secret_edit.blockSignals(True)
            self.radio_dropbox.blockSignals(True)
            self.radio_custom_api.blockSignals(True)
        except AttributeError:
            # Passiert, wenn load_settings vor create_cloud_tab aufgerufen würde
            self.log.warning("Cloud-Tab-Widgets noch nicht initialisiert beim Laden.")
            pass

        # Allgemein
        self.monitor_path_edit.setText(self.config.get_setting("monitor_path"))
        self.archive_path_edit.setText(self.config.get_setting("archive_path"))
        self.log_path_edit.setText(self.config.get_setting("log_file_path"))
        self.scan_interval_spin.setValue(int(self.config.get_setting("scan_interval", 10)))
        folder_stability_enabled_str = self.config.get_setting("folder_stability_enabled", "true")
        self.folder_stability_check.setChecked(folder_stability_enabled_str.lower() != "false")
        self.folder_stability_seconds_spin.setValue(
            int(self.config.get_setting("folder_stability_seconds", 15))
        )

        # Dropbox
        self.db_app_key_edit.setText(self.config.get_secret("db_app_key"))
        self.db_app_secret_edit.setText(self.config.get_secret("db_app_secret"))

        # Link-Shortener
        shortener_enabled = self.config.get_setting("link_shortener_enabled", "false")
        self.shortener_enabled_check.setChecked(shortener_enabled.lower() == "true")
        base_url = self.config.get_secret("shortener_base_url")
        api_key = self.config.get_secret("shortener_api_key")
        if not base_url:
            legacy_url = self.config.get_secret("skylink_api_url")
            if legacy_url:
                base_url = LinkShortener._legacy_url_to_base(legacy_url)
        if not api_key:
            api_key = self.config.get_secret("skylink_api_key") or ""
        self.shortener_base_edit.setText(base_url or "")
        self.shortener_api_key_edit.setText(api_key or "")
        preset = self.config.get_setting(
            "shortener_expires_preset", EXPIRES_PRESET_PERMANENT
        )
        idx = self.shortener_expires_combo.findData(preset)
        self.shortener_expires_combo.setCurrentIndex(idx if idx >= 0 else 0)

        # Custom API
        self.custom_api_url_edit.setText(self.config.get_secret("custom_api_url"))
        self.custom_api_token_edit.setText(self.config.get_secret("custom_api_bearer_token"))
        self.aero_customer_base_url_edit.setText(self.config.get_secret("aero_customer_base_url"))
        self.aero_customer_api_token_edit.setText(self.config.get_secret("aero_customer_api_token"))
        self.custom_api_upload_endpoint_edit.setText(self.config.get_setting("custom_api_upload_endpoint", "/upload"))
        self.custom_api_share_endpoint_edit.setText(self.config.get_setting("custom_api_share_endpoint", "/share"))
        self.custom_api_health_endpoint_edit.setText(self.config.get_setting("custom_api_health_endpoint", "/health"))
        custom_mode = self.config.get_setting("custom_api_upload_mode", "proxied_session")
        idx = self.custom_api_mode_combo.findData(custom_mode)
        self.custom_api_mode_combo.setCurrentIndex(idx if idx >= 0 else 0)

        self.custom_db_app_key_edit.setText(self.config.get_secret(CUSTOM_DB_APP_KEY) or "")
        self.custom_db_app_secret_edit.setText(self.config.get_secret(CUSTOM_DB_APP_SECRET) or "")

        # Cloud-Dienst Auswahl
        selected_cloud = self.config.get_setting("selected_cloud_service", "dropbox")
        if selected_cloud == "custom_api":
            self.radio_custom_api.setChecked(True)
        else:
            self.radio_dropbox.setChecked(True)

        # E-Mail
        self.smtp_host_edit.setText(self.config.get_setting("smtp_host"))
        self.smtp_port_edit.setValue(int(self.config.get_setting("smtp_port", 587)))
        self.smtp_user_edit.setText(self.config.get_secret("smtp_user"))
        self.smtp_pass_edit.setText(self.config.get_secret("smtp_pass"))
        self.smtp_sender_addr_edit.setText(self.config.get_setting("smtp_sender_addr"))
        self.smtp_sender_name_edit.setText(self.config.get_setting("smtp_sender_name", "Dropbox Uploader"))
        self.smtp_fallback_recipient_edit.setText(self.config.get_setting("smtp_fallback_recipient"))
        smtp_sandbox_mode_str = self.config.get_setting("smtp_sandbox_mode", "false")
        self.smtp_sandbox_check.setChecked(smtp_sandbox_mode_str.lower() == "true")

        imap_save_sent_str = self.config.get_setting("imap_save_sent_enabled", "true")
        self.imap_save_sent_check.setChecked(imap_save_sent_str.lower() == "true")
        self.imap_host_edit.setText(self.config.get_setting("imap_host", ""))
        self.imap_port_edit.setValue(int(self.config.get_setting("imap_port", 993)))
        self.imap_sent_folder_edit.setText(self.config.get_setting("imap_sent_folder", ""))
        imap_same_credentials_str = self.config.get_setting("imap_same_credentials", "true")
        self.imap_same_credentials_check.setChecked(imap_same_credentials_str.lower() == "true")
        self.imap_user_edit.setText(self.config.get_secret("imap_user") or "")
        self.imap_pass_edit.setText(self.config.get_secret("imap_pass") or "")
        self._toggle_imap_credentials_fields(self.imap_same_credentials_check.isChecked())

        # SMS
        self.sms_api_key_edit.setText(self.config.get_secret("seven_api_key"))
        self.sms_sandbox_api_key_edit.setText(self.config.get_secret("seven_sandbox_api_key"))
        self.sms_sender_edit.setText(self.config.get_setting("seven_sender"))

        # Sandbox-Modus (als String "true"/"false" gespeichert)
        sandbox_mode_str = self.config.get_setting("seven_sandbox_mode", "false")
        self.sms_sandbox_check.setChecked(sandbox_mode_str.lower() == "true")

        # -----------------------

        # Signale wieder freigeben
        try:
            self.db_app_key_edit.blockSignals(False)
            self.db_app_secret_edit.blockSignals(False)
            self.radio_dropbox.blockSignals(False)
            self.radio_custom_api.blockSignals(False)
        except AttributeError:
            pass  # Fehler wurde bereits geloggt

    def save_settings(self):
        """Speichert alle Einstellungen aus den GUI-Feldern im ConfigManager."""
        self.log.info("Speichere Einstellungen...")

        # Signale des ConfigManagers blockieren, um Signal-Sturm zu verhindern
        try:
            self.config.blockSignals(True)
        except AttributeError:
            self.log.warning("Konnte Signale des ConfigManagers nicht blockieren. "
                             "Signal-Sturm ist möglich.")

        # --- Alle save_setting/save_secret Aufrufe ---
        try:
            # Allgemein
            self.config.save_setting("monitor_path", self.monitor_path_edit.text())
            self.config.save_setting("archive_path", self.archive_path_edit.text())
            self.config.save_setting("log_file_path", self.log_path_edit.text())
            self.config.save_setting("scan_interval", self.scan_interval_spin.value())
            folder_stability_enabled_str = (
                "true" if self.folder_stability_check.isChecked() else "false"
            )
            self.config.save_setting("folder_stability_enabled", folder_stability_enabled_str)
            self.config.save_setting(
                "folder_stability_seconds", self.folder_stability_seconds_spin.value()
            )

            # Dropbox (Key/Secret werden nur gespeichert, nicht der Token)
            self.config.save_secret("db_app_key", self.db_app_key_edit.text())
            self.config.save_secret("db_app_secret", self.db_app_secret_edit.text())

            # Link-Shortener
            shortener_enabled_str = (
                "true" if self.shortener_enabled_check.isChecked() else "false"
            )
            self.config.save_setting("link_shortener_enabled", shortener_enabled_str)
            self.config.save_secret("shortener_base_url", self.shortener_base_edit.text().strip())
            self.config.save_secret("shortener_api_key", self.shortener_api_key_edit.text().strip())
            self.config.save_setting(
                "shortener_expires_preset",
                self.shortener_expires_combo.currentData() or EXPIRES_PRESET_PERMANENT,
            )

            # Custom API
            self.config.save_secret("custom_api_url", self.custom_api_url_edit.text())
            self.config.save_secret("custom_api_bearer_token", self.custom_api_token_edit.text())
            self.config.save_secret("aero_customer_base_url", self.aero_customer_base_url_edit.text())
            self.config.save_secret("aero_customer_api_token", self.aero_customer_api_token_edit.text())
            self.config.save_setting("custom_api_upload_endpoint", self.custom_api_upload_endpoint_edit.text())
            self.config.save_setting("custom_api_share_endpoint", self.custom_api_share_endpoint_edit.text())
            self.config.save_setting("custom_api_health_endpoint", self.custom_api_health_endpoint_edit.text())
            self.config.save_setting(
                "custom_api_upload_mode",
                self.custom_api_mode_combo.currentData() or "proxied_session",
            )
            self.config.save_secret(CUSTOM_DB_APP_KEY, self.custom_db_app_key_edit.text())
            self.config.save_secret(CUSTOM_DB_APP_SECRET, self.custom_db_app_secret_edit.text())

            # Cloud-Dienst Auswahl
            if self.radio_custom_api.isChecked():
                self.config.save_setting("selected_cloud_service", "custom_api")
            else:
                self.config.save_setting("selected_cloud_service", "dropbox")

            # E-Mail
            self.config.save_setting("smtp_host", self.smtp_host_edit.text())
            self.config.save_setting("smtp_port", self.smtp_port_edit.value())
            self.config.save_secret("smtp_user", self.smtp_user_edit.text())
            self.config.save_secret("smtp_pass", self.smtp_pass_edit.text())
            self.config.save_setting("smtp_sender_addr", self.smtp_sender_addr_edit.text())
            self.config.save_setting("smtp_sender_name", self.smtp_sender_name_edit.text())
            self.config.save_setting("smtp_fallback_recipient", self.smtp_fallback_recipient_edit.text())
            smtp_sandbox_mode_str = "true" if self.smtp_sandbox_check.isChecked() else "false"
            self.config.save_setting("smtp_sandbox_mode", smtp_sandbox_mode_str)

            imap_save_sent_str = "true" if self.imap_save_sent_check.isChecked() else "false"
            self.config.save_setting("imap_save_sent_enabled", imap_save_sent_str)
            self.config.save_setting("imap_host", self.imap_host_edit.text().strip())
            self.config.save_setting("imap_port", self.imap_port_edit.value())
            self.config.save_setting("imap_sent_folder", self.imap_sent_folder_edit.text().strip())
            imap_same_credentials_str = (
                "true" if self.imap_same_credentials_check.isChecked() else "false"
            )
            self.config.save_setting("imap_same_credentials", imap_same_credentials_str)
            if self.imap_same_credentials_check.isChecked():
                self.config.delete_secret("imap_user")
                self.config.delete_secret("imap_pass")
            else:
                self.config.save_secret("imap_user", self.imap_user_edit.text())
                self.config.save_secret("imap_pass", self.imap_pass_edit.text())

            # SMS
            self.config.save_secret("seven_api_key", self.sms_api_key_edit.text())
            self.config.save_secret("seven_sandbox_api_key", self.sms_sandbox_api_key_edit.text())
            self.config.save_setting("seven_sender", self.sms_sender_edit.text())

            sandbox_mode_str = "true" if self.sms_sandbox_check.isChecked() else "false"
            self.config.save_setting("seven_sandbox_mode", sandbox_mode_str)

        finally:
            # Signale in jedem Fall wieder freigeben
            try:
                self.config.blockSignals(False)
            except AttributeError:
                pass  # Fehler wurde bereits oben geloggt

        # Das Signal manuell *einmal* auslösen,
        # damit MainWindow.on_settings_changed() genau einmal aufgerufen wird.
        try:
            self.config.settings_changed.emit()
            self.log.info("Einstellungen gespeichert.")
        except AttributeError:
            self.log.error("Konnte 'settings_changed' Signal nicht manuell auslösen. "
                           "Hauptfenster wurde nicht benachrichtigt.")

        self.accept()  # Schließt den Dialog mit "OK"

    # --- Dropbox-Verbindungslogik ---

    def on_cloud_service_changed(self):
        """Handler für Änderungen der Cloud-Dienst Auswahl."""
        if self.radio_dropbox.isChecked():
            self.dropbox_group.setVisible(True)
            self.custom_api_group.setVisible(False)
            self.log.info("Cloud-Dienst gewechselt zu: Dropbox")
        elif self.radio_custom_api.isChecked():
            self.dropbox_group.setVisible(False)
            self.custom_api_group.setVisible(True)
            self.log.info("Cloud-Dienst gewechselt zu: Custom API")
            self.update_custom_dropbox_status()

    def _resolve_custom_api_client(self):
        if self.custom_api_client is not None:
            return self.custom_api_client
        if self.parent() is not None:
            return getattr(self.parent(), "custom_api_client", None)
        return None

    def update_custom_dropbox_connect_button(self, *_args):
        if not hasattr(self, "custom_db_connect_button"):
            return
        client = self._resolve_custom_api_client()
        status = client.get_dropbox_connection_status() if client else "Nicht verbunden"
        is_connected = status.startswith("Verbunden")
        key_ok = bool(self.custom_db_app_key_edit.text())
        secret_ok = bool(self.custom_db_app_secret_edit.text())
        if is_connected:
            self.custom_db_connect_button.setText("Dropbox-Verbindung trennen (Upload-Konto)")
            self.custom_db_connect_button.setEnabled(True)
            self.custom_db_app_key_edit.setEnabled(False)
            self.custom_db_app_secret_edit.setEnabled(False)
        else:
            self.custom_db_connect_button.setText("Mit Dropbox verbinden (Upload-Konto)")
            self.custom_db_app_key_edit.setEnabled(True)
            self.custom_db_app_secret_edit.setEnabled(True)
            self.custom_db_connect_button.setEnabled(key_ok and secret_ok)

    def update_custom_dropbox_status(self):
        if not hasattr(self, "custom_db_status_label"):
            return
        client = self._resolve_custom_api_client()
        if client is None:
            self.custom_db_status_label.setText("Status: Client nicht verfügbar")
            self.update_custom_dropbox_connect_button()
            return
        status = client.get_dropbox_connection_status()
        self.custom_db_status_label.setText(f"Status: {status}")
        self.update_custom_dropbox_connect_button()

    def toggle_custom_dropbox_connection(self):
        client = self._resolve_custom_api_client()
        if client is None:
            QMessageBox.warning(self, "Fehler", "Custom-API-Client ist nicht verfügbar.")
            return

        self.config.save_secret(CUSTOM_DB_APP_KEY, self.custom_db_app_key_edit.text())
        self.config.save_secret(CUSTOM_DB_APP_SECRET, self.custom_db_app_secret_edit.text())

        status = client.get_dropbox_connection_status()
        if status.startswith("Verbunden"):
            client.disconnect_dropbox()
            self.update_custom_dropbox_status()
            return

        if not self.custom_db_app_key_edit.text() or not self.custom_db_app_secret_edit.text():
            QMessageBox.warning(
                self,
                "Fehlende Daten",
                "Bitte App Key und App Secret für die Upload-Dropbox eingeben.",
            )
            return

        self.custom_db_status_label.setText("Status: Warte auf OAuth...")
        self.custom_db_connect_button.setEnabled(False)

        success = client.connect_dropbox(auth_callback=self.get_dropbox_auth_code)
        if success:
            self.log.info("Custom-Dropbox-Verbindung erfolgreich hergestellt.")
        else:
            self.log.warning("Custom-Dropbox-Verbindung fehlgeschlagen.")

        self.update_custom_dropbox_status()

    def update_dropbox_status(self):
        """
        Aktualisiert die GUI und ruft update_connect_button_state
        mit dem bereits abgerufenen Status auf.
        """
        status = self.client.get_connection_status()  # ERSTER UND EINZIGER AUFRUF
        self.db_status_label.setText(f"Status: {status}")

        is_connected = (status == "Verbunden")

        if is_connected:
            self.db_connect_button.setText("Verbindung trennen")
            self.db_app_key_edit.setEnabled(False)
            self.db_app_secret_edit.setEnabled(False)
        else:
            self.db_connect_button.setText("Mit Dropbox verbinden")
            self.db_app_key_edit.setEnabled(True)
            self.db_app_secret_edit.setEnabled(True)

        # Ruft die Helferfunktion mit dem Ergebnis auf, um zweiten API-Call zu vermeiden
        self.update_connect_button_state(is_connected)

    # --- Custom API Verbindungslogik ---

    def toggle_custom_api_connection(self):
        """Startet den Verbindungs- oder Trennungsvorgang für Custom API."""
        # Hinweis: Für Custom API brauchen wir eine separate Client-Instanz
        # Wir müssen prüfen, ob die MainWindow-Instanz einen custom_api_client hat
        # Da wir hier im Settings-Dialog sind, müssen wir das über self.client lösen
        # ODER einen separaten custom_api_client Parameter übergeben

        # Für jetzt: Warnung, dass dies noch nicht implementiert ist
        # Dies erfordert Änderungen in app.py, um beide Clients zu unterstützen

        api_url = self.custom_api_url_edit.text()
        bearer_token = self.custom_api_token_edit.text()

        if not api_url or not bearer_token:
            QMessageBox.warning(self, "Fehlende Daten",
                              "Bitte geben Sie sowohl die API URL als auch den Bearer Token ein.")
            return

        # Speichere die Einstellungen
        self.config.save_secret("custom_api_url", api_url)
        self.config.save_secret("custom_api_bearer_token", bearer_token)
        self.config.save_secret("aero_customer_base_url", self.aero_customer_base_url_edit.text())
        self.config.save_secret("aero_customer_api_token", self.aero_customer_api_token_edit.text())
        self.config.save_setting("custom_api_upload_endpoint",
                                self.custom_api_upload_endpoint_edit.text() or "/upload")
        self.config.save_setting("custom_api_share_endpoint",
                                self.custom_api_share_endpoint_edit.text() or "/share")
        self.config.save_setting("custom_api_health_endpoint",
                                self.custom_api_health_endpoint_edit.text() or "/health")
        self.config.save_setting(
            "custom_api_upload_mode",
            self.custom_api_mode_combo.currentData() or "proxied_session",
        )

        # Test-Verbindung (vereinfacht, ohne echten Client-Zugriff)
        try:
            health_endpoint = self.custom_api_health_endpoint_edit.text() or "/health"
            test_url = api_url.rstrip('/') + '/' + health_endpoint.lstrip('/')

            self.custom_api_status_label.setText("Status: Teste Verbindung...")
            self.custom_api_connect_button.setEnabled(False)

            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": "AeroMediaService/1.0"
            }

            response = requests.get(test_url, headers=headers, timeout=10)

            if response.status_code == 200:
                # Teste ob die Response JSON ist und den erwarteten Status hat
                try:
                    data = response.json()
                    status = data.get('status')

                    if status == 'healthy':
                        self.custom_api_status_label.setText("Status: Verbunden ✓")

                        # Verbinde auch den tatsächlichen Client, falls parent verfügbar ist
                        if hasattr(self.parent(), 'custom_api_client'):
                            self.parent().custom_api_client.connect()
                            self.log.info("Custom API Client wurde erfolgreich verbunden")

                            # Aktualisiere den aktiven Cloud-Client im MainWindow
                            if hasattr(self.parent(), 'active_cloud_client'):
                                self.parent().active_cloud_client = self.parent().custom_api_client
                                self.log.info("Aktiver Cloud-Client wurde auf Custom API gesetzt")

                                # Aktualisiere auch den UploaderThread
                                if hasattr(self.parent(), 'uploader_thread'):
                                    self.parent().uploader_thread.client = self.parent().custom_api_client
                                    self.log.info("UploaderThread wurde aktualisiert")

                                # Aktualisiere das Status-Light im MainWindow
                                if hasattr(self.parent(), 'update_status_light'):
                                    self.parent().update_status_light()
                                    self.log.info("Status-Light wurde aktualisiert")

                        QMessageBox.information(self, "Verbindung erfolgreich",
                                              "Die Verbindung zur Custom API wurde erfolgreich getestet!")
                    elif status == 'unauthorized':
                        self.custom_api_status_label.setText("Status: Ungültiger API-Key")
                        QMessageBox.warning(self, "Authentifizierung fehlgeschlagen",
                                          "Der API-Key ist ungültig oder hat keine Berechtigung.")
                    else:
                        self.custom_api_status_label.setText(f"Status: {status}")
                        QMessageBox.warning(self, "Unerwarteter Status",
                                          f"Die API antwortete mit Status: {status}")
                except ValueError:
                    # Response ist kein JSON
                    self.custom_api_status_label.setText("Status: Verbunden ✓")

                    # Verbinde auch den tatsächlichen Client, falls parent verfügbar ist
                    if hasattr(self.parent(), 'custom_api_client'):
                        self.parent().custom_api_client.connect()
                        self.log.info("Custom API Client wurde erfolgreich verbunden")

                        # Aktualisiere den aktiven Cloud-Client im MainWindow
                        if hasattr(self.parent(), 'active_cloud_client'):
                            self.parent().active_cloud_client = self.parent().custom_api_client
                            self.log.info("Aktiver Cloud-Client wurde auf Custom API gesetzt")

                            # Aktualisiere auch den UploaderThread
                            if hasattr(self.parent(), 'uploader_thread'):
                                self.parent().uploader_thread.client = self.parent().custom_api_client
                                self.log.info("UploaderThread wurde aktualisiert")

                            # Aktualisiere das Status-Light im MainWindow
                            if hasattr(self.parent(), 'update_status_light'):
                                self.parent().update_status_light()
                                self.log.info("Status-Light wurde aktualisiert")

                    QMessageBox.information(self, "Verbindung erfolgreich",
                                          "Die Verbindung zur Custom API wurde erfolgreich getestet!")
            else:
                self.custom_api_status_label.setText(f"Status: Fehler HTTP {response.status_code}")
                QMessageBox.warning(self, "Verbindungsfehler",
                                  f"Die API antwortete mit HTTP {response.status_code}")

        except requests.exceptions.ConnectionError:
            self.custom_api_status_label.setText("Status: Verbindungsfehler")
            QMessageBox.critical(self, "Verbindungsfehler",
                               "Konnte keine Verbindung zur API herstellen.\nBitte prüfen Sie die URL.")
        except requests.exceptions.Timeout:
            self.custom_api_status_label.setText("Status: Timeout")
            QMessageBox.warning(self, "Timeout",
                              "Die Verbindung zur API hat zu lange gedauert.")
        except Exception as e:
            self.custom_api_status_label.setText(f"Status: Fehler")
            QMessageBox.critical(self, "Fehler", f"Ein Fehler ist aufgetreten:\n{str(e)}")
        finally:
            self.custom_api_connect_button.setEnabled(True)


    def update_connect_button_state(self, is_connected=None):
        """
        Akzeptiert einen optionalen Status, um API-Calls zu vermeiden.
        Wenn 'is_connected' None ist (z.B. bei textChanged), wird der Status neu abgerufen.
        """
        if is_connected is None:
            # Wird nur noch von textChanged aufgerufen, nicht mehr beim Start
            is_connected = self.client.get_connection_status() == "Verbunden"

        if is_connected:
            self.db_connect_button.setEnabled(True)
        else:
            # Nur aktivieren, wenn Key und Secret ausgefüllt sind
            key_ok = bool(self.db_app_key_edit.text())
            secret_ok = bool(self.db_app_secret_edit.text())
            self.db_connect_button.setEnabled(key_ok and secret_ok)

    def toggle_dropbox_connection(self):
        """Startet den Verbindungs- oder Trennungsvorgang."""
        if self.client.get_connection_status() == "Verbunden":
            # Verbindung trennen
            self.client.disconnect()
            self.update_dropbox_status()
        else:
            # Verbindung herstellen
            self.log.info("Starte Dropbox-Verbindungsvorgang...")

            # Zuerst die eingegebenen Keys speichern
            self.config.save_secret("db_app_key", self.db_app_key_edit.text())
            self.config.save_secret("db_app_secret", self.db_app_secret_edit.text())

            self.db_status_label.setText("Status: Warte auf OAuth...")
            self.db_connect_button.setEnabled(False)

            # Client 'connect' aufrufen und das Callback für den Auth-Code übergeben
            success = self.client.connect(auth_callback=self.get_dropbox_auth_code)

            if success:
                self.log.info("Dropbox-Verbindung erfolgreich hergestellt.")
                # Anforderung: Nach erfolgreicher Verbindung Monitoring starten
                # signals.monitoring_status_changed.emit(True)  # Signal zum Starten
            else:
                self.log.warning("Dropbox-Verbindung fehlgeschlagen.")

            self.update_dropbox_status()

    def get_dropbox_auth_code(self, authorize_url):
        """
        Callback-Funktion für den Dropbox-Client.
        Öffnet den Browser und fragt den Benutzer nach dem Code.
        """
        # 1. Benutzer informieren und Browser öffnen
        QMessageBox.information(self, "Dropbox-Authentifizierung",
                                "Ein Browser-Fenster wird nun geöffnet, um die App zu autorisieren.\n\n"
                                "Bitte kopieren Sie den angezeigten Code und fügen Sie ihn im nächsten Dialog ein.")
        QDesktopServices.openUrl(QUrl(authorize_url))

        # 2. Auf Code-Eingabe warten
        code, ok = QInputDialog.getText(self, "Dropbox-Authentifizierung", "Eingabe-Code von Dropbox:")

        if ok and code:
            return code.strip()
        else:
            return None

    # Slot für den Klick auf "Updates prüfen"
    @Slot()
    def on_check_for_updates_clicked(self):
        """
        Startet die Update-Prüfung im Hauptfenster und informiert den Benutzer,
        dass das Ergebnis (aufgrund des modalen Dialogs) eventuell erst
        nach dem Schließen des Dialogs erscheint.
        """
        self.log.info("Manuelle Update-Prüfung initialisiert.")

        # Deaktiviere Button, um doppeltes Klicken zu verhindern
        self.update_check_button.setEnabled(False)
        self.update_check_button.setText("Prüfe...")

        # Rufe die Methode des Parents auf (MainWindow.check_for_updates_manual)
        if self.parent() and hasattr(self.parent(), 'check_for_updates_manual'):
            self.parent().check_for_updates_manual()

    # Slot, der das Signal vom MainWindow empfängt
    @Slot(str)
    def on_update_check_finished(self, status_message):
        """Aktualisiert das Update-Status-Label, während der Dialog geöffnet ist."""
        self.log.debug(f"Empfange Update-Status im Einstellungsdialog: {status_message}")
        self.update_status_label.setText(f"Update-Status: <b>{status_message}</b>")

        # Button wieder aktivieren
        self.update_check_button.setEnabled(True)
        self.update_check_button.setText("Jetzt auf Updates prüfen")

    def load_switchable_versions(self):
        """Lädt alle stabilen, wechselbaren Versionen von GitHub."""
        self.version_combo.clear()
        self.version_combo.addItem("Lade Versionen...")
        self.version_combo.setEnabled(False)
        self.version_notes_edit.setPlainText("Release-Notes werden geladen...")
        self.version_switch_button.setEnabled(False)
        self.refresh_versions_button.setEnabled(False)
        include_prereleases = self.show_prereleases_check.isChecked()

        initialize_version_list_loader(
            self,
            self.on_switchable_versions_loaded,
            self.on_switchable_versions_error,
            include_prereleases=include_prereleases
        )

    @Slot(list)
    def on_switchable_versions_loaded(self, releases):
        """Füllt die Versionsauswahl mit den gefilterten Releases."""
        self._switchable_versions = releases or []
        self.version_combo.clear()

        if not self._switchable_versions:
            self.version_combo.addItem("Keine passenden Versionen gefunden")
            self.version_notes_edit.setPlainText("Es wurden keine Versionen gefunden.")
            self.version_combo.setEnabled(False)
            self.version_switch_button.setEnabled(False)
            self.refresh_versions_button.setEnabled(True)
            return

        current_str = self.app_version.lstrip("v")
        for release in self._switchable_versions:
            tag_name = release.get("tag_name", "")
            release_str = release.get("version_str", tag_name.lstrip("v"))
            is_prerelease = release.get("is_prerelease", False)
            label = tag_name
            if is_prerelease:
                label = f"{label} (Prerelease)"
            if release_str == current_str:
                label = f"{label} (aktuell)"
            self.version_combo.addItem(label, release)

        self.version_combo.setEnabled(True)
        self.refresh_versions_button.setEnabled(True)
        self.on_version_selected(self.version_combo.currentIndex())

    @Slot(str)
    def on_switchable_versions_error(self, message):
        """Zeigt Ladefehler für die Versionsliste an."""
        self.version_combo.clear()
        self.version_combo.addItem("Fehler beim Laden")
        self.version_combo.setEnabled(False)
        self.version_notes_edit.setPlainText(message)
        self.version_switch_button.setEnabled(False)
        self.refresh_versions_button.setEnabled(True)
        QMessageBox.warning(self, "Versionsliste", message)

    @Slot(int)
    def on_version_selected(self, index):
        """Aktualisiert Notes und Button-Status zur ausgewählten Version."""
        if index < 0:
            self.version_notes_edit.setPlainText("")
            self.version_switch_button.setEnabled(False)
            return

        release = self.version_combo.itemData(index)
        if not isinstance(release, dict):
            self.version_notes_edit.setPlainText("")
            self.version_switch_button.setEnabled(False)
            return

        notes = release.get("release_notes") or "Keine Release-Notes verfügbar."
        self.version_notes_edit.setMarkdown(notes)

        selected_version = release.get("version_str", "").lstrip("v")
        current_version = self.app_version.lstrip("v")
        self.version_switch_button.setEnabled(bool(release.get("installer_url")) and selected_version != current_version)

    @Slot()
    def on_switch_version_clicked(self):
        """Startet den Wechsel zur ausgewählten Version."""
        index = self.version_combo.currentIndex()
        release = self.version_combo.itemData(index)
        if not isinstance(release, dict):
            return

        installer_url = release.get("installer_url", "")
        target_tag = release.get("tag_name", "")
        target_version_str = release.get("version_str", target_tag.lstrip("v"))
        if not installer_url:
            QMessageBox.warning(self, "Versionswechsel", "Für diese Version wurde kein Installer gefunden.")
            return

        try:
            current_version = version.parse(self.app_version.lstrip("v"))
            target_version = version.parse(target_version_str.lstrip("v"))
        except Exception:
            QMessageBox.warning(self, "Versionswechsel", "Die Version konnte nicht ausgewertet werden.")
            return

        if target_version == current_version:
            QMessageBox.information(self, "Versionswechsel", "Diese Version ist bereits installiert.")
            return

        if target_version < current_version:
            reply = QMessageBox.warning(
                self,
                "Downgrade bestätigen",
                (
                    f"Sie wechseln von Version {self.app_version} auf die ältere Version {target_tag}.\n\n"
                    "Achtung: Neuere Einstellungen oder Daten können inkompatibel sein.\n"
                    "Möchten Sie trotzdem fortfahren?"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
        else:
            reply = QMessageBox.question(
                self,
                "Versionswechsel bestätigen",
                f"Möchten Sie wirklich auf Version {target_tag} wechseln?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )

        if reply != QMessageBox.StandardButton.Yes:
            return

        self.log.info("Starte manuellen Versionswechsel auf %s", target_tag)
        progress_dialog = UpdateProgressDialog(self.parent(), installer_url)
        progress_dialog.exec()

    @Slot(bool)
    def on_show_prereleases_toggled(self, checked):
        """Lädt die Versionen neu, wenn die Prerelease-Option geändert wird."""
        if checked:
            self.version_hint_label.setText("Verfügbare stabile Versionen und Prereleases")
        else:
            self.version_hint_label.setText("Verfügbare stabile Versionen")
        self.load_switchable_versions()


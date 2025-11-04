import logging
import requests
from PySide6.QtWidgets import (
    QDialog, QTabWidget, QWidget, QVBoxLayout, QFormLayout,
    QLineEdit, QPushButton, QFileDialog, QSpinBox, QLabel,
    QRadioButton, QButtonGroup, QGroupBox, QMessageBox, QInputDialog,
    QCheckBox
)
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl, Slot
from core.config import ConfigManager
from services.base_client import BaseClient


class SettingsDialog(QDialog):
    """
    Der Einstellungsdialog, der dem Benutzer die Konfiguration
    der Anwendung in Tabs (Allgemein, Dropbox, E-Mail, SMS) ermöglicht.
    """

    def __init__(self, config_manager: ConfigManager, client: BaseClient,
                 app_version: str, latest_version_info: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Einstellungen")
        self.setMinimumWidth(500)

        self.config = config_manager
        self.client = client
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
        self.tabs.addTab(self.create_email_tab(), "E-Mail (SMTP)")
        self.tabs.addTab(self.create_sms_tab(), "SMS-Dienst")

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

        # 3. Initiale Sichtbarkeit der Cloud-Gruppen setzen
        self.on_cloud_service_changed()

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
        monitor_layout.addRow("Archiv-Ordner (für 'erfolgreich' / 'fehler'):",
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

        layout.addWidget(monitor_group)

        # --- Gruppe 2: Software-Update ---
        update_group = QGroupBox("Software-Update")
        update_layout = QFormLayout(update_group)  # QFormLayout, damit der Button eine ganze Zeile einnimmt

        # Aktuelle Version anzeigen
        self.current_version_label = QLabel(f"Aktuell installierte Version: <b>{self.app_version}</b>")
        update_layout.addRow(self.current_version_label)

        # Letzten bekannten Update-Status anzeigen
        self.update_status_label = QLabel(f"Update-Status: <b>{self.latest_version_info}</b>")
        update_layout.addRow(self.update_status_label)

        # Update-Button
        self.update_check_button = QPushButton("Jetzt auf Updates prüfen")

        # Verbinde mit der 'check_for_updates_manual'-Methode des Parent-Widgets (MainWindow)
        if self.parent() and hasattr(self.parent(), 'check_for_updates_manual'):
            self.update_check_button.clicked.connect(self.on_check_for_updates_clicked)
        else:
            # Fallback, falls die Methode nicht gefunden wird
            self.update_check_button.setEnabled(False)
            self.update_check_button.setToolTip("Konnte keine Update-Funktion im Hauptfenster finden.")

        update_layout.addRow(self.update_check_button)
        layout.addWidget(update_group)

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

        # --- SkyLink Shortener Einstellungen ---
        self.skylink_group = QGroupBox("SkyLink API (Link Shortener)")
        skylink_layout = QFormLayout()

        self.skylink_url_edit = QLineEdit()
        self.skylink_url_edit.setPlaceholderText("z.B. https://skydive.de/api/create")
        skylink_layout.addRow("SkyLink API URL:", self.skylink_url_edit)
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

        # Verbindungs-Steuerung für Custom API
        self.custom_api_connect_button = QPushButton("Mit Custom API verbinden")
        self.custom_api_connect_button.clicked.connect(self.toggle_custom_api_connection)
        self.custom_api_status_label = QLabel("Status: Unbekannt")
        custom_api_layout.addRow(self.custom_api_connect_button, self.custom_api_status_label)

        self.custom_api_group.setLayout(custom_api_layout)
        layout.addWidget(self.custom_api_group)


        self.skylink_key_edit = QLineEdit()
        self.skylink_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        skylink_layout.addRow("SkyLink API Key:", self.skylink_key_edit)

        self.skylink_group.setLayout(skylink_layout)
        layout.addWidget(self.skylink_group)

        layout.addStretch(1)  # Schiebt alles nach oben

        return widget

    def create_email_tab(self):
        """Erstellt den Tab 'E-Mail (SMTP)'."""
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

        layout.addWidget(sender_group)

        layout.addStretch(1)  # Schiebt alles nach oben

        return widget

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

        self.seven_group.setLayout(seven_layout)
        layout.addWidget(self.seven_group)

        layout.addStretch(1)  # Füllt den restlichen Platz nach unten auf

        return widget

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

        # Dropbox
        self.db_app_key_edit.setText(self.config.get_secret("db_app_key"))
        self.db_app_secret_edit.setText(self.config.get_secret("db_app_secret"))

        # SkyLink
        self.skylink_url_edit.setText(self.config.get_secret("skylink_api_url"))
        self.skylink_key_edit.setText(self.config.get_secret("skylink_api_key"))

        # Custom API
        self.custom_api_url_edit.setText(self.config.get_secret("custom_api_url"))
        self.custom_api_token_edit.setText(self.config.get_secret("custom_api_bearer_token"))
        self.custom_api_upload_endpoint_edit.setText(self.config.get_setting("custom_api_upload_endpoint", "/upload"))
        self.custom_api_share_endpoint_edit.setText(self.config.get_setting("custom_api_share_endpoint", "/share"))
        self.custom_api_health_endpoint_edit.setText(self.config.get_setting("custom_api_health_endpoint", "/health"))

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

            # Dropbox (Key/Secret werden nur gespeichert, nicht der Token)
            self.config.save_secret("db_app_key", self.db_app_key_edit.text())
            self.config.save_secret("db_app_secret", self.db_app_secret_edit.text())

            # SkyLink
            # Wir verwenden save_secret, da der LinkShortener get_secret erwartet.
            self.config.save_secret("skylink_api_url", self.skylink_url_edit.text())
            self.config.save_secret("skylink_api_key", self.skylink_key_edit.text())

            # Custom API
            self.config.save_secret("custom_api_url", self.custom_api_url_edit.text())
            self.config.save_secret("custom_api_bearer_token", self.custom_api_token_edit.text())
            self.config.save_setting("custom_api_upload_endpoint", self.custom_api_upload_endpoint_edit.text())
            self.config.save_setting("custom_api_share_endpoint", self.custom_api_share_endpoint_edit.text())
            self.config.save_setting("custom_api_health_endpoint", self.custom_api_health_endpoint_edit.text())

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
            self.skylink_group.setVisible(True)  # SkyLink wird für beide angezeigt
            self.log.info("Cloud-Dienst gewechselt zu: Dropbox")
        elif self.radio_custom_api.isChecked():
            self.dropbox_group.setVisible(False)
            self.custom_api_group.setVisible(True)
            self.skylink_group.setVisible(True)  # SkyLink wird für beide angezeigt
            self.log.info("Cloud-Dienst gewechselt zu: Custom API")

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
        self.config.save_setting("custom_api_upload_endpoint",
                                self.custom_api_upload_endpoint_edit.text() or "/upload")
        self.config.save_setting("custom_api_share_endpoint",
                                self.custom_api_share_endpoint_edit.text() or "/share")
        self.config.save_setting("custom_api_health_endpoint",
                                self.custom_api_health_endpoint_edit.text() or "/health")

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


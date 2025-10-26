import logging
from PySide6.QtWidgets import (
    QDialog, QTabWidget, QWidget, QVBoxLayout, QFormLayout,
    QLineEdit, QPushButton, QFileDialog, QSpinBox, QLabel,
    QRadioButton, QButtonGroup, QGroupBox, QMessageBox, QInputDialog,
    QCheckBox
)
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl
from core.config import ConfigManager
from services.base_client import BaseClient
from core.signals import signals


class SettingsDialog(QDialog):
    """
    Der Einstellungsdialog, der dem Benutzer die Konfiguration
    der Anwendung in Tabs (Allgemein, Dropbox, E-Mail, SMS) ermöglicht.
    """

    def __init__(self, config_manager: ConfigManager, client: BaseClient, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Einstellungen")
        self.setMinimumWidth(500)

        self.config = config_manager
        self.client = client
        self.log = logging.getLogger(__name__)

        # Hauptlayout
        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Tabs erstellen
        self.tabs.addTab(self.create_general_tab(), "Allgemein")
        self.tabs.addTab(self.create_cloud_tab(), "Cloud-Dienst")
        self.tabs.addTab(self.create_email_tab(), "E-Mail (SMTP)")
        self.tabs.addTab(self.create_sms_tab(), "SMS-Dienst")  # Geändert

        # Standard-Buttons (Speichern, Abbrechen)
        button_layout = QVBoxLayout()  # Eigener Layout-Container für Buttons
        self.save_button = QPushButton("Speichern")
        self.save_button.clicked.connect(self.save_settings)
        button_layout.addWidget(self.save_button)
        main_layout.addLayout(button_layout)

        # Einstellungen laden, wenn der Dialog geöffnet wird
        self.load_settings()

    # --- Tab-Erstellung ---

    def create_general_tab(self):
        """Erstellt den Tab 'Allgemein'."""
        widget = QWidget()
        layout = QFormLayout(widget)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        # Überwachungsordner
        self.monitor_path_edit = QLineEdit()
        self.monitor_path_button = QPushButton("Durchsuchen...")
        self.monitor_path_button.clicked.connect(lambda: self.select_directory(self.monitor_path_edit))
        layout.addRow("Zu überwachender Ordner:",
                      self.create_path_widget(self.monitor_path_edit, self.monitor_path_button))

        # Archivordner
        self.archive_path_edit = QLineEdit()
        self.archive_path_button = QPushButton("Durchsuchen...")
        self.archive_path_button.clicked.connect(lambda: self.select_directory(self.archive_path_edit))
        layout.addRow("Archiv-Ordner (für 'erfolgreich' / 'fehler'):",
                      self.create_path_widget(self.archive_path_edit, self.archive_path_button))

        # Log-Ordner
        self.log_path_edit = QLineEdit()
        self.log_path_button = QPushButton("Durchsuchen...")
        self.log_path_button.clicked.connect(lambda: self.select_directory(self.log_path_edit))
        layout.addRow("Log-Datei-Ordner:", self.create_path_widget(self.log_path_edit, self.log_path_button))

        # Scan-Intervall
        self.scan_interval_spin = QSpinBox()
        self.scan_interval_spin.setRange(5, 3600)
        self.scan_interval_spin.setSuffix(" Sekunden")
        layout.addRow("Scan-Intervall:", self.scan_interval_spin)

        return widget

    def create_cloud_tab(self):
        """Erstellt den Tab 'Cloud-Dienst'."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Auswahl des Dienstes (momentan nur Dropbox)
        service_group = QGroupBox("Cloud-Dienst auswählen")
        service_layout = QVBoxLayout()
        self.radio_dropbox = QRadioButton("Dropbox")
        self.radio_dropbox.setChecked(True)  # Standard
        self.radio_group = QButtonGroup()
        self.radio_group.addButton(self.radio_dropbox)
        service_layout.addWidget(self.radio_dropbox)
        service_group.setLayout(service_layout)
        layout.addWidget(service_group)

        # Dropbox-Einstellungen
        self.dropbox_group = QGroupBox("Dropbox-Einstellungen")
        db_layout = QFormLayout()

        self.db_app_key_edit = QLineEdit()
        self.db_app_key_edit.textChanged.connect(self.update_connect_button_state)
        db_layout.addRow("App Key:", self.db_app_key_edit)

        self.db_app_secret_edit = QLineEdit()
        self.db_app_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.db_app_secret_edit.textChanged.connect(self.update_connect_button_state)
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

        # --- SkyLink Shortener Einstellungen (NEUE GRUPPE) ---
        self.skylink_group = QGroupBox("SkyLink API (Link Shortener)")
        skylink_layout = QFormLayout()

        self.skylink_url_edit = QLineEdit()
        self.skylink_url_edit.setPlaceholderText("z.B. https://skydive.de/api/create")
        skylink_layout.addRow("SkyLink API URL:", self.skylink_url_edit)

        self.skylink_key_edit = QLineEdit()
        self.skylink_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        skylink_layout.addRow("SkyLink API Key:", self.skylink_key_edit)

        self.skylink_group.setLayout(skylink_layout)
        layout.addWidget(self.skylink_group)
        # --- ENDE ---

        # Initialen Status setzen
        self.update_dropbox_status()
        self.update_connect_button_state()

        return widget

    def create_email_tab(self):
        """Erstellt den Tab 'E-Mail (SMTP)'."""
        widget = QWidget()
        layout = QFormLayout(widget)

        self.smtp_host_edit = QLineEdit()
        layout.addRow("SMTP-Host:", self.smtp_host_edit)

        self.smtp_port_edit = QSpinBox()
        self.smtp_port_edit.setRange(1, 65535)
        self.smtp_port_edit.setValue(587)
        layout.addRow("SMTP-Port:", self.smtp_port_edit)

        self.smtp_user_edit = QLineEdit()
        layout.addRow("Benutzername:", self.smtp_user_edit)

        self.smtp_pass_edit = QLineEdit()
        self.smtp_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addRow("Passwort:", self.smtp_pass_edit)

        layout.addRow(QLabel("---"))

        self.smtp_sender_addr_edit = QLineEdit()
        layout.addRow("Absender-Adresse:", self.smtp_sender_addr_edit)

        self.smtp_sender_name_edit = QLineEdit()
        layout.addRow("Absender-Name:", self.smtp_sender_name_edit)

        self.smtp_fallback_recipient_edit = QLineEdit()
        layout.addRow("Fallback-Empfänger (für Status-Mails):", self.smtp_fallback_recipient_edit)

        return widget

    def create_sms_tab(self):
        """Erstellt den Tab 'SMS (Seven.io)'."""
        widget = QWidget()
        # Hauptlayout ist QVBoxLayout, um Gruppen zu stapeln
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

        # TODO: Später hier Logik hinzufügen, um Gruppen basierend auf
        # self.sms_radio_group Auswahl ein-/auszublenden

        layout.addStretch(1) # Füllt den restlichen Platz nach unten auf

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

    def save_settings(self):
        """Speichert alle Einstellungen aus den GUI-Feldern im ConfigManager."""
        self.log.info("Speichere Einstellungen...")
        # Allgemein
        self.config.save_setting("monitor_path", self.monitor_path_edit.text())
        self.config.save_setting("archive_path", self.archive_path_edit.text())
        self.config.save_setting("log_file_path", self.log_path_edit.text())
        self.config.save_setting("scan_interval", self.scan_interval_spin.value())

        # Dropbox (Key/Secret werden nur gespeichert, nicht der Token)
        self.config.save_secret("db_app_key", self.db_app_key_edit.text())
        self.config.save_secret("db_app_secret", self.db_app_secret_edit.text())

        # SkyLink
        # Wir verwenden save_secret, da der dropbox_uploader get_secret erwartet.
        self.config.save_secret("skylink_api_url", self.skylink_url_edit.text())
        self.config.save_secret("skylink_api_key", self.skylink_key_edit.text())

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

        self.log.info("Einstellungen gespeichert.")
        self.accept()  # Schließt den Dialog mit "OK"

    # --- Dropbox-Verbindungslogik ---

    def update_dropbox_status(self):
        """Aktualisiert die GUI (Button-Text, Status-Label) basierend auf dem Client-Status."""
        status = self.client.get_connection_status()
        self.db_status_label.setText(f"Status: {status}")

        if status == "Verbunden":
            self.db_connect_button.setText("Verbindung trennen")
            self.db_app_key_edit.setEnabled(False)
            self.db_app_secret_edit.setEnabled(False)
        else:
            self.db_connect_button.setText("Mit Dropbox verbinden")
            self.db_app_key_edit.setEnabled(True)
            self.db_app_secret_edit.setEnabled(True)

        self.update_connect_button_state()

    def update_connect_button_state(self):
        """Aktiviert/Deaktiviert den 'Verbinden'-Button."""
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


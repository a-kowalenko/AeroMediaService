from PySide6.QtCore import QSettings, Signal, QObject
import keyring
import logging

# Dienstname für Keyring
KEYRING_SERVICE_NAME = "DropboxUploaderApp"


class ConfigManager(QObject):
    """
    Verwaltet die Anwendungs-Einstellungen.
    - Nicht-sensible Daten werden in QSettings gespeichert.
    - Sensible Daten (Passwörter, Token) werden in keyring gespeichert.
    """

    # Signal wird ausgelöst, wenn Einstellungen gespeichert werden
    settings_changed = Signal()

    def __init__(self):
        super().__init__()
        # QSettings verwendet system-spezifische Speicherorte
        # (z.B. Registry in Windows, .plist in macOS, .conf in Linux)
        self.settings = QSettings()
        self.log = logging.getLogger(__name__)

    # --- Nicht-sensible Daten (QSettings) ---

    def save_setting(self, key, value):
        """Speichert eine nicht-sensible Einstellung."""
        self.log.debug(f"Speichere Einstellung: {key}")
        self.settings.setValue(key, value)
        self.settings_changed.emit()

    def get_setting(self, key, default=None):
        """Ruft eine nicht-sensible Einstellung ab."""
        return self.settings.value(key, default)

    # --- Sensible Daten (Keyring) ---

    def save_secret(self, key, value):
        """Speichert ein Geheimnis sicher im System-Keyring."""
        try:
            keyring.set_password(KEYRING_SERVICE_NAME, key, value)
            self.log.debug(f"Geheimnis für '{key}' sicher gespeichert.")
            self.settings_changed.emit()
        except Exception as e:
            self.log.error(f"Fehler beim Speichern des Geheimnisses für '{key}': {e}")

    def get_secret(self, key):
        """Ruft ein Geheimnis sicher aus dem System-Keyring ab."""
        try:
            secret = keyring.get_password(KEYRING_SERVICE_NAME, key)
            if secret:
                self.log.debug(f"Geheimnis für '{key}' geladen.")
            else:
                self.log.debug(f"Kein Geheimnis für '{key}' gefunden.")
            return secret
        except Exception as e:
            self.log.error(f"Fehler beim Abrufen des Geheimnisses für '{key}': {e}")
            return None

    def delete_secret(self, key):
        """Löscht ein Geheimnis sicher aus dem System-Keyring."""
        try:
            keyring.delete_password(KEYRING_SERVICE_NAME, key)
            self.log.debug(f"Geheimnis für '{key}' gelöscht.")
            self.settings_changed.emit()
        except keyring.errors.PasswordDeleteError:
            self.log.warning(f"Geheimnis für '{key}' konnte nicht gelöscht werden (evtl. nicht vorhanden).")
        except Exception as e:
            self.log.error(f"Fehler beim Löschen des Geheimnisses für '{key}': {e}")

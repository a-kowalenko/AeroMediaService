from abc import ABC, abstractmethod

class BaseClient(ABC):
    """
    Abstrakte Basisklasse für alle Cloud-Storage-Clients.
    Definiert die Schnittstelle, die ein Client implementieren muss.
    """

    @abstractmethod
    def connect(self, auth_callback=None):
        """
        Stellt die Verbindung zum Dienst her.
        'auth_callback' kann eine Funktion sein, die den Benutzer
        nach einem Auth-Code fragt.
        Gibt True bei Erfolg, False bei Misserfolg zurück.
        """
        pass

    @abstractmethod
    def disconnect(self):
        """Trennt die Verbindung und löscht lokale Tokens."""
        pass

    @abstractmethod
    def get_connection_status(self):
        """Gibt einen String zurück, der den aktuellen Verbindungsstatus beschreibt."""
        pass

    @abstractmethod
    def upload_directory(self, local_dir_path, remote_base_path):
        """
        Lädt ein komplettes Verzeichnis hoch.
        Muss Signale aus core.signals verwenden, um den Fortschritt zu melden.
        Gibt True bei Erfolg, False bei Misserfolg zurück.
        """
        pass

    @abstractmethod
    def get_shareable_link(self, remote_path):
        """
        Erstellt einen öffentlichen Freigabelink für einen Pfad.
        Gibt den Link-String oder None bei einem Fehler zurück.
        """
        pass

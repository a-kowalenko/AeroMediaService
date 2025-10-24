import sys
from PySide6.QtWidgets import QApplication
from app import MainWindow

# Haupt-Einstiegspunkt der Anwendung
if __name__ == "__main__":
    # Erstellt die Qt-Anwendung
    app = QApplication(sys.argv)

    # Setzt den Anwendungsnamen und die Organisation,
    # was QSettings für die Speicherung verwendet.
    app.setOrganizationName("AKSoftware")
    app.setApplicationName("AeroMediaService")

    # Erstellt und zeigt das Hauptfenster
    window = MainWindow()
    window.show()

    # Startet die Event-Schleife der Anwendung
    sys.exit(app.exec())

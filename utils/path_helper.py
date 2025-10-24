import sys
import os


def get_resource_path(relative_path):
    """
    Ermittelt den korrekten Pfad zu einer Ressource,
    egal ob im IDE-Modus, als --onefile EXE oder als --onedir EXE.
    """
    if getattr(sys, 'frozen', False):
        # Fall 1: App ist "eingefroren" (gepackt von PyInstaller)

        if hasattr(sys, '_MEIPASS'):
            # Fall 1a: --onefile Bundle
            base_path = sys._MEIPASS
        else:
            # Fall 1b: --onedir Bundle
            base_path = os.path.dirname(os.path.abspath(sys.executable))

    else:
        # Fall 2: App läuft im IDE-Modus (nicht "eingefroren")
        # Annahme: Diese Datei (path_helper.py) liegt in /utils/
        # Das Haupt-Projektverzeichnis ist eine Ebene höher
        base_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

    return os.path.join(base_path, relative_path)

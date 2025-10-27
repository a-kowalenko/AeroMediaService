import os

# Pfad zum Stammverzeichnis des Projekts (eine Ebene über 'core')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERSION_FILE = os.path.join(BASE_DIR, 'VERSION.txt')

try:
    # 'utf-8-sig' verwenden, um ein BOM (ï»¿) automatisch zu entfernen
    with open(VERSION_FILE, 'r', encoding='utf-8-sig') as f:
        APP_VERSION = f.read().strip()
except FileNotFoundError:
    print(f"WARNUNG: {VERSION_FILE} nicht gefunden. Verwende Standardversion.")
    APP_VERSION = "0.0.0-dev"
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Vollständiger Build-Prozess für Aero Media Service
Verwendung:
    python build.py              # Nur PyInstaller Build (Build-Version +1)
    python build.py setup        # PyInstaller + NSIS Installer (Build-Version +1)
    python build.py minor        # Nur PyInstaller (Minor-Version +1)
    python build.py minor setup  # PyInstaller + Installer (Minor-Version +1)
    python build.py patch setup  # PyInstaller + Installer (Patch-Version +1)
    python build.py major setup  # PyInstaller + Installer (Major-Version +1)
"""
import os
import sys
import subprocess
import shutil
from pathlib import Path

def bump_version(level="build"):
    """
    Liest VERSION.txt, erhöht die gewählte Versionskomponente und schreibt sie zurück.
    """
    version_file = Path("VERSION.txt")
    version = version_file.read_text(encoding="utf-8").strip()
    parts = [int(p) for p in version.split(".")]
    while len(parts) < 4:
        parts.append(0)

    major, minor, patch, build = parts

    if level == "major":
        major += 1
        minor = patch = build = 0
    elif level == "minor":
        minor += 1
        patch = build = 0
    elif level == "patch":
        patch += 1
        build = 0
    elif level == "build":
        build += 1
    else:
        raise ValueError(f"Unknown level '{level}', use: major | minor | patch | build")

    new_version = f"{major}.{minor}.{patch}.{build}"
    version_file.write_text(new_version + "\n", encoding="utf-8")
    print(f"   Version: {version} → {new_version}")
    return new_version

def update_version_info():
    """
    Aktualisiert version_info.txt mit der aktuellen Version aus VERSION.txt
    Diese Datei wird von PyInstaller für Windows-Metadaten verwendet.
    """
    version_file = Path("VERSION.txt")
    version_str = version_file.read_text(encoding="utf-8").strip()
    version_parts = tuple(int(x) for x in version_str.split('.'))

    while len(version_parts) < 4:
        version_parts = version_parts + (0,)

    version_info_content = f'''# UTF-8
#
# Version Information für Aero Media Service
# Diese Datei wird von PyInstaller in die .exe eingebettet
# AUTOMATISCH GENERIERT - Nicht manuell bearbeiten!
#

VSVersionInfo(
  ffi=FixedFileInfo(
    filevers={version_parts},
    prodvers={version_parts},
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo(
      [
      StringTable(
        u'040704B0',
        [StringStruct(u'CompanyName', u'Andreas Kowalenko'),
        StringStruct(u'FileDescription', u'Aero Media Service'),
        StringStruct(u'FileVersion', u'{version_str}'),
        StringStruct(u'InternalName', u'AeroMediaService'),
        StringStruct(u'LegalCopyright', u'Copyright © 2026 Andreas Kowalenko'),
        StringStruct(u'OriginalFilename', u'Aero Media Service.exe'),
        StringStruct(u'ProductName', u'Aero Media Service'),
        StringStruct(u'ProductVersion', u'{version_str}')])
      ]
    ),
    VarFileInfo([VarStruct(u'Translation', [0x0407, 0x04B0])])
  ]
)
'''
    version_info_file = Path("version_info.txt")
    version_info_file.write_text(version_info_content, encoding="utf-8")
    print(f"   ✅ version_info.txt aktualisiert mit Version {version_str}")

def find_makensis():
    """Findet makensis.exe in üblichen Installationspfaden"""
    possible_paths = [
        r"C:\Program Files (x86)\NSIS\makensis.exe",
        r"C:\Program Files\NSIS\makensis.exe",
        shutil.which("makensis"),
    ]

    for path in possible_paths:
        if path and Path(path).exists():
            return path

    return None

def main():
    print("=" * 70)
    print("🚀 Aero Media Service - Build-Prozess")
    print("=" * 70)
    print()

    args = [arg.lower() for arg in sys.argv[1:]]
    create_installer = 'setup' in args

    build_levels = [arg for arg in args if arg != 'setup']
    level = build_levels[0] if build_levels else "build"

    valid_levels = ["build", "minor", "patch", "major"]
    if level not in valid_levels:
        print(f"❌ Ungültiger Build-Level: '{level}'")
        print(f"   Gültige Werte: {', '.join(valid_levels)}")
        return 1

    print(f"📋 Build-Level: {level}")
    print(f"📦 Installer erstellen: {'Ja' if create_installer else 'Nein'}")
    print()

    print("📋 Aktualisiere Version...")
    new_version = bump_version(level)
    update_version_info()
    print()

    print("🧹 Bereinige alte Build-Dateien...")
    for d in ["build", "dist"]:
        if os.path.exists(d):
            shutil.rmtree(d)

    # Bereinige alte Installer
    for f in Path(".").glob("AeroMediaService_Installer_*.exe"):
        f.unlink()

    total_steps = 2 if create_installer else 1
    print(f"🔨 Schritt 1/{total_steps}: PyInstaller Build")
    print("-" * 70)

    try:
        subprocess.run(["pyinstaller", "Aero Media Service.spec"], check=True)
        print()
        print("✅ PyInstaller Build erfolgreich!")
        print()
    except subprocess.CalledProcessError as e:
        print()
        print(f"❌ PyInstaller Build fehlgeschlagen: {e}")
        return 1

    if not create_installer:
        print("ℹ️  NSIS Installer wird NICHT erstellt (kein 'setup' Parameter)")
        print("   Zum Erstellen des Installers verwenden Sie: python build.py setup")
        print()
    else:
        print(f"📦 Schritt 2/{total_steps}: NSIS Installer erstellen")
        print("-" * 70)

        makensis_path = find_makensis()

        if not makensis_path:
            print("⚠️  NSIS (makensis.exe) nicht gefunden!")
            print()
            print("Der PyInstaller Build war erfolgreich, aber der Installer konnte")
            print("nicht erstellt werden.")
            return 0

        print(f"NSIS gefunden: {makensis_path}")

        try:
            result = subprocess.run(
                [makensis_path, "installer.nsi"],
                capture_output=True,
                text=True,
                encoding='cp850'
            )

            if result.returncode == 0:
                print()
                print("✅ NSIS Installer erfolgreich erstellt!")
                print()
                version = Path("VERSION.txt").read_text(encoding="utf-8").strip()
                installer_name = f"AeroMediaService_Installer_v{version}.exe"
                if Path(installer_name).exists():
                    installer_size = Path(installer_name).stat().st_size / (1024 * 1024)
                    print(f"📦 Installer: {installer_name} ({installer_size:.1f} MB)")
            else:
                print()
                print(f"❌ NSIS Installer Build fehlgeschlagen (Exit Code: {result.returncode})")
                if result.stderr:
                    print("Fehlerausgabe:")
                    print(result.stderr)
                return 1

        except Exception as e:
            print()
            print(f"❌ Fehler beim Erstellen des Installers: {e}")
            return 1

    print("=" * 70)
    print("🎉 Build-Prozess erfolgreich abgeschlossen!")
    print("=" * 70)
    return 0

if __name__ == "__main__":
    sys.exit(main())

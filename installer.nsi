; --- NSIS Installer Skript für Aero Media Service ---

!define APP_NAME "Aero Media Service"
!define APP_VERSION "0.0.1.1337"
!define APP_EXE "Aero Media Service.exe"
!define APP_PUBLISHER "Andreas Kowalenko"
!define APP_WEBSITE "kowalenko.io"

SetCompressor lzma
Name "${APP_NAME}"
OutFile "AeroMediaService_Installer_v${APP_VERSION}.exe"
InstallDir "$PROGRAMFILES64\${APP_NAME}"
; Fordert Adminrechte für den Installer selbst an
RequestExecutionLevel admin

; Diese Befehle setzen die "Details"-Eigenschaften der EXE-Datei.
VIProductVersion "${APP_VERSION}"
VIAddVersionKey "Publisher" "${APP_PUBLISHER}"
VIAddVersionKey "FileDescription" "${APP_NAME} Installer"
VIAddVersionKey "LegalCopyright" "${APP_PUBLISHER}"
VIAddVersionKey "ProductName" "${APP_NAME}"
VIAddVersionKey "ProductVersion" "${APP_VERSION}"
VIAddVersionKey "FileVersion" "${APP_VERSION}"
VIAddVersionKey "CompanyName" "${APP_PUBLISHER}"
; --- Ende Metadaten ---

!define MUI_ICON "assets\icon.ico"
!define MUI_UNICON "assets\icon.ico"

!include "MUI2.nsh"

; --- Willkommensseite anpassen ---
!define MUI_WELCOMEPAGE_TITLE "Willkommen beim ${APP_NAME} Setup"
!define MUI_WELCOMEPAGE_TEXT "Dieses Setup-Programm installiert ${APP_NAME} auf deinem Computer.$\r$\n$\r$\nKlicke auf 'Weiter', um fortzufahren."
!insertmacro MUI_PAGE_WELCOME

; --- Lizenz-Seite ---
!insertmacro MUI_PAGE_LICENSE "license.txt"

; --- Verzeichnis-Auswahl-Seite ---
!insertmacro MUI_PAGE_DIRECTORY

; --- Installations-Seite ---
!insertmacro MUI_PAGE_INSTFILES

; --- Abschluss-Seite (Mit "App starten"-Checkbox) ---
!define MUI_FINISHPAGE_TITLE "Installation von ${APP_NAME} abgeschlossen"
!define MUI_FINISHPAGE_TEXT "Das Setup hat ${APP_NAME} erfolgreich auf Ihrem Computer installiert."
!define MUI_FINISHPAGE_RUN "$INSTDIR\${APP_EXE}"
!define MUI_FINISHPAGE_RUN_TEXT "${APP_NAME} jetzt starten"
!insertmacro MUI_PAGE_FINISH

; --- Deinstaller-Seiten ---
!insertmacro MUI_UNPAGE_WELCOME
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_UNPAGE_FINISH

; --- Sprache ---
!insertmacro MUI_LANGUAGE "German"


; ===================================================================
; ======================== INSTALLATIONS-SEKTIONEN ==================
; ===================================================================

; --- Hauptinstallations-Sektion  ---
Section "Aero Media Service (erforderlich)" SecApp
  SetOutPath $INSTDIR

  ; HIER PASSIERT DIE MAGIE (NEU):
  ; 1. Kopiere den gesamten Inhalt des PyInstaller-Ordners
  ; Dies kopiert Aero Media Service.exe und alle Python-DLLs/Abhängigkeiten
  File /r "dist\Aero Media Service\*"

  ; Setze den Pfad zurück auf das Stammverzeichnis für den Uninstaller
  SetOutPath $INSTDIR

  ; Deinstallations-Informationen
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${APP_NAME}" "DisplayName" "${APP_NAME}"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${APP_NAME}" "UninstallString" '"$INSTDIR\uninstall.exe"'
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${APP_NAME}" "DisplayVersion" "${APP_VERSION}"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${APP_NAME}" "Publisher" "${APP_PUBLISHER}"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${APP_NAME}" "DisplayIcon" "$INSTDIR\${APP_EXE}"

  WriteUninstaller "$INSTDIR\uninstall.exe"
SectionEnd

; --- Sektionen für Verknüpfungen (bleiben gleich) ---
Section "Desktop-Verknüpfung" SecDesktopShortcut
  CreateShortcut "$DESKTOP\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE}"
SectionEnd

Section "Startmenü-Verknüpfung" SecStartMenu
  CreateDirectory "$SMPROGRAMS\${APP_NAME}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\Deinstallieren.lnk" "$INSTDIR\uninstall.exe"
SectionEnd


; ===================================================================
; ============================ UNINSTALLER ===========================
; ===================================================================

Section "Uninstall"
    SetDetailsPrint both
    DetailPrint "Beende ${APP_NAME}..."

    nsExec::ExecToStack 'taskkill /F /IM "${APP_EXE}"'
    Sleep 2000

    DetailPrint "Entferne Dateien..."
    RMDir /r "$INSTDIR"

    DetailPrint "Entferne Verknüpfungen..."
    Delete "$DESKTOP\${APP_NAME}.lnk"
    RMDir /r "$SMPROGRAMS\${APP_NAME}"

    DetailPrint "Bereinige Registry..."
    DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${APP_NAME}"
    DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\App Paths\${APP_EXE}"

    DetailPrint "Deinstallation abgeschlossen."
    SetDetailsPrint lastused
SectionEnd

; ===================================================================
; ====================== INITIALISIERUNGS-FUNKTIONEN =================
; ===================================================================

Function .onInstSuccess
    IfFileExists "$INSTDIR\${APP_EXE}" installed missing
    installed:
        Return
    missing:
        MessageBox MB_ICONEXCLAMATION "Warnung: Die Hauptanwendung wurde möglicherweise nicht korrekt installiert."
FunctionEnd

Function .onInit
    ; Prüfe auf bereits laufende Instanzen der App (robust, sprachunabhängig)
    ; wir verwenden cmd /C mit findstr: wenn der Prozess gefunden wird, gibt findstr eine Zeile aus
    nsExec::ExecToStack 'cmd /C tasklist /FI "IMAGENAME eq ${APP_EXE}" | findstr /I /C:"${APP_EXE}"'
    Pop $0
    Pop $1
    ; $1 enthält die Ausgabezeile, falls gefunden; leer wenn nicht
    StrCmp $1 "" continue_install found_running

    found_running:
        MessageBox MB_YESNO|MB_ICONEXCLAMATION \
            "${APP_NAME} scheint bereits zu laufen.$\r$\nBitte beenden Sie die Anwendung vor der Installation.$\r$\n$\r$\nJetzt beenden und fortfahren?" \
            /SD IDYES IDYES kill_app IDNO cancel_install

        kill_app:
            nsExec::Exec 'taskkill /F /IM "${APP_EXE}"'
            Sleep 2000
            Goto continue_install

        cancel_install:
            Abort

    continue_install:
FunctionEnd

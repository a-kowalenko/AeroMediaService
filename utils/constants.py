import sys

from utils.path_helper import get_resource_path

# --- OS-Erkennung ---
IS_WINDOWS = (sys.platform == "win32")
IS_MACOS = (sys.platform == "darwin")
IS_LINUX = (sys.platform == "linux")


# --- Asset-Pfade ---
ICON_PATH = get_resource_path("assets/icon.ico")
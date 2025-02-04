"""
Poetry post-install script to install libmagic on Linux and macOS.
"""

import subprocess
import sys


def install_deps():
    """
    Check the platform and install libmagic.
    """
    if sys.platform.startswith("linux"):
        subprocess.run(["sudo", "apt-get", "install", "-y", "libmagic1"], check=True)
    elif sys.platform == "darwin":
        subprocess.run(["brew", "install", "libmagic"], check=True)

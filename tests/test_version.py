"""The package version, the version the app reports and VERSION must agree.

setup.py and src.utils.get_app_version() both read VERSION. They once read
different keys, so the installed package stayed at its fallback version
whatever VERSION said, and src.run reports the installed package's version.
"""
import json
from importlib.metadata import version
from pathlib import Path

from src.utils import get_app_version

ROOT = Path(__file__).resolve().parent.parent
VERSION = json.loads((ROOT / "VERSION").read_text())["freesurfer-nidm"]["version"]


def test_app_reports_the_version_file():
    assert get_app_version() == VERSION


def test_installed_package_version_is_the_version_file():
    assert version("freesurfer-nidm") == VERSION, (
        "the installed freesurfer-nidm package disagrees with VERSION; if VERSION "
        "changed since the last install, reinstall with `pip install -e .`"
    )

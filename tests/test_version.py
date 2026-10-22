"""The package version, the version the app reports and VERSION must agree.

setup.py and src.utils.get_app_version() both read VERSION. They once read
different keys, so the installed package stayed at its fallback version
whatever VERSION said, and src.run reports the installed package's version.
"""
import json
import re
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


def test_image_version_label_is_the_app_version():
    """Both image recipes label the image with the app version.

    The Singularity recipe writes the label in %post: without --force, a
    %labels entry does not replace a label the base image already sets
    (Apptainer keeps the base's org.opencontainers.image.version, e.g.
    Ubuntu's 22.04, and only warns).
    """
    singularity = (ROOT / "Singularity").read_text()
    labels = singularity.split("\n%labels\n", 1)[1].split("\n%", 1)[0]
    assert "org.opencontainers.image.version" not in labels
    post = re.findall(
        r'^\s+echo "org\.opencontainers\.image\.version (\S+)" '
        r'>> "\$\{APPTAINER_LABELS:-\$SINGULARITY_LABELS\}"$',
        singularity, re.MULTILINE,
    )
    assert post == [VERSION]
    docker = re.findall(
        r"org\.opencontainers\.image\.version=(\S+)", (ROOT / "Dockerfile").read_text()
    )
    assert docker == [VERSION]

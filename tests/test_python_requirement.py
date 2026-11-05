"""PyNIDM 5.0.0, which requirements.txt pins, requires Python 3.10 or newer.

Declaring an older floor makes `pip install` on 3.9 fail in dependency
resolution with a message about pynidm instead of the app's own Python
requirement.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_setup_py_requires_python_3_10():
    text = (ROOT / "setup.py").read_text()
    assert re.findall(r'python_requires="([^"]+)"', text) == [">=3.10"]
    versions = re.findall(r"Programming Language :: Python :: (3\.\d+)", text)
    assert versions and all(int(v.split(".")[1]) >= 10 for v in versions), versions


def test_ci_runs_python_3_10_or_newer():
    for workflow in (ROOT / ".github" / "workflows").glob("*.y*ml"):
        for v in re.findall(r"python-version: ['\"]?(3\.\d+)", workflow.read_text()):
            assert int(v.split(".")[1]) >= 10, (workflow.name, v)

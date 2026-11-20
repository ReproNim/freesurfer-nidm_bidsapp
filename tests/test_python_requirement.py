"""PyNIDM 5.0.0, which requirements.txt pins, requires Python 3.10 or newer.

Declaring an older floor makes `pip install` on 3.9 fail in dependency
resolution with a message about pynidm instead of the app's own Python
requirement.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def test_setup_py_requires_python_3_10():
    text = (ROOT / "setup.py").read_text()
    assert re.findall(r'python_requires="([^"]+)"', text) == [">=3.10"]
    versions = re.findall(r"Programming Language :: Python :: (3\.\d+)", text)
    assert versions and all(int(v.split(".")[1]) >= 10 for v in versions), versions


def ci_python_versions(text):
    """Every Python version a workflow names under a `python-version:` key:
    a scalar, an inline list (`["3.9", "3.10"]`) or a block list. A
    `${{ matrix.python-version }}` reference names none itself."""
    versions = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = re.match(r"^(\s*)-?\s*python-version:\s*(.*?)\s*(#.*)?$", line)
        if not m:
            continue
        value = m.group(2)
        if value:
            versions += re.findall(r"\b3\.\d+\b", value) if "${{" not in value else []
            continue
        indent = len(m.group(1))
        for item in lines[i + 1:]:
            item_m = re.match(r"^(\s*)-\s*(.*)$", item)
            if not item_m or len(item_m.group(1)) < indent:
                break
            versions += re.findall(r"\b3\.\d+\b", item_m.group(2))
    return versions


@pytest.mark.parametrize("text, expected", [
    ("python-version: '3.10'", ["3.10"]),
    ('python-version: ["3.9", "3.10"]', ["3.9", "3.10"]),
    ("matrix:\n  python-version:\n    - '3.9'\n    - 3.11\n  os: [x]", ["3.9", "3.11"]),
    ("python-version: ${{ matrix.python-version }}", []),
])
def test_ci_python_versions_reads_every_form(text, expected):
    assert ci_python_versions(text) == expected


def test_ci_runs_python_3_10_or_newer():
    found = []
    for workflow in (ROOT / ".github" / "workflows").glob("*.y*ml"):
        for v in ci_python_versions(workflow.read_text()):
            found.append((workflow.name, v))
    assert found, "no workflow names a python-version"
    assert all(int(v.split(".")[1]) >= 10 for _, v in found), found

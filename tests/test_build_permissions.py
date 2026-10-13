"""The images must work for every account, not only for the one that built them.

Both recipes copy the app from a checkout and keep its file modes. A file that
is mode 600 in that checkout is unreadable for anyone else running the image;
the mriqc-nidm 0.1.0 SIF shipped its own src/ that way, so the app failed at
import for every account but the builder's. The builder never sees this
failure, so the build itself has to check.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = (ROOT / "Dockerfile").read_text()
SINGULARITY = (ROOT / "Singularity").read_text()
RECIPES = [(DOCKERFILE, "Dockerfile"), (SINGULARITY, "Singularity")]
IDS = ["Dockerfile", "Singularity"]

APP_PATHS = "/opt/src /opt/requirements.txt /opt/setup.py /opt/setup.cfg /opt/VERSION"
CHMOD = re.compile(r"\bchmod -R a\+rX " + re.escape(APP_PATHS) + r"$")
CHECK = re.compile(
    r"find " + re.escape(APP_PATHS)
    + r" \\\( -type f ! -perm -o=r \\\) -o \\\( -type d ! -perm -o=rx \\\)"
)
INSTALL = re.compile(r"\bpip install\b")


def _commands(text):
    """Non-blank, non-comment lines, stripped."""
    return [line.strip() for line in text.splitlines()
            if line.strip() and not line.strip().startswith("#")]


def _post_commands():
    post = re.search(r"^%post\s*$", SINGULARITY, re.MULTILINE)
    assert post, "Singularity has no %post section"
    rest = SINGULARITY[post.end():]
    nxt = re.search(r"^%\w+\s*$", rest, re.MULTILINE)
    return _commands(rest[: nxt.start()] if nxt else rest)


def test_singularity_post_fails_fast():
    """%post's exit status is its last command's. The permission check is now
    the last command, so without `set -e` a failed pip install builds fine."""
    commands = _post_commands()
    assert commands and commands[0] == "set -e", (
        f"the first statement in %post must be `set -e`; found {commands[:1]!r}"
    )


@pytest.mark.parametrize("text,name", RECIPES, ids=IDS)
def test_recipe_makes_the_app_world_readable(text, name):
    commands = _post_commands() if name == "Singularity" else _commands(text)
    chmods = [i for i, c in enumerate(commands) if CHMOD.search(c)]
    installs = [i for i, c in enumerate(commands) if INSTALL.search(c)]
    assert chmods, f"{name} must run `chmod -R a+rX {APP_PATHS}`"
    assert installs and chmods[0] < installs[0], (
        f"{name}: make the copied files readable before installing from them"
    )
    if name == "Dockerfile":
        copy = next(i for i, c in enumerate(commands) if c.startswith("COPY . /opt/"))
        assert chmods[0] > copy, "the chmod must follow `COPY . /opt/`"


@pytest.mark.parametrize("text,name", RECIPES, ids=IDS)
def test_build_fails_on_files_other_users_cannot_read(text, name):
    commands = _post_commands() if name == "Singularity" else _commands(text)
    checks = [i for i, c in enumerate(commands) if CHECK.search(c)]
    installs = [i for i, c in enumerate(commands) if INSTALL.search(c)]
    assert checks, f"{name} must fail the build on files other users cannot read"
    assert checks[-1] > installs[-1], (
        f"{name}: the readability check must come after the last pip install"
    )
    tail = "\n".join(commands[checks[-1]:checks[-1] + 6])
    assert "exit 1" in tail, f"{name}: the readability check must exit non-zero"

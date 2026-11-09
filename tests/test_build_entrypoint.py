"""Both images start the app through the freesurfer-nidm console script.

`python3 /opt/src/run.py` puts /opt/src first on sys.path. The converter's
checkout there (/opt/src/segstats_jsonld, no __init__.py) then shadows the
installed package as a namespace package: its __init__ never runs, and any
converter submodule named like an entry of the checkout loads the wrong file.
`apptainer run` (how BABS starts the app) uses %runscript, so the runscript and
the Docker entrypoint must go through the console script, and the build must
fail if the console script is missing or the NIDM input check's import does
not resolve the way the console script runs it.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = (ROOT / "Dockerfile").read_text()
SINGULARITY = (ROOT / "Singularity").read_text()

CONSOLE_SCRIPT = 'exec freesurfer-nidm "$@"'
LOOKUP_IMPORT = "from segstats_jsonld.fs_to_nidm import SubjectLookupError, find_subject"
SCRIPT_DIR_FIRST = "sys.path[0] = os.path.dirname(shutil.which('freesurfer-nidm'))"
CONVERTER_FILE = "'/opt/src/segstats_jsonld/segstats_jsonld/fs_to_nidm.py'"


def _commands(text):
    """Non-blank, non-comment lines, stripped."""
    return [line.strip() for line in text.splitlines()
            if line.strip() and not line.strip().startswith("#")]


def _singularity_section(name):
    head = re.search(rf"^%{name}\s*$", SINGULARITY, re.MULTILINE)
    assert head, f"Singularity has no %{name} section"
    rest = SINGULARITY[head.end():]
    nxt = re.search(r"^%\w+\s*$", rest, re.MULTILINE)
    return _commands(rest[: nxt.start()] if nxt else rest)


def _docker_instructions():
    """Dockerfile instructions with backslash continuations joined."""
    joined = re.sub(r"\s*\\\n\s*", " ", DOCKERFILE)
    return _commands(joined)


def test_console_script_is_declared():
    setup = (ROOT / "setup.py").read_text()
    assert re.search(r"""["']freesurfer-nidm\s*=\s*src\.run:cli["']""", setup)


def test_runscript_starts_the_console_script():
    runscript = _singularity_section("runscript")
    assert runscript == [CONSOLE_SCRIPT], runscript
    assert not any(re.search(r"\brun\.py\b", line) for line in runscript)


def test_docker_entrypoint_starts_the_console_script():
    entrypoints = [c for c in _docker_instructions() if c.startswith("ENTRYPOINT")]
    assert entrypoints == ['ENTRYPOINT ["freesurfer-nidm"]'], entrypoints


def test_no_recipe_runs_run_py_directly():
    for name, text in (("Singularity", SINGULARITY), ("Dockerfile", DOCKERFILE)):
        assert not any(re.search(r"/opt/src/run\.py", c) for c in _commands(text)), name


def _lookup_checks(commands):
    return [i for i, c in enumerate(commands)
            if SCRIPT_DIR_FIRST in c and LOOKUP_IMPORT in c and CONVERTER_FILE in c]


def test_singularity_build_checks_the_console_script_and_its_lookup_import():
    post = _singularity_section("post")
    app_install = [i for i, c in enumerate(post) if c == "python3 -m pip install -e ."]
    assert app_install, "app install not found in %post"
    exists = [i for i, c in enumerate(post) if c == "command -v freesurfer-nidm"]
    check = _lookup_checks(post)
    runs = [i for i, c in enumerate(post) if c == "PYTHONPATH=/opt freesurfer-nidm --version"]
    assert exists and check and runs, "the console-script checks are missing from %post"
    assert min(exists[0], check[0], runs[0]) > app_install[-1]
    # %environment sets PYTHONPATH=/opt at run time; the check must see the same.
    assert post[check[0]].startswith("PYTHONPATH=/opt python3 -c ")
    assert post[0] == "set -e"


def test_docker_build_checks_the_console_script_and_its_lookup_import():
    instructions = _docker_instructions()
    app_install = [i for i, c in enumerate(instructions)
                   if c.startswith("RUN ") and "python3 -m pip install -e ." in c]
    check = [i for i in _lookup_checks(instructions)
             if instructions[i].startswith("RUN command -v freesurfer-nidm && ")
             and instructions[i].endswith("&& freesurfer-nidm --version")]
    assert app_install and check, "the console-script check is missing from the Dockerfile"
    assert check[0] > app_install[-1]


# --- Run-time sys.path and what the lookup check proves ---------------------

PACKAGE_FILE = "'/opt/src/segstats_jsonld/segstats_jsonld/__init__.py'"


def _pythonpath_after(assignment, host_value):
    """PYTHONPATH after `assignment` runs in sh, from host_value (None = unset)."""
    import subprocess
    prelude = "unset PYTHONPATH" if host_value is None else f"export PYTHONPATH='{host_value}'"
    out = subprocess.run(["sh", "-c", f'{prelude}; {assignment}; printf %s "$PYTHONPATH"'],
                         capture_output=True, text=True, check=True)
    return out.stdout


@pytest.mark.parametrize("host_value", [None, "", "/host/lib"])
def test_environment_pythonpath_never_adds_the_working_directory(host_value):
    """Python reads an empty PYTHONPATH entry as the working directory. With
    no host PYTHONPATH (or an empty one), `/opt:$PYTHONPATH` is `/opt:`, and a
    converter checkout in the working directory shadows the installed
    converter. This pins only what %environment adds: a host value is kept as
    given, so a host PYTHONPATH with an empty component still adds the working
    directory (tests/test_converter_isolation.py covers what the app then
    filters)."""
    lines = [c for c in _singularity_section("environment") if "PYTHONPATH" in c]
    assert lines == ["export PYTHONPATH=/opt${PYTHONPATH:+:$PYTHONPATH}"], lines
    entries = _pythonpath_after(lines[0], host_value).split(":")
    assert "" not in entries, entries
    assert entries[0] == "/opt"
    if host_value:
        assert entries[1:] == [host_value]


@pytest.mark.parametrize("assignment", [
    "export PYTHONPATH=/opt:$PYTHONPATH", "export PYTHONPATH=$PYTHONPATH:/opt"])
def test_pythonpath_check_rejects_a_trailing_empty_entry(assignment):
    """Positive control: the old form leaves an empty entry with no host value."""
    assert "" in _pythonpath_after(assignment, None).split(":")


def test_dockerfile_pythonpath_is_only_opt():
    """The base image sets no PYTHONPATH, so `/opt:$PYTHONPATH` would end in
    an empty entry, the working directory."""
    env = [c for c in _docker_instructions() if re.match(r"ENV\s+PYTHONPATH\b", c)]
    assert env == ["ENV PYTHONPATH=/opt"], env


def _shell_outside_quotes(line):
    """The line with double-quoted strings removed: the shell part only."""
    return re.sub(r'"(?:[^"\\]|\\.)*"', '""', line)


def _swallows_status(shell):
    """Anything that can hide the python command's exit status, or `-O`,
    which drops asserts."""
    return bool(re.search(r"\|\||;|&|\|", shell.replace("&&", ""))) or " -O" in shell


def _singularity_check():
    post = _singularity_section("post")
    check = _lookup_checks(post)
    assert len(check) == 1, check
    return post[check[0]]


def _docker_check():
    instructions = _docker_instructions()
    check = _lookup_checks(instructions)
    assert len(check) == 1, check
    return instructions[check[0]]


def test_lookup_checks_name_the_real_package_not_a_namespace():
    """With the checkout first on sys.path, fs_to_nidm still resolves to the
    right file through the editable install while segstats_jsonld itself is a
    namespace package. The check must also name the package's __init__.py,
    and exit through sys.exit (python3 -O drops an assert)."""
    for check in (_singularity_check(), _docker_check()):
        assert f"m.__file__ != {CONVERTER_FILE}" in check, check
        assert f"p.__file__ != {PACKAGE_FILE}" in check, check
        assert "sys.exit(" in check and "assert " not in check, check


@pytest.mark.parametrize("suffix", [
    "", " || true", " || :", " || exit 0", "; true", " || echo skipped", " &", " | cat"])
def test_lookup_check_can_fail_the_build(suffix):
    """Nothing may swallow the python command's exit status (Singularity's
    %post runs under `set -e`, Docker's RUN is one && chain). The suffixes are
    mutants of the real lines; only "" passes."""
    for check in (_singularity_check(), _docker_check()):
        mutated = _shell_outside_quotes(check + suffix)
        assert _swallows_status(mutated) == (suffix != ""), (suffix, mutated)

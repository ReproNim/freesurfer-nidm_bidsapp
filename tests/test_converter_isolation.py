#!/usr/bin/env python3
"""The conversion runs the converter file the NIDM input check imported.

The NIDM input check imports segstats_jsonld.fs_to_nidm in the app process;
the conversion runs fs_to_nidm in a new interpreter started with
`python -c CONVERTER_BOOTSTRAP`. `-c` puts the working directory first on
that interpreter's sys.path, and an empty PYTHONPATH component (a host
PYTHONPATH such as `/x:`) puts it on both processes' sys.path. A package or
module there named like the converter or one of its imports would replace
the code the check ran. These tests run the real bootstrap and the real
converter from working directories holding such decoys.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from src.run import (CONVERTER_BOOTSTRAP, CONVERTER_FILE_ENV, CONVERTER_MODULE, _import_fs_to_nidm,
                     nidm_conversion)
from tests.test_nidm_real_converter import INPUT_TTL, convert, recon_all_tree, stats_subject_agents

REPO_ROOT = Path(__file__).resolve().parents[1]
# The bootstrap as it was before it filtered sys.path (positive control).
UNFILTERED_BOOTSTRAP = f"import sys; from {CONVERTER_MODULE} import main; sys.argv[0] = \"fs_to_nidm\"; sys.exit(main())"


def _decoy_package(directory, marker):
    """A segstats_jsonld package whose fs_to_nidm.main() leaves a marker and
    returns 0 without converting anything."""
    package = Path(directory) / "segstats_jsonld"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "fs_to_nidm.py").write_text(
        f"def main():\n    open({str(marker)!r}, 'w').close()\n    return 0\n")
    return package


def _decoy_module(directory, name, marker):
    """A top-level module that leaves a marker when it is imported."""
    (Path(directory) / f"{name}.py").write_text(f"open({str(marker)!r}, 'w').close()\n")


def _installed_converter_file():
    return os.path.realpath(_import_fs_to_nidm().__file__)


def _run(code, cwd, *args, pythonpath=None, converter_file=None):
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT), str(REPO_ROOT / "src"), str(REPO_ROOT / "src" / "segstats_jsonld")]
        + ([pythonpath] if pythonpath is not None else []))
    env[CONVERTER_FILE_ENV] = converter_file or _installed_converter_file()
    return subprocess.run([sys.executable, "-c", code, *args], cwd=cwd, env=env,
                          capture_output=True, text=True, timeout=300)


def _input_nidm_dir(tmp_path):
    nidm_in = tmp_path / "NIDM" / "sub-001"
    nidm_in.mkdir(parents=True)
    (nidm_in / "nidm.ttl").write_text(INPUT_TTL)
    return tmp_path / "NIDM"


@pytest.mark.parametrize("host_pythonpath", [None, "/nonexistent/host/lib:", ":/nonexistent/host/lib"])
def test_conversion_ignores_a_converter_package_in_the_working_directory(
        tmp_path, monkeypatch, host_pythonpath):
    """A segstats_jsonld package in the working directory (the job started
    from a converter checkout) must not replace the converter. Without the
    filter its main() returned 0 without writing, the input copy became
    nidm.ttl and the job reported success with no FreeSurfer measures in it.
    A host PYTHONPATH with an empty component adds the working directory a
    second time, to both processes."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    marker = tmp_path / "decoy-converter-ran"
    _decoy_package(cwd, marker)

    # Positive control: without the filter the decoy is what runs.
    result = _run(UNFILTERED_BOOTSTRAP, cwd, "-h")
    assert marker.exists(), (result.returncode, result.stderr[-2000:])
    marker.unlink()

    out = tmp_path / "out"
    recon_all_tree(out)
    if host_pythonpath is None:
        monkeypatch.delenv("PYTHONPATH", raising=False)
    else:
        monkeypatch.setenv("PYTHONPATH", host_pythonpath)
    monkeypatch.chdir(cwd)
    g = convert(out, nidm_input_dir=_input_nidm_dir(tmp_path))
    assert not marker.exists(), "the working directory's converter ran"
    assert stats_subject_agents(g), "nidm.ttl has no FreeSurfer stats activity"


@pytest.mark.parametrize("host_pythonpath", [None, "cwd", "."])
def test_conversion_ignores_a_module_named_like_a_converter_import(tmp_path, monkeypatch, host_pythonpath):
    """fs_to_nidm imports glob; a glob.py in the working directory replaced
    the standard library's in the conversion, and the subject failed with
    "no supported stats file" although the stats were there. Also when the
    host PYTHONPATH names the working directory, absolute or as ".", which
    is not an empty entry and must still be dropped."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    marker = tmp_path / "decoy-glob-imported"
    _decoy_module(cwd, "glob", marker)

    result = _run(UNFILTERED_BOOTSTRAP, cwd, "-h")
    assert marker.exists(), (result.returncode, result.stderr[-2000:])
    marker.unlink()

    out = tmp_path / "out"
    recon_all_tree(out)
    if host_pythonpath is None:
        monkeypatch.delenv("PYTHONPATH", raising=False)
    else:
        monkeypatch.setenv("PYTHONPATH", str(cwd) if host_pythonpath == "cwd" else host_pythonpath)
    monkeypatch.chdir(cwd)
    g = convert(out, nidm_input_dir=_input_nidm_dir(tmp_path))
    assert not marker.exists(), "the working directory's glob.py was imported"
    assert stats_subject_agents(g), "nidm.ttl has no FreeSurfer stats activity"


def test_app_import_ignores_the_working_directory(tmp_path):
    """The app process starts from the console script, so `-c`'s entry never
    reaches it, but an empty host PYTHONPATH component does. The NIDM input
    check must still import the installed converter, not the working
    directory's."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    decoy = _decoy_package(cwd, tmp_path / "unused")
    # sys.path.pop(0) drops `-c`'s own entry, as the console script has none.
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=f"{REPO_ROOT}{os.pathsep}")

    def imported(code):
        result = subprocess.run([sys.executable, "-c", "import sys; sys.path.pop(0); " + code],
                                cwd=cwd, env=env, capture_output=True, text=True, timeout=300)
        assert result.returncode == 0, result.stderr[-2000:]
        return Path(result.stdout.strip().splitlines()[-1]).resolve()

    # Positive control: the trailing colon puts the decoy first.
    plain = imported(f"import {CONVERTER_MODULE} as m; print(m.__file__)")
    assert plain.is_relative_to(decoy.resolve()), plain
    checked = imported("from src.run import _import_fs_to_nidm; print(_import_fs_to_nidm().__file__)")
    assert not checked.is_relative_to(cwd.resolve()), checked
    assert checked == Path(_installed_converter_file())


def test_bootstrap_starts_when_the_working_directory_was_removed(tmp_path):
    """A working directory removed after the app started: os.getcwd() raises
    in the converter's interpreter. The bootstrap itself must still start.
    (The old bootstrap failed here on Python < 3.12: urllib3's
    `from backports import zstd` reached pkgutil.extend_path, whose
    get_importer('') calls os.getcwd().)"""
    gone = tmp_path / "gone"
    gone.mkdir()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    # No empty component: nidm_conversion drops them, and on Python 3.12 one
    # makes the interpreter itself fail to start from a removed directory.
    env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT), str(REPO_ROOT / "src" / "segstats_jsonld")])
    env[CONVERTER_FILE_ENV] = _installed_converter_file()
    result = subprocess.run(
        ["sh", "-c", 'cd "$1" && rmdir "$1" && shift && exec "$@"', "sh", str(gone),
         sys.executable, "-c", CONVERTER_BOOTSTRAP, "-h"],
        env=env, capture_output=True, text=True, timeout=300)
    assert not gone.exists()
    assert result.returncode == 0, result.stderr[-2000:]
    assert "usage: fs_to_nidm" in result.stdout


def test_bootstrap_runs_the_file_it_is_given(tmp_path):
    """The bootstrap loads the converter from the file named in
    CONVERTER_FILE_ENV, even when a different segstats_jsonld comes first on
    sys.path."""
    other = tmp_path / "other"
    other.mkdir()
    marker = tmp_path / "other-converter-ran"
    _decoy_package(other, marker)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    result = _run(CONVERTER_BOOTSTRAP, cwd, "-h", pythonpath=str(other))
    assert result.returncode == 0, result.stderr[-2000:]
    assert not marker.exists()
    assert "usage: fs_to_nidm" in result.stdout


@pytest.mark.parametrize("named", ["missing", "namespace", "not_fs_to_nidm", "unset"])
def test_bootstrap_refuses_a_converter_file_it_cannot_load(tmp_path, named):
    """No file, a file outside a regular segstats_jsonld package, or no
    CONVERTER_FILE_ENV at all: the conversion exits non-zero and says why,
    rather than importing whatever sys.path offers."""
    namespace = tmp_path / "ns" / "segstats_jsonld"
    namespace.mkdir(parents=True)
    (namespace / "fs_to_nidm.py").write_text("def main():\n    return 0\n")
    other = tmp_path / "pkg" / "segstats_jsonld"
    other.mkdir(parents=True)
    (other / "__init__.py").write_text("")
    (other / "fsutils.py").write_text("")
    path = {"missing": str(tmp_path / "nowhere" / "fs_to_nidm.py"),
            "namespace": str(namespace / "fs_to_nidm.py"),
            "not_fs_to_nidm": str(other / "fsutils.py"),
            "unset": None}[named]
    if path is None:
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        env.pop(CONVERTER_FILE_ENV, None)
        result = subprocess.run([sys.executable, "-c", CONVERTER_BOOTSTRAP, "-h"], cwd=tmp_path,
                                env=env, capture_output=True, text=True, timeout=300)
    else:
        result = _run(CONVERTER_BOOTSTRAP, tmp_path, "-h", converter_file=path)
    assert result.returncode != 0
    assert "usage: fs_to_nidm" not in result.stdout
    assert CONVERTER_FILE_ENV in result.stderr, result.stderr
    if path is not None:
        assert path in result.stderr, result.stderr


def test_bootstrap_refuses_when_the_package_swaps_the_module(tmp_path):
    """If the package it loads hands it another fs_to_nidm module, the
    bootstrap exits non-zero naming both files."""
    package = tmp_path / "pkg" / "segstats_jsonld"
    package.mkdir(parents=True)
    (package / "fs_to_nidm.py").write_text("def main():\n    return 0\n")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "fs_to_nidm.py").write_text("def main():\n    return 0\n")
    (package / "__init__.py").write_text(
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location(__name__ + '.fs_to_nidm', "
        f"{str(elsewhere / 'fs_to_nidm.py')!r})\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "sys.modules[__name__ + '.fs_to_nidm'] = module\n")
    result = _run(CONVERTER_BOOTSTRAP, tmp_path, "-h", converter_file=str(package / "fs_to_nidm.py"))
    assert result.returncode != 0
    assert str(package / "fs_to_nidm.py") in result.stderr, result.stderr
    assert str(elsewhere / "fs_to_nidm.py") in result.stderr, result.stderr


def test_conversion_is_given_the_file_the_app_imported(tmp_path, monkeypatch):
    """nidm_conversion passes the converter file its own process imported,
    replacing a value the host environment already has (Apptainer passes the
    host's environment through unless --cleanenv), and drops empty
    components of the host PYTHONPATH."""
    monkeypatch.setenv("PYTHONPATH", f":/host/lib{os.pathsep}{os.pathsep}/host/other{os.pathsep}")
    monkeypatch.setenv(CONVERTER_FILE_ENV, str(tmp_path / "host" / "segstats_jsonld" / "fs_to_nidm.py"))
    from unittest.mock import MagicMock, patch
    out = tmp_path / "out"
    recon_all_tree(out)
    wrapper = MagicMock()
    wrapper.get_subject_t1_info.return_value = {"T1w_images": [], "T2w_images": []}
    with patch("src.run.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        nidm_conversion(str(out), "001", wrapper, nidm_input_dir=str(_input_nidm_dir(tmp_path)))
    env = run.call_args.kwargs["env"]
    assert os.path.realpath(env[CONVERTER_FILE_ENV]) == _installed_converter_file()
    entries = env["PYTHONPATH"].split(os.pathsep)
    assert "" not in entries and entries[-2:] == ["/host/lib", "/host/other"], entries

"""The image builds' checks can fail the build: run them, do not parse them.

Each recipe's build steps are executed in a shell against a fake /opt tree,
with stand-ins for the commands the checks call. Singularity's whole %post
runs as one `sh` script, as Apptainer runs it, so whatever wraps a check
(`set +e`, `|| true`, `!`, `if`) is exercised along with it. The
Dockerfile's RUN instructions run one by one under `sh -c`, as Docker runs
them, and the first that exits non-zero stops the build. The Python in the
checks is the real code from the recipes, run by this interpreter (with -O,
so an assert would not count).

For every fault a check exists to catch, the build must fail; with no
fault, it must pass.

- The package and module layout is built so the checks' paths resolve: the
  converter checkout at /opt/src/segstats_jsonld, its package one level
  down, and a sitecustomize on PYTHONPATH that maps segstats_jsonld to that
  package the way setuptools' editable finder does (appended after the
  path-based finder, as in the image).
- Every /opt in the recipe text is replaced by the fake tree's /opt.
"""
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SENTINEL = "BUILD-CHECKS-PASSED"

EDITABLE_FINDER = '''
import importlib.util, os, sys
_PACKAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "segstats_jsonld", "segstats_jsonld")
# Drop the test venv's own editable finders, which would map segstats_jsonld
# to the real checkout instead of the fake one.
sys.meta_path[:] = [f for f in sys.meta_path
                    if not getattr(f, "__module__", "").startswith("__editable__")]


class _EditableFinder:
    @classmethod
    def find_spec(cls, name, path=None, target=None):
        if name == "segstats_jsonld":
            return importlib.util.spec_from_file_location(
                name, os.path.join(_PACKAGE, "__init__.py"), submodule_search_locations=[_PACKAGE])
        if name.startswith("segstats_jsonld."):
            f = os.path.join(_PACKAGE, *name.split(".")[1:]) + ".py"
            if os.path.isfile(f):
                return importlib.util.spec_from_file_location(name, f)
        return None


sys.meta_path.append(_EditableFinder)
'''

CONVERTER = "class SubjectLookupError(Exception):\n    pass\n\n\ndef find_subject(*args, **kwargs):\n    pass\n"

# Faults, each meant to make (at least) one check fail.
FAULTS = {
    # segstats_jsonld resolves as a namespace package rooted at a checkout
    # on sys.path; fs_to_nidm still resolves to the right file.
    "namespace-package": "converter",
    # a regular segstats_jsonld package elsewhere comes first
    "other-converter": "converter",
    # the console script runs but fails
    "version-fails": "version",
    # the console script is missing
    "no-console-script": "console script",
    # a file the app reads is not readable by others
    "unreadable-file": "permissions",
    # an older PyNIDM's metadata comes first on sys.path
    "old-pynidm": "PyNIDM",
    # a nidm package outside the image's site-packages comes first
    "nidm-elsewhere": "PyNIDM",
}


def _sh(path, text):
    path.write_text("#!/bin/sh\n" + text)
    path.chmod(0o755)


def build_tree(tmp_path, fault=None):
    """Fake /opt tree, stand-in commands, and the environment to run in."""
    root = tmp_path / "root"
    opt = root / "opt"
    package = opt / "src" / "segstats_jsonld" / "segstats_jsonld"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "fs_to_nidm.py").write_text(CONVERTER)
    for name in ("requirements.txt", "setup.py", "setup.cfg", "VERSION"):
        (opt / name).write_text("")
    (opt / "sitecustomize.py").write_text(EDITABLE_FINDER)
    if fault == "namespace-package":
        (opt / "segstats_jsonld").mkdir()
    if fault == "other-converter":
        (opt / "segstats_jsonld").mkdir()
        (opt / "segstats_jsonld" / "__init__.py").write_text("")
        (opt / "segstats_jsonld" / "fs_to_nidm.py").write_text(CONVERTER)
    if fault == "old-pynidm":
        info = opt / "pynidm-4.5.0.dist-info"
        info.mkdir()
        (info / "METADATA").write_text("Metadata-Version: 2.1\nName: pynidm\nVersion: 4.5.0\n")
    if fault == "nidm-elsewhere":
        (opt / "nidm").mkdir()
        (opt / "nidm" / "__init__.py").write_text('__version__ = "5.0.0"\n')
    if fault == "unreadable-file":
        secret = opt / "src" / "private.py"
        secret.write_text("")
        secret.chmod(0o600)

    shims = root / "shims"
    shims.mkdir()
    # pip and apt are not run; `python3 -c` runs the check code for real
    _sh(shims / "python3",
        'if [ "$1" = "-m" ]; then exit 0; fi\n'
        f'exec {sys.executable} -O "$@"\n')
    _sh(shims / "apt-get", "exit 0\n")
    if fault == "unreadable-file":
        # the recipes' `chmod -R a+rX` would repair the file
        _sh(shims / "chmod", "exit 0\n")
    bindir = root / "bin"
    bindir.mkdir()
    if fault != "no-console-script":
        _sh(bindir / "freesurfer-nidm", "exit 1\n" if fault == "version-fails" else "echo 0.1.4\n")

    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYTHON", "APPTAINER", "SINGULARITY"))}
    env.update(PATH=os.pathsep.join([str(shims), str(bindir), "/usr/bin", "/bin"]),
               PYTHONDONTWRITEBYTECODE="1", APPTAINER_LABELS=str(root / "labels"))
    return root, env


def _on_fake_root(text, root):
    """Recipe text with /opt (and the apt lists the recipes clean) moved
    under the fake root."""
    return (text.replace("/opt", str(root / "opt"))
            .replace("/var/lib/apt/lists", str(root / "apt-lists")))


def singularity_post(recipe):
    m = re.search(r"^%post[^\n]*\n(.*?)(?=^%\w+)", recipe, re.MULTILINE | re.DOTALL)
    assert m, "Singularity has no %post section"
    return m.group(1)


def run_singularity(tmp_path, recipe, fault=None):
    """Run %post as Apptainer does (one sh script); return (passed, output).
    The build passes when the script exits 0, also if it exits early; the
    no-fault test checks that the whole script ran."""
    root, env = build_tree(tmp_path, fault)
    script = root / "post.sh"
    script.write_text(_on_fake_root(singularity_post(recipe), root) + f"\necho {SENTINEL}\n")
    result = subprocess.run(["sh", str(script)], cwd=root, env=env, capture_output=True, text=True,
                            timeout=300)
    return result.returncode == 0, result.stdout + result.stderr


def docker_instructions(recipe):
    joined = re.sub(r"\\\n", " ", recipe)
    return [line.strip() for line in joined.splitlines()
            if line.strip() and not line.strip().startswith("#")]


def run_docker(tmp_path, recipe, fault=None):
    """Run the Dockerfile's RUN instructions under `sh -c`, honouring WORKDIR
    and ENV PYTHONPATH; the first failing RUN stops the build."""
    root, env = build_tree(tmp_path, fault)
    cwd = root
    outputs = []
    for instruction in docker_instructions(recipe):
        keyword, _, rest = instruction.partition(" ")
        if keyword == "WORKDIR":
            cwd = Path(_on_fake_root(rest.strip(), root))
        elif keyword == "ENV":
            for key, value in re.findall(r"(\w+)=(\S+)", rest):
                if key != "PATH":
                    env[key] = _on_fake_root(value, root)
        elif keyword == "RUN":
            result = subprocess.run(["sh", "-c", _on_fake_root(rest, root)], cwd=cwd, env=env,
                                    capture_output=True, text=True, timeout=300)
            outputs.append(result.stdout + result.stderr)
            if result.returncode != 0:
                return False, "\n".join(outputs)
    return True, "\n".join(outputs)


RUNNERS = {"Singularity": run_singularity, "Dockerfile": run_docker}


@pytest.mark.parametrize("recipe", sorted(RUNNERS))
def test_build_checks_pass_without_a_fault(tmp_path, recipe):
    passed, output = RUNNERS[recipe](tmp_path, (ROOT / recipe).read_text())
    assert passed, output
    if recipe == "Singularity":
        assert SENTINEL in output, output
    assert "freesurfer-nidm resolves the converter lookup from" in output, output
    assert "freesurfer-nidm uses PyNIDM 5.0.0 from" in output, output


@pytest.mark.parametrize("fault", sorted(FAULTS))
@pytest.mark.parametrize("recipe", sorted(RUNNERS))
def test_each_fault_fails_the_build(tmp_path, recipe, fault):
    passed, output = RUNNERS[recipe](tmp_path, (ROOT / recipe).read_text(), fault)
    assert not passed, f"{recipe} built although the {FAULTS[fault]} check should fail it:\n{output}"


def test_namespace_fault_reaches_the_converter_check(tmp_path):
    """The namespace fault is the one the package comparison exists for:
    the module file is right and only the package is wrong."""
    root, env = build_tree(tmp_path, "namespace-package")
    env["PYTHONPATH"] = str(root / "opt")
    code = ("import os, shutil, sys; sys.path[0] = os.path.dirname(shutil.which('freesurfer-nidm')); "
            "import segstats_jsonld as p, segstats_jsonld.fs_to_nidm as m; print(p.__file__, m.__file__)")
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True,
                            timeout=60)
    assert result.returncode == 0, result.stderr
    package_file, module_file = result.stdout.split()
    assert package_file == "None"
    assert module_file == str(root / "opt" / "src" / "segstats_jsonld" / "segstats_jsonld" / "fs_to_nidm.py")

#!/usr/bin/env python3
"""Tests for the FreeSurfer license lookup in the wrapper."""

import logging
import os
from unittest.mock import patch

import pytest

from src.freesurfer.wrapper import FreeSurferWrapper


def make_wrapper(tmp_path, env, license_file=None, cwd=None, docker_license=None):
    """Build a wrapper under a controlled environment (no FS_LICENSE, HOME in tmp).

    The Docker location is moved to a tmp path (absent unless docker_license is
    given), so the host's /license.txt cannot matter and precedence against it
    can be tested.
    """
    bids = tmp_path / "bids"
    bids.mkdir(exist_ok=True)
    out = tmp_path / "out"
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    full_env = {"HOME": str(home)}
    full_env.update(env)
    docker = str(docker_license or tmp_path / "no-docker-license.txt")
    with patch.dict(os.environ, full_env, clear=True), \
            patch("src.freesurfer.wrapper.DOCKER_LICENSE", docker), \
            patch("src.freesurfer.wrapper.get_freesurfer_version", return_value="x"):
        if cwd is not None:
            old = os.getcwd()
            os.chdir(cwd)
        try:
            w = FreeSurferWrapper(bids, out, freesurfer_license=license_file)
            return w, os.environ.get("FS_LICENSE")
        finally:
            if cwd is not None:
                os.chdir(old)


def test_empty_freesurfer_home_is_treated_as_unset(tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    (cwd / "license.txt").write_text("stray")
    with pytest.raises(EnvironmentError, match="FREESURFER_HOME environment variable not set"):
        make_wrapper(tmp_path, {"FREESURFER_HOME": ""}, cwd=cwd)


def test_unset_freesurfer_home_still_stops(tmp_path):
    with pytest.raises(EnvironmentError, match="FREESURFER_HOME environment variable not set"):
        make_wrapper(tmp_path, {})


def test_fs_license_env_is_used_when_it_names_a_file(tmp_path):
    lic = tmp_path / "bound.txt"
    lic.write_text("lic")
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("other")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": str(lic)})
    assert used == str(lic)


def test_option_wins_over_fs_license_env(tmp_path):
    env_lic = tmp_path / "env.txt"
    env_lic.write_text("a")
    opt_lic = tmp_path / "opt.txt"
    opt_lic.write_text("b")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(tmp_path), "FS_LICENSE": str(env_lic)},
        license_file=str(opt_lic))
    assert used == str(opt_lic)


def test_fs_license_env_wins_over_freesurfer_home_file(tmp_path):
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("home")
    lic = tmp_path / "env.txt"
    lic.write_text("env")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": str(lic)})
    assert used == str(lic)


def test_fs_license_env_wins_over_docker_location(tmp_path):
    lic = tmp_path / "env.txt"
    lic.write_text("env")
    docker = tmp_path / "docker.txt"
    docker.write_text("docker")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(tmp_path), "FS_LICENSE": str(lic)},
        docker_license=docker)
    assert used == str(lic)


def test_docker_location_is_used_when_fs_license_names_nothing(tmp_path):
    docker = tmp_path / "docker.txt"
    docker.write_text("docker")
    _, used = make_wrapper(
        tmp_path,
        {"FREESURFER_HOME": str(tmp_path), "FS_LICENSE": str(tmp_path / "gone.txt")},
        docker_license=docker)
    assert used == str(docker)


@pytest.mark.parametrize("value", ["", "MISSING"])
def test_fs_license_env_naming_nothing_falls_back_to_the_fixed_locations(tmp_path, value):
    # The images always export FS_LICENSE, so a path with nothing bound at it is
    # the normal state and must not block the fixed locations.
    if value == "MISSING":
        value = str(tmp_path / "nothing-here.txt")
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("home")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": value})
    assert used == str(fs_home / "license.txt")


def test_fs_license_env_naming_a_directory_is_an_error(tmp_path):
    # A failed bind (Docker creates a directory) must not end on the license
    # bundled with FreeSurfer at $FREESURFER_HOME/license.txt.
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("bundled")
    (tmp_path / "adir").mkdir()
    with pytest.raises(IsADirectoryError, match="is a directory, not a file"):
        make_wrapper(
            tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": str(tmp_path / "adir")})


def test_directory_at_the_docker_location_is_an_error(tmp_path):
    docker = tmp_path / "docker-dir"
    docker.mkdir()
    with pytest.raises(IsADirectoryError, match="is a directory, not a file"):
        make_wrapper(tmp_path, {"FREESURFER_HOME": str(tmp_path)}, docker_license=docker)


def test_relative_fs_license_env_is_exported_as_an_absolute_path(tmp_path):
    cwd = tmp_path / "launch"
    cwd.mkdir()
    (cwd / "lic.txt").write_text("lic")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(tmp_path), "FS_LICENSE": "lic.txt"}, cwd=cwd)
    assert used == str(cwd / "lic.txt")
    assert os.path.isabs(used)


def test_missing_fs_license_is_a_warning_unless_it_is_the_image_default(tmp_path, caplog):
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("home")
    other = tmp_path / "gone.txt"
    with caplog.at_level(logging.INFO, logger="freesurfer-bidsapp.wrapper"):
        make_wrapper(tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": str(other)})
    records = [r for r in caplog.records if "FS_LICENSE is set to" in r.getMessage()]
    assert [r.levelno for r in records] == [logging.WARNING]
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="freesurfer-bidsapp.wrapper"):
        make_wrapper(tmp_path, {"FREESURFER_HOME": str(fs_home),
                                "FS_LICENSE": "/SGLR/FREESURFER_HOME/license.txt"})
    records = [r for r in caplog.records if "FS_LICENSE is set to" in r.getMessage()]
    assert [r.levelno for r in records] == [logging.INFO]


def test_empty_fs_license_env_is_ignored_silently(tmp_path, caplog):
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("home")
    with caplog.at_level(logging.INFO, logger="freesurfer-bidsapp.wrapper"):
        make_wrapper(tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": ""})
    assert not [r for r in caplog.records if "FS_LICENSE is set to" in r.getMessage()]


def test_fs_license_env_naming_nothing_and_no_other_license_fails(tmp_path):
    with pytest.raises(FileNotFoundError, match="FreeSurfer license not found"):
        make_wrapper(
            tmp_path,
            {"FREESURFER_HOME": str(tmp_path / "empty"),
             "FS_LICENSE": str(tmp_path / "nothing-here.txt")})

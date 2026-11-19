#!/usr/bin/env python3
"""Tests for the FreeSurfer license lookup in the wrapper."""

import os
from unittest.mock import patch

import pytest

from src.freesurfer.wrapper import FreeSurferWrapper


def make_wrapper(tmp_path, env, license_file=None, cwd=None):
    """Build a wrapper under a controlled environment (no FS_LICENSE, HOME in tmp)."""
    bids = tmp_path / "bids"
    bids.mkdir(exist_ok=True)
    out = tmp_path / "out"
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    full_env = {"HOME": str(home)}
    full_env.update(env)
    with patch.dict(os.environ, full_env, clear=True), \
            patch("src.freesurfer.wrapper.get_freesurfer_version", return_value="x"), \
            patch("src.freesurfer.wrapper.os.path.exists",
                  side_effect=lambda p: os.path.lexists(p) if p != "/license.txt" else False):
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


@pytest.mark.parametrize("value", ["", "MISSING", "DIR"])
def test_fs_license_env_naming_no_file_falls_back_to_the_fixed_locations(tmp_path, value):
    # The images always export FS_LICENSE, so a path with nothing bound at it is the
    # normal state and must not block the documented fixed locations.
    if value == "MISSING":
        value = str(tmp_path / "nothing-here.txt")
    elif value == "DIR":
        (tmp_path / "adir").mkdir()
        value = str(tmp_path / "adir")
    fs_home = tmp_path / "fshome"
    fs_home.mkdir()
    (fs_home / "license.txt").write_text("home")
    _, used = make_wrapper(
        tmp_path, {"FREESURFER_HOME": str(fs_home), "FS_LICENSE": value})
    assert used == str(fs_home / "license.txt")


def test_fs_license_env_naming_no_file_and_no_other_license_fails(tmp_path):
    with pytest.raises(FileNotFoundError, match="FreeSurfer license not found"):
        make_wrapper(
            tmp_path,
            {"FREESURFER_HOME": str(tmp_path / "empty"),
             "FS_LICENSE": str(tmp_path / "nothing-here.txt")})

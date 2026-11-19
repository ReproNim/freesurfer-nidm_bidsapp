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

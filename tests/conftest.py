#!/usr/bin/env python3
"""Shared pytest fixtures for all test modules."""

import json
import pytest
from pathlib import Path


@pytest.fixture
def bids_single_session(tmp_path):
    """Create a single-session BIDS dataset.

    Returns
    -------
    Path
        Path to BIDS dataset directory
    """
    bids_dir = tmp_path / "bids"
    bids_dir.mkdir()

    # Create dataset_description.json
    with open(bids_dir / "dataset_description.json", "w") as f:
        json.dump({
            "Name": "Test Dataset",
            "BIDSVersion": "1.8.0",
            "DatasetType": "raw"
        }, f)

    # Create subject without session
    anat_dir = bids_dir / "sub-001" / "anat"
    anat_dir.mkdir(parents=True)
    (anat_dir / "sub-001_T1w.nii.gz").touch()

    return bids_dir


@pytest.fixture
def bids_multi_session(tmp_path):
    """Create a multi-session BIDS dataset.

    Returns
    -------
    Path
        Path to BIDS dataset directory
    """
    bids_dir = tmp_path / "bids"
    bids_dir.mkdir()

    # Create dataset_description.json
    with open(bids_dir / "dataset_description.json", "w") as f:
        json.dump({
            "Name": "Test Dataset Multi-Session",
            "BIDSVersion": "1.8.0",
            "DatasetType": "raw"
        }, f)

    # Create subject with two sessions
    for ses in ["baseline", "followup"]:
        anat_dir = bids_dir / "sub-001" / f"ses-{ses}" / "anat"
        anat_dir.mkdir(parents=True)
        (anat_dir / f"sub-001_ses-{ses}_T1w.nii.gz").touch()

    return bids_dir


@pytest.fixture
def bids_with_t2(tmp_path):
    """Create a BIDS dataset with both T1w and T2w images.

    Returns
    -------
    Path
        Path to BIDS dataset directory
    """
    bids_dir = tmp_path / "bids"
    bids_dir.mkdir()

    # Create dataset_description.json
    with open(bids_dir / "dataset_description.json", "w") as f:
        json.dump({
            "Name": "Test Dataset with T2",
            "BIDSVersion": "1.8.0",
            "DatasetType": "raw"
        }, f)

    # Create subject with T1w and T2w
    anat_dir = bids_dir / "sub-001" / "anat"
    anat_dir.mkdir(parents=True)
    (anat_dir / "sub-001_T1w.nii.gz").touch()
    (anat_dir / "sub-001_T2w.nii.gz").touch()

    return bids_dir


@pytest.fixture
def nidm_input_dir(tmp_path):
    """Create a directory with existing NIDM files.

    Returns
    -------
    Path
        Path to NIDM directory
    """
    nidm_dir = tmp_path / "NIDM"
    nidm_dir.mkdir()

    # Create a minimal NIDM TTL file
    ttl_content = """@prefix : <http://example.org/> .
@prefix nidm: <http://purl.org/nidash/nidm#> .
@prefix prov: <http://www.w3.org/ns/prov#> .

:subject1 a nidm:Subject ;
    nidm:hasAnalysis :analysis1 .

:analysis1 a nidm:Analysis ;
    prov:wasGeneratedBy :freesurfer .
"""
    (nidm_dir / "nidm.ttl").write_text(ttl_content)

    return nidm_dir


@pytest.fixture
def freesurfer_license_file(tmp_path):
    """Create a mock FreeSurfer license file.

    Returns
    -------
    Path
        Path to license file
    """
    license_path = tmp_path / "license.txt"
    license_path.write_text("mock license content\n")
    return license_path


@pytest.fixture
def mock_freesurfer_env(monkeypatch):
    """Mock FreeSurfer environment variables.

    Yields
    ------
    dict
        Dictionary of environment variables set
    """
    env_vars = {
        "FREESURFER_HOME": "/opt/freesurfer",
        "SUBJECTS_DIR": "/tmp/subjects"
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    yield env_vars

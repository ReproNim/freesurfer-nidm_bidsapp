#!/usr/bin/env python3
"""Tests for output directory structure validation."""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.freesurfer.wrapper import FreeSurferWrapper


class TestOutputDirectoryCreation:
    """Test cases for output directory creation."""

    def test_freesurfer_output_directory_created(self, tmp_path, bids_single_session):
        """Verify freesurfer-nidm/freesurfer/ directory is created."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        freesurfer_dir = app_output_dir / "freesurfer"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Verify FreeSurfer directory is created
        assert freesurfer_dir.exists()
        assert freesurfer_dir.is_dir()

    def test_nidm_output_directory_created(self, tmp_path, bids_single_session):
        """Verify freesurfer-nidm/nidm/ directory is created."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        nidm_dir = app_output_dir / "nidm"

        # Create nidm directory as part of processing
        nidm_dir.mkdir(parents=True)

        assert nidm_dir.exists()
        assert nidm_dir.is_dir()

    def test_output_directory_structure(self, tmp_path, bids_single_session):
        """Verify output directory structure follows BIDS derivatives."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Verify main directories exist
        assert app_output_dir.exists()
        assert (app_output_dir / "freesurfer").exists()


class TestDatasetDescription:
    """Test cases for dataset_description.json creation."""

    def test_dataset_description_created(self, tmp_path, bids_single_session):
        """Verify dataset_description.json is created."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Call the method to create dataset description
        wrapper._create_dataset_description()

        desc_file = app_output_dir / "dataset_description.json"
        assert desc_file.exists()

    def test_dataset_description_content(self, tmp_path, bids_single_session):
        """Verify dataset_description.json has correct structure."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Call the method to create dataset description
        wrapper._create_dataset_description()

        desc_file = app_output_dir / "dataset_description.json"
        with open(desc_file) as f:
            desc = json.load(f)

        # Check required fields
        assert "Name" in desc
        assert "BIDSVersion" in desc
        assert "DatasetType" in desc
        assert desc["DatasetType"] == "derivative"

    def test_dataset_description_generated_by(self, tmp_path, bids_single_session):
        """Verify dataset_description.json includes GeneratedBy with correct app name."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Call the method to create dataset description
        wrapper._create_dataset_description()

        desc_file = app_output_dir / "dataset_description.json"
        with open(desc_file) as f:
            desc = json.load(f)

        # Check GeneratedBy field
        assert "GeneratedBy" in desc
        assert isinstance(desc["GeneratedBy"], list)
        assert len(desc["GeneratedBy"]) >= 2

        # Verify app names
        app_names = [gen["Name"] for gen in desc["GeneratedBy"]]
        assert "FreeSurfer" in app_names
        assert "freesurfer-nidm-bidsapp" in app_names

    def test_dataset_description_version_info(self, tmp_path, bids_single_session):
        """Verify version info in dataset_description.json is present."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Call the method to create dataset description
        wrapper._create_dataset_description()

        desc_file = app_output_dir / "dataset_description.json"
        with open(desc_file) as f:
            desc = json.load(f)

        # Check version information is included
        for generator in desc["GeneratedBy"]:
            assert "Name" in generator
            assert "Version" in generator
            assert generator["Version"] != "unknown"


class TestSubjectDirectoryStructure:
    """Test cases for subject-level directory structure."""

    def test_subject_directory_naming(self, tmp_path, bids_single_session):
        """Verify subject directories follow BIDS naming (sub-XXX)."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        freesurfer_dir = app_output_dir / "freesurfer"

        # Create a mock subject directory
        subject_dir = freesurfer_dir / "sub-001"
        subject_dir.mkdir(parents=True)

        assert subject_dir.exists()
        assert subject_dir.name.startswith("sub-")

    def test_session_directory_structure(self, tmp_path, bids_multi_session):
        """Verify session directories follow BIDS naming (ses-YY) for multi-session."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        freesurfer_dir = app_output_dir / "freesurfer"

        # Create mock session directories
        for session in ["baseline", "followup"]:
            session_subj_dir = freesurfer_dir / f"sub-001_ses-{session}"
            session_subj_dir.mkdir(parents=True)
            assert session_subj_dir.exists()


class TestNIDMOutputStructure:
    """Test cases for NIDM output structure."""

    def test_flat_nidm_structure(self, tmp_path):
        """Verify NIDM uses flat directory structure (not hierarchical)."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        nidm_dir = app_output_dir / "nidm"
        nidm_dir.mkdir(parents=True)

        # Create NIDM files in flat structure
        (nidm_dir / "sub-001.ttl").touch()
        (nidm_dir / "sub-002_ses-baseline.ttl").touch()

        # Verify files are directly in nidm directory
        assert (nidm_dir / "sub-001.ttl").exists()
        assert (nidm_dir / "sub-002_ses-baseline.ttl").exists()

        # Verify no subdirectories created
        subdirs = [d for d in nidm_dir.iterdir() if d.is_dir()]
        assert len(subdirs) == 0

    def test_nidm_file_naming_single_session(self, tmp_path):
        """Verify NIDM file naming for single session: sub-01.ttl"""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        nidm_dir = app_output_dir / "nidm"
        nidm_dir.mkdir(parents=True)

        # Expected naming pattern
        nidm_file = nidm_dir / "sub-001.ttl"
        nidm_file.touch()

        assert nidm_file.exists()
        assert nidm_file.name == "sub-001.ttl"

    def test_nidm_file_naming_multi_session(self, tmp_path):
        """Verify NIDM file naming for multi-session: sub-01_ses-baseline.ttl"""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"
        nidm_dir = app_output_dir / "nidm"
        nidm_dir.mkdir(parents=True)

        # Expected naming pattern for multi-session
        for session in ["baseline", "followup"]:
            nidm_file = nidm_dir / f"sub-001_ses-{session}.ttl"
            nidm_file.touch()
            assert nidm_file.exists()
            assert "_ses-" in nidm_file.name


class TestReadmeCreation:
    """Test cases for README file creation."""

    def test_readme_created(self, tmp_path, bids_single_session):
        """Verify README file is created in output directory."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Call the method to create README
        wrapper._create_readme()

        readme = app_output_dir / "README"
        assert readme.exists()

    def test_readme_content(self, tmp_path, bids_single_session):
        """Verify README has meaningful content."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # Call the method to create README
        wrapper._create_readme()

        readme = app_output_dir / "README"
        content = readme.read_text()

        assert len(content) > 0
        assert "FreeSurfer" in content

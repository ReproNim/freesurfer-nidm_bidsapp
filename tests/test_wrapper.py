#!/usr/bin/env python3
"""
Tests for the FreeSurfer wrapper class.
"""

import json
import os
import pytest
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.freesurfer.wrapper import FreeSurferWrapper

@pytest.fixture
def bids_dataset(tmp_path):
    """Create a temporary BIDS dataset for testing."""
    # Create basic BIDS structure
    bids_dir = tmp_path / "bids_dataset"
    bids_dir.mkdir(parents=True)

    # Create dataset_description.json
    with open(bids_dir / "dataset_description.json", "w") as f:
        json.dump({
            "Name": "Test BIDS Dataset",
            "BIDSVersion": "1.4.0",
            "DatasetType": "raw"
        }, f)

    # Create subject directory
    subject_dir = bids_dir / "sub-001"
    subject_dir.mkdir()

    # Create anat directory
    anat_dir = subject_dir / "anat"
    anat_dir.mkdir()

    # Create dummy T1w image
    t1w_file = anat_dir / "sub-001_T1w.nii.gz"
    t1w_file.touch()

    # Create dummy T2w image
    t2w_file = anat_dir / "sub-001_T2w.nii.gz"
    t2w_file.touch()

    return bids_dir


@pytest.fixture
def freesurfer_license(tmp_path):
    """Create a dummy FreeSurfer license file."""
    license_file = tmp_path / "license.txt"
    license_file.write_text("dummy license")
    return license_file


@pytest.fixture
def output_dir(tmp_path):
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_bids_layout():
    """Create a mock BIDS layout."""
    layout = MagicMock()
    
    def get_mock(**kwargs):
        if kwargs.get('suffix') == 'T1w':
            return ["sub-001/anat/sub-001_T1w.nii.gz"]
        elif kwargs.get('suffix') == 'T2w':
            return ["sub-001/anat/sub-001_T2w.nii.gz"]
        return []
    
    layout.get.side_effect = get_mock
    layout.get_subjects.return_value = ["sub-001"]
    return layout


def test_wrapper_initialization(bids_dataset, output_dir, freesurfer_license):
    """Test wrapper initialization."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        assert wrapper.bids_dir == Path(bids_dataset)
        assert wrapper.output_dir == Path(output_dir)
        # recon-all stages here; the finished tree is relocated to
        # <output_dir>/sub-<id>/freesurfer so the subject dir is the zip top-level.
        assert wrapper.freesurfer_dir == Path(output_dir) / ".fs_staging"
        assert wrapper.subject_output_dir("sub-001") == Path(output_dir) / "sub-001"
        assert (
            wrapper.subject_output_dir("sub-001", "baseline")
            == Path(output_dir) / "sub-001" / "ses-baseline"
        )
        assert wrapper.freesurfer_license == str(freesurfer_license)


def test_create_recon_all_command(bids_dataset, output_dir, freesurfer_license):
    """Test creation of recon-all command."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        t1w_images = ["sub-001/anat/sub-001_T1w.nii.gz"]
        t2w_images = ["sub-001/anat/sub-001_T2w.nii.gz"]
        cmd = wrapper._create_recon_all_command("sub-001", t1w_images, t2w_images)

        # Check command structure
        assert cmd[0:3] == ["recon-all", "-subjid", "sub-001"]
        assert "-i" in cmd
        assert t1w_images[0] in cmd
        assert "-T2" in cmd
        assert t2w_images[0] in cmd
        assert "-T2pial" in cmd
        assert cmd[-1] == "-all"  # Check -all is at the end


def test_find_images(bids_dataset, output_dir, freesurfer_license, mock_bids_layout):
    """Test finding T1w images."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        t1w_images = wrapper._find_images(mock_bids_layout, "sub-001", "T1w")
        assert len(t1w_images) == 1
        assert "T1w.nii.gz" in t1w_images[0]


def test_process_subject(bids_dataset, output_dir, freesurfer_license, mock_bids_layout):
    """Test processing a subject."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        def fake_recon_all(cmd, *args, **kwargs):
            """recon-all is mocked, so fabricate the tree it would have staged."""
            staged = wrapper.freesurfer_dir / "sub-001"
            (staged / "scripts").mkdir(parents=True, exist_ok=True)
            (staged / "mri").mkdir(exist_ok=True)
            (staged / "stats").mkdir(exist_ok=True)
            (staged / "scripts" / "recon-all.done").write_text("")
            return MagicMock(returncode=0)

        with patch('subprocess.run', side_effect=fake_recon_all) as mock_run:
            success = wrapper.process_subject("sub-001", mock_bids_layout)
            assert success
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert "recon-all" in cmd
            assert "-subjid" in cmd
            assert "sub-001" in cmd
            assert "-all" in cmd[-1]  # Check -all is at the end

            # The staged tree must have been relocated under the subject dir.
            final_fs = Path(output_dir) / "sub-001" / "freesurfer"
            assert final_fs.is_dir(), f"expected relocated tree at {final_fs}"
            assert (final_fs / "scripts" / "recon-all.done").exists()
            assert not (wrapper.freesurfer_dir / "sub-001").exists(), "staging not cleaned up"


def test_process_subject_fails_when_recon_all_produces_nothing(
    bids_dataset, output_dir, freesurfer_license, mock_bids_layout
):
    """A recon-all that exits 0 but writes nothing must not be reported as success."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        with patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0  # exits clean, produces no tree
            success = wrapper.process_subject("sub-001", mock_bids_layout)
            assert success is False
            assert "sub-001" in wrapper.results["failure"]


def test_processing_summary(bids_dataset, output_dir, freesurfer_license):
    """Test generation of processing summary."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        # Add some results
        wrapper.results["success"].append("sub-001")
        wrapper.results["failure"].append("sub-002")
        wrapper.results["skipped"].append("sub-003")

        # Get summary
        summary = wrapper.get_processing_summary()

        assert summary["total"] == 3
        assert summary["success"] == 1
        assert summary["failure"] == 1
        assert summary["skipped"] == 1
        assert "sub-001" in summary["success_list"]
        assert "sub-002" in summary["failure_list"]
        assert "sub-003" in summary["skipped_list"]


def test_save_processing_summary(bids_dataset, output_dir, freesurfer_license):
    """Test saving processing summary to file."""
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        # Add some results
        wrapper.results["success"].append("sub-001")
        wrapper.results["failure"].append("sub-002")
        wrapper.results["skipped"].append("sub-003")

        # Get the summary and print it for debugging
        summary = wrapper.get_processing_summary()
        print("\nDebug - Summary content:", summary)

        # Save summary and print the return value
        summary_file = wrapper.save_processing_summary(summary)
        print("\nDebug - Returned summary_file:", summary_file)
        
        if summary_file is None:
            print("\nDebug - summary_file is None!")
        
        # Print the freesurfer directory structure
        print("\nDebug - FreeSurfer directory contents:")
        print(os.listdir(wrapper.freesurfer_dir))

        # Original assertions...
        assert os.path.exists(summary_file), f"Summary file does not exist at: {summary_file}"
        with open(summary_file) as f:
            saved_summary = json.load(f)
            print("\nDebug - Loaded summary content:", saved_summary)
            assert saved_summary["total"] == 3
            assert saved_summary["success"] == 1
            assert saved_summary["failure"] == 1
            assert saved_summary["skipped"] == 1


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

from src.freesurfer.wrapper import (
    FreeSurferWrapper,
    recon_all_finished,
    recorded_recon_inputs,
    select_single_image,
    select_t1w_inputs,
)

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



# --- Input image selection ---------------------------------------------------
# ABIDE II UCLA_Long sessions hold both a standard T1w and an acq-hires T1w.
# Runs of one acquisition are averaged by recon-all; different acquisitions
# must not be.

UCLA_ANAT = "sub-29001/ses-1/anat"


def test_select_t1w_prefers_images_without_acq():
    """Non-acq T1w images win; every acq- variant is excluded."""
    paths = [
        f"{UCLA_ANAT}/sub-29001_ses-1_acq-hires_run-1_T1w.nii.gz",
        f"{UCLA_ANAT}/sub-29001_ses-1_run-2_T1w.nii.gz",
        f"{UCLA_ANAT}/sub-29001_ses-1_run-1_T1w.nii.gz",
    ]
    chosen, excluded = select_t1w_inputs(paths)
    assert chosen == [
        f"{UCLA_ANAT}/sub-29001_ses-1_run-1_T1w.nii.gz",
        f"{UCLA_ANAT}/sub-29001_ses-1_run-2_T1w.nii.gz",
    ]
    assert excluded == [f"{UCLA_ANAT}/sub-29001_ses-1_acq-hires_run-1_T1w.nii.gz"]


def test_select_t1w_uses_one_acq_label_when_only_variants_exist():
    """With only acq- variants, the best-ranked image (lowest run, then name) fixes the
    label, and every run of that label is used."""
    paths = [
        f"{UCLA_ANAT}/sub-29001_ses-1_acq-lores_run-1_T1w.nii.gz",
        f"{UCLA_ANAT}/sub-29001_ses-1_acq-hires_run-2_T1w.nii.gz",
        f"{UCLA_ANAT}/sub-29001_ses-1_acq-hires_run-1_T1w.nii.gz",
    ]
    chosen, excluded = select_t1w_inputs(paths)
    assert chosen == [
        f"{UCLA_ANAT}/sub-29001_ses-1_acq-hires_run-1_T1w.nii.gz",
        f"{UCLA_ANAT}/sub-29001_ses-1_acq-hires_run-2_T1w.nii.gz",
    ]
    assert excluded == [f"{UCLA_ANAT}/sub-29001_ses-1_acq-lores_run-1_T1w.nii.gz"]


def test_select_t1w_single_image_and_empty():
    """A lone T1w is used as-is; no images gives nothing to use or exclude."""
    assert select_t1w_inputs(["sub-001/anat/sub-001_T1w.nii.gz"]) == (
        ["sub-001/anat/sub-001_T1w.nii.gz"], []
    )
    assert select_t1w_inputs([]) == ([], [])


def test_select_t1w_orders_runs_numerically():
    """run-10 sorts after run-2, so the -i order is stable and readable."""
    paths = ["sub-001_run-10_T1w.nii.gz", "sub-001_run-2_T1w.nii.gz"]
    chosen, _ = select_t1w_inputs(paths)
    assert chosen == ["sub-001_run-2_T1w.nii.gz", "sub-001_run-10_T1w.nii.gz"]


def test_select_single_image_prefers_no_acq_then_lowest_run():
    """T2w: recon-all takes one; pick no-acq first, then lowest run, then name."""
    paths = [
        "sub-001_acq-fast_run-1_T2w.nii.gz",
        "sub-001_run-2_T2w.nii.gz",
        "sub-001_run-1_T2w.nii.gz",
    ]
    chosen, excluded = select_single_image(paths)
    assert chosen == "sub-001_run-1_T2w.nii.gz"
    assert excluded == ["sub-001_acq-fast_run-1_T2w.nii.gz", "sub-001_run-2_T2w.nii.gz"]
    assert select_single_image([]) == (None, [])


@pytest.fixture
def ucla_layout(bids_dataset):
    """Mock layout for a session holding run-1 T1w, an acq-hires T1w and two T2w."""
    anat = bids_dataset / "sub-001" / "ses-1" / "anat"
    files = {
        "T1w": [
            str(anat / "sub-001_ses-1_acq-hires_run-1_T1w.nii.gz"),
            str(anat / "sub-001_ses-1_run-1_T1w.nii.gz"),
        ],
        "T2w": [
            str(anat / "sub-001_ses-1_run-2_T2w.nii.gz"),
            str(anat / "sub-001_ses-1_run-1_T2w.nii.gz"),
        ],
    }
    layout = MagicMock()
    layout.get.side_effect = lambda **kwargs: list(files.get(kwargs.get("suffix"), []))
    return layout, anat


def test_process_subject_excludes_acq_variants_and_records_inputs(
    bids_dataset, output_dir, freesurfer_license, ucla_layout
):
    """recon-all gets only the chosen images; the summary records used and unused inputs."""
    layout, anat = ucla_layout
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )

        def fake_recon_all(cmd, *args, **kwargs):
            staged = wrapper.freesurfer_dir / "sub-001_ses-1"
            (staged / "scripts").mkdir(parents=True, exist_ok=True)
            (staged / "scripts" / "recon-all.done").write_text("")
            return MagicMock(returncode=0)

        with patch('subprocess.run', side_effect=fake_recon_all) as mock_run, \
             patch("src.freesurfer.wrapper.voxel_size", return_value=(1.0, 1.0, 1.0)):
            assert wrapper.process_subject("sub-001", layout, session_label="1")
            cmd = mock_run.call_args[0][0]

        inputs = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-i"]
        assert inputs == [str(anat / "sub-001_ses-1_run-1_T1w.nii.gz")]
        assert cmd[cmd.index("-T2") + 1] == str(anat / "sub-001_ses-1_run-1_T2w.nii.gz")

        summary = wrapper.get_processing_summary()
        assert summary["input_files"] == [
            "sub-001/ses-1/anat/sub-001_ses-1_run-1_T1w.nii.gz",
            "sub-001/ses-1/anat/sub-001_ses-1_run-1_T2w.nii.gz",
        ]
        assert [entry["path"] for entry in summary["inputs_not_used"]] == [
            "sub-001/ses-1/anat/sub-001_ses-1_acq-hires_run-1_T1w.nii.gz",
            "sub-001/ses-1/anat/sub-001_ses-1_run-2_T2w.nii.gz",
        ]
        assert "acq-" in summary["inputs_not_used"][0]["reason"]


# --- Voxel-size limit ----------------------------------------------------------
# ABIDE II UCLA_Long acq-hires T1w images are 1.5x1.5x4.0 mm slabs, not high
# resolution. recon-all on such an image exits 0 with a meaningless result, so
# images coarser than --max-voxel-size are refused before it runs.

FINE = (1.2, 1.0, 1.0)
SLAB = (1.5, 1.5, 4.0)


def _layout(anat, t1w, t2w=()):
    files = {"T1w": [str(anat / f) for f in t1w], "T2w": [str(anat / f) for f in t2w]}
    layout = MagicMock()
    layout.get.side_effect = lambda **kwargs: list(files.get(kwargs.get("suffix"), []))
    return layout


def _zooms(sizes):
    """voxel_size stand-in: look the file name up in sizes."""
    return lambda path: sizes[Path(path).name]


def _wrapper(bids_dataset, output_dir, freesurfer_license, **kwargs):
    return FreeSurferWrapper(
        str(bids_dataset), str(output_dir), freesurfer_license=str(freesurfer_license), **kwargs
    )


def _fake_recon_all(wrapper, fs_subject_id):
    def run(cmd, *args, **kwargs):
        staged = wrapper.freesurfer_dir / fs_subject_id
        (staged / "scripts").mkdir(parents=True, exist_ok=True)
        (staged / "scripts" / "recon-all.done").write_text("")
        return MagicMock(returncode=0)
    return run


def test_coarse_t1w_run_is_dropped_when_another_run_passes(bids_dataset, output_dir, freesurfer_license):
    """A too-coarse run is left out with a recorded reason; the others still go to recon-all."""
    anat = bids_dataset / "sub-001" / "anat"
    layout = _layout(anat, ["sub-001_run-1_T1w.nii.gz", "sub-001_run-2_T1w.nii.gz"])
    sizes = {"sub-001_run-1_T1w.nii.gz": SLAB, "sub-001_run-2_T1w.nii.gz": FINE}
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms(sizes)), \
             patch("subprocess.run", side_effect=_fake_recon_all(wrapper, "sub-001")) as mock_run:
            assert wrapper.process_subject("sub-001", layout)
            cmd = mock_run.call_args[0][0]

    inputs = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-i"]
    assert inputs == [str(anat / "sub-001_run-2_T1w.nii.gz")]
    summary = wrapper.get_processing_summary()
    assert summary["input_files"] == ["sub-001/anat/sub-001_run-2_T1w.nii.gz"]
    dropped = summary["inputs_not_used"]
    assert [entry["path"] for entry in dropped] == ["sub-001/anat/sub-001_run-1_T1w.nii.gz"]
    assert "4.0" in dropped[0]["reason"] and "2.0" in dropped[0]["reason"]
    assert "failure_reason" not in summary


def test_subject_fails_when_every_t1w_is_too_coarse(bids_dataset, output_dir, freesurfer_license):
    """UCLA_Long ses-1 with only acq-hires: recon-all never runs and the reason is recorded."""
    anat = bids_dataset / "sub-001" / "ses-1" / "anat"
    name = "sub-001_ses-1_acq-hires_run-1_T1w.nii.gz"
    layout = _layout(anat, [name])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms({name: SLAB})), \
             patch("subprocess.run") as mock_run:
            assert wrapper.process_subject("sub-001", layout, session_label="1") is False
            mock_run.assert_not_called()

    assert "sub-001_ses-1" in wrapper.results["failure"]
    summary = wrapper.get_processing_summary()
    assert summary["input_files"] == []
    assert [entry["path"] for entry in summary["inputs_not_used"]] == [f"sub-001/ses-1/anat/{name}"]
    assert name in summary["failure_reason"]
    assert "1.5x1.5x4.0" in summary["failure_reason"]


def test_coarse_t2w_is_skipped_not_fatal(bids_dataset, output_dir, freesurfer_license):
    """A too-coarse T2w is left out; recon-all runs on the T1w alone."""
    anat = bids_dataset / "sub-001" / "anat"
    layout = _layout(anat, ["sub-001_T1w.nii.gz"], ["sub-001_T2w.nii.gz"])
    sizes = {"sub-001_T1w.nii.gz": FINE, "sub-001_T2w.nii.gz": SLAB}
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms(sizes)), \
             patch("subprocess.run", side_effect=_fake_recon_all(wrapper, "sub-001")) as mock_run:
            assert wrapper.process_subject("sub-001", layout)
            cmd = mock_run.call_args[0][0]

    assert "-T2" not in cmd and "-T2pial" not in cmd
    summary = wrapper.get_processing_summary()
    assert summary["input_files"] == ["sub-001/anat/sub-001_T1w.nii.gz"]
    assert [entry["path"] for entry in summary["inputs_not_used"]] == ["sub-001/anat/sub-001_T2w.nii.gz"]


def test_max_voxel_size_is_configurable(bids_dataset, output_dir, freesurfer_license):
    """Raising the limit admits the slab (an explicit opt-in)."""
    anat = bids_dataset / "sub-001" / "anat"
    layout = _layout(anat, ["sub-001_T1w.nii.gz"])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license, max_voxel_size=4.0)
        assert wrapper.max_voxel_size == 4.0
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms({"sub-001_T1w.nii.gz": SLAB})), \
             patch("subprocess.run", side_effect=_fake_recon_all(wrapper, "sub-001")):
            assert wrapper.process_subject("sub-001", layout)


def test_default_max_voxel_size(bids_dataset, output_dir, freesurfer_license):
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        assert _wrapper(bids_dataset, output_dir, freesurfer_license).max_voxel_size == 2.0


def test_voxel_size_reads_header_zooms(tmp_path):
    """voxel_size returns the first three zooms of a NIfTI header."""
    import nibabel as nib
    import numpy as np

    img = nib.Nifti1Image(np.zeros((4, 4, 2, 3), dtype=np.uint8), np.eye(4))
    img.header.set_zooms((1.5, 1.5, 4.0, 2.0))
    path = tmp_path / "sub-001_T1w.nii.gz"
    nib.save(img, str(path))

    from src.freesurfer.wrapper import voxel_size
    assert voxel_size(path) == (1.5, 1.5, 4.0)


# --- Coarse images are set aside before the choice ------------------------------
# A too-coarse image must not decide which acquisition or which T2w is used, and
# every recorded "not used" reason must be true.

def test_coarse_t2w_run_does_not_hide_a_fine_one(bids_dataset, output_dir, freesurfer_license):
    """T2w run-1 is a slab and run-2 is 1 mm: run-2 goes to -T2, run-1 is left out as too coarse."""
    anat = bids_dataset / "sub-001" / "anat"
    layout = _layout(anat, ["sub-001_T1w.nii.gz"],
                     ["sub-001_run-1_T2w.nii.gz", "sub-001_run-2_T2w.nii.gz"])
    sizes = {"sub-001_T1w.nii.gz": FINE, "sub-001_run-1_T2w.nii.gz": (0.45, 0.45, 4.0),
             "sub-001_run-2_T2w.nii.gz": FINE}
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms(sizes)), \
             patch("subprocess.run", side_effect=_fake_recon_all(wrapper, "sub-001")) as mock_run:
            assert wrapper.process_subject("sub-001", layout)
            cmd = mock_run.call_args[0][0]

    assert cmd[cmd.index("-T2") + 1] == str(anat / "sub-001_run-2_T2w.nii.gz")
    assert wrapper.get_processing_summary()["inputs_not_used"] == [{
        "path": "sub-001/anat/sub-001_run-1_T2w.nii.gz",
        "reason": "voxel size 0.45x0.45x4.0 mm exceeds --max-voxel-size 2.0 mm",
    }]


def test_coarse_plain_t1w_does_not_exclude_a_fine_variant(bids_dataset, output_dir, freesurfer_license):
    """The plain T1w is a slab and the acq-mprage T1w is 1 mm: the variant is used, and the
    plain image is recorded as too coarse rather than the variant as the wrong acquisition."""
    anat = bids_dataset / "sub-001" / "anat"
    layout = _layout(anat, ["sub-001_T1w.nii.gz", "sub-001_acq-mprage_T1w.nii.gz"])
    sizes = {"sub-001_T1w.nii.gz": SLAB, "sub-001_acq-mprage_T1w.nii.gz": FINE}
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms(sizes)), \
             patch("subprocess.run", side_effect=_fake_recon_all(wrapper, "sub-001")) as mock_run:
            assert wrapper.process_subject("sub-001", layout)
            cmd = mock_run.call_args[0][0]

    assert [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-i"] == [
        str(anat / "sub-001_acq-mprage_T1w.nii.gz")
    ]
    summary = wrapper.get_processing_summary()
    assert summary["input_files"] == ["sub-001/anat/sub-001_acq-mprage_T1w.nii.gz"]
    assert summary["inputs_not_used"] == [{
        "path": "sub-001/anat/sub-001_T1w.nii.gz",
        "reason": "voxel size 1.5x1.5x4.0 mm exceeds --max-voxel-size 2.0 mm",
    }]


# --- Same acquisition as ants-nidm ------------------------------------------------
# Both apps process the same sessions, so they must start from the same image. The
# expected ants-nidm choices below were produced by running its own
# choose_anat_input (ReproNim/ants-nidm_bidsapp d7009d4, src/antspy/inputs.py,
# max_voxel_size=2.0) on these same file names and voxel sizes. FreeSurfer then
# averages every run of that image's acquisition.

ISO = (1.0, 1.0, 1.0)

ANTS_PARITY = [
    # case, {file name: voxel size}, ants-nidm's choice, FreeSurfer's -i inputs
    ("rec_variant",
     {"sub-01_T1w.nii.gz": ISO, "sub-01_rec-norm_T1w.nii.gz": ISO},
     "sub-01_T1w.nii.gz", ["sub-01_T1w.nii.gz"]),
    ("plain_slab_acq_fine",
     {"sub-01_T1w.nii.gz": SLAB, "sub-01_acq-mprage_T1w.nii.gz": ISO},
     "sub-01_acq-mprage_T1w.nii.gz", ["sub-01_acq-mprage_T1w.nii.gz"]),
    ("two_acq_labels",
     {"sub-01_acq-a_run-2_T1w.nii.gz": ISO, "sub-01_acq-b_run-1_T1w.nii.gz": ISO},
     "sub-01_acq-b_run-1_T1w.nii.gz", ["sub-01_acq-b_run-1_T1w.nii.gz"]),
    ("just_over_limit_in_float",
     {"sub-01_T1w.nii.gz": (1.0, 1.0, 2.0004)},
     "sub-01_T1w.nii.gz", ["sub-01_T1w.nii.gz"]),
    ("runs_and_acq_variant",
     {"sub-01_run-1_T1w.nii.gz": ISO, "sub-01_run-2_T1w.nii.gz": ISO,
      "sub-01_acq-hires_run-1_T1w.nii.gz": ISO},
     "sub-01_run-1_T1w.nii.gz", ["sub-01_run-1_T1w.nii.gz", "sub-01_run-2_T1w.nii.gz"]),
    ("slab_run_fine_run",
     {"sub-01_run-1_T1w.nii.gz": SLAB, "sub-01_run-2_T1w.nii.gz": ISO},
     "sub-01_run-2_T1w.nii.gz", ["sub-01_run-2_T1w.nii.gz"]),
    ("only_slab_variant",
     {"sub-01_acq-hires_run-1_T1w.nii.gz": SLAB},
     None, []),
    ("rec_variant_without_run",
     {"sub-01_run-1_T1w.nii.gz": ISO, "sub-01_rec-norm_T1w.nii.gz": ISO},
     "sub-01_run-1_T1w.nii.gz", ["sub-01_run-1_T1w.nii.gz"]),
    ("rec_and_acq_variants_only",
     {"sub-01_acq-fast_T1w.nii.gz": ISO, "sub-01_rec-norm_run-2_T1w.nii.gz": ISO},
     "sub-01_rec-norm_run-2_T1w.nii.gz", ["sub-01_rec-norm_run-2_T1w.nii.gz"]),
]


@pytest.mark.parametrize("case,files,ants_choice,freesurfer_inputs", ANTS_PARITY,
                         ids=[c[0] for c in ANTS_PARITY])
def test_t1w_choice_matches_ants_nidm(tmp_path, freesurfer_license, case, files, ants_choice,
                                      freesurfer_inputs):
    """Real NIfTI headers and a real BIDSLayout, as in a run."""
    import nibabel as nib
    import numpy as np
    from bids import BIDSLayout

    bids = tmp_path / "bids"
    anat = bids / "sub-01" / "anat"
    anat.mkdir(parents=True)
    (bids / "dataset_description.json").write_text(json.dumps({"Name": "parity", "BIDSVersion": "1.8.0"}))
    for name, zooms in files.items():
        img = nib.Nifti1Image(np.zeros((4, 4, 4), dtype=np.uint8), np.eye(4))
        img.header.set_zooms(zooms)
        nib.save(img, str(anat / name))
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(str(bids), str(tmp_path / "out"),
                                    freesurfer_license=str(freesurfer_license))
        info = wrapper._select_inputs(BIDSLayout(str(bids), validate=False), "01", None, "sub-01")

    chosen = [Path(p).name for p in info["T1w_images"]]
    assert chosen == freesurfer_inputs
    assert (chosen[0] if chosen else None) == ants_choice
    assert ("failure_reason" in info) == (ants_choice is None)


# --- Reusing an existing reconstruction --------------------------------------------
# A re-run over an existing recon-all tree, and --skip-freesurfer, judge the tree by
# the images it was made from, as recon-all recorded them -- not by what the current
# selection would choose, which may name other files.

# scripts/recon-all.done of a finished run, in the format FreeSurfer 8.0.0 writes
# (copied from $FREESURFER_HOME/subjects/bert/scripts/recon-all.done, with the
# CMDARGS line replaced by the arguments this app passes).
RECON_ALL_DONE = """------------------------------
SUBJECT {subject}
START_TIME Wed Jan 29 23:00:08 EST 2025
END_TIME Thu Jan 30 01:36:42 EST 2025
RUNTIME_HOURS 2.609
USER someone
HOST node1
PROCESSOR x86_64
OS Linux
UNAME Linux node1 3.10.0 #1 SMP x86_64 x86_64 x86_64 GNU/Linux
VERSION freesurfer-linux-ubuntu22_x86_64-8.0.0-20250129-fb416cd
CMDPATH /opt/freesurfer-8.0.0/bin/recon-all
CMDARGS {cmdargs}
"""

# The BIDS root of the job that made the tree (a BABS job's clone), long gone.
PREVIOUS_ROOT = "/scratch/job-41/ds/inputs/data/BIDS"


def _recon_tree(fs_dir, cmdargs=None, failed=False):
    """A recon-all tree as recon-all leaves it: finished (with CMDARGS) or failed."""
    scripts = fs_dir / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    if failed:
        # recon-all's error exit: recon-all.error, and "1" written to recon-all.done.
        (scripts / "recon-all.error").write_text("ERROR: mri_convert failed\n")
        (scripts / "recon-all.done").write_text("1\n")
    else:
        (scripts / "recon-all.done").write_text(
            RECON_ALL_DONE.format(subject=fs_dir.name, cmdargs=cmdargs or "-all")
        )
    return fs_dir


def _cmdargs(subjid, anat, t1w, t2w=None):
    args = ["-subjid", subjid] + [a for name in t1w for a in ("-i", f"{PREVIOUS_ROOT}/{anat}/{name}")]
    if t2w:
        args += ["-T2", f"{PREVIOUS_ROOT}/{anat}/{t2w}", "-T2pial"]
    return " ".join(args + ["-all"])


def test_recorded_recon_inputs_reads_recon_all_done(tmp_path):
    fs_dir = _recon_tree(tmp_path / "sub-001", _cmdargs(
        "sub-001", "sub-001/anat", ["sub-001_run-1_T1w.nii.gz", "sub-001_run-2_T1w.nii.gz"],
        t2w="sub-001_T2w.nii.gz"))
    assert recorded_recon_inputs(fs_dir) == (
        [f"{PREVIOUS_ROOT}/sub-001/anat/sub-001_run-1_T1w.nii.gz",
         f"{PREVIOUS_ROOT}/sub-001/anat/sub-001_run-2_T1w.nii.gz"],
        [f"{PREVIOUS_ROOT}/sub-001/anat/sub-001_T2w.nii.gz"],
    )
    assert recon_all_finished(fs_dir)


def test_recorded_recon_inputs_without_an_i_argument(tmp_path):
    """A later recon-all call without -i (e.g. after manual edits) rewrites recon-all.done."""
    fs_dir = _recon_tree(tmp_path / "sub-001", "-s sub-001 -autorecon2-wm -autorecon3")
    assert recorded_recon_inputs(fs_dir) is None


def test_failed_recon_all_is_not_finished(tmp_path):
    fs_dir = _recon_tree(tmp_path / "sub-001", failed=True)
    assert (fs_dir / "scripts" / "recon-all.done").exists()
    assert not recon_all_finished(fs_dir)


def test_rerun_reuses_a_tree_with_the_inputs_it_records(bids_dataset, output_dir, freesurfer_license):
    """A tree made from run-1 and acq-hires together (before input selection existed) keeps
    that record: the summary names both images, not what the current rule would choose."""
    anat = bids_dataset / "sub-001" / "ses-1" / "anat"
    t1w = ["sub-001_ses-1_run-1_T1w.nii.gz", "sub-001_ses-1_acq-hires_run-1_T1w.nii.gz"]
    layout = _layout(anat, t1w, ["sub-001_ses-1_T2w.nii.gz"])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.subject_output_dir("sub-001", "1") / "freesurfer",
                    _cmdargs("sub-001_ses-1", "sub-001/ses-1/anat", t1w))
        with patch("src.freesurfer.wrapper.voxel_size", return_value=FINE), \
             patch("subprocess.run") as mock_run:
            assert wrapper.process_subject("sub-001", layout, session_label="1")
            mock_run.assert_not_called()

    assert "sub-001_ses-1" in wrapper.results["skipped"]
    summary = wrapper.get_processing_summary()
    assert summary["input_files"] == [f"sub-001/ses-1/anat/{name}" for name in t1w]
    assert summary["inputs_not_used"] == [{
        "path": "sub-001/ses-1/anat/sub-001_ses-1_T2w.nii.gz",
        "reason": "not an input of the existing reconstruction",
    }]
    assert wrapper.get_subject_t1_info("sub-001", "1")["T1w_images"] == [str(anat / n) for n in t1w]


def test_rerun_refuses_a_tree_made_from_a_too_coarse_image(bids_dataset, output_dir, freesurfer_license):
    """UCLA_Long ses-1 with only acq-hires, reconstructed before the voxel check existed: the
    tree is not reused and the subject fails; recon-all is not started either."""
    anat = bids_dataset / "sub-001" / "ses-1" / "anat"
    name = "sub-001_ses-1_acq-hires_run-1_T1w.nii.gz"
    layout = _layout(anat, [name])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.subject_output_dir("sub-001", "1") / "freesurfer",
                    _cmdargs("sub-001_ses-1", "sub-001/ses-1/anat", [name]))
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms({name: SLAB})), \
             patch("subprocess.run") as mock_run:
            assert wrapper.process_subject("sub-001", layout, session_label="1") is False
            mock_run.assert_not_called()

    assert "sub-001_ses-1" in wrapper.results["failure"]
    reason = wrapper.get_processing_summary()["failure_reason"]
    assert f"{name} (1.5x1.5x4.0 mm)" in reason
    assert "coarser than --max-voxel-size 2.0 mm" in reason


def test_rerun_refuses_a_coarse_t2w_the_tree_was_made_with(bids_dataset, output_dir, freesurfer_license):
    """recon-all refined the pial surface with the T2w, so a too-coarse T2w disqualifies the tree."""
    anat = bids_dataset / "sub-001" / "anat"
    layout = _layout(anat, ["sub-001_T1w.nii.gz"], ["sub-001_T2w.nii.gz"])
    sizes = {"sub-001_T1w.nii.gz": FINE, "sub-001_T2w.nii.gz": (0.45, 0.45, 4.0)}
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.subject_output_dir("sub-001") / "freesurfer",
                    _cmdargs("sub-001", "sub-001/anat", ["sub-001_T1w.nii.gz"], t2w="sub-001_T2w.nii.gz"))
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms(sizes)):
            assert wrapper.process_subject("sub-001", layout) is False
    assert "sub-001_T2w.nii.gz (0.45x0.45x4.0 mm)" in wrapper.get_processing_summary()["failure_reason"]


def test_failed_recon_all_in_staging_is_not_relocated(bids_dataset, output_dir, freesurfer_license):
    """recon-all writes "1" to recon-all.done on an error exit; that tree is not taken for finished."""
    layout = _layout(bids_dataset / "sub-001" / "anat", ["sub-001_T1w.nii.gz"])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.freesurfer_dir / "sub-001", failed=True)
        with patch("subprocess.run") as mock_run:
            assert wrapper.process_subject("sub-001", layout) is False
            mock_run.assert_not_called()

    assert not (wrapper.subject_output_dir("sub-001") / "freesurfer").exists()
    assert (wrapper.freesurfer_dir / "sub-001").exists()
    assert "recon-all.error" in wrapper.get_processing_summary()["failure_reason"]


def test_reuse_refuses_inputs_of_another_subject(bids_dataset, output_dir, freesurfer_license):
    layout = _layout(bids_dataset / "sub-001" / "anat", ["sub-001_T1w.nii.gz"])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.subject_output_dir("sub-001") / "freesurfer",
                    _cmdargs("sub-002", "sub-002/anat", ["sub-002_T1w.nii.gz"]))
        with patch("src.freesurfer.wrapper.voxel_size", return_value=FINE):
            assert wrapper.process_subject("sub-001", layout) is False
    assert "not an anat image of sub-001" in wrapper.get_processing_summary()["failure_reason"]


def test_reuse_falls_back_to_the_processing_summary(bids_dataset, output_dir, freesurfer_license):
    """When recon-all.done no longer names the inputs, the summary of the run that made the
    tree does."""
    layout = _layout(bids_dataset / "sub-001" / "anat", ["sub-001_run-1_T1w.nii.gz",
                                                         "sub-001_run-2_T1w.nii.gz"])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        subject_dir = wrapper.subject_output_dir("sub-001")
        _recon_tree(subject_dir / "freesurfer", "-s sub-001 -autorecon3")
        (subject_dir / "processing_summary.json").write_text(json.dumps({
            "input_files": ["sub-001/anat/sub-001_run-2_T1w.nii.gz"]}))
        with patch("src.freesurfer.wrapper.voxel_size", return_value=FINE):
            assert wrapper.process_subject("sub-001", layout)
    summary = wrapper.get_processing_summary()
    assert summary["input_files"] == ["sub-001/anat/sub-001_run-2_T1w.nii.gz"]
    assert [entry["path"] for entry in summary["inputs_not_used"]] == [
        "sub-001/anat/sub-001_run-1_T1w.nii.gz"]


def test_reuse_fails_when_nothing_records_the_inputs(bids_dataset, output_dir, freesurfer_license):
    layout = _layout(bids_dataset / "sub-001" / "anat", ["sub-001_T1w.nii.gz"])
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.subject_output_dir("sub-001") / "freesurfer", "-s sub-001 -autorecon3")
        with patch("subprocess.run") as mock_run:
            assert wrapper.process_subject("sub-001", layout) is False
            mock_run.assert_not_called()
    assert "does not record the images it was made from" in (
        wrapper.get_processing_summary()["failure_reason"])


def test_record_subject_images_returns_the_recorded_inputs(bids_dataset, output_dir, freesurfer_license):
    """--skip-freesurfer: the inputs come from the tree, and a too-coarse one fails the subject."""
    anat = bids_dataset / "sub-001" / "ses-1" / "anat"
    t1w = ["sub-001_ses-1_run-1_T1w.nii.gz", "sub-001_ses-1_acq-hires_run-1_T1w.nii.gz"]
    layout = _layout(anat, t1w)
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = _wrapper(bids_dataset, output_dir, freesurfer_license)
        _recon_tree(wrapper.subject_output_dir("sub-001", "1") / "freesurfer",
                    _cmdargs("sub-001_ses-1", "sub-001/ses-1/anat", t1w))
        with patch("src.freesurfer.wrapper.voxel_size", return_value=FINE):
            info = wrapper.record_subject_images("sub-001", layout, session_label="ses-1")
        assert info["T1w_images"] == [str(anat / name) for name in t1w]
        assert "failure_reason" not in info

        sizes = {t1w[0]: FINE, t1w[1]: SLAB}
        with patch("src.freesurfer.wrapper.voxel_size", side_effect=_zooms(sizes)):
            info = wrapper.record_subject_images("sub-001", layout, session_label="ses-1")
    assert f"{t1w[1]} (1.5x1.5x4.0 mm)" in info["failure_reason"]


# --- Paths recorded relative to the BIDS root ---------------------------------------

def test_bids_relative_keeps_the_bids_path_of_an_annexed_image(tmp_path, freesurfer_license, monkeypatch):
    """In a git-annex dataset an image is a symlink into .git/annex/objects. The recorded
    path must stay its BIDS path, also when the BIDS root is given with '..' (pybids then
    reports paths with '..' in them)."""
    ds = tmp_path / "ds"
    anat = ds / "sub-001" / "anat"
    anat.mkdir(parents=True)
    annexed = ds / ".git" / "annex" / "objects" / "Xx" / "Yy" / "MD5E-s1--d41d8.nii.gz"
    annexed.parent.mkdir(parents=True)
    annexed.write_bytes(b"\0")
    (anat / "sub-001_T1w.nii.gz").symlink_to(annexed)
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper("../ds", str(tmp_path / "out"),
                                    freesurfer_license=str(freesurfer_license))

    expected = "sub-001/anat/sub-001_T1w.nii.gz"
    assert wrapper._bids_relative(f"{work}/../ds/sub-001/anat/sub-001_T1w.nii.gz") == expected
    assert wrapper._bids_relative(str(anat / "sub-001_T1w.nii.gz")) == expected
    assert wrapper._bids_relative(str(tmp_path / "elsewhere" / "x_T1w.nii.gz")) == str(
        tmp_path / "elsewhere" / "x_T1w.nii.gz")

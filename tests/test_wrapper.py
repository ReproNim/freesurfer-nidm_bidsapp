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

from src.freesurfer.wrapper import FreeSurferWrapper, select_single_image, select_t1w_inputs

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
    """With only acq- variants, use every run of the alphabetically first label."""
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

        with patch('subprocess.run', side_effect=fake_recon_all) as mock_run:
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


def test_record_subject_images_applies_same_selection(
    bids_dataset, output_dir, freesurfer_license, ucla_layout
):
    """--skip-freesurfer records the same images a full run would have used."""
    layout, anat = ucla_layout
    with patch.dict(os.environ, {"FREESURFER_HOME": "/opt/freesurfer"}):
        wrapper = FreeSurferWrapper(
            str(bids_dataset),
            str(output_dir),
            freesurfer_license=str(freesurfer_license)
        )
        info = wrapper.record_subject_images("sub-001", layout, session_label="ses-1")

    assert info["T1w_images"] == [str(anat / "sub-001_ses-1_run-1_T1w.nii.gz")]
    assert info["T2w_images"] == [str(anat / "sub-001_ses-1_run-1_T2w.nii.gz")]


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

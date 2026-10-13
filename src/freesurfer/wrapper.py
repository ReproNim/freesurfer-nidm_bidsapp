#!/usr/bin/env python3
"""
FreeSurfer Wrapper for BIDS App

This module provides a wrapper around FreeSurfer's recon-all command
to process BIDS datasets and generate FreeSurfer derivatives in a
BIDS-compliant structure.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import time

import nibabel as nib
from bids import BIDSLayout
from src.utils import get_freesurfer_version, get_version_info

# Configure logging
logger = logging.getLogger("freesurfer-bidsapp.wrapper")

# BIDS specification version this app's derivatives conform to. This is the spec
# version, not the version of any library -- pybids' __version__ (e.g. "0.16.4")
# is a library version and is not a valid BIDSVersion value.
BIDS_VERSION = "1.8.0"


# Default limit on any voxel dimension of an anatomical input, in mm. recon-all
# on a thick-slab image (e.g. ABIDE II UCLA_Long acq-hires, 1.5x1.5x4.0 mm)
# exits 0 with a meaningless reconstruction, so such inputs are refused.
DEFAULT_MAX_VOXEL_SIZE = 2.0


def voxel_size(path):
    """Voxel dimensions (mm) of the first three axes of an image header."""
    return tuple(float(z) for z in nib.load(str(path)).header.get_zooms()[:3])


def _format_size(zooms):
    return "x".join(str(round(z, 3)) for z in zooms)


def _entity(path, name):
    """Value of a BIDS entity (e.g. 'acq', 'run') in a file name, or None."""
    match = re.search(rf"(?:^|_){name}-([a-zA-Z0-9]+)(?=_)", Path(str(path)).name)
    return match.group(1) if match else None


def _run_sort_key(path):
    """Order by run number (numerically; no run first), then by file name."""
    run = _entity(path, "run")
    return (int(run) if run and run.isdigit() else -1, Path(str(path)).name)


def select_t1w_inputs(paths):
    """
    Choose which T1w images go to recon-all.

    recon-all averages every -i image, which is right for repeated runs of one
    acquisition but not for different acquisitions (e.g. ABIDE II UCLA_Long
    sessions hold both a standard and an acq-hires T1w). If any T1w without an
    acq- entity exists, use all of those and none of the acq- variants;
    otherwise use every run of the alphabetically first acq label.

    Returns
    -------
    tuple of (list, list)
        Chosen images ordered by run, and the excluded images.
    """
    paths = list(paths)
    plain = [p for p in paths if _entity(p, "acq") is None]
    if plain:
        chosen = plain
    elif paths:
        first_acq = min(_entity(p, "acq") for p in paths)
        chosen = [p for p in paths if _entity(p, "acq") == first_acq]
    else:
        chosen = []
    excluded = [p for p in paths if p not in chosen]
    return sorted(chosen, key=_run_sort_key), sorted(excluded, key=lambda p: Path(str(p)).name)


def select_single_image(paths):
    """
    Choose one image where recon-all takes only one (e.g. -T2).

    Preference: no acq- entity, then lowest run, then file name.

    Returns
    -------
    tuple of (str or None, list)
        The chosen image and the excluded images.
    """
    if not paths:
        return None, []
    ordered = sorted(paths, key=lambda p: (_entity(p, "acq") is not None,) + _run_sort_key(p))
    return ordered[0], sorted(ordered[1:], key=lambda p: Path(str(p)).name)


class FreeSurferWrapper:
    """Wrapper for FreeSurfer's recon-all command."""

    def __init__(self, bids_dir, output_dir, freesurfer_license=None,
                 max_voxel_size=DEFAULT_MAX_VOXEL_SIZE):
        """
        Initialize FreeSurfer wrapper.

        Parameters
        ----------
        bids_dir : str or Path
            Path to BIDS dataset directory
        output_dir : str or Path
            Path to output derivatives directory
        freesurfer_license : str or Path, optional
            Path to FreeSurfer license file
        max_voxel_size : float, optional
            Largest voxel dimension (mm) accepted for a T1w/T2w input
        """
        self.bids_dir = Path(bids_dir)
        self.output_dir = Path(output_dir)
        # recon-all writes to $SUBJECTS_DIR/<subjid>, so it cannot write straight to
        # the final location (<output_dir>/sub-<id>/freesurfer). Stage it here, then
        # relocate with a rename once recon-all succeeds -- same filesystem, so O(1).
        # Dot-prefixed and outside sub-<id>/, so BABS never zips it.
        self.freesurfer_dir = self.output_dir / ".fs_staging"
        self.freesurfer_license = freesurfer_license
        self.max_voxel_size = float(max_voxel_size)

        # Track processing results and image information
        self.results = {"success": [], "failure": [], "skipped": []}
        self.subject_t1_mapping = {}  # Store subject->T1 mapping
        self.temp_files = []
        # Set once a subject is handled, so the processing summary can be written
        # inside that subject's directory (BABS zips only sub-<id>/).
        self.last_subject_dir = None
        # Inputs chosen for (and left out of) the last subject, relative to the
        # BIDS root, for the processing summary.
        self.last_inputs = None

        # Ensure output directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.freesurfer_dir.mkdir(parents=True, exist_ok=True)

        # Setup FreeSurfer environment
        self._setup_freesurfer_env()
        logger.info(f"Using FreeSurfer version: {get_freesurfer_version()}")

    def _setup_freesurfer_env(self):
        """Setup FreeSurfer environment and license."""
        if "FREESURFER_HOME" not in os.environ:
            logger.error("FREESURFER_HOME environment variable not set")
            raise EnvironmentError("FREESURFER_HOME environment variable not set")

        os.environ["SUBJECTS_DIR"] = str(self.freesurfer_dir)

        if self.freesurfer_license:
            if os.path.exists(self.freesurfer_license):
                os.environ["FS_LICENSE"] = str(self.freesurfer_license)
                logger.info(f"Using provided FreeSurfer license: {self.freesurfer_license}")
            else:
                logger.error(f"FreeSurfer license not found at: {self.freesurfer_license}")
                raise FileNotFoundError(f"FreeSurfer license not found at: {self.freesurfer_license}")
        else:
            # Try standard locations
            license_locations = [
                "/license.txt",  # Docker mount location
                os.path.join(os.environ.get("FREESURFER_HOME", ""), "license.txt"),
                os.path.expanduser("~/.freesurfer.txt"),
            ]

            for loc in license_locations:
                if os.path.exists(loc):
                    logger.info(f"Using FreeSurfer license from {loc}")
                    os.environ["FS_LICENSE"] = loc
                    break
            else:
                logger.error("FreeSurfer license not found in standard locations")
                raise FileNotFoundError("FreeSurfer license not found. Please specify with --freesurfer_license")

    def _create_recon_all_command(self, subject_id, t1w_images, t2w_images=None, session_label=None):
        """
        Create FreeSurfer recon-all command.

        Parameters
        ----------
        subject_id : str
            Subject ID (including 'sub-' prefix)
        t1w_images : list
            List of T1w image paths
        t2w_images : list, optional
            List of T2w image paths
        session_label : str, optional
            Session label (if processing a specific session)

        Returns
        -------
        list
            Command list for subprocess
        """
        # If processing a session, modify the subject ID
        if session_label:
            fs_subject_id = f"{subject_id}_ses-{session_label}"
        else:
            fs_subject_id = subject_id

        cmd = ["recon-all", "-subjid", fs_subject_id]

        # Add T1w images
        for t1w in t1w_images:
            cmd.extend(["-i", str(t1w)])

        # Add T2w image if available
        if t2w_images and len(t2w_images) > 0:
            cmd.extend(["-T2", str(t2w_images[0]), "-T2pial"])

        cmd.append("-all")
        return cmd

    def process_subject(self, subject_id, layout=None, session_label=None):
        """
        Process a single subject with FreeSurfer.

        Parameters
        ----------
        subject_id : str
            Subject ID (including 'sub-' prefix, e.g., 'sub-001')
        layout : BIDSLayout, optional
            BIDS layout object (if not provided, one will be created)
        session_label : str, optional
            Session label (if processing a specific session)

        Returns
        -------
        bool
            True if processing was successful, False otherwise
        """
        logger.info(f"Processing {subject_id}" +
                   (f" session {session_label}" if session_label else ""))

        try:
            if layout is None:
                layout = BIDSLayout(self.bids_dir)

            # Strip 'sub-' for BIDS queries
            if not subject_id.startswith("sub-"):
                raise ValueError(f"Subject ID must start with 'sub-', got {subject_id}")
            
            bids_subject = subject_id[4:]  # Always strip 'sub-' for BIDS queries

            # Determine session for queries
            bids_session = None
            if session_label:
                if session_label.startswith("ses-"):
                    bids_session = session_label[4:]  # Strip 'ses-' for BIDS queries
                else:
                    bids_session = session_label

            # Find T1w and T2w images
            fs_subject_id = f"{subject_id}_ses-{session_label}" if session_label else subject_id
            info = self._select_inputs(layout, bids_subject, bids_session, fs_subject_id)
            t1w_images = info['T1w_images']
            if info.get('failure_reason'):
                self.results["failure"].append(fs_subject_id)
                return False
            if not t1w_images:
                logger.error(f"No T1w images found for {subject_id}" +
                           (f" session {session_label}" if session_label else ""))
                self.results["skipped"].append(f"{subject_id}" +
                                             (f"_ses-{bids_session}" if bids_session else ""))
                return False

            # Store T1 image information
            info['session'] = session_label
            self.subject_t1_mapping[fs_subject_id] = info
            t2w_images = info.get('T2w_images', [])

            # Check if subject already processed. A completed run has already been
            # relocated out of staging, so check the final location; fall back to
            # staging to also catch a run interrupted between recon-all and the move.
            session_for_output = bids_session if session_label else None
            final_fs_dir = self.subject_output_dir(subject_id, session_for_output) / "freesurfer"
            staged_fs_dir = self.freesurfer_dir / fs_subject_id
            if (final_fs_dir / "scripts" / "recon-all.done").exists():
                logger.info(f"{fs_subject_id} already processed. Skipping...")
                self.results["skipped"].append(fs_subject_id)
                self.last_subject_dir = self.subject_output_dir(subject_id, session_for_output)
                return True
            if (staged_fs_dir / "scripts" / "recon-all.done").exists():
                logger.info(
                    f"{fs_subject_id} recon-all already complete in staging; "
                    "relocating without rerunning."
                )
                self._relocate_freesurfer_output(subject_id, session_for_output)
                self.last_subject_dir = self.subject_output_dir(subject_id, session_for_output)
                self._organize_bids_output(subject_id, session_for_output)
                self.results["skipped"].append(fs_subject_id)
                return True

            # Run recon-all
            cmd = self._create_recon_all_command(subject_id, t1w_images, t2w_images,
                                                bids_session if session_label else None)
            logger.info(f"Running command: {' '.join(cmd)}")
            
            subprocess.run(cmd, check=True, capture_output=True, text=True)

            # Move the staged recon-all tree into <output_dir>/sub-<id>/freesurfer,
            # then write the BIDS-ified copies alongside it.
            session_for_output = bids_session if session_label else None
            if self._relocate_freesurfer_output(subject_id, session_for_output) is None:
                raise RuntimeError(f"recon-all produced no output for {fs_subject_id}")
            self.last_subject_dir = self.subject_output_dir(subject_id, session_for_output)
            self._organize_bids_output(subject_id, session_for_output)

            self.results["success"].append(fs_subject_id)
            logger.info(f"Successfully processed {fs_subject_id}")
            return True

        except Exception as e:
            logger.error(f"Error processing {subject_id}" +
                        (f" session {session_label}" if session_label else "") +
                        f": {str(e)}")
            self.results["failure"].append(f"{subject_id}" +
                                         (f"_ses-{bids_session}" if bids_session else ""))
            return False

    def _find_images(self, layout, subject_id, suffix, session_id=None):
        """
        Find images for a subject with given suffix.

        Parameters
        ----------
        layout : BIDSLayout
            BIDS layout object
        subject_id : str
            Subject ID (without 'sub-' prefix)
        suffix : str
            Image suffix (e.g., 'T1w', 'T2w')
        session_id : str, optional
            Session ID (without 'ses-' prefix)

        Returns
        -------
        list
            List of image paths
        """
        query = {
            "return_type": "file",
            "subject": subject_id,
            "datatype": "anat",
            "suffix": suffix,
            "extension": [".nii", ".nii.gz"]
        }
        
        if session_id:
            query["session"] = session_id
            
        return layout.get(**query)

    def _select_inputs(self, layout, bids_subject, bids_session, fs_subject_id):
        """
        Find T1w/T2w images and choose which ones recon-all uses.

        Selection follows select_t1w_inputs / select_single_image, then drops
        any chosen image with a voxel dimension above self.max_voxel_size. If
        every chosen T1w is too coarse the subject cannot be processed and
        'failure_reason' is set; a too-coarse T2w is only skipped. Chosen and
        excluded files are logged and recorded, relative to the BIDS root, in
        self.last_inputs for the processing summary.

        Returns
        -------
        dict
            'T1w_images' (chosen, may be empty), 'T2w_images' (a single chosen
            image, if any) and 'failure_reason' (if no T1w is usable).
        """
        t1w_found = [str(img) for img in self._find_images(layout, bids_subject, "T1w", bids_session)]
        t1w_selected, t1w_excluded = select_t1w_inputs(t1w_found)
        acq_used = _entity(t1w_selected[0], "acq") if t1w_selected else None
        not_used = [
            (p, f"acq-{_entity(p, 'acq')} variant; using "
                + (f"acq-{acq_used}" if acq_used else "T1w without acq-"))
            for p in t1w_excluded
        ]

        t1w_images = []
        too_coarse = []
        for img in t1w_selected:
            reason = self._too_coarse(img)
            if reason:
                logger.warning(f"Not using T1w {img} for {fs_subject_id}: {reason}")
                not_used.append((img, reason))
                too_coarse.append(img)
            else:
                t1w_images.append(img)
        info = {'T1w_images': t1w_images}

        failure_reason = None
        if too_coarse and not t1w_images:
            failure_reason = (
                f"No T1w within --max-voxel-size {self.max_voxel_size} mm: "
                + ", ".join(
                    f"{Path(p).name} ({_format_size(voxel_size(p))} mm)" for p in too_coarse
                )
            )
            logger.error(f"{fs_subject_id}: {failure_reason}; not running recon-all")
            info['failure_reason'] = failure_reason

        t2w_found = [str(img) for img in self._find_images(layout, bids_subject, "T2w", bids_session)]
        t2w_image, t2w_excluded = select_single_image(t2w_found)
        not_used += [(p, f"only one T2w is used; chose {Path(t2w_image).name}") for p in t2w_excluded]
        if t2w_image:
            reason = self._too_coarse(t2w_image)
            if reason:
                logger.warning(f"Not using T2w {t2w_image} for {fs_subject_id}: {reason}")
                not_used.append((t2w_image, reason))
                t2w_image = None
            else:
                info['T2w_images'] = [t2w_image]

        for img in t1w_images:
            logger.info(f"Using T1w for {fs_subject_id}: {img}")
        if t2w_image:
            logger.info(f"Using T2w for {fs_subject_id}: {t2w_image}")
        for img, reason in not_used:
            logger.info(f"Not using {img} for {fs_subject_id} ({reason})")

        used = t1w_images + ([t2w_image] if t2w_image else [])
        self.last_inputs = {
            "input_files": [self._bids_relative(p) for p in used],
            "inputs_not_used": [
                {"path": self._bids_relative(p), "reason": reason} for p, reason in not_used
            ],
        }
        if failure_reason:
            self.last_inputs["failure_reason"] = failure_reason
        return info

    def _too_coarse(self, path):
        """Reason string if any voxel dimension exceeds the limit, else None.

        An unreadable header is not treated as too coarse: recon-all reports
        that failure itself.
        """
        try:
            zooms = voxel_size(path)
        except Exception as e:
            logger.warning(f"Could not read voxel size of {path}: {e}")
            return None
        if max(zooms) > self.max_voxel_size:
            return (
                f"voxel size {_format_size(zooms)} mm exceeds "
                f"--max-voxel-size {self.max_voxel_size} mm"
            )
        return None

    def _bids_relative(self, path):
        """Path relative to the BIDS root, or the path unchanged if outside it."""
        path = Path(path)
        try:
            relative = path.relative_to(self.bids_dir)
            if ".." not in relative.parts:
                return relative.as_posix()
        except ValueError:
            pass
        # Resolve the directory, not the file: an image in a git-annex dataset is a
        # symlink into .git/annex/objects, and following it loses the BIDS path.
        real = Path(os.path.realpath(path.parent)) / path.name
        try:
            return real.relative_to(os.path.realpath(self.bids_dir)).as_posix()
        except ValueError:
            return str(path)

    def _copy_file(self, src, dest):
        """Copy file if it exists."""
        if src.exists():
            shutil.copy2(src, dest)
            return True
        return False

    def subject_output_dir(self, subject_id, session_label=None):
        """
        Per-subject output directory -- the unit BABS zips.

        Returns <output_dir>/sub-<id>[/ses-<session>]. Everything produced for a
        subject (recon-all tree, BIDS-ified copies, nidm.ttl, fs_cde.ttl) lives here,
        so the zip's top-level folder is the subject directory.

        Parameters
        ----------
        subject_id : str
            Subject ID (including 'sub-' prefix)
        session_label : str, optional
            Session label (without 'ses-' prefix)
        """
        subject_dir = self.output_dir / subject_id
        if session_label:
            subject_dir = subject_dir / f"ses-{session_label}"
        return subject_dir

    def _relocate_freesurfer_output(self, subject_id, session_label=None):
        """
        Move the staged recon-all tree into the per-subject output directory.

        Staging exists only because recon-all insists on $SUBJECTS_DIR/<subjid>.
        Returns the final freesurfer directory, or None if staging is missing.
        """
        fs_subject_id = f"{subject_id}_ses-{session_label}" if session_label else subject_id
        staged = self.freesurfer_dir / fs_subject_id
        if not staged.exists():
            logger.error(f"Staged FreeSurfer output not found: {staged}")
            return None

        dest = self.subject_output_dir(subject_id, session_label) / "freesurfer"
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            # Idempotency: a rerun into an existing output. Keep the newer tree.
            logger.warning(f"Replacing existing FreeSurfer output at {dest}")
            shutil.rmtree(dest)

        try:
            staged.rename(dest)
        except OSError:
            # Different filesystem (or other rename failure) -- fall back to a copy.
            logger.info(f"rename failed; copying {staged} -> {dest}")
            shutil.copytree(staged, dest)
            shutil.rmtree(staged, ignore_errors=True)

        # Leave no empty staging directory behind in the output tree.
        try:
            self.freesurfer_dir.rmdir()
        except OSError:
            pass  # not empty (other subjects mid-flight) or already gone

        logger.info(f"FreeSurfer output placed at {dest}")
        return dest

    def _organize_bids_output(self, subject_id, session_label=None):
        """
        Organize FreeSurfer outputs in BIDS-compliant format.

        Parameters
        ----------
        subject_id : str
            Subject ID (including 'sub-' prefix)
        session_label : str, optional
            BIDS session label
        """
        # Set up directories
        session_part = f"_ses-{session_label}" if session_label else ""
        bids_subject_dir = self.subject_output_dir(subject_id, session_label)

        anat_dir = bids_subject_dir / "anat"
        stats_dir = bids_subject_dir / "stats"
        anat_dir.mkdir(parents=True, exist_ok=True)
        stats_dir.mkdir(parents=True, exist_ok=True)

        # The recon-all tree now lives beside these, under the same subject dir.
        # Keeping it namespaced under freesurfer/ is what stops recon-all's own
        # stats/ from colliding with the BIDS-named copies written below.
        fs_subject_dir = bids_subject_dir / "freesurfer"
        if not fs_subject_dir.exists():
            logger.error(f"FreeSurfer subject directory not found: {fs_subject_dir}")
            return

        # Copy MRI files
        mri_files = {
            "brain.mgz": f"{subject_id}{session_part}_desc-brain_T1w.nii.gz",
            "aparc.DKTatlas+aseg.mgz": f"{subject_id}{session_part}_desc-aparcaseg_dseg.nii.gz",
            "wmparc.mgz": f"{subject_id}{session_part}_desc-wmparc_dseg.nii.gz"
        }

        for src_name, dest_name in mri_files.items():
            src_file = fs_subject_dir / "mri" / src_name
            dest_file = anat_dir / dest_name
            self._copy_file(src_file, dest_file)

        # Copy stats files
        if (fs_subject_dir / "stats").exists():
            for stat_file in (fs_subject_dir / "stats").glob("*.stats"):
                dest_file = stats_dir / f"{subject_id}{session_part}_{stat_file.name}"
                self._copy_file(stat_file, dest_file)

        # Create dataset description and README if they don't exist
        self._create_dataset_description()
        self._create_readme()

    def _create_dataset_description(self):
        """Create dataset_description.json if it doesn't exist."""
        desc_file = self.output_dir / "dataset_description.json"
        if not desc_file.exists():
            # Get version information from utils
            version_info = get_version_info()

            with open(desc_file, "w") as f:
                json.dump({
                    "Name": "FreeSurfer Derivatives",
                    "BIDSVersion": BIDS_VERSION,
                    "DatasetType": "derivative",
                    "GeneratedBy": [
                        {
                            "Name": "FreeSurfer",
                            "Version": version_info.get("freesurfer", {}).get("version", get_freesurfer_version()),
                            "Description": "FreeSurfer cortical reconstruction and parcellation"
                        },
                        {
                            "Name": "freesurfer-nidm-bidsapp",
                            "Version": version_info.get("freesurfer-nidm", {}).get("version", "unknown"),
                            "Description": "BIDS App for FreeSurfer with NIDM Output"
                        }
                    ]
                }, f, indent=2)

    def _create_readme(self):
        """Create README if it doesn't exist."""
        readme = self.output_dir / "README"
        if not readme.exists():
            with open(readme, "w") as f:
                f.write("""FreeSurfer Derivatives
====================

This directory contains FreeSurfer derivatives organized according to the BIDS specification.
The following files are included:
- Brain-extracted T1w images
- Cortical parcellation (aparc+aseg)
- White matter parcellation (wmparc)
- Statistical measurements in the stats directory

For more information about FreeSurfer, visit: http://surfer.nmr.mgh.harvard.edu/
""")

    def get_processing_summary(self):
        """Get summary of processing results."""
        summary = {
            "total": len(self.results["success"]) + len(self.results["failure"]) + len(self.results["skipped"]),
            "success": len(self.results["success"]),
            "failure": len(self.results["failure"]),
            "skipped": len(self.results["skipped"]),
            "success_list": self.results["success"],
            "failure_list": self.results["failure"],
            "skipped_list": self.results["skipped"],
        }
        if self.last_inputs is not None:
            summary.update(self.last_inputs)
        return summary

    def save_processing_summary(self, summary=None):
        """Save processing summary to JSON file."""
        if summary is None:
            summary = self.get_processing_summary()
        # Write inside the subject directory when we know it -- BABS zips only
        # sub-<id>/, so anything outside it is dropped from the results.
        target_dir = self.last_subject_dir or self.output_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        output_path = target_dir / "processing_summary.json"
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Processing summary saved to {output_path}")
        return output_path

    def record_subject_images(self, subject_id, layout, session_label=None):
        """Record T1w/T2w provenance without running recon-all.

        Used by --skip-freesurfer so that NIDM export logging reflects the
        images of a previously reconstructed subject/session.

        Parameters
        ----------
        subject_id : str
            Subject ID (including 'sub-' prefix)
        layout : BIDSLayout
            BIDS layout object
        session_label : str, optional
            Session label (with or without 'ses-' prefix)

        Returns
        -------
        dict
            The recorded image information.
        """
        if not subject_id.startswith("sub-"):
            raise ValueError(f"Subject ID must start with 'sub-', got {subject_id}")

        bids_subject = subject_id[4:]
        bids_session = session_label
        if bids_session and bids_session.startswith("ses-"):
            bids_session = bids_session[4:]

        fs_subject_id = f"{subject_id}_ses-{bids_session}" if bids_session else subject_id
        info = self._select_inputs(layout, bids_subject, bids_session, fs_subject_id)
        info["session"] = bids_session

        self.subject_t1_mapping[fs_subject_id] = info
        # --skip-freesurfer reuses an existing reconstruction, so the subject
        # directory is still where per-subject artefacts belong.
        self.last_subject_dir = self.subject_output_dir(subject_id, bids_session)
        return info

    def get_subject_t1_info(self, subject_id, session_label=None):
        """Get T1 image information for a subject.

        Parameters
        ----------
        subject_id : str
            Subject ID (including 'sub-' prefix)
        session_label : str, optional
            Session label

        Returns
        -------
        dict
            Dictionary containing T1 image information
        """
        fs_subject_id = f"{subject_id}_ses-{session_label}" if session_label else subject_id
        return self.subject_t1_mapping.get(fs_subject_id, {})

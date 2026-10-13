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
from pathlib import Path, PurePosixPath
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
    """Voxel dimensions (mm) of the first three axes of an image header.

    Rounded to 3 decimals, as the sibling ants-nidm app does, so a header that
    stores 2 mm as 2.0004 is not refused by a 2.0 mm limit in one app and
    accepted by the other.
    """
    return tuple(round(float(z), 3) for z in nib.load(str(path)).header.get_zooms()[:3])


def _format_size(zooms):
    return "x".join(str(round(z, 3)) for z in zooms)


_ENTITY_PAIR = re.compile(r"^([a-zA-Z0-9]+)-([a-zA-Z0-9]+)$")


def _entities(path):
    """BIDS entities in a file name other than sub and ses, e.g. {'acq': 'hires', 'run': '1'}."""
    stem = Path(str(path)).name.split(".", 1)[0]
    entities = {}
    for part in stem.split("_")[:-1]:  # the last part is the suffix (T1w, T2w)
        match = _ENTITY_PAIR.match(part)
        if match and match.group(1) not in ("sub", "ses"):
            entities[match.group(1)] = match.group(2)
    return entities


def _acquisition(path):
    """The entities that identify an acquisition: all of them except run."""
    return {key: value for key, value in _entities(path).items() if key != "run"}


def _acquisition_label(path):
    """The acquisition as entity pairs, e.g. 'acq-hires'; '' for a plain image."""
    return "_".join(f"{key}-{value}" for key, value in _acquisition(path).items())


def _rank(path):
    """
    Sort key for anatomical inputs, the same order ants-nidm uses
    (ReproNim/ants-nidm_bidsapp, src/antspy/inputs.py):

    1. no acq- entity before any acq- entity,
    2. then no other optional entities (ce-, rec-, ...) before some,
    3. then the lowest run (a file with no run counts as run 0),
    4. then the file name.
    """
    entities = _entities(path)
    others = set(entities) - {"acq", "run"}
    run = entities.get("run", "0")
    return ("acq" in entities, bool(others),
            int(run) if run.isdigit() else float("inf"), Path(str(path)).name)


def select_t1w_inputs(paths):
    """
    Choose which T1w images go to recon-all.

    recon-all averages every -i image, which is right for repeated runs of one
    acquisition but not for different acquisitions (e.g. ABIDE II UCLA_Long
    sessions hold both a standard and an acq-hires T1w). The best-ranked image
    (see _rank) fixes the acquisition, and every run of that acquisition --
    same entities apart from run -- is used. The best-ranked image is the one
    ants-nidm segments, so both apps start from the same acquisition.

    Call this on images that already passed the voxel-size check.

    Returns
    -------
    tuple of (list, list)
        Chosen images ordered by run, and the excluded images ordered by name.
    """
    ranked = sorted(paths, key=_rank)
    if not ranked:
        return [], []
    lead = _acquisition(ranked[0])
    chosen = [p for p in ranked if _acquisition(p) == lead]
    excluded = [p for p in ranked if _acquisition(p) != lead]
    return chosen, sorted(excluded, key=lambda p: Path(str(p)).name)


def select_single_image(paths):
    """
    Choose one image where recon-all takes only one (e.g. -T2): the best-ranked
    one (see _rank).

    Returns
    -------
    tuple of (str or None, list)
        The chosen image and the excluded images ordered by name.
    """
    ranked = sorted(paths, key=_rank)
    if not ranked:
        return None, []
    return ranked[0], sorted(ranked[1:], key=lambda p: Path(str(p)).name)


class UnusableReconstruction(Exception):
    """An existing recon-all tree cannot be used; the message says why."""


def recon_all_finished(fs_dir):
    """True if recon-all completed in fs_dir.

    recon-all.done alone is not proof: on an error exit recon-all writes "1"
    to it and leaves recon-all.error beside it, and recon-all itself treats a
    subject as done only when the first exists without the second.
    """
    scripts = Path(fs_dir) / "scripts"
    return (scripts / "recon-all.done").exists() and not (scripts / "recon-all.error").exists()


def recorded_recon_inputs(fs_dir):
    """
    The T1w and T2w images a finished recon-all tree was made from, as
    recorded by recon-all itself.

    When it finishes, recon-all writes its own argument list into
    scripts/recon-all.done as a 'CMDARGS ...' line (FreeSurfer 8.0.0:
    ``echo "CMDARGS $argv0" >> $DoneFile``), so the -i and -T2 arguments there
    name the inputs of exactly this tree. The file is rewritten by every
    recon-all invocation, so after a later run without -i (for example
    ``recon-all -s <subj> -autorecon2-wm`` after manual edits) it no longer
    names them.

    Returns
    -------
    tuple of (list, list) or None
        The -i paths and the -T2 paths as recon-all received them, or None if
        recon-all.done has no CMDARGS line with an -i argument.
    """
    try:
        lines = (Path(fs_dir) / "scripts" / "recon-all.done").read_text(errors="replace").splitlines()
    except OSError:
        return None
    args = next((line.split()[1:] for line in lines if line.startswith("CMDARGS ")), [])
    t1w = [value for flag, value in zip(args, args[1:]) if flag in ("-i", "--i")]
    t2w = [value for flag, value in zip(args, args[1:]) if flag in ("-T2", "-t2")]
    return (t1w, t2w) if t1w else None


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

            fs_subject_id = f"{subject_id}_ses-{session_label}" if session_label else subject_id
            session_for_output = bids_session if session_label else None

            reused = self._reuse_existing_output(layout, subject_id, session_label, session_for_output,
                                                 fs_subject_id)
            if reused is not None:
                return reused

            # Find T1w and T2w images
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

    def _reuse_existing_output(self, layout, subject_id, session_label, session_for_output, fs_subject_id):
        """
        Use an existing recon-all tree for the subject, if there is one.

        A completed run has already been relocated out of staging, so the final
        location is checked first; staging is checked too, to catch a run
        interrupted between recon-all and the move. A tree is judged by the
        inputs it was made from, read from its own record, not by what the
        input selection would choose now (see _reuse_reconstruction).

        Returns
        -------
        bool or None
            None if there is no existing tree, else whether it could be used.
        """
        final_fs_dir = self.subject_output_dir(subject_id, session_for_output) / "freesurfer"
        staged_fs_dir = self.freesurfer_dir / fs_subject_id
        fs_dir = next((d for d in (final_fs_dir, staged_fs_dir)
                       if (d / "scripts" / "recon-all.done").exists()), None)
        if fs_dir is None:
            return None
        info = self._reuse_reconstruction(fs_dir, layout, subject_id, session_for_output, fs_subject_id)
        if info.get('failure_reason'):
            self.results["failure"].append(fs_subject_id)
            return False
        info['session'] = session_label
        self.subject_t1_mapping[fs_subject_id] = info
        if fs_dir == staged_fs_dir:
            logger.info(f"{fs_subject_id} recon-all already complete in staging; relocating without rerunning.")
            self._relocate_freesurfer_output(subject_id, session_for_output)
            self._organize_bids_output(subject_id, session_for_output)
        else:
            logger.info(f"{fs_subject_id} already processed. Skipping...")
        self.last_subject_dir = self.subject_output_dir(subject_id, session_for_output)
        self.results["skipped"].append(fs_subject_id)
        return True

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

        Images with a voxel dimension above self.max_voxel_size are set aside
        first and the choice is made among the rest (select_t1w_inputs /
        select_single_image), so a too-coarse image never decides which
        acquisition is used. An image whose header cannot be read ranks after
        every readable one: it is used only when nothing readable is left, and
        recon-all then reports the read failure itself. If every T1w is too
        coarse the subject cannot be processed and 'failure_reason' is set; a
        too-coarse T2w is only skipped. Chosen and excluded files are logged and
        recorded, relative to the BIDS root, in self.last_inputs for the
        processing summary.

        Returns
        -------
        dict
            'T1w_images' (chosen, may be empty), 'T2w_images' (a single chosen
            image, if any) and 'failure_reason' (if no T1w is usable).
        """
        t1w_found = [str(img) for img in self._find_images(layout, bids_subject, "T1w", bids_session)]
        t1w_images, not_used, too_coarse = self._choose_t1w(t1w_found)
        info = {'T1w_images': t1w_images}

        failure_reason = None
        if too_coarse and not t1w_images:
            failure_reason = (
                f"No T1w within --max-voxel-size {self.max_voxel_size} mm: "
                + ", ".join(f"{Path(p).name} ({_format_size(zooms)} mm)" for p, zooms in too_coarse)
            )
            logger.error(f"{fs_subject_id}: {failure_reason}; not running recon-all")
            info['failure_reason'] = failure_reason

        t2w_found = [str(img) for img in self._find_images(layout, bids_subject, "T2w", bids_session)]
        t2w_image, t2w_not_used, t2w_too_coarse = self._choose_t2w(t2w_found)
        not_used += t2w_not_used
        if t2w_image:
            info['T2w_images'] = [t2w_image]

        for suffix, pairs in (("T1w", too_coarse), ("T2w", t2w_too_coarse)):
            for p, zooms in pairs:
                logger.warning(f"Not using {suffix} {p} for {fs_subject_id}: {self._coarse_reason(zooms)}")
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

    def _choose_t1w(self, found):
        """
        T1w images for recon-all, each excluded image with its reason, and the
        (image, voxel size) pairs over the limit.
        """
        usable, unreadable, too_coarse = self._screen(found)
        chosen, other_acquisitions = select_t1w_inputs(usable or unreadable)
        not_used = []
        if chosen:
            using = Path(chosen[0]).name + (" and its other runs" if len(chosen) > 1 else "")
            not_used += [(p, f"{_acquisition_label(p)} variant; using {using}") for p in other_acquisitions]
        if usable:
            not_used += [(p, "image header could not be read") for p in unreadable]
        not_used += [(p, self._coarse_reason(zooms)) for p, zooms in too_coarse]
        return chosen, not_used, too_coarse

    def _choose_t2w(self, found):
        """
        The one T2w for recon-all (or None), each excluded image with its
        reason, and the (image, voxel size) pairs over the limit.
        """
        usable, unreadable, too_coarse = self._screen(found)
        chosen, others = select_single_image(usable or unreadable)
        not_used = [(p, f"only one T2w is used; using {Path(chosen).name}") for p in others]
        if usable:
            not_used += [(p, "image header could not be read") for p in unreadable]
        not_used += [(p, self._coarse_reason(zooms)) for p, zooms in too_coarse]
        return chosen, not_used, too_coarse

    def _screen(self, paths):
        """
        Split images by the voxel-size limit.

        Returns
        -------
        tuple of (list, list, list)
            Images within the limit, images whose header cannot be read, and
            (image, voxel size) pairs over the limit.
        """
        usable, unreadable, too_coarse = [], [], []
        for path in paths:
            try:
                zooms = voxel_size(path)
            except Exception as e:
                logger.warning(f"Could not read voxel size of {path}: {e}")
                unreadable.append(path)
                continue
            if max(zooms) > self.max_voxel_size:
                too_coarse.append((path, zooms))
            else:
                usable.append(path)
        return usable, unreadable, too_coarse

    def _coarse_reason(self, zooms):
        return f"voxel size {_format_size(zooms)} mm exceeds --max-voxel-size {self.max_voxel_size} mm"

    def _reuse_reconstruction(self, fs_dir, layout, subject_id, bids_session, fs_subject_id):
        """
        Check an existing recon-all tree against the images it was made from.

        The images come from the tree's own record (see _recorded_images), never
        from choosing again under the current selection rule, which can name
        other files than the ones the tree was built from. They must pass the
        same --max-voxel-size check as a new run -- the T2w included, since
        recon-all refined the pial surface with it. Otherwise 'failure_reason'
        is set and the tree is not used. The recorded images are logged and
        stored in self.last_inputs, and every other T1w/T2w of the subject is
        listed as not used.

        Parameters
        ----------
        fs_dir : Path
            The recon-all subject directory (final location or staging)
        layout : BIDSLayout
            BIDS layout object
        subject_id : str
            Subject ID (including 'sub-' prefix)
        bids_session : str or None
            Session label (without 'ses-' prefix)
        fs_subject_id : str
            Subject/session identifier for log messages

        Returns
        -------
        dict
            'T1w_images', 'T2w_images' (if any) and 'failure_reason' (if the tree
            cannot be used).
        """
        info = {'T1w_images': []}
        self.last_inputs = {"input_files": [], "inputs_not_used": []}
        try:
            images, source = self._recorded_images(fs_dir, subject_id, bids_session)
            self._check_recorded_voxel_sizes(fs_dir, images["T1w"] + images["T2w"])
        except UnusableReconstruction as e:
            logger.error(f"{fs_subject_id}: {e}")
            info['failure_reason'] = self.last_inputs['failure_reason'] = str(e)
            return info

        used = images["T1w"] + images["T2w"]
        used_relative = {self._bids_relative(p) for p in used}
        not_used = [
            str(p)
            for suffix in ("T1w", "T2w")
            for p in self._find_images(layout, subject_id[4:], suffix, bids_session)
            if self._bids_relative(p) not in used_relative
        ]
        logger.info(
            f"Reusing {fs_dir} for {fs_subject_id}; it was made from "
            f"{', '.join(Path(p).name for p in used)} (recorded in {source})"
        )
        for p in not_used:
            logger.info(f"Not using {p} for {fs_subject_id} (not an input of the existing reconstruction)")

        info['T1w_images'] = images["T1w"]
        if images["T2w"]:
            info['T2w_images'] = images["T2w"]
        self.last_inputs = {
            "input_files": [self._bids_relative(p) for p in used],
            "inputs_not_used": [
                {"path": self._bids_relative(p), "reason": "not an input of the existing reconstruction"}
                for p in not_used
            ],
        }
        return info

    def _recorded_images(self, fs_dir, subject_id, bids_session):
        """
        The T1w and T2w images a finished tree records it was made from, under
        the current BIDS root, and where that record was read.

        The record is recon-all's own (recorded_recon_inputs) or, if
        recon-all.done no longer names the inputs, input_files in the subject's
        processing_summary.json.

        Returns
        -------
        tuple of (dict, Path)
            {'T1w': [...], 'T2w': [...]} and the file the record came from.

        Raises
        ------
        UnusableReconstruction
            If the tree did not finish, records no inputs, or names an image that
            is not an anat image of this subject (and session).
        """
        scripts = Path(fs_dir) / "scripts"
        if not (scripts / "recon-all.done").exists():
            raise UnusableReconstruction(f"no finished recon-all output at {fs_dir} (no scripts/recon-all.done)")
        if not recon_all_finished(fs_dir):
            raise UnusableReconstruction(
                f"recon-all did not finish in {fs_dir} (scripts/recon-all.error exists); "
                "remove that directory to run recon-all again"
            )
        recorded = recorded_recon_inputs(fs_dir)
        source = scripts / "recon-all.done"
        if recorded is None:
            source = self.subject_output_dir(subject_id, bids_session) / "processing_summary.json"
            recorded = self._summary_inputs(source)
        if recorded is None:
            raise UnusableReconstruction(
                f"{fs_dir} does not record the images it was made from (no -i argument "
                f"in scripts/recon-all.done, and no input_files in {source}); "
                "remove that directory to reconstruct the subject again"
            )

        images = {"T1w": [], "T2w": []}
        for suffix, paths in zip(("T1w", "T2w"), recorded):
            for recorded_path in paths:
                path = self._recorded_to_bids(recorded_path, subject_id, bids_session)
                if path is None:
                    owner = subject_id + (f" ses-{bids_session}" if bids_session else "")
                    raise UnusableReconstruction(
                        f"{fs_dir} was made from {recorded_path}, which is not an anat image "
                        f"of {owner} (recorded in {source})"
                    )
                images[suffix].append(str(path))
        return images, source

    def _check_recorded_voxel_sizes(self, fs_dir, paths):
        """Raise UnusableReconstruction if any image is unreadable or over --max-voxel-size."""
        too_coarse = []
        for path in paths:
            try:
                zooms = voxel_size(path)
            except Exception as e:
                raise UnusableReconstruction(
                    f"cannot read {path}, an input of {fs_dir}, to check it against "
                    f"--max-voxel-size: {e}"
                )
            if max(zooms) > self.max_voxel_size:
                too_coarse.append(f"{Path(path).name} ({_format_size(zooms)} mm)")
        if too_coarse:
            raise UnusableReconstruction(
                f"{fs_dir} was made from images coarser than --max-voxel-size "
                f"{self.max_voxel_size} mm: {', '.join(too_coarse)}; not reusing it"
            )

    @staticmethod
    def _summary_inputs(summary_path):
        """(T1w, T2w) input_files of a processing_summary.json, or None if it records no T1w."""
        try:
            files = json.loads(Path(summary_path).read_text()).get("input_files")
        except (OSError, ValueError, AttributeError):
            return None
        if not isinstance(files, list) or not all(isinstance(f, str) for f in files):
            return None

        def suffix(path):
            return Path(path).name.split(".", 1)[0].rsplit("_", 1)[-1]

        t1w = [f for f in files if suffix(f) == "T1w"]
        t2w = [f for f in files if suffix(f) == "T2w"]
        return (t1w, t2w) if t1w else None

    def _recorded_to_bids(self, recorded, subject_id, bids_session):
        """
        A recorded input path under the current BIDS root, or None.

        recon-all records the absolute path it was given, under the BIDS root
        of the run that made the tree (a BABS job's own clone, for example),
        which is usually gone. The part from sub-<id>/ on must name this
        subject's (and session's) anat directory, and is re-rooted at the
        current BIDS root. processing_summary.json paths are already relative
        and are handled the same way.
        """
        parts = PurePosixPath(str(recorded)).parts
        tail = [subject_id] + ([f"ses-{bids_session}"] if bids_session else []) + ["anat"]
        if len(parts) < len(tail) + 1 or list(parts[-len(tail) - 1:-1]) != tail:
            return None
        return self.bids_dir.joinpath(*parts[-len(tail) - 1:])

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
        """Record the inputs of an existing reconstruction without running recon-all.

        Used by --skip-freesurfer. The images are the ones the reconstruction
        records it was made from, and they must pass the same --max-voxel-size
        check as a new run (see _reuse_reconstruction); they are not chosen
        again under the current selection rule.

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
            The recorded image information, with 'failure_reason' set when the
            existing reconstruction cannot be used.
        """
        if not subject_id.startswith("sub-"):
            raise ValueError(f"Subject ID must start with 'sub-', got {subject_id}")

        bids_session = session_label
        if bids_session and bids_session.startswith("ses-"):
            bids_session = bids_session[4:]

        fs_subject_id = f"{subject_id}_ses-{bids_session}" if bids_session else subject_id
        fs_dir = self.subject_output_dir(subject_id, bids_session) / "freesurfer"
        info = self._reuse_reconstruction(fs_dir, layout, subject_id, bids_session, fs_subject_id)
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

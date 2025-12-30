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
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import time

from bids import BIDSLayout
from src.utils import get_freesurfer_version, get_version_info

# Configure logging
logger = logging.getLogger("freesurfer-bidsapp.wrapper")


class FreeSurferWrapper:
    """Wrapper for FreeSurfer's recon-all command."""

    def __init__(self, bids_dir, output_dir, freesurfer_license=None):
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
        """
        self.bids_dir = Path(bids_dir)
        self.output_dir = Path(output_dir)
        self.freesurfer_dir = self.output_dir / "freesurfer"
        self.freesurfer_license = freesurfer_license    

        # Track processing results and image information
        self.results = {"success": [], "failure": [], "skipped": []}
        self.subject_t1_mapping = {}  # Store subject->T1 mapping
        self.temp_files = []

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
            t1w_images = self._find_images(layout, bids_subject, "T1w", bids_session)
            if not t1w_images:
                logger.error(f"No T1w images found for {subject_id}" +
                           (f" session {session_label}" if session_label else ""))
                self.results["skipped"].append(f"{subject_id}" +
                                             (f"_ses-{bids_session}" if bids_session else ""))
                return False

            # Store T1 image information
            fs_subject_id = f"{subject_id}_ses-{session_label}" if session_label else subject_id
            self.subject_t1_mapping[fs_subject_id] = {
                'T1w_images': [str(img) for img in t1w_images],
                'session': session_label
            }

            t2w_images = self._find_images(layout, bids_subject, "T2w", bids_session)
            if t2w_images:
                logger.info(f"Found {len(t2w_images)} T2w images for {subject_id}" +
                           (f" session {session_label}" if session_label else ""))
                self.subject_t1_mapping[fs_subject_id]['T2w_images'] = [str(img) for img in t2w_images]

            # Check if subject already processed
            if (self.freesurfer_dir / fs_subject_id / "scripts" / "recon-all.done").exists():
                logger.info(f"{fs_subject_id} already processed. Skipping...")
                self.results["skipped"].append(fs_subject_id)
                return True

            # Run recon-all
            cmd = self._create_recon_all_command(subject_id, t1w_images, t2w_images,
                                                bids_session if session_label else None)
            logger.info(f"Running command: {' '.join(cmd)}")
            
            subprocess.run(cmd, check=True, capture_output=True, text=True)

            # Organize outputs
            self._organize_bids_output(subject_id, bids_session if session_label else None)

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

    def _copy_file(self, src, dest):
        """Copy file if it exists."""
        if src.exists():
            shutil.copy2(src, dest)
            return True
        return False

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
        bids_subject_dir = self.output_dir / subject_id
        if session_label:
            bids_subject_dir = bids_subject_dir / f"ses-{session_label}"

        anat_dir = bids_subject_dir / "anat"
        stats_dir = bids_subject_dir / "stats"
        anat_dir.mkdir(parents=True, exist_ok=True)
        stats_dir.mkdir(parents=True, exist_ok=True)

        # Determine FreeSurfer subject directory name
        fs_subject_id = subject_id
        if session_label:
            fs_subject_id = f"{subject_id}_ses-{session_label}"

        # Check FreeSurfer subject directory
        fs_subject_dir = self.freesurfer_dir / fs_subject_id
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
            
            # Get BIDS version from pybids
            try:
                from bids import __version__ as bids_version
            except ImportError:
                bids_version = "1.4.0"  # Fallback to default if pybids not available
                
            with open(desc_file, "w") as f:
                json.dump({
                    "Name": "FreeSurfer Derivatives",
                    "BIDSVersion": bids_version,
                    "DatasetType": "derivative",
                    "GeneratedBy": [
                        {
                            "Name": "FreeSurfer",
                            "Version": version_info.get("freesurfer", {}).get("version", get_freesurfer_version()),
                            "Description": "FreeSurfer cortical reconstruction and parcellation"
                        },
                        {
                            "Name": "freesurfer-nidm-bidsapp",
                            "Version": version_info.get("freesurfer-nidm_bidsapp", {}).get("version", "unknown"),
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
        return {
            "total": len(self.results["success"]) + len(self.results["failure"]) + len(self.results["skipped"]),
            "success": len(self.results["success"]),
            "failure": len(self.results["failure"]),
            "skipped": len(self.results["skipped"]),
            "success_list": self.results["success"],
            "failure_list": self.results["failure"],
            "skipped_list": self.results["skipped"],
        }

    def save_processing_summary(self, summary=None):
        """Save processing summary to JSON file."""
        if summary is None:
            summary = self.get_processing_summary()
        output_path = self.freesurfer_dir / "processing_summary.json"
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Processing summary saved to {output_path}")
        return output_path

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

#!/usr/bin/env python3
"""
Utility functions for BIDS-FreeSurfer Application.

This module provides common utility functions used across the BIDS-FreeSurfer
application, including environment setup, version checks, and logging configuration.
"""

import logging
import os
import re
import subprocess
import sys
from pathlib import Path
import datetime
import json


def get_freesurfer_version():
    """
    Get FreeSurfer version string from VERSION file.

    Returns
    -------
    str
        FreeSurfer version string, or "unknown" if not available
    """
    try:
        version_path = Path(__file__).parent.parent / "VERSION"
        if version_path.exists():
            with open(version_path, "r") as f:
                version_data = json.load(f)
                if "freesurfer" in version_data and "version" in version_data["freesurfer"]:
                    return version_data["freesurfer"]["version"]
        return "unknown"
    except Exception as e:
        logging.warning(f"Failed to get FreeSurfer version from VERSION file: {str(e)}")
        return "unknown"

def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Configure logging for the application.

    Parameters
    ----------
    log_level : int, optional
        Logging level (e.g., logging.INFO, logging.DEBUG)
    log_file : str, optional
        Path to log file (if None, logs to console only)
    """
    # Define log format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Reset root logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure logging
    if log_file:
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Set up file handler
        handlers = [logging.FileHandler(log_file), logging.StreamHandler()]
    else:
        handlers = [logging.StreamHandler()]

    # Configure root logger
    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)

    # Set log level for specific loggers
    # Silence overly verbose packages
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("nibabel").setLevel(logging.WARNING)


def check_dependencies():
    """
    Check if all required dependencies are installed.

    Returns
    -------
    bool
        True if all dependencies are met, False otherwise
    """
    # Check for FreeSurfer
    if "FREESURFER_HOME" not in os.environ:
        logging.error("FREESURFER_HOME environment variable not set")
        return False

    # Check for FreeSurfer license
    license_locations = [
        os.environ.get("FS_LICENSE"),
        "/license.txt",  # Docker mount location
        os.path.join(os.environ.get("FREESURFER_HOME", ""), "license.txt"),
        os.path.expanduser("~/.freesurfer.txt"),
    ]

    has_license = False
    for loc in license_locations:
        if loc and os.path.exists(loc):
            has_license = True
            break

    if not has_license:
        logging.warning("FreeSurfer license not found in standard locations")

    # Check for recon-all command
    try:
        subprocess.run(
            ["recon-all", "--help"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        logging.error("recon-all command not found in PATH")
        return False

    return True


def validate_bids_dataset(bids_dir, validate=True):
    """
    Validate BIDS dataset, returning a BIDSLayout object.

    Parameters
    ----------
    bids_dir : str
        Path to BIDS dataset
    validate : bool, optional
        Whether to validate the dataset against BIDS specification

    Returns
    -------
    BIDSLayout
        BIDS layout object

    Raises
    ------
    ValueError
        If dataset validation fails
    """
    try:
        from bids import BIDSLayout

        layout = BIDSLayout(bids_dir, validate=validate)
        subjects = layout.get_subjects()

        if not subjects:
            logging.warning(f"No subjects found in BIDS dataset at {bids_dir}")
        else:
            logging.info(f"Found {len(subjects)} subjects in BIDS dataset")

        return layout
    except Exception as e:
        logging.error(f"Failed to validate BIDS dataset: {str(e)}")
        raise ValueError(f"Invalid BIDS dataset: {str(e)}")


def get_version_info():
    """
    Get comprehensive version information for the application.

    Returns
    -------
    dict
        Dictionary containing version information for all components
    """
    version_info = {
        "freesurfer-nidm_bidsapp": {
            "version": "unknown",
            "source": "package",
            "timestamp": datetime.datetime.now().isoformat()
        },
        "freesurfer": {
            "version": get_freesurfer_version(),
            "source": "base_image",
            "build_stamp": None,
            "image": None
        },
        "python": {
            "version": sys.version,
            "packages": {}
        }
    }

    # Try to read version from VERSION file first
    try:
        version_path = Path(__file__).parent.parent / "VERSION"
        if version_path.exists():
            with open(version_path, "r") as f:
                version_data = json.load(f)
                # Update freesurfer-nidm_bidsapp version
                if "freesurfer-nidm_bidsapp" in version_data:
                    version_info["freesurfer-nidm_bidsapp"].update(version_data["freesurfer-nidm_bidsapp"])
                # Update freesurfer version
                if "freesurfer" in version_data:
                    version_info["freesurfer"].update(version_data["freesurfer"])
                return version_info
    except Exception as e:
        logging.warning(f"Failed to read VERSION file: {str(e)}")

    # Fallback to reading from setup.py if VERSION file not available
    try:
        setup_path = Path(__file__).parent.parent / "setup.py"
        if setup_path.exists():
            with open(setup_path, "r") as f:
                for line in f:
                    if line.startswith('    version="'):
                        version = line.strip().split('"')[1]
                        version_info["freesurfer-nidm_bidsapp"]["version"] = version
                        version_info["freesurfer-nidm_bidsapp"]["source"] = "setup.py"
                        break
    except Exception as e:
        logging.warning(f"Failed to read setup.py: {str(e)}")

    # Fallback to package version if setup.py not available
    if version_info["freesurfer-nidm_bidsapp"]["version"] == "unknown":
        try:
            from importlib.metadata import version
            version_info["freesurfer-nidm_bidsapp"]["version"] = version("freesurfer-nidm-bidsapp")
            version_info["freesurfer-nidm_bidsapp"]["source"] = "package"
        except ImportError:
            try:
                from pkg_resources import get_distribution
                version_info["freesurfer-nidm_bidsapp"]["version"] = get_distribution("freesurfer-nidm-bidsapp").version
            except Exception:
                pass

    # Get build stamp if available (as additional information)
    fs_home = os.environ.get("FREESURFER_HOME")
    if fs_home:
        build_stamp_path = Path(fs_home) / "build-stamp.txt"
        if build_stamp_path.exists():
            try:
                with open(build_stamp_path, "r") as f:
                    build_stamp = f.read().strip()
                    version_info["freesurfer"]["build_stamp"] = build_stamp
            except Exception as e:
                logging.warning(f"Failed to read build-stamp.txt: {str(e)}")

    # Get Python package versions
    try:
        import pkg_resources
        for package in ["numpy", "pandas", "nibabel", "bids", "rdflib"]:
            try:
                version_info["python"]["packages"][package] = pkg_resources.get_distribution(package).version
            except pkg_resources.DistributionNotFound:
                pass
    except Exception:
        pass

    return version_info

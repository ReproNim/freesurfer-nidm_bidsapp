#!/usr/bin/env python3
"""
BIDS App for FreeSurfer processing with NIDM output support.

This BIDS App runs FreeSurfer's recon-all pipeline on T1w and optionally T2w images
from a BIDS dataset and outputs both standard FreeSurfer results and NIDM format results.
"""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

import click

from bids import BIDSLayout
from src.freesurfer.wrapper import FreeSurferWrapper
from src.utils import get_version_info, setup_logging
from rdflib import Graph

try:
    from importlib.metadata import version

    __version__ = version("bids-freesurfer")
except ImportError:
    __version__ = "0.1.0"

logger = logging.getLogger("bids-freesurfer")


def _log_version_info(version_info):
    """Log version information."""
    logger.info(f"BIDS-FreeSurfer version: {version_info['freesurfer_bidsapp']['version']}")
    logger.info(f"FreeSurfer version: {version_info['freesurfer']['version']}")
    if version_info["freesurfer"]["build_stamp"]:
        logger.info(f"FreeSurfer build stamp: {version_info['freesurfer']['build_stamp']}")
    logger.info(f"Python version: {version_info['python']['version']}")
    if version_info["python"]["packages"]:
        logger.info("Python package versions:")
        for package, version in version_info["python"]["packages"].items():
            logger.info(f"  {package}: {version}")


def initialize(bids_dir, freesurfer_license, output_dir, skip_bids_validation, verbose):
    """Initialize the BIDS-FreeSurfer app.
    Args:
        bids_dir (str): Path to BIDS root directory
        freesurfer_license (str): Path to FreeSurfer license file
        output_dir (str): Path to output directory
        skip_bids_validation (bool): Skip BIDS validation
        verbose (bool): Enable verbose output
    """
    # Convert paths to Path objects
    bids_dir = Path(bids_dir)

    nidm_input_dir = bids_dir.parent / "NIDM"
    if not nidm_input_dir.exists():
        nidm_input_dir = None

    # First create the main output directory
    app_output_dir = Path(output_dir) / "freesurfer_bidsapp"
    freesurfer_dir = app_output_dir / "freesurfer"
    nidm_dir = app_output_dir / "nidm"

    # Set logging level and print version info
    setup_logging(logging.DEBUG if verbose else logging.INFO)
    version_info = get_version_info()
    _log_version_info(version_info)

    if freesurfer_license:
        freesurfer_license = Path(freesurfer_license)

    # Set FreeSurfer environment variables
    os.environ["FS_ALLOW_DEEP"] = "1"  # Enable ML routines
    os.environ["SUBJECTS_DIR"] = str(freesurfer_dir)

    # Create FreeSurfer output directory
    os.makedirs(freesurfer_dir, exist_ok=True)

    # Load BIDS dataset
    try:
        layout = BIDSLayout(str(bids_dir), validate=not skip_bids_validation)
        logger.info("Found BIDS dataset")
    except Exception as e:
        logger.error(f"Error loading BIDS dataset: {str(e)}")
        sys.exit(1)

    # Let the FreeSurfer wrapper handle its directory
    try:
        freesurfer_wrapper = FreeSurferWrapper(
            bids_dir,
            app_output_dir,  # Pass the app_output_dir to FreeSurferWrapper
            freesurfer_license,
        )
        freesurfer_wrapper.nidm_input_dir = nidm_input_dir
    except Exception as e:
        logger.error(f"Error initializing FreeSurfer wrapper: {str(e)}")
        sys.exit(1)

    return layout, freesurfer_wrapper, freesurfer_dir, nidm_dir, nidm_input_dir, version_info


def nidm_conversion(
    nidm_dir,
    freesurfer_dir,
    participant_label,
    freesurfer_wrapper,
    bids_session=None,
    verbose=False,
    nidm_input_dir=None,
):
    """Convert FreeSurfer outputs to NIDM format.
    Args:
        nidm_dir (str): Path to NIDM output directory
        freesurfer_dir (str): Path to FreeSurfer output directory
        participant_label (str): Participant label (without "sub-" prefix)
        freesurfer_wrapper (FreeSurferWrapper): Instance of FreeSurferWrapper containing T1 info
        bids_session (str): Session label (without "ses-" prefix)
        verbose (bool): Enable verbose output
        nidm_input_dir (Path or None): Optional path to existing NIDM resources
    """
    # Determine subject directory with session info (add sub- prefix for FreeSurfer directory)
    if bids_session is None:
        fs_subject_id = f"sub-{participant_label}"
    else:
        fs_subject_id = f"sub-{participant_label}_ses-{bids_session}"
    subject_dir = os.path.join(freesurfer_dir, fs_subject_id)

    # Get T1 and T2 image information (use subject ID with prefix for wrapper)
    t1_info = freesurfer_wrapper.get_subject_t1_info(fs_subject_id, bids_session)
    t1_images = t1_info.get('T1w_images', [])
    t2_images = t1_info.get('T2w_images', [])
    if not t1_images:
        logger.warning(f"No T1 image information found for {fs_subject_id}")

    os.makedirs(nidm_dir, exist_ok=True)

    existing_nidm_file = None
    if nidm_input_dir and nidm_input_dir.exists():
        # Prefer top-level nidm.ttl then fall back to any .ttl/.jsonld file present
        primary_candidate = nidm_input_dir / "nidm.ttl"
        if primary_candidate.exists():
            existing_nidm_file = primary_candidate
        else:
            for pattern in ("*.ttl", "*.jsonld", "*.json-ld"):
                try:
                    existing_nidm_file = next(nidm_input_dir.glob(pattern))
                    break
                except StopIteration:
                    continue

    # Build the command
    module_name = "segstats_jsonld.fs_to_nidm"
    
    copied_nidm = None
    if existing_nidm_file:
        copied_nidm = Path(nidm_dir) / existing_nidm_file.name
        try:
            if existing_nidm_file.resolve() != copied_nidm.resolve():
                shutil.copy2(existing_nidm_file, copied_nidm)
                logger.info(f"Copied existing NIDM file to output directory: {copied_nidm}")
            else:
                logger.info(f"Input and output NIDM paths are the same: {copied_nidm}")
        except OSError as copy_error:
            logger.error(
                "Failed to copy existing NIDM file to output directory (%s). Error: %s",
                copied_nidm,
                copy_error,
            )
            logger.error("Cannot proceed without copying to avoid overwriting input file.")
            sys.exit(1)

    # Build command: use -n for existing NIDM file, -o for new output
    # Note: fs_to_nidm does not allow both -n and -o at the same time
    if copied_nidm:
        cmd = [sys.executable, "-m", module_name, "-s", subject_dir, "-n", str(copied_nidm), "-j", "--forcenidm"]
        if verbose:
            logger.info(f"Found existing NIDM file: {existing_nidm_file}")
            logger.info(f"Adding data to existing NIDM file: {copied_nidm}")
    else:
        cmd = [sys.executable, "-m", module_name, "-s", subject_dir, "-o", str(nidm_dir), "-j"]
        if nidm_input_dir and verbose:
            logger.info(f"No NIDM file found in {nidm_input_dir}; creating new output")

    # Log the command for debugging and replay purposes
    logger.info(f"Running command: {' '.join(cmd)}")

    existing_outputs = {path.name for path in Path(nidm_dir).glob("*")}

    env = os.environ.copy()
    project_root = Path(__file__).resolve().parents[1]
    existing_pythonpath = env.get("PYTHONPATH")
    pythonpath_entries = [str(project_root), str(project_root / "src"), str(project_root / "src" / "segstats_jsonld")]
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    env.setdefault("RDFLIB_STORE_NO_BIND_OVERRIDE", "1")
    env.setdefault("SEGSTATS_JSONLD_ALLOW_NEW_KEYS", "1")
    env.pop("PYTHONHOME", None)

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        logger.error(f"NIDM conversion failed for {fs_subject_id}")
        logger.error(f"Error output: {result.stderr}")
        if verbose:
            logger.error(f"Command output: {result.stdout}")
        sys.exit(1)
    
    # Log what files exist in output directory after conversion
    logger.info(f"Files in NIDM output directory after conversion:")
    for file in Path(nidm_dir).glob("*"):
        logger.info(f"  - {file.name} ({file.stat().st_size} bytes)")

    new_outputs = [path for path in Path(nidm_dir).glob("*") if path.name not in existing_outputs]

    if not new_outputs and not copied_nidm:
        logger.warning("No new NIDM outputs were generated; skipping aggregation step")
        return

    aggregation_sources = []
    if copied_nidm and Path(copied_nidm).exists():
        aggregation_sources.append(Path(copied_nidm))
    elif existing_nidm_file and existing_nidm_file.exists():
        aggregation_sources.append(existing_nidm_file)

    for candidate in new_outputs:
        if candidate.suffix.lower() in {".ttl", ".json", ".jsonld", ".json-ld"}:
            aggregation_sources.append(candidate)

    if not aggregation_sources:
        logger.warning("No NIDM sources found to aggregate after conversion")
        return

    def _guess_rdf_format(path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".ttl", ".turtle", ".n3"}:
            return "turtle"
        if suffix in {".json", ".jsonld", ".json-ld"}:
            return "json-ld"
        return "xml"

    aggregated_graph = Graph()
    for src in aggregation_sources:
        try:
            aggregated_graph.parse(str(src), format=_guess_rdf_format(src))
        except Exception as parse_error:  # pragma: no cover - depends on runtime env
            logger.warning(f"Skipping NIDM source {src} due to parse error: {parse_error}")

    if len(aggregated_graph) == 0:
        logger.warning("Aggregated NIDM graph is empty; skipping serialization")
        return

    target_base = Path(nidm_dir) / fs_subject_id
    target_ttl = target_base.with_suffix(".ttl")
    target_jsonld = target_base.with_suffix(".jsonld")

    try:
        aggregated_graph.serialize(destination=str(target_ttl), format="turtle")
    except Exception as serialize_error:  # pragma: no cover
        logger.warning(f"Failed to write aggregated TTL output {target_ttl}: {serialize_error}")

    try:
        aggregated_graph.serialize(destination=str(target_jsonld), format="json-ld", indent=2)
    except Exception as serialize_error:  # pragma: no cover
        logger.warning(f"Failed to write aggregated JSON-LD output {target_jsonld}: {serialize_error}")

    logger.info("================================")
    logger.info(f"NIDM conversion complete for {fs_subject_id}")
    logger.info("================================")


def process_participant(
    bids_dir,
    output_dir,
    participant_label,
    freesurfer_license,
    skip_bids_validation,
    skip_nidm,
    verbose,
):
    """Process a single participant with FreeSurfer.

    Args:
        bids_dir (str): Path to BIDS root directory
        output_dir (str): Path to output directory
        participant_label (str): Participant label (with or without "sub-" prefix)
        freesurfer_license (str): Path to FreeSurfer license file
        skip_bids_validation (bool): Skip BIDS validation
        skip_nidm (bool): Skip NIDM export
        verbose (bool): Enable verbose output
    """
    layout, freesurfer_wrapper, freesurfer_dir, nidm_dir, nidm_input_dir, version_info = initialize(
        bids_dir, freesurfer_license, output_dir, skip_bids_validation, verbose
    )

    # Strip "sub-" prefix if present (BABS may pass it with the prefix)
    if participant_label.startswith("sub-"):
        participant_label = participant_label[4:]

    # Validate that the subject exists (participant_label is without "sub-" prefix)
    available_subjects = layout.get_subjects()
    if participant_label not in available_subjects:
        logger.error(f"Subject sub-{participant_label} not found in dataset")
        sys.exit(1)

    # Add sub- prefix for FreeSurfer subject ID
    fs_subject_id = f"sub-{participant_label}"

    # Run participant analysis
    try:
        success = freesurfer_wrapper.process_subject(fs_subject_id, layout)
        # Save processing summary
        summary = freesurfer_wrapper.get_processing_summary()
        summary["version_info"] = version_info
        freesurfer_wrapper.save_processing_summary(summary)

        logger.info("================================")
        logger.info("Processing complete!")
        logger.info("================================")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        sys.exit(1)

    if success and not skip_nidm:
        nidm_conversion(
            nidm_dir,
            freesurfer_dir,
            participant_label,
            freesurfer_wrapper,
            verbose=verbose,
            nidm_input_dir=nidm_input_dir,
        )

    logger.info("BIDS-FreeSurfer processing complete.")
    return 0


def process_session(
    bids_dir,
    output_dir,
    participant_label,
    session_label,
    freesurfer_license,
    skip_bids_validation,
    skip_nidm,
    verbose,
):
    """Process a single session for a participant with FreeSurfer.

    Args:
        bids_dir (str): Path to BIDS root directory
        output_dir (str): Path to output directory
        participant_label (str): Participant label (with or without "sub-" prefix)
        session_label (str): Session label (with or without "ses-" prefix)
        freesurfer_license (str): Path to FreeSurfer license file
        skip_bids_validation (bool): Skip BIDS validation
        skip_nidm (bool): Skip NIDM export
        verbose (bool): Enable verbose output
    """
    layout, freesurfer_wrapper, freesurfer_dir, nidm_dir, nidm_input_dir, version_info = initialize(
        bids_dir, freesurfer_license, output_dir, skip_bids_validation, verbose
    )

    # Strip "sub-" prefix if present (BABS may pass it with the prefix)
    if participant_label.startswith("sub-"):
        participant_label = participant_label[4:]

    # Strip "ses-" prefix if present (BABS may pass it with the prefix)
    if session_label.startswith("ses-"):
        session_label = session_label[4:]

    # Validate that the subject exists (participant_label is without "sub-" prefix)
    available_subjects = layout.get_subjects()
    if participant_label not in available_subjects:
        logger.error(f"Subject sub-{participant_label} not found in dataset")
        sys.exit(1)

    # Validate that the session exists (session_label is without "ses-" prefix)
    available_sessions = layout.get_sessions(subject=participant_label)
    if session_label not in available_sessions:
        logger.error(f"Session ses-{session_label} not found for subject sub-{participant_label}")
        sys.exit(1)

    # Add sub- prefix for FreeSurfer subject ID
    fs_subject_id = f"sub-{participant_label}"

    # Run session-level analysis
    try:
        # Use the enhanced process_subject method with session_label
        success = freesurfer_wrapper.process_subject(fs_subject_id, layout, session_label=session_label)
        # Save processing summary
        summary = freesurfer_wrapper.get_processing_summary()
        summary["version_info"] = version_info
        freesurfer_wrapper.save_processing_summary(summary)
        logger.info("================================")
        logger.info("Processing complete!")
        logger.info("================================")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        sys.exit(1)

    if success and not skip_nidm:
        nidm_conversion(
            nidm_dir,
            freesurfer_dir,
            participant_label,
            freesurfer_wrapper,
            session_label,
            verbose=verbose,
            nidm_input_dir=nidm_input_dir,
        )

    logger.info("BIDS-FreeSurfer processing complete.")
    return 0


@click.command()
@click.version_option(version=__version__)
@click.argument("bids_dir", type=click.Path(exists=True, file_okay=False, resolve_path=True))
@click.argument("output_dir", type=click.Path(file_okay=False, resolve_path=True))
@click.argument(
    "analysis_level",
    type=click.Choice(["participant", "session"]),
)
@click.option(
    "--participant_label",
    "--participant-label",
    help='The label of the participant to analyze (with or without "sub-" prefix, e.g., "001" or "sub-001").',
)
@click.option(
    "--session_label",
    "--session-label",
    help='The label of the session to analyze (with or without "ses-" prefix, e.g., "01" or "ses-01"). Only used with "session" analysis level.',
)
@click.option(
    "--freesurfer_license",
    "--fs-license-file",
    type=click.Path(exists=True, resolve_path=True),
    help="Path to FreeSurfer license file.",
)
@click.option("--skip-bids-validation", is_flag=True, help="Skip BIDS validation.")
@click.option("--skip_nidm", "--skip-nidm", is_flag=True, help="Skip NIDM output generation.")
@click.option("--verbose", is_flag=True, help="Enable verbose output.")
def cli(
    bids_dir,
    output_dir,
    analysis_level,
    participant_label,
    session_label,
    freesurfer_license,
    skip_bids_validation,
    skip_nidm,
    verbose,
):
    """FreeSurfer BIDS App with NIDM Output.

    This BIDS App runs FreeSurfer's recon-all pipeline on T1w images from a BIDS dataset.
    It supports individual participant analysis and can generate NIDM outputs.

    BIDS_DIR is the path to the BIDS dataset directory.

    OUTPUT_DIR is the path where results will be stored.

    ANALYSIS_LEVEL determines the processing stage to be run:
    - 'participant': processes a single subject
    - 'session': processes a single session for a subject
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    if analysis_level == "participant":
        if not participant_label:
            logger.error("Participant label is required for participant-level analysis")
            sys.exit(1)
        return process_participant(
            bids_dir,
            output_dir,
            participant_label,
            freesurfer_license,
            skip_bids_validation,
            skip_nidm,
            verbose,
        )
    elif analysis_level == "session":
        if not participant_label or not session_label:
            logger.error("Both participant and session labels are required for session-level analysis")
            sys.exit(1)
        return process_session(
            bids_dir,
            output_dir,
            participant_label,
            session_label,
            freesurfer_license,
            skip_bids_validation,
            skip_nidm,
            verbose,
        )


def main():
    """Entry point for the CLI."""
    try:
        cli()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

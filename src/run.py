#!/usr/bin/env python3
"""
BIDS App for FreeSurfer processing with NIDM output support.

This BIDS App runs FreeSurfer's recon-all pipeline on T1w and optionally T2w images
from a BIDS dataset and outputs both standard FreeSurfer results and NIDM format results.
"""

import contextlib
import logging
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

import click

from bids import BIDSLayout
from src.freesurfer.wrapper import DEFAULT_MAX_VOXEL_SIZE, FreeSurferWrapper
from src.utils import get_app_version, get_version_info, setup_logging
from rdflib import Graph

try:
    from importlib.metadata import version

    __version__ = version("freesurfer-nidm")
except ImportError:
    __version__ = get_app_version()

logger = logging.getLogger("freesurfer-nidm")


def _log_version_info(version_info):
    """Log version information."""
    logger.info(f"BIDS-FreeSurfer version: {version_info['freesurfer-nidm']['version']}")
    logger.info(f"FreeSurfer version: {version_info['freesurfer']['version']}")
    if version_info["freesurfer"]["build_stamp"]:
        logger.info(f"FreeSurfer build stamp: {version_info['freesurfer']['build_stamp']}")
    logger.info(f"Python version: {version_info['python']['version']}")
    if version_info["python"]["packages"]:
        logger.info("Python package versions:")
        for package, version in version_info["python"]["packages"].items():
            logger.info(f"  {package}: {version}")


def initialize(bids_dir, freesurfer_license, output_dir, skip_bids_validation, verbose, nidm_input_dir=None,
               max_voxel_size=DEFAULT_MAX_VOXEL_SIZE):
    """Initialize the BIDS-FreeSurfer app.
    Args:
        bids_dir (str): Path to BIDS root directory
        freesurfer_license (str): Path to FreeSurfer license file
        output_dir (str): Path to output directory
        skip_bids_validation (bool): Skip BIDS validation
        verbose (bool): Enable verbose output
        nidm_input_dir (str or None): Path to existing NIDM directory (defaults to <bids_dir>/../NIDM)
        max_voxel_size (float): Largest voxel dimension (mm) accepted for an anatomical input
    """
    # Convert paths to Path objects
    bids_dir = Path(bids_dir)

    # Use CLI argument if provided, otherwise default to BABS location
    explicit_nidm_input_dir = nidm_input_dir is not None
    if nidm_input_dir is None:
        nidm_input_dir = bids_dir.parent / "NIDM"
    else:
        nidm_input_dir = Path(nidm_input_dir)

    if not nidm_input_dir.exists():
        if explicit_nidm_input_dir:
            logger.warning(f"--nidm-input-dir {nidm_input_dir} does not exist; "
                           "NIDM will be built without an input document")
        nidm_input_dir = None

    # Per-subject output layout (study-wide standard): everything an app produces
    # for a subject lives under <output_dir>/sub-<id>/, so the BABS zip's top-level
    # folder is the subject directory and results land as
    # <derivative_name>/sub-<id>/... when unzipped. There is no app-name wrapper
    # directory and no shared nidm/ directory -- a shared nidm/ made every subject's
    # nidm.ttl collide on the same path, so only the first subject's survived.
    app_output_dir = Path(output_dir)
    freesurfer_dir = app_output_dir / ".fs_staging"

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
            max_voxel_size=max_voxel_size,
        )
        freesurfer_wrapper.nidm_input_dir = nidm_input_dir
    except Exception as e:
        logger.error(f"Error initializing FreeSurfer wrapper: {str(e)}")
        sys.exit(1)

    return layout, freesurfer_wrapper, freesurfer_dir, nidm_input_dir, version_info


def subject_output_dir(output_dir, participant_label, session=None):
    """Per-subject output directory -- the unit BABS zips.

    <output_dir>/sub-<label>[/ses-<session>]. This is the single source of truth
    for the output layout at the run level; FreeSurferWrapper mirrors it for its
    own bookkeeping. Deliberately independent of the wrapper so the layout does
    not depend on FreeSurfer-specific state.
    """
    subject_dir = Path(output_dir) / f"sub-{participant_label}"
    if session:
        subject_dir = subject_dir / f"ses-{session}"
    return subject_dir


class NIDMInputNotRetrieved(FileNotFoundError):
    """The subject's NIDM input is a symlink with no content behind it.

    That is what a datalad/git-annex clone looks like before ``datalad get``.
    It is not the same as the subject having no NIDM record: treating it as
    a miss would build nidm.ttl without the inherited demographics and
    provenance and still exit 0, for every subject in the dataset.
    """


class NIDMInputUnreadable(ValueError):
    """The subject's NIDM input exists but rdflib cannot parse it."""


def _looks_unfetched(path):
    """True if path looks like git-annex content that was never fetched: a
    symlink into .git/annex with nothing behind it, or a small regular file
    holding an unlocked annex pointer (/annex/objects/<key>)."""
    path = Path(path)
    if path.is_symlink():
        return not path.exists() and ".git/annex/" in os.readlink(path)
    try:
        if path.stat().st_size > 1024:
            return False
        with open(path, "rb") as f:
            return f.read(64).lstrip().startswith(b"/annex/objects/")
    except OSError:
        return False


def find_nidm_input_file(nidm_input_dir, participant_label, session=None, warn=False):
    """This subject's existing NIDM document, or None.

    Candidates are tried most specific first, and are the same as ants-nidm's
    find_nidm_input_file. The per-subject nidm.ttl layout comes first because
    that is what the shared NIDM datasets use (e.g. nidm_4.5.0/sub-<id>/nidm.ttl);
    the sub-<id>.ttl and flat forms are kept for older or hand-built inputs.

    There is no "any top-level file" fallback: in a flat directory of
    per-subject files it handed every subject whichever file was listed first,
    usually another subject's record. A root-level nidm.ttl is accepted as a
    dataset-level document covering all subjects.

    The first candidate present decides. A dangling symlink (annexed content
    not fetched) raises NIDMInputNotRetrieved rather than falling through.
    With warn=True, a miss in an existing input directory is logged as a
    warning.
    """
    if nidm_input_dir is None:
        return None
    nidm_input_dir = Path(nidm_input_dir)
    if not nidm_input_dir.exists():
        return None

    subject_dirname = f"sub-{participant_label}"
    candidates = []
    if session:
        candidates += [
            nidm_input_dir / subject_dirname / f"ses-{session}" / "nidm.ttl",
            nidm_input_dir / f"{subject_dirname}_ses-{session}" / "nidm.ttl",
            nidm_input_dir / subject_dirname / f"ses-{session}" / f"{subject_dirname}_ses-{session}.ttl",
            nidm_input_dir / f"{subject_dirname}_ses-{session}.ttl",
        ]
    candidates += [
        nidm_input_dir / subject_dirname / "nidm.ttl",
        nidm_input_dir / subject_dirname / f"{subject_dirname}.ttl",
        nidm_input_dir / f"{subject_dirname}.ttl",
        # Dataset-level document covering all subjects.
        nidm_input_dir / "nidm.ttl",
    ]
    for c in candidates:
        if c.exists():
            return c
        if c.is_symlink():
            raise NIDMInputNotRetrieved(
                f"NIDM input {c} is a symlink with no content behind it "
                f"(run `datalad get` on the NIDM input dataset)"
            )

    if warn:
        logger.warning(
            f"No NIDM input for {subject_dirname}"
            + (f" ses-{session}" if session else "")
            + f" in {nidm_input_dir}; nidm.ttl will not include its"
            " demographics or input provenance"
        )
    return None


# fs_to_nidm is run in the app's own interpreter by calling its main(), not
# with `python -m segstats_jsonld.fs_to_nidm`: the package __init__ imports
# that module, so -m executed it twice and runpy warned (RuntimeWarning,
# "found in sys.modules ...") on every run.
#
# The NIDM input check (app process) and the conversion (a new interpreter
# started with `python -c CONVERTER_BOOTSTRAP`) must run the same converter
# file. Two things put the working directory on sys.path, where a package or
# module named like the converter or one of its imports (segstats_jsonld,
# glob, ...) would replace the installed code:
# - `-c` makes '' the conversion's sys.path[0] (Python 3.10, the image's, has
#   no -P);
# - an empty PYTHONPATH component, in both processes. The image's own
#   PYTHONPATH (Dockerfile ENV, Singularity %environment) has none, but a host
#   PYTHONPATH that Apptainer passes through (it does unless the run uses
#   --cleanenv) can end or start with a colon.
# So, in both processes, '' and every entry that is the working directory are
# dropped before the converter is imported: in the app process by
# _import_fs_to_nidm, for that import only; in the conversion by the
# bootstrap, for the whole run, before anything else is imported. When the
# working directory no longer exists (os.getcwd() raises), only '' entries are
# dropped. nidm_conversion also drops empty components of the host PYTHONPATH
# it passes to the conversion. The bootstrap then loads segstats_jsonld from the directory of the
# file named by CONVERTER_FILE_ENV, which nidm_conversion sets to the
# fs_to_nidm file the app process imported, and exits non-zero naming both
# files if it ends up with another one. Other entries (/opt, /opt/src, a host
# PYTHONPATH's directories) are not filtered.
CONVERTER_MODULE = "segstats_jsonld.fs_to_nidm"
CONVERTER_FILE_ENV = "FREESURFER_NIDM_CONVERTER_FILE"
CONVERTER_BOOTSTRAP = f"""\
import os, sys
try:
    cwd = os.path.realpath(os.getcwd())
except OSError:
    cwd = None
sys.path[:] = [p for p in sys.path if p and (cwd is None or os.path.realpath(p) != cwd)]
import importlib.util
want = os.environ.get("{CONVERTER_FILE_ENV}", "")
package_dir = os.path.dirname(want)
init = os.path.join(package_dir, "__init__.py")
if os.path.basename(want) != "fs_to_nidm.py" or not os.path.isfile(want) or not os.path.isfile(init):
    sys.exit("fs_to_nidm: {CONVERTER_FILE_ENV}=%r does not name fs_to_nidm.py in a regular "
             "segstats_jsonld package" % want)
spec = importlib.util.spec_from_file_location("segstats_jsonld", init,
                                              submodule_search_locations=[package_dir])
package = importlib.util.module_from_spec(spec)
sys.modules["segstats_jsonld"] = package
spec.loader.exec_module(package)
from {CONVERTER_MODULE} import main
got = sys.modules["{CONVERTER_MODULE}"].__file__
if os.path.realpath(got) != os.path.realpath(want):
    sys.exit("fs_to_nidm: the conversion would run %s, not %s, which the NIDM input check "
             "ran" % (got, want))
sys.argv[0] = "fs_to_nidm"
sys.exit(main())
"""
# The converter's checkout; nidm_conversion puts it on the converter's
# PYTHONPATH, so it is found there when segstats_jsonld is not installed.
CONVERTER_DIR = Path(__file__).resolve().parents[1] / "src" / "segstats_jsonld"


def converter_subject_id(participant_label):
    """The -subjid nidm_conversion passes to fs_to_nidm for a participant label."""
    return f"sub-{participant_label}"


def _without_working_directory(paths):
    """paths without '' and without entries that are the working directory
    (only without '' when the working directory no longer exists), as
    CONVERTER_BOOTSTRAP filters the conversion's sys.path."""
    try:
        cwd = os.path.realpath(os.getcwd())
    except OSError:
        cwd = None
    return [p for p in paths if p and (cwd is None or os.path.realpath(p) != cwd)]


def _import_fs_to_nidm():
    """Import the converter with the working directory off sys.path for this
    import (see CONVERTER_BOOTSTRAP); the conversion runs the same file."""
    import importlib

    saved = list(sys.path)
    sys.path[:] = _without_working_directory(sys.path)
    try:
        try:
            return importlib.import_module(CONVERTER_MODULE)
        except ModuleNotFoundError as err:
            if err.name != "segstats_jsonld":
                raise
        sys.path.append(str(CONVERTER_DIR))
        return importlib.import_module(CONVERTER_MODULE)
    finally:
        sys.path[:] = saved


def nidm_input_subject_problem(nidm_input_file, participant_label):
    """Run fs_to_nidm's own subject lookup on the input NIDM.

    nidm_conversion later converts a copy of this file with -subjid
    converter_subject_id(participant_label) and --forcenidm, and fs_to_nidm
    refuses it when the subject cannot be identified (the file has subjects
    but not this one, or several nodes with its id and not exactly one owning
    an anatomical MRI acquisition). Called before recon-all, so that costs
    seconds rather than hours. The file is only parsed with rdflib; a file
    rdflib cannot parse raises NIDMInputUnreadable, naming the file.

    Returns:
        str or None: fs_to_nidm's message when it would refuse the input, None
        when it would find (or, under --forcenidm, add) the subject.
    """
    from contextlib import redirect_stdout
    from io import StringIO

    from rdflib import util

    fs_to_nidm = _import_fs_to_nidm()
    path = str(nidm_input_file)
    graph = Graph()
    rdf_format = util.guess_format(path)
    try:
        graph.parse(path, format=rdf_format)
    except Exception as err:
        # rdflib's message names no file ("at line 1 of <>: Bad syntax")
        detail = " ".join(str(err).split())
        message = (f"NIDM input {path} could not be parsed as "
                   f"{'Turtle' if rdf_format == 'turtle' else rdf_format} "
                   f"({type(err).__name__}: {detail})")
        if _looks_unfetched(nidm_input_file):
            message += ("; it looks like an unfetched git-annex file (run `datalad get` "
                        "on the NIDM input dataset)")
        raise NIDMInputUnreadable(message) from err
    printed = StringIO()
    try:
        with redirect_stdout(printed):
            # forceagent=True: nidm_conversion always passes --forcenidm with -n
            fs_to_nidm.find_subject(graph, converter_subject_id(participant_label), forceagent=True)
    except fs_to_nidm.SubjectLookupError as err:
        return str(err)
    finally:
        for line in printed.getvalue().splitlines():
            if line.strip():
                logger.info(f"NIDM input check: {line.strip()}")
    return None


def check_nidm_input(nidm_input_dir, participant_label, session):
    """The pre-recon-all NIDM input check: find the input (an unfetched annex
    file raises) and exit 1 if fs_to_nidm could not find the subject in it."""
    nidm_input_file = find_nidm_input_file(nidm_input_dir, participant_label, session, warn=True)
    if nidm_input_file is None:
        return
    who = f"sub-{participant_label}" + (f" ses-{session}" if session else "")
    try:
        problem = nidm_input_subject_problem(nidm_input_file, participant_label)
    except NIDMInputUnreadable as err:
        logger.error(str(err))
        logger.error(f"Not running recon-all for {who}: fetch or fix the NIDM input, or pass --skip-nidm")
        sys.exit(1)
    if problem is None:
        return
    logger.error(f"NIDM input {nidm_input_file} cannot take the FreeSurfer stats of {who}: {problem}")
    logger.error(f"Not running recon-all for {who}: fix the NIDM input, or pass --skip-nidm")
    sys.exit(1)


# nidm_conversion's temporary copy of the input NIDM: <subject dir>/.nidm.XXXX.ttl
TEMPORARY_PREFIX = ".nidm."


def _remove_interrupted_temporaries(nidm_dir):
    """Delete temporary copies left in nidm_dir by a run that was killed while
    converting (SIGKILL, or a node failure), before this run converts."""
    for leftover in sorted(Path(nidm_dir).glob(f"{TEMPORARY_PREFIX}*.ttl")):
        logger.warning(f"Removing {leftover.name}, a temporary NIDM file left by an interrupted run")
        leftover.unlink(missing_ok=True)


@contextlib.contextmanager
def _exit_on_sigterm():
    """Raise SystemExit(143) on SIGTERM while active, so finally blocks run;
    Python's default SIGTERM action ends the process without them."""
    def _raise(signum, frame):
        raise SystemExit(128 + signum)

    try:
        previous = signal.signal(signal.SIGTERM, _raise)
    except ValueError:  # not the main thread; leave SIGTERM alone
        yield
        return
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _log_no_nidm_written(final_nidm, fs_subject_id):
    """Say that this run wrote no nidm.ttl and, if one is there, that it is
    an earlier run's (a failed re-conversion with --skip-freesurfer leaves it
    in place)."""
    final_nidm = Path(final_nidm)
    if final_nidm.exists():
        import datetime

        mtime = datetime.datetime.fromtimestamp(final_nidm.stat().st_mtime).isoformat(timespec="seconds")
        logger.error(
            f"{final_nidm} (modified {mtime}) is left from an earlier run: it is not from this run "
            f"and does not hold this run's FreeSurfer measures; no {final_nidm.name} was written "
            f"for {fs_subject_id}"
        )
    else:
        logger.error(f"No {final_nidm.name} written for {fs_subject_id}")


def nidm_conversion(
    output_dir,
    participant_label,
    freesurfer_wrapper,
    bids_session=None,
    verbose=False,
    nidm_input_dir=None,
):
    """Convert FreeSurfer outputs to NIDM format.

    Writes nidm.ttl (and the shared fs_cde.ttl vocabulary) into the subject's own
    output directory, <output_dir>/sub-<id>[/ses-<session>]/, alongside the analysis
    results. Output is always named nidm.ttl -- the subject identity is carried by
    the directory, which is also what keeps concurrent subjects from colliding.

    Args:
        output_dir (str or Path): Base output directory (the derivative root)
        participant_label (str): Participant label (without "sub-" prefix)
        freesurfer_wrapper (FreeSurferWrapper): Instance of FreeSurferWrapper containing T1 info
        bids_session (str): Session label (without "ses-" prefix)
        verbose (bool): Enable verbose output
        nidm_input_dir (Path or None): Optional path to existing NIDM resources
    """
    # fs_subject_id is the *provenance* identifier (used for logging and for the
    # T1 mapping key); the on-disk location is the subject directory.
    if bids_session is None:
        fs_subject_id = f"sub-{participant_label}"
    else:
        fs_subject_id = f"sub-{participant_label}_ses-{bids_session}"

    # NIDM output goes in the subject dir; recon-all's tree is namespaced under it.
    nidm_dir = subject_output_dir(output_dir, participant_label, bids_session)
    subject_dir = str(nidm_dir / "freesurfer")

    # Get T1 and T2 image information. The mapping is keyed by the base subject
    # ID plus session, so pass those separately rather than the combined ID.
    t1_info = freesurfer_wrapper.get_subject_t1_info(f"sub-{participant_label}", bids_session)
    t1_images = t1_info.get('T1w_images', [])
    t2_images = t1_info.get('T2w_images', [])
    if not t1_images:
        logger.warning(
            f"No record of the T1w recon-all used for {fs_subject_id}; with an input NIDM "
            "the FreeSurfer stats are linked to an acquisition only if it has a single "
            "anatomical one for this subject"
        )

    os.makedirs(nidm_dir, exist_ok=True)
    _remove_interrupted_temporaries(nidm_dir)

    existing_nidm_file = find_nidm_input_file(nidm_input_dir, participant_label, bids_session)

    # Build the command
    converter = [sys.executable, "-c", CONVERTER_BOOTSTRAP]

    # From the temporary copy's creation to its rename into nidm.ttl, remove it
    # on any exit: a failed conversion, an exception, Ctrl-C, or SIGTERM (a
    # time limit, scancel), which is turned into SystemExit for this stretch
    # so the cleanup runs (subprocess.run then also kills fs_to_nidm).
    pending = None
    with _exit_on_sigterm():
        try:
            copied_nidm = None
            final_nidm = Path(nidm_dir) / "nidm.ttl"
            if existing_nidm_file:
                # fs_to_nidm adds the stats to its -n file in place. Give it a temporary
                # copy of the input next to nidm.ttl (it writes fs_cde.ttl beside its -n
                # file) and rename the copy to nidm.ttl only after a successful run: a
                # failed conversion then leaves no nidm.ttl of its own, rather than an
                # unmodified or half-written copy of the input that looks like a finished
                # output. An earlier run's nidm.ttl stays (the failure log says so).
                # The input itself is never written, even when it is this nidm.ttl.
                fd, tmp_name = tempfile.mkstemp(prefix=TEMPORARY_PREFIX, suffix=".ttl", dir=nidm_dir)
                os.close(fd)
                copied_nidm = Path(tmp_name)
                pending = copied_nidm
                try:
                    shutil.copyfile(existing_nidm_file, copied_nidm)
                    # mkstemp makes the file private; give it the mode a new file gets
                    umask = os.umask(0)
                    os.umask(umask)
                    os.chmod(copied_nidm, 0o666 & ~umask)
                    logger.info(f"Copied existing NIDM file {existing_nidm_file} to {copied_nidm.name} for conversion")
                except OSError as copy_error:
                    copied_nidm.unlink(missing_ok=True)
                    logger.error(
                        "Failed to copy existing NIDM file to output directory (%s). Error: %s",
                        copied_nidm,
                        copy_error,
                    )
                    logger.error("Cannot proceed without copying to avoid overwriting input file.")
                    sys.exit(1)

            # Build command: use -n for existing NIDM file, -o for new output
            # Note: fs_to_nidm does not allow both -n and -o at the same time
            # -subjid: the recon-all tree is <subject dir>/freesurfer, and fs_to_nidm would
            # otherwise take the subject ID from that directory name ("freesurfer") and
            # attach every measure to a new agent instead of the input NIDM's subject.
            # With an input NIDM, fs_to_nidm compares IDs with a leading "sub-" and
            # leading zeros removed on both sides (sub-0051456 matches 51456). If the
            # input has subjects and none or several match, it fails and writes nothing,
            # even with --forcenidm, which only adds a subject to an input that has none;
            # the subject then fails with fs_to_nidm's message in the log. Without an
            # input NIDM, each stats file gets its own new subject agent with ID
            # "sub-<id>" as given.
            subjid_args = ["-subjid", converter_subject_id(participant_label)]
            if copied_nidm:
                # NOTE: do NOT pass -j here. In fs_to_nidm's append branch the per-stats-file
                # loop accumulates by re-reading the -n file each iteration and writing back to
                # it. With -j the result is written to "<nidm_file>.json" instead, so the next
                # iteration re-reads the *unmodified* -n file and every stats file except the
                # last-globbed one is silently discarded (135 vs 5188 measurements on ABIDE
                # sub-0051456, and nondeterministic because glob order is filesystem-dependent).
                # fs_to_nidm 17f82df and later refuse -j with -s and -n instead.
                cmd = [*converter, "-s", subject_dir, *subjid_args, "-n", str(copied_nidm), "--forcenidm"]
                # A subject can have several anatomical acquisitions in the input (other
                # sessions, acq- variants, repeated runs); name every T1w recon-all used so
                # the stats link to exactly those.
                # (No T1w record is logged above; fs_to_nidm then links only a single
                # anatomical acquisition.)
                if t1_images:
                    cmd += ["-acq_filename", *(os.path.basename(str(p)) for p in t1_images)]
                if verbose:
                    logger.info(f"Found existing NIDM file: {existing_nidm_file}")
                    logger.info(f"Adding data to existing NIDM file: {copied_nidm}")
            else:
                cmd = [*converter, "-s", subject_dir, *subjid_args, "-o", str(nidm_dir), "-j"]
                if nidm_input_dir and verbose:
                    logger.info(f"No NIDM file found in {nidm_input_dir}; creating new output")

            # Log the command for debugging and replay purposes
            logger.info(f"Running command: {shlex.join(cmd)}")

            # Snapshot by modification time, not just name: a re-run (e.g. regenerating
            # NIDM with --skip-freesurfer) rewrites the converter's files under the same
            # names, and those are this run's outputs.
            def _output_state():
                return {path.name: (path.stat().st_mtime_ns, path.stat().st_size) for path in Path(nidm_dir).glob("*")}

            existing_outputs = _output_state()

            env = os.environ.copy()
            # The conversion runs the converter file this process imports.
            env[CONVERTER_FILE_ENV] = os.path.realpath(_import_fs_to_nidm().__file__)
            project_root = Path(__file__).resolve().parents[1]
            existing_pythonpath = env.get("PYTHONPATH")
            pythonpath_entries = [str(project_root), str(project_root / "src"), str(project_root / "src" / "segstats_jsonld")]
            if existing_pythonpath:
                # without empty components: each is the working directory (on
                # Python 3.12 one also stops the interpreter from starting when
                # the working directory has been removed)
                pythonpath_entries += [p for p in existing_pythonpath.split(os.pathsep) if p]
            env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
            env.setdefault("RDFLIB_STORE_NO_BIND_OVERRIDE", "1")
            env.setdefault("SEGSTATS_JSONLD_ALLOW_NEW_KEYS", "1")
            env.pop("PYTHONHOME", None)

            result = subprocess.run(cmd, capture_output=True, text=True, env=env)

            if result.returncode != 0:
                logger.error(f"NIDM conversion failed for {fs_subject_id}")
                logger.error(f"Error output: {result.stderr}")
                if result.stdout:
                    logger.error(f"Command output: {result.stdout}")
                if copied_nidm:
                    copied_nidm.unlink(missing_ok=True)
                _log_no_nidm_written(final_nidm, fs_subject_id)
                sys.exit(1)
            if copied_nidm:
                os.replace(copied_nidm, final_nidm)
                pending = None
                copied_nidm = final_nidm
        finally:
            if pending is not None:
                pending.unlink(missing_ok=True)
    # fs_to_nidm reports a run it completed with reduced provenance (stats left
    # unlinked to an acquisition, a new subject agent under --forcenidm) only on
    # stderr, so every stderr line of a successful run is logged as a warning.
    # A Python warning is logged by its message, without the file:line prefix.
    for line in result.stderr.splitlines():
        line = line.strip()
        if not line:
            continue
        if "UserWarning:" in line:
            line = line.split("UserWarning:", 1)[1].strip()
        logger.warning(f"fs_to_nidm: {line}")

    # Log what files exist in output directory after conversion. These are the
    # intermediate converter outputs; the aggregated per-subject TTL is written
    # further below and logged separately.
    logger.info(f"Intermediate NIDM converter outputs:")
    for file in Path(nidm_dir).glob("*"):
        logger.info(f"  - {file.name} ({file.stat().st_size} bytes)")

    current_outputs = _output_state()
    new_outputs = [Path(nidm_dir) / name for name in sorted(current_outputs)
                   if existing_outputs.get(name) != current_outputs[name]]

    # From here on, every way of not writing nidm.ttl fails the subject: an
    # exit 0 without it is counted as done by a scheduler such as BABS.
    def _fail(reason):
        logger.error(f"NIDM conversion failed for {fs_subject_id}: {reason}")
        if copied_nidm == target_ttl:
            # this run's converted copy, already renamed: not a finished output
            target_ttl.unlink(missing_ok=True)
        _log_no_nidm_written(target_ttl, fs_subject_id)
        sys.exit(1)

    # The product is always <subject_dir>/nidm.ttl.
    target_ttl = Path(nidm_dir) / "nidm.ttl"

    if not new_outputs and not copied_nidm:
        _fail("fs_to_nidm exited 0 but wrote no new output")

    aggregation_sources = []
    if copied_nidm and Path(copied_nidm).exists():
        aggregation_sources.append(Path(copied_nidm))
    elif existing_nidm_file and existing_nidm_file.exists():
        aggregation_sources.append(existing_nidm_file)

    # The FreeSurfer CDE vocabulary (fs_cde.ttl) is a static, deterministic lookup
    # table -- byte-identical for every subject and every run. It is required to
    # interpret the fs_* measurement predicates, but it is written to this same
    # subject directory as its own file, so folding it into nidm.ttl is pure
    # duplication (~2.0 MB of a 2.3 MB output, per subject). Excluding it keeps
    # nidm.ttl at ~250 KB of genuinely subject-specific triples and lets git-annex
    # store the shared vocabulary once. This matches the sibling fsl-nidm BIDSapp,
    # which likewise ships fsl_cde.ttl alongside rather than inside its nidm.ttl.
    # Consumers must load fs_cde.ttl to resolve fs_* terms.
    CDE_FILENAMES = {"fs_cde.ttl"}
    for candidate in new_outputs:
        if candidate.name in CDE_FILENAMES:
            logger.info(
                f"Not merging shared CDE vocabulary into nidm.ttl: {candidate.name} "
                "(shipped alongside it)"
            )
            continue
        if candidate == target_ttl:
            continue
        if candidate.name.startswith(TEMPORARY_PREFIX):
            # another run's temporary copy, e.g. rewritten by a converter
            # orphaned when that run was killed; never part of this output
            logger.warning(f"Not merging temporary file {candidate.name} into nidm.ttl")
            continue
        if candidate.suffix.lower() in {".ttl", ".json", ".jsonld", ".json-ld"}:
            aggregation_sources.append(candidate)

    if not aggregation_sources:
        _fail("fs_to_nidm wrote no NIDM output to merge into nidm.ttl")

    # Append path: fs_to_nidm has already accumulated everything into the copied
    # file, which *is* the target. Re-serializing it would be a no-op round-trip.
    if aggregation_sources == [target_ttl]:
        logger.info(
            f"NIDM output already in place: {target_ttl.name} "
            f"({target_ttl.stat().st_size} bytes)"
        )
        logger.info("================================")
        logger.info(f"NIDM conversion complete for {fs_subject_id}")
        logger.info("================================")
        return

    def _guess_rdf_format(path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".ttl", ".turtle", ".n3"}:
            return "turtle"
        if suffix in {".json", ".jsonld", ".json-ld"}:
            return "json-ld"
        return "xml"

    # A source that cannot be parsed fails the subject rather than being
    # left out: nidm.ttl would silently lack that stats file's measures.
    aggregated_graph = Graph()
    for src in aggregation_sources:
        try:
            aggregated_graph.parse(str(src), format=_guess_rdf_format(src))
        except Exception as parse_error:
            _fail(f"cannot parse its output {src} ({type(parse_error).__name__}: {parse_error})")

    if len(aggregated_graph) == 0:
        _fail(f"its outputs ({', '.join(p.name for p in aggregation_sources)}) hold no triples")

    # Serialize to a temporary file in the subject directory and rename it to
    # nidm.ttl, so a failed or interrupted write (a full disk, SIGTERM) leaves
    # no partial nidm.ttl; the temporary file is removed on any exit.
    fd, tmp_name = tempfile.mkstemp(prefix=TEMPORARY_PREFIX, suffix=".ttl", dir=nidm_dir)
    os.close(fd)
    merged = Path(tmp_name)
    with _exit_on_sigterm():
        try:
            try:
                aggregated_graph.serialize(destination=str(merged), format="turtle")
                umask = os.umask(0)
                os.umask(umask)
                os.chmod(merged, 0o666 & ~umask)
                os.replace(merged, target_ttl)
            except OSError as serialize_error:
                _fail(f"cannot write {target_ttl} ({serialize_error})")
        finally:
            merged.unlink(missing_ok=True)
    logger.info(
        f"Wrote aggregated NIDM output: {target_ttl.name} "
        f"({target_ttl.stat().st_size} bytes, {len(aggregated_graph)} triples) "
        f"from {len(aggregation_sources)} source(s)"
    )

    logger.info("================================")
    logger.info(f"NIDM conversion complete for {fs_subject_id}")
    logger.info("================================")


def process_participant(
    bids_dir,
    output_dir,
    participant_label,
    session_label,
    freesurfer_license,
    skip_bids_validation,
    skip_freesurfer,
    skip_nidm,
    nidm_input_dir,
    verbose,
    max_voxel_size=DEFAULT_MAX_VOXEL_SIZE,
):
    """Process a single participant with FreeSurfer.

    Args:
        bids_dir (str): Path to BIDS root directory
        output_dir (str): Path to output directory
        participant_label (str): Participant label (with or without "sub-" prefix)
        session_label (str or None): Optional session label supplied by BABS or the user
        freesurfer_license (str): Path to FreeSurfer license file
        skip_bids_validation (bool): Skip BIDS validation
        skip_freesurfer (bool): Skip FreeSurfer processing, only run NIDM conversion
        skip_nidm (bool): Skip NIDM export
        nidm_input_dir (str or None): Path to existing NIDM directory
        verbose (bool): Enable verbose output
        max_voxel_size (float): Largest voxel dimension (mm) accepted for an anatomical input
    """
    layout, freesurfer_wrapper, freesurfer_dir, nidm_input_dir, version_info = initialize(
        bids_dir, freesurfer_license, output_dir, skip_bids_validation, verbose, nidm_input_dir,
        max_voxel_size,
    )

    # Strip "sub-" prefix if present (BABS may pass it with the prefix)
    if participant_label.startswith("sub-"):
        participant_label = participant_label[4:]

    # Validate that the subject exists (participant_label is without "sub-" prefix)
    available_subjects = layout.get_subjects()
    if participant_label not in available_subjects:
        logger.error(f"Subject sub-{participant_label} not found in dataset")
        sys.exit(1)

    available_sessions = layout.get_sessions(subject=participant_label)
    detected_session = session_label
    if detected_session and detected_session.startswith("ses-"):
        detected_session = detected_session[4:]

    if detected_session:
        if detected_session not in available_sessions:
            logger.error(f"Session ses-{detected_session} not found for subject sub-{participant_label}")
            sys.exit(1)
        logger.info(f"Using requested session: ses-{detected_session}")
    elif len(available_sessions) == 1:
        # BABS session jobs sparse-check out one session but still invoke the
        # standard BIDS App positional analysis level, "participant".
        detected_session = available_sessions[0]
        logger.info(f"Auto-detected session: ses-{detected_session}")
    elif len(available_sessions) > 1:
        # Refuse to guess: without a session, the wrapper would glob T1w images
        # across every session into a single recon-all run.
        logger.error(
            f"Subject sub-{participant_label} has multiple sessions "
            f"({', '.join('ses-' + s for s in available_sessions)}); "
            "specify one with --session-label"
        )
        sys.exit(1)

    # Add sub- prefix for FreeSurfer subject ID
    fs_subject_id = f"sub-{participant_label}"

    # Run participant analysis
    try:
        # Look the NIDM input up before recon-all: an unfetched annex file, or
        # one fs_to_nidm would refuse because it cannot find this subject in
        # it, is an error, and it should cost seconds, not hours of
        # reconstruction.
        if not skip_nidm:
            check_nidm_input(nidm_input_dir, participant_label, detected_session)

        if skip_freesurfer:
            # Validate that FreeSurfer outputs exist
            fs_output_subject_id = (
                f"{fs_subject_id}_ses-{detected_session}" if detected_session else fs_subject_id
            )
            _session_for_output = detected_session
            # Existing outputs live at <output_dir>/sub-<id>[/ses-<x>]/freesurfer,
            # not in the recon-all staging directory.
            subject_dir = (
                subject_output_dir(output_dir, participant_label, _session_for_output)
                / "freesurfer"
            )
            done_marker = subject_dir / "scripts" / "recon-all.done"

            if not subject_dir.exists():
                logger.error(f"Cannot skip FreeSurfer: subject directory not found at {subject_dir}")
                raise FileNotFoundError(f"FreeSurfer outputs missing for {fs_output_subject_id}")

            if not done_marker.exists():
                logger.error(f"Cannot skip FreeSurfer: recon-all not complete for {fs_output_subject_id}")
                logger.error(f"Expected completion marker: {done_marker}")
                raise FileNotFoundError(f"FreeSurfer processing not complete for {fs_output_subject_id}")

            logger.info("================================")
            logger.info(f"Skipping FreeSurfer processing for {fs_output_subject_id} (--skip-freesurfer)")
            logger.info(f"Using existing outputs at {subject_dir}")
            logger.info("================================")
            info = freesurfer_wrapper.record_subject_images(fs_subject_id, layout, session_label=detected_session)
            # The reconstruction's own inputs must pass --max-voxel-size, as a new
            # run's would; the reason is logged by the wrapper.
            success = not info.get("failure_reason")
        else:
            success = freesurfer_wrapper.process_subject(fs_subject_id, layout, session_label=detected_session)
            # Save processing summary
            summary = freesurfer_wrapper.get_processing_summary()
            summary["version_info"] = version_info
            summary_path = freesurfer_wrapper.save_processing_summary(summary)

            logger.info("================================")
            logger.info("Processing complete!")
            logger.info("================================")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        sys.exit(1)

    if not success:
        # Exit non-zero so a scheduler (e.g. BABS) does not count the job as done.
        # --skip-freesurfer writes no processing summary; the reason is in the log.
        if skip_freesurfer:
            logger.error("Cannot use the existing FreeSurfer outputs; see the error above")
        else:
            logger.error(f"FreeSurfer processing failed; see {summary_path}")
        sys.exit(1)

    if success and not skip_nidm:
        nidm_conversion(
            output_dir,
            participant_label,
            freesurfer_wrapper,
            bids_session=detected_session,
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
    skip_freesurfer,
    skip_nidm,
    nidm_input_dir,
    verbose,
    max_voxel_size=DEFAULT_MAX_VOXEL_SIZE,
):
    """Process a single session for a participant with FreeSurfer.

    Args:
        bids_dir (str): Path to BIDS root directory
        output_dir (str): Path to output directory
        participant_label (str): Participant label (with or without "sub-" prefix)
        session_label (str): Session label (with or without "ses-" prefix)
        freesurfer_license (str): Path to FreeSurfer license file
        skip_bids_validation (bool): Skip BIDS validation
        skip_freesurfer (bool): Skip FreeSurfer processing, only run NIDM conversion
        skip_nidm (bool): Skip NIDM export
        nidm_input_dir (str or None): Path to existing NIDM directory
        verbose (bool): Enable verbose output
        max_voxel_size (float): Largest voxel dimension (mm) accepted for an anatomical input
    """
    layout, freesurfer_wrapper, freesurfer_dir, nidm_input_dir, version_info = initialize(
        bids_dir, freesurfer_license, output_dir, skip_bids_validation, verbose, nidm_input_dir,
        max_voxel_size,
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
        # Look the NIDM input up before recon-all (see process_participant).
        if not skip_nidm:
            check_nidm_input(nidm_input_dir, participant_label, session_label)

        if skip_freesurfer:
            # Validate that session-specific FreeSurfer outputs exist.
            fs_output_subject_id = f"{fs_subject_id}_ses-{session_label}"
            _session_for_output = session_label
            # Existing outputs live at <output_dir>/sub-<id>[/ses-<x>]/freesurfer,
            # not in the recon-all staging directory.
            subject_dir = (
                subject_output_dir(output_dir, participant_label, _session_for_output)
                / "freesurfer"
            )
            done_marker = subject_dir / "scripts" / "recon-all.done"

            if not subject_dir.exists():
                logger.error(f"Cannot skip FreeSurfer: subject directory not found at {subject_dir}")
                raise FileNotFoundError(f"FreeSurfer outputs missing for {fs_output_subject_id}")

            if not done_marker.exists():
                logger.error(f"Cannot skip FreeSurfer: recon-all not complete for {fs_output_subject_id}")
                logger.error(f"Expected completion marker: {done_marker}")
                raise FileNotFoundError(f"FreeSurfer processing not complete for {fs_output_subject_id}")

            logger.info("================================")
            logger.info(f"Skipping FreeSurfer processing for {fs_output_subject_id} (--skip-freesurfer)")
            logger.info(f"Using existing outputs at {subject_dir}")
            logger.info("================================")
            info = freesurfer_wrapper.record_subject_images(fs_subject_id, layout, session_label=session_label)
            # The reconstruction's own inputs must pass --max-voxel-size, as a new
            # run's would; the reason is logged by the wrapper.
            success = not info.get("failure_reason")
        else:
            # Use the enhanced process_subject method with session_label
            success = freesurfer_wrapper.process_subject(fs_subject_id, layout, session_label=session_label)
            # Save processing summary
            summary = freesurfer_wrapper.get_processing_summary()
            summary["version_info"] = version_info
            summary_path = freesurfer_wrapper.save_processing_summary(summary)
            logger.info("================================")
            logger.info("Processing complete!")
            logger.info("================================")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        sys.exit(1)

    if not success:
        # Exit non-zero so a scheduler (e.g. BABS) does not count the job as done.
        # --skip-freesurfer writes no processing summary; the reason is in the log.
        if skip_freesurfer:
            logger.error("Cannot use the existing FreeSurfer outputs; see the error above")
        else:
            logger.error(f"FreeSurfer processing failed; see {summary_path}")
        sys.exit(1)

    if success and not skip_nidm:
        nidm_conversion(
            output_dir,
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
    help=(
        'The session to analyze (with or without "ses-" prefix). '
        'Accepted with both analysis levels for BABS session-wise jobs.'
    ),
)
@click.option(
    "--freesurfer_license",
    "--fs-license-file",
    type=click.Path(exists=True, resolve_path=True),
    help="Path to FreeSurfer license file.",
)
@click.option("--skip-bids-validation", is_flag=True, help="Skip BIDS validation.")
@click.option(
    "--skip-freesurfer",
    is_flag=True,
    help="Skip FreeSurfer processing and run NIDM conversion on existing outputs.",
)
@click.option("--skip_nidm", "--skip-nidm", is_flag=True, help="Skip NIDM output generation.")
@click.option(
    "--nidm-input-dir",
    type=click.Path(exists=False, file_okay=False, resolve_path=True),
    default=None,
    help="Path to existing NIDM files directory. Defaults to <bids_dir>/../NIDM for BABS workflows.",
)
@click.option(
    "--max-voxel-size",
    type=click.FloatRange(min=0, min_open=True),
    default=DEFAULT_MAX_VOXEL_SIZE,
    show_default=True,
    help=(
        "Largest voxel dimension (mm) accepted for a T1w/T2w input. Coarser T1w runs are "
        "dropped (the subject fails if none remain); a coarser T2w is skipped."
    ),
)
@click.option("--verbose", is_flag=True, help="Enable verbose output.")
def cli(
    bids_dir,
    output_dir,
    analysis_level,
    participant_label,
    session_label,
    freesurfer_license,
    skip_bids_validation,
    skip_freesurfer,
    skip_nidm,
    nidm_input_dir,
    max_voxel_size,
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
            session_label,
            freesurfer_license,
            skip_bids_validation,
            skip_freesurfer,
            skip_nidm,
            nidm_input_dir,
            verbose,
            max_voxel_size,
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
            skip_freesurfer,
            skip_nidm,
            nidm_input_dir,
            verbose,
            max_voxel_size,
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

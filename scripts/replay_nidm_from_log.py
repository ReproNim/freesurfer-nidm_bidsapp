#!/usr/bin/env python3
"""
Replay a recorded NIDM conversion command from a FreeSurfer BABS log.

This helper parses the command logged by ``src/run.py`` and re-executes the
conversion against the referenced FreeSurfer outputs using the locally checked-in
``segstats_jsonld`` package.

Example usage:
    python scripts/replay_nidm_from_log.py /path/to/freesurfer.e5728235_3 --forcenidm
"""

import argparse
import importlib
import shlex
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure the JSON-LD serializer is registered with rdflib if available.
try:  # pragma: no cover - optional dependency
    import rdflib
    from rdflib.plugin import Serializer, register
    import rdflib_jsonld  # noqa: F401

    register(
        name="jsonld",
        kind=Serializer,
        module_path="rdflib_jsonld.serializer",
        class_name="JsonLDSerializer",
    )
except ImportError:
    rdflib = None  # optional dependency missing


def _parse_log(log_path: Path) -> List[Dict[str, Optional[str]]]:
    """Collect each ``python -m segstats_jsonld.fs_to_nidm`` command block from the log."""
    entries: List[Dict[str, Optional[str]]] = []
    current: Optional[Dict[str, Optional[str]]] = None

    for raw_line in log_path.read_text().splitlines():
        line = raw_line.strip()
        if "Running command:" in line and "fs_to_nidm" in line:
            if current:
                entries.append(current)
            current = {"command": line.split("Running command:", 1)[1].strip(), "nidm": None}
        elif current and "Adding data to existing NIDM file:" in line:
            marker = "Adding data to existing NIDM file:"
            current["nidm"] = line.split(marker, 1)[1].strip()

    if current:
        entries.append(current)

    # Check if we found any entries; if not, try to reconstruct from log hints
    if not entries:
        log_content = log_path.read_text()
        print("\n" + "="*70)
        print("COMMAND NOT FOUND - ATTEMPTING RECONSTRUCTION")
        print("="*70)
        
        # Try to reconstruct command from log information
        reconstructed = _reconstruct_from_log(log_content)
        if reconstructed:
            print("\nSuccessfully reconstructed command from log hints!")
            entries.append(reconstructed)
        else:
            print("\nNIDM conversion was attempted but cannot reconstruct command.")
            
            # Look for the actual error
            if "fs_to_nidm.py: error:" in log_content:
                print("\nFound fs_to_nidm error in log:")
                for line in log_content.splitlines():
                    if "fs_to_nidm.py: error:" in line:
                        print(f"  {line.strip()}")
            
            if "NIDM conversion failed" in log_content:
                print("\nNIDM conversion failure detected:")
                lines = log_content.splitlines()
                for i, line in enumerate(lines):
                    if "NIDM conversion failed" in line:
                        for j in range(i, min(i+5, len(lines))):
                            print(f"  {lines[j].strip()}")
                        break
            
            print("\nCould not find enough information in log to reconstruct command.")
            print("="*70 + "\n")

    return entries


def _reconstruct_from_log(log_content: str) -> Optional[Dict[str, Optional[str]]]:
    """Attempt to reconstruct fs_to_nidm command from log hints."""
    import re
    
    # Find FreeSurfer subject processing info
    subject_match = re.search(r'Processing (sub-[a-zA-Z0-9_]+)', log_content)
    if not subject_match:
        subject_match = re.search(r'participant-label (sub-[a-zA-Z0-9_]+)', log_content)
    
    # Find the FreeSurfer output directory (SUBJECTS_DIR)
    subjects_dir_match = re.search(r'(/[^\s]+/outputs/freesurfer_bidsapp/freesurfer)', log_content)
    if not subjects_dir_match:
        # Try alternative pattern from singularity binding
        subjects_dir_match = re.search(r'(/[^\s]+/outputs)/freesurfer_bidsapp', log_content)
        if subjects_dir_match:
            subjects_dir = subjects_dir_match.group(1) + "/freesurfer_bidsapp/freesurfer"
        else:
            subjects_dir = None
    else:
        subjects_dir = subjects_dir_match.group(1)
    
    # Find existing NIDM file reference
    nidm_file_match = re.search(r'Adding data to existing NIDM file:\s*([^\s]+)', log_content)
    nidm_file = nidm_file_match.group(1) if nidm_file_match else None
    
    if not nidm_file:
        # Try finding from "Found existing NIDM file"
        nidm_file_match = re.search(r'Found existing NIDM file:\s*([^\s]+)', log_content)
        nidm_file = nidm_file_match.group(1) if nidm_file_match else None
    
    if not subject_match or not subjects_dir:
        print("\nCannot reconstruct: missing subject or FreeSurfer output directory")
        return None
    
    subject_id = subject_match.group(1)
    subject_dir = Path(subjects_dir) / subject_id
    
    # Check if the subject directory exists
    if not subject_dir.exists():
        print(f"\nWarning: Subject directory not found: {subject_dir}")
        print("Attempting to find it...")
        # Try to find it based on log's working directory patterns
        ds_match = re.search(r'(/[^\s]+/ds)/', log_content)
        if ds_match:
            base_ds = Path(ds_match.group(1))
            subject_dir = base_ds / "outputs" / "freesurfer_bidsapp" / "freesurfer" / subject_id
            if subject_dir.exists():
                print(f"Found at: {subject_dir}")
            else:
                print(f"Still not found at: {subject_dir}")
    
    # Determine output directory for NIDM
    if subjects_dir:
        nidm_output_dir = str(Path(subjects_dir).parent / "nidm")
    else:
        nidm_output_dir = "/tmp/nidm_output"
    
    # Build the command
    if nidm_file:
        # Use -n mode (merge with existing NIDM)
        cmd = f"python -m segstats_jsonld.fs_to_nidm -s {subject_dir} -n {nidm_file} -j --forcenidm"
    else:
        # Use -o mode (create new NIDM)
        cmd = f"python -m segstats_jsonld.fs_to_nidm -s {subject_dir} -o {nidm_output_dir} -j"
    
    print(f"\nReconstructed information:")
    print(f"  Subject: {subject_id}")
    print(f"  Subject directory: {subject_dir}")
    if nidm_file:
        print(f"  Existing NIDM file: {nidm_file}")
    print(f"  Output directory: {nidm_output_dir}")
    
    return {"command": cmd, "nidm": nidm_file}


def _split_command(command: str) -> Tuple[str, List[str]]:
    """Split the recorded command into module path and arguments."""
    tokens = shlex.split(command)
    try:
        idx = tokens.index("-m")
    except ValueError as exc:
        raise ValueError("Command does not include a '-m' module invocation") from exc

    try:
        module = tokens[idx + 1]
    except IndexError as exc:
        raise ValueError("Module name missing after '-m'") from exc

    return module, tokens[idx + 2 :]


def _sanitize_args(args: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """Drop unsupported flags and return cleaned args."""
    cleaned: List[str] = []
    removed: Dict[str, str] = {}
    skip_next = False

    for idx, token in enumerate(args):
        if skip_next:
            skip_next = False
            continue

        # Remove -j/--jsonld to default to TTL output if desired
        if token in {"-j", "--jsonld"}:
            removed[token] = ""
            continue

        cleaned.append(token)

    return cleaned, removed


def _ensure_pythonpath(script_path: Path) -> None:
    """Include the repo's ``src`` tree on sys.path so imports succeed."""
    repo_root = script_path.resolve().parents[1]
    src_root = repo_root / "src"

    for candidate in (src_root, repo_root):
        path_str = str(candidate)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


def _import_module(module: str):
    """Import the module containing ``main`` for execution."""
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as exc:
        raise ImportError(
            f"Unable to import '{module}'. Ensure the repo is on PYTHONPATH or the "
            "package is installed (pip install -e .)."
        ) from exc


def _run_module(module_name: str, argv: List[str]) -> int:
    """Invoke ``main`` from the targeted module, returning its exit code."""
    import os
    module = _import_module(module_name)
    if not hasattr(module, "main"):
        raise AttributeError(f"Module '{module_name}' does not expose a 'main' callable")

    saved_argv = sys.argv
    # Set environment variable to allow new keys in FreeSurfer stats
    os.environ.setdefault("SEGSTATS_JSONLD_ALLOW_NEW_KEYS", "1")
    sys.argv = [module_name.split(".")[-1]] + argv
    try:
        module.main()  # type: ignore[attr-defined]
    except ValueError as exc:
        # Catch errors and report them for debugging
        error_msg = str(exc)
        print(f"\n{'='*70}")
        print("ERROR DETECTED:")
        print(f"{'='*70}")
        print(error_msg)
        print(f"{'='*70}\n")
        return 1
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        return code
    finally:
        sys.argv = saved_argv

    return 0


def _summarize_inputs(args: List[str]) -> Dict[str, List[Path]]:
    """Collect referenced filesystem inputs for a quick existence check."""
    summary: Dict[str, List[Path]] = {}
    idx = 0
    while idx < len(args):
        token = args[idx]
        if token in {"-s", "--subjects_dir"} and idx + 1 < len(args):
            summary.setdefault("subjects_dir", []).append(Path(args[idx + 1]))
            idx += 1
        elif token in {"-o", "--output_dir"} and idx + 1 < len(args):
            summary.setdefault("output_dir", []).append(Path(args[idx + 1]))
            idx += 1
        elif token in {"-n", "--nidm_file"} and idx + 1 < len(args):
            summary.setdefault("nidm", []).append(Path(args[idx + 1]))
            idx += 1
        elif token in {"-t1", "--t1"} and idx + 1 < len(args):
            summary.setdefault("t1", []).append(Path(args[idx + 1]))
            idx += 1
        elif token in {"-t2", "--t2"} and idx + 1 < len(args):
            summary.setdefault("t2", []).append(Path(args[idx + 1]))
            idx += 1
        idx += 1
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay a logged fs_to_nidm invocation against existing FreeSurfer outputs.",
    )
    parser.add_argument("log_path", type=Path, help="Path to the FreeSurfer log file")
    parser.add_argument(
        "--occurrence",
        type=int,
        default=1,
        help="Which occurrence of the command to run (1 = first)",
    )
    parser.add_argument(
        "--module",
        type=str,
        default=None,
        help="Override the module path to execute instead of the one in the log",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the reconstructed command without running it",
    )
    parser.add_argument(
        "--forcenidm",
        action="store_true",
        help="Append --forcenidm to the replayed command if it is not already present",
    )

    args = parser.parse_args()
    entries = _parse_log(args.log_path)
    if not entries:
        raise SystemExit("No fs_to_nidm commands found in the provided log.")

    if args.occurrence < 1 or args.occurrence > len(entries):
        raise SystemExit(
            f"Occurrence {args.occurrence} is out of range; found {len(entries)} commands."
        )

    entry = entries[args.occurrence - 1]
    module_name, module_args = _split_command(entry["command"])
    replay_module = args.module or module_name

    sanitized_args, removed = _sanitize_args(module_args)
    
    # If we extracted a NIDM file path from the log, copy it to output directory first
    # to avoid overwriting the input file
    if entry["nidm"]:
        import shutil
        
        input_nidm = Path(entry["nidm"])
        if input_nidm.exists():
            # Determine output directory from subject directory
            subject_dir_idx = None
            for i, arg in enumerate(sanitized_args):
                if arg in ["-s", "--subjects_dir"] and i + 1 < len(sanitized_args):
                    subject_dir = Path(sanitized_args[i + 1])
                    # Construct output NIDM directory based on FreeSurfer subject structure
                    output_nidm_dir = subject_dir.parent.parent / "nidm"
                    output_nidm_dir.mkdir(parents=True, exist_ok=True)
                    output_nidm_file = output_nidm_dir / input_nidm.name
                    
                    # Copy input NIDM to output directory
                    if input_nidm.resolve() != output_nidm_file.resolve():
                        shutil.copy2(input_nidm, output_nidm_file)
                        print(f"Copied input NIDM to output directory: {output_nidm_file}")
                    
                    # Update the -n argument to point to the copied file
                    if "--nidm_file" in sanitized_args:
                        idx = sanitized_args.index("--nidm_file")
                        if idx + 1 < len(sanitized_args):
                            sanitized_args[idx + 1] = str(output_nidm_file)
                        else:
                            sanitized_args.append(str(output_nidm_file))
                    elif "-n" in sanitized_args:
                        idx = sanitized_args.index("-n")
                        if idx + 1 < len(sanitized_args):
                            sanitized_args[idx + 1] = str(output_nidm_file)
                        else:
                            sanitized_args.append(str(output_nidm_file))
                    else:
                        sanitized_args.extend(["-n", str(output_nidm_file)])
                    break
        else:
            print(f"Warning: Input NIDM file not found: {input_nidm}")

    if args.forcenidm and "--forcenidm" not in sanitized_args and "-forcenidm" not in sanitized_args:
        sanitized_args.append("--forcenidm")

    command_preview = " ".join(shlex.quote(part) for part in sanitized_args)
    print(f"Resolved module: {replay_module}")
    print(f"Arguments: {command_preview}")
    if removed:
        print("Removed unsupported options:")
        for flag, value in removed.items():
            if value:
                print(f"  {flag} {value}")
            else:
                print(f"  {flag}")

    summary = _summarize_inputs(sanitized_args)
    for label, paths in summary.items():
        for path in paths:
            status = "OK" if path.exists() else "MISSING"
            print(f"{label.upper()}: {path} [{status}]")

    if args.dry_run:
        return 0

    _ensure_pythonpath(Path(__file__))
    try:
        exit_code = _run_module(replay_module, sanitized_args)
    except (ImportError, AttributeError) as exc:
        print(str(exc))
        return 1
    if exit_code == 0:
        print("NIDM conversion finished successfully.")
    else:
        print(f"NIDM conversion exited with code {exit_code}.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

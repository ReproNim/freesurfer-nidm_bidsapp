import json
import subprocess
import sys
from pathlib import Path

from setuptools import find_packages, setup


def get_version_from_file():
    """Get version from VERSION file."""
    version_path = Path(__file__).parent / "VERSION"
    if version_path.exists():
        with open(version_path) as f:
            data = json.load(f)
            return data.get("freesurfer-nidm_bidsapp", {}).get("version", "0.1.0")
    return "0.1.0"


__version__ = get_version_from_file()


def build_docker():
    """Build Docker container"""
    print("Building Docker image...")
    try:
        subprocess.run(["docker", "build", "-t", "freesurfer-nidm", "."], check=True)
        print("Docker image built successfully")
    except subprocess.CalledProcessError as e:
        print(f"Docker build failed: {e}")
        return False
    return True


def build_singularity(output_path=None):
    """Build Singularity/Apptainer container"""
    print("Building container image...")
    try:
        # Check for apptainer first (more common on clusters), then singularity
        if (
            subprocess.run(["which", "apptainer"], capture_output=True).returncode == 0
        ):
            print("\nDetected Apptainer on cluster environment.")
            print("For cluster environments, please build directly with apptainer:")
            print("\napptainer build --remote freesurfer.sif Singularity")
            print("or")
            print("apptainer build --fakeroot freesurfer.sif Singularity\n")
            return False
        elif (
            subprocess.run(["which", "singularity"], capture_output=True).returncode == 0
        ):
            container_cmd = "singularity"
        else:
            print("Neither apptainer nor singularity found. Cannot build image.")
            return False

        # Use custom output path if provided, otherwise use default
        output_file = output_path if output_path else "freesurfer.sif"
        output_file = str(Path(output_file).resolve())
        
        # Build command
        cmd = [container_cmd, "build"]
        
        # For regular Singularity installations, try fakeroot if available
        if subprocess.run(["which", "fakeroot"], capture_output=True).returncode == 0:
            cmd.append("--fakeroot")
        
        # Add output file and Singularity definition
        cmd.extend([output_file, "Singularity"])
        
        print(f"Running command: {' '.join(cmd)}")
        
        # Run the build command
        subprocess.run(cmd, check=True)
        print(f"Container image built successfully at: {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Build failed: {e}")
        print("\nFor cluster environments, please build directly with apptainer:")
        print("apptainer build --remote freesurfer.sif Singularity")
        print("or")
        print("apptainer build --fakeroot freesurfer.sif Singularity")
        return False


def init_git_submodules():
    """Initialize git submodules if .git directory exists and --init-git flag is set"""
    if "--init-git" in sys.argv:
        if Path(".git").exists():
            print("Initializing git submodules...")
            try:
                subprocess.run(["git", "submodule", "update", "--init", "--recursive"], check=True)
                print("Git submodules initialized successfully")
            except subprocess.CalledProcessError as e:
                print(f"Git submodule initialization failed: {e}")
                print("Continuing without git submodules...")
        else:
            print("No .git directory found, skipping git submodule initialization")
    else:
        print("Skipping git submodule initialization (use --init-git to enable)")


# Handle dependency conflicts by defining dependencies with proper constraints
install_requires = [
    "click>=8.0.0",
    "pybids>=0.15.1",
    "nipype>=1.8.5",
    "nibabel>=5.0.0",
    "numpy>=1.20.0",
    "pandas>=1.3.0",
    "pytest>=7.0.0",
    "rdflib>=6.3.2",
]

# Check if we're being called with a container build command
if len(sys.argv) > 1 and sys.argv[1] in ["docker", "singularity", "containers"]:
    command = sys.argv[1]
    # Remove the custom argument so setup() doesn't see it
    sys.argv.pop(1)

    if command == "docker":
        build_docker()
    elif command == "singularity":
        # Check for custom output path in the next argument
        output_path = None
        if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
            output_path = sys.argv.pop(1)
        build_singularity(output_path)
    elif command == "containers":
        build_docker()
        build_singularity()

    # Exit if we were just building containers
    if len(sys.argv) == 1:
        sys.exit(0)

# Initialize git submodules only if explicitly requested
init_git_submodules()

setup(
    name="freesurfer-nidm",
    version=__version__,
    description="BIDS App for FreeSurfer with NIDM Output",
    author="ReproNim",
    author_email="repronim@gmail.com",
    packages=find_packages(),
    include_package_data=True,
    license="MIT",
    url="https://github.com/sensein/freesurfer_bidsapp",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    entry_points={
        "console_scripts": [
            "freesurfer-nidm=src.run:cli",
        ],
    },
    python_requires=">=3.9",
    install_requires=install_requires,
)

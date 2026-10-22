Bootstrap: docker
From: vnmd/freesurfer_8.0.0

# The commit this image was built from, read back with
# `apptainer inspect --labels <image>`. Pass it at build time:
#   apptainer build --build-arg GIT_COMMIT=$(git rev-parse HEAD) ...
# Without --build-arg the label reads "unknown".
# org.opencontainers.image.version is this app's version. It is written in
# %post, not %labels: without --force, %labels does not replace a label the
# base image already sets, so the base's own (e.g. Ubuntu's 22.04) would
# show through.
# tests/test_version.py checks it against the app version.
%arguments
    GIT_COMMIT=unknown

%labels
    org.opencontainers.image.revision {{ GIT_COMMIT }}
    org.opencontainers.image.source https://github.com/ReproNim/freesurfer-nidm_bidsapp

%files
    ./src /opt/src
    ./requirements.txt /opt/requirements.txt
    ./setup.py /opt/setup.py
    ./setup.cfg /opt/setup.cfg
    ./VERSION /opt/VERSION

%post
    # Fail the build on the first failing command. %post is one scriptlet whose
    # exit status is its last command's, so without this a failed converter or
    # requirements install is hidden by whatever runs after it.
    set -e

    # Unless the build uses --force, Apptainer keeps an inherited label over the
    # same key in %labels (it warns "already exists ... not overwriting"); a
    # label written here replaces it either way. SingularityCE sets only
    # SINGULARITY_LABELS, hence the fallback.
    echo "org.opencontainers.image.version 0.1.2" >> "${APPTAINER_LABELS:-$SINGULARITY_LABELS}"

    # Create opt directory for application code
    mkdir -p /opt

    # %files keeps each file's mode from the checkout it was copied from, and a
    # SIF keeps the builder's uid, so a file that is mode 600 there would be
    # unreadable for every other account running the image (this happened to
    # the mriqc-nidm 0.1.0 image). a+rX: files readable, directories
    # traversable, nothing made executable.
    chmod -R a+rX /opt/src /opt/requirements.txt /opt/setup.py /opt/setup.cfg /opt/VERSION
    
    # Install system dependencies
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3-dev \
        python3-pip \
        git \
        && apt-get clean && \
        rm -rf /var/lib/apt/lists/*
    
    # Install Python dependencies
    cd /opt
    
    # Install dependencies in the same order as Dockerfile
    python3 -m pip install --upgrade pip setuptools wheel
    
    cd src/segstats_jsonld
    python3 -m pip install -e .
    
    cd /opt
    python3 -m pip install -r requirements.txt
    python3 -m pip install --no-cache-dir --upgrade 'rdflib>=7.0.0,<8'
    python3 -m pip install -e .

    # Last step: everything this recipe put under /opt must be usable by
    # whoever runs the image, not only by the account that built it.
    unreadable="$(find /opt/src /opt/requirements.txt /opt/setup.py /opt/setup.cfg /opt/VERSION \( -type f ! -perm -o=r \) -o \( -type d ! -perm -o=rx \))"
    if [ -n "$unreadable" ]; then
        echo "ERROR: not readable by other users (the image only works for its builder):" >&2
        echo "$unreadable" >&2
        exit 1
    fi

%environment
    # Set runtime license path to match BABS mount point
    export FS_LICENSE=/SGLR/FREESURFER_HOME/license.txt
    # Add opt to Python path
    export PYTHONPATH=/opt:$PYTHONPATH
    # Add Python packages to path
    export PATH=/usr/local/bin:$PATH

%runscript
    # Execute the Python entry point directly
    python3 /opt/src/run.py "$@"

%help
    FreeSurfer BIDS App with NIDM Output

    This container is designed to work with BABS.

    For version information, run:
      singularity run [container] --version

    For full usage information, run:
      singularity run [container] --help

    Usage:
      singularity run -B [workdir] -B [license.txt]:/SGLR/FREESURFER_HOME/license.txt [container] [input_dir] [output_dir] participant --fs-license-file /SGLR/FREESURFER_HOME/license.txt [options]

    Example:
      singularity run -B $PWD -B license.txt:/SGLR/FREESURFER_HOME/license.txt freesurfer-nidm.sif $PWD/inputs/data/BIDS $PWD/outputs/freesurfer participant --fs-license-file /SGLR/FREESURFER_HOME/license.txt --skip-bids-validation

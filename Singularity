Bootstrap: docker
From: vnmd/freesurfer_8.0.0

# The commit this image was built from, read back with
# `apptainer inspect --labels <image>`. Pass it at build time:
#   apptainer build --build-arg GIT_COMMIT=$(git rev-parse HEAD) ...
# Without --build-arg the label reads "unknown".
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
    # Create opt directory for application code
    mkdir -p /opt
    
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

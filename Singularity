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
    echo "org.opencontainers.image.version 0.1.4" >> "${APPTAINER_LABELS:-$SINGULARITY_LABELS}"

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

    # The runscript starts the console script. Check that it exists and that,
    # with its directory first on sys.path and PYTHONPATH as %environment sets
    # it with no host value (as at run time), the NIDM input check's import
    # resolves to the installed converter through the editable install: the
    # module file and the real package's __init__.py, not a namespace package
    # rooted at the checkout. Exit non-zero otherwise (sys.exit, not assert:
    # python3 -O would drop an assert).
    command -v freesurfer-nidm
    PYTHONPATH=/opt python3 -c "import os, shutil, sys; sys.path[0] = os.path.dirname(shutil.which('freesurfer-nidm')); from segstats_jsonld.fs_to_nidm import SubjectLookupError, find_subject; import segstats_jsonld as p, segstats_jsonld.fs_to_nidm as m; (m.__file__ != '/opt/src/segstats_jsonld/segstats_jsonld/fs_to_nidm.py' or p.__file__ != '/opt/src/segstats_jsonld/segstats_jsonld/__init__.py') and sys.exit('freesurfer-nidm would import the converter from %s (package %s)' % (m.__file__, p.__file__)); print('freesurfer-nidm resolves the converter lookup from', m.__file__)"
    # PyNIDM: the converter silently uses PyNIDM 4.x conventions when
    # nidm.linkml is absent, so check the version requirements.txt pins
    # and that nidm imports from the image's site-packages.
    PYTHONPATH=/opt python3 -c "import importlib.metadata, os, shutil, site, sys; sys.path[0] = os.path.dirname(shutil.which('freesurfer-nidm')); import nidm; v = importlib.metadata.version('pynidm'); f = os.path.realpath(nidm.__file__); (v != '5.0.0' or os.path.dirname(os.path.dirname(f)) not in [os.path.realpath(p) for p in site.getsitepackages()]) and sys.exit('freesurfer-nidm would use PyNIDM %s from %s, not 5.0.0 from the image site-packages %s' % (v, f, site.getsitepackages())); print('freesurfer-nidm uses PyNIDM', v, 'from', f)"
    PYTHONPATH=/opt freesurfer-nidm --version

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
    # Add /opt to the Python path, with no empty entry when the host sets no
    # PYTHONPATH (Python reads an empty entry as the working directory, where
    # a converter checkout would shadow the installed converter). A host
    # PYTHONPATH, which Apptainer passes in unless the run uses --cleanenv, is
    # kept after /opt as given, so one with an empty component (`/x:`) still
    # puts the working directory on the app's sys.path. The app drops the
    # working directory from sys.path only around its converter import and in
    # the conversion's own interpreter (src/run.py, CONVERTER_BOOTSTRAP).
    export PYTHONPATH=/opt${PYTHONPATH:+:$PYTHONPATH}
    # Add Python packages to path
    export PATH=/usr/local/bin:$PATH

%runscript
    # Start the app through its console script, as the Dockerfile's entrypoint
    # does. Not `python3 /opt/src/run.py`: that puts /opt/src first on
    # sys.path, so the converter checkout /opt/src/segstats_jsonld (no
    # __init__.py) shadows the installed package as a namespace package, and
    # any converter submodule named like an entry of that checkout (tests,
    # examples, segstats_jsonld) would load the wrong file.
    exec freesurfer-nidm "$@"

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

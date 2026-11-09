FROM vnmd/freesurfer_8.0.0

# Install additional Python dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-setuptools \
    git \
    build-essential \
    python3-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# =======================================
# Environment Configuration
# =======================================
# Make license path match BABS mount point
ENV FS_LICENSE=/SGLR/FREESURFER_HOME/license.txt
# Only /opt: the base image sets no PYTHONPATH, and `/opt:$PYTHONPATH` would
# end in an empty entry, which Python reads as the working directory. This
# covers the image's own setting only; a PYTHONPATH given with `docker run -e`
# replaces it. The app drops the working directory from sys.path only around
# its converter import and in the conversion (src/run.py, CONVERTER_BOOTSTRAP).
ENV PYTHONPATH=/opt
ENV PATH=/usr/local/bin:$PATH

# =======================================
# BIDS App Setup
# =======================================
# Copy application files to a location that won't conflict. COPY keeps the
# build context's file modes, so a file that is mode 600 in the checkout would
# be unreadable for a non-root --user; make the app's files readable by everyone.
COPY . /opt/
RUN chmod -R a+rX /opt/src /opt/requirements.txt /opt/setup.py /opt/setup.cfg /opt/VERSION

# Install Python dependencies and submodules
WORKDIR /opt
RUN python3 -m pip install --upgrade pip setuptools wheel && \
    cd src/segstats_jsonld && \
    python3 -m pip install -e . && \
    cd /opt && \
    python3 -m pip install -r requirements.txt && \
    python3 -m pip install --no-cache-dir --upgrade 'rdflib>=7.0.0,<8' && \
    python3 -m pip install -e .

# The entrypoint is the console script. Fail the build if it is missing or if,
# with its directory first on sys.path and PYTHONPATH=/opt (as at run time),
# the NIDM input check's import does not resolve to the installed converter
# through the editable install (module file and the real package, not a
# namespace package rooted at the checkout). The same checks run in
# Singularity's %post.
RUN command -v freesurfer-nidm && \
    python3 -c "import os, shutil, sys; sys.path[0] = os.path.dirname(shutil.which('freesurfer-nidm')); from segstats_jsonld.fs_to_nidm import SubjectLookupError, find_subject; import segstats_jsonld as p, segstats_jsonld.fs_to_nidm as m; (m.__file__ != '/opt/src/segstats_jsonld/segstats_jsonld/fs_to_nidm.py' or p.__file__ != '/opt/src/segstats_jsonld/segstats_jsonld/__init__.py') and sys.exit('freesurfer-nidm would import the converter from %s (package %s)' % (m.__file__, p.__file__)); print('freesurfer-nidm resolves the converter lookup from', m.__file__)" && \
    freesurfer-nidm --version

# Everything the app reads must be usable by a non-root --user, the same check
# Singularity runs at the end of %post.
RUN unreadable="$(find /opt/src /opt/requirements.txt /opt/setup.py /opt/setup.cfg /opt/VERSION \( -type f ! -perm -o=r \) -o \( -type d ! -perm -o=rx \))" && \
    if [ -n "$unreadable" ]; then \
        echo "ERROR: not readable by other users:" >&2; \
        echo "$unreadable" >&2; \
        exit 1; \
    fi

# =======================================
# Runtime Configuration
# =======================================
# Entrypoint that expects input/output paths as arguments: the console script,
# not `python3 /opt/src/run.py` (which puts /opt/src first on sys.path, where
# the converter checkout shadows the installed package).
ENTRYPOINT ["freesurfer-nidm"]

# The commit this image was built from (docker build --build-arg
# GIT_COMMIT=$(git rev-parse HEAD) ...). Declared last so that a new commit
# does not invalidate the cached layers above.
ARG GIT_COMMIT=unknown
# The version label overrides the base image's own (e.g. Ubuntu's 22.04).
LABEL org.opencontainers.image.version=0.1.4 \
      org.opencontainers.image.revision=$GIT_COMMIT \
      org.opencontainers.image.source=https://github.com/ReproNim/freesurfer-nidm_bidsapp

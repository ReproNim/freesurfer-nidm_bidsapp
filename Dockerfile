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
ENV PYTHONPATH=/opt:$PYTHONPATH
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
# Entrypoint that expects input/output paths as arguments
ENTRYPOINT ["python3", "/opt/src/run.py"]

# The commit this image was built from (docker build --build-arg
# GIT_COMMIT=$(git rev-parse HEAD) ...). Declared last so that a new commit
# does not invalidate the cached layers above.
ARG GIT_COMMIT=unknown
LABEL org.opencontainers.image.revision=$GIT_COMMIT \
      org.opencontainers.image.source=https://github.com/ReproNim/freesurfer-nidm_bidsapp

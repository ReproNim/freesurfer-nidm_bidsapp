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
# Copy application files to a location that won't conflict
COPY . /opt/

# Install Python dependencies and submodules
WORKDIR /opt
RUN python3 -m pip install --upgrade pip setuptools wheel && \
    cd src/segstats_jsonld && \
    python3 -m pip install -e . && \
    cd /opt && \
    python3 -m pip install -r requirements.txt && \
    python3 -m pip install --no-cache-dir --upgrade 'rdflib>=7.0.0,<8' && \
    python3 -m pip install -e .

# =======================================
# Runtime Configuration
# =======================================
# Entrypoint that expects input/output paths as arguments
ENTRYPOINT ["python3", "/opt/src/run.py"]

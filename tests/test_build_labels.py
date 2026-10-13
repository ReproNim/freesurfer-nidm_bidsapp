"""The container recipes stamp the commit they were built from."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = (ROOT / "Dockerfile").read_text()
SINGULARITY = (ROOT / "Singularity").read_text()


def test_images_record_their_commit():
    """Both recipes stamp org.opencontainers.image.revision from GIT_COMMIT,
    defaulting to "unknown" so a build without --build-arg says so."""
    assert re.search(r"^%arguments\n\s+GIT_COMMIT=unknown$", SINGULARITY, re.M)
    assert re.search(
        r"^\s+org\.opencontainers\.image\.revision \{\{ GIT_COMMIT \}\}$",
        SINGULARITY,
        re.M,
    )
    assert re.search(r"^ARG GIT_COMMIT=unknown$", DOCKERFILE, re.M)
    assert "org.opencontainers.image.revision=$GIT_COMMIT" in DOCKERFILE


def test_docker_commit_label_comes_after_every_build_step():
    """An ARG used by a later RUN invalidates the cache from that point on every
    new commit, so the label must follow the last RUN."""
    assert DOCKERFILE.index("ARG GIT_COMMIT") > DOCKERFILE.rindex("\nRUN ")

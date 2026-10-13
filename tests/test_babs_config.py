"""Regression checks for the documented BABS PR #369 configuration."""

import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
APP_VERSION = json.loads((ROOT / "VERSION").read_text())["freesurfer-nidm"]["version"]


def test_bids_study_config_declares_new_babs_paths():
    config_path = Path(__file__).parents[1] / "examples" / "babs-freesurfer-nidm-bids-study.yaml"
    config = config_path.read_text()

    assert 'analysis_path: "."' in config
    assert 'input_ria_path: ".babs/input_ria"' in config
    assert 'output_ria_path: ".babs/output_ria"' in config
    assert '$SUBJECT_SELECTION_FLAG: "--participant-label"' in config
    assert '# $SESSION_SELECTION_FLAG: "--session-label"' in config
    # Per-subject layout: zip the subject folder, under this app's version.
    # An app-name entry here zips a folder the app no longer writes.
    assert f'    ${{subid}}: "{APP_VERSION.replace(".", "-")}"' in config
    assert "freesurfer-nidm_bidsapp:" not in config

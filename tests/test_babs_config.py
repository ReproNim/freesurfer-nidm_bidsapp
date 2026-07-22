"""Regression checks for the documented BABS PR #369 configuration."""

from pathlib import Path


def test_bids_study_config_declares_new_babs_paths():
    config_path = Path(__file__).parents[1] / "examples" / "babs-freesurfer-nidm-bids-study.yaml"
    config = config_path.read_text()

    assert 'analysis_path: "."' in config
    assert 'input_ria_path: ".babs/input_ria"' in config
    assert 'output_ria_path: ".babs/output_ria"' in config
    assert '$SUBJECT_SELECTION_FLAG: "--participant-label"' in config
    assert '# $SESSION_SELECTION_FLAG: "--session-label"' in config
    assert 'freesurfer-nidm_bidsapp: "0-1-0"' in config

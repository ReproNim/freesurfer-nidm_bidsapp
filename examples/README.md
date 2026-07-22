# BABS examples

`babs-freesurfer-nidm-bids-study.yaml` targets the configurable BIDS-study
layout introduced by PennLINC/babs PR #369. In this layout, the BABS analysis
DataLad dataset is the project directory itself and internal RIA stores live
under `.babs/`.

The paths for the input BIDS dataset, FreeSurfer license, compute space, and
site-specific module setup are placeholders and must be updated before use.
For a session-level BABS project, uncomment `$SESSION_SELECTION_FLAG`; leave it
commented for subject-level projects because BABS only defines `$sesid` in
session-wise job scripts.
The same container remains compatible with BABS's legacy layout: omit
`analysis_path`, `input_ria_path`, and `output_ria_path` to retain the default
`analysis/`, `input_ria/`, and `output_ria/` directories.

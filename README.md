# BIDS FreeSurfer App

A BIDS App implementation for FreeSurfer 8.0.0 that provides standardized surface reconstruction and morphometric analysis with NIDM output.

## Description

This BIDS App runs **only** FreeSurfer's `recon-all` pipeline on structural T1w and (optionally) T2w images from a BIDS-valid dataset. It organizes outputs in a BIDS-compliant derivatives structure and provides additional NIDM format outputs for improved interoperability.

**Important Note:** This app is designed to run the complete `recon-all` cortical reconstruction pipeline. It does not provide access to other FreeSurfer command-line tools (e.g., `tkmedit`, `tksurfer`, `mri_vol2vol`, etc.). For interactive FreeSurfer tools or additional analyses, you will need a full FreeSurfer installation.

The app implements:
1. Automatic identification and processing of T1w images (required)
2. Utilization of T2w images when available (optional)
3. Multi-session data handling with appropriate processing paths
4. NIDM format output generation for standardized data exchange
5. BIDS provenance documentation for reproducibility
6. Comprehensive version tracking for all components (FreeSurfer, BIDS app, Python packages)

## Installation

### Requirements

- Docker (for containerized execution)
- FreeSurfer license file (obtainable from https://surfer.nmr.mgh.harvard.edu/registration.html)

### Quick Installation

Clone the repository (including submodules) using SSH:

```bash
git clone --recursive git@github.com:ReproNim/freesurfer-nidm_bidsapp.git
cd freesurfer-nidm_bidsapp
```

> **Note:** The `--recursive` flag is required to also clone the git submodules used by this project (e.g., `src/segstats_jsonld`).

## Container Support

This BIDS App supports both Docker and Singularity/Apptainer, allowing you to run the application in various environments including HPC clusters.

## Building Containers

```bash
# Build Docker image (for local development)
python setup.py docker

# Build Singularity/Apptainer image (e.g., on a cluster)
apptainer build --fakeroot --build-arg GIT_COMMIT=$(git rev-parse HEAD) freesurfer-nidm-bidsapp.sif Singularity

# Or specify a custom output location
apptainer build --fakeroot --build-arg GIT_COMMIT=$(git rev-parse HEAD) /path/to/output/freesurfer-nidm-bidsapp.sif Singularity
```

> **Note:** The `--fakeroot` option is recommended for cluster environments because it avoids permission issues on shared systems and does not require root privileges. If you encounter issues, contact your system administrator to ensure your user is configured for fakeroot, and make sure you have write permissions in the build directory.

## Docker Usage

```bash
# Run the container
docker run -v /path/to/license.txt:/license.txt \
  -v /path/to/bids/data:/data \
  -v /path/to/output:/output \
  freesurfer-nidm-bidsapp \
  /data /output participant \
  --participant-label 01 \
  --fs-license-file /license.txt
```

## Singularity/Apptainer Usage

```bash
# Run the container
apptainer run \
  --bind /path/to/license.txt:/license.txt,/path/to/bids/data:/data,/path/to/output:/output \
  /path/to/freesurfer-nidm-bidsapp.sif \
  /data /output participant \
  --participant-label 01 \
  --fs-license-file /license.txt
```

Note: The application files are included in the container image, so there's no need to bind the repository directory. Only the license file, input data, and output directory need to be bound.

## HPC/Cluster Usage

After building the Singularity/Apptainer image (see [Building Containers](#building-containers)), create a job submission script like this:

```bash
#!/bin/bash
#SBATCH --job-name=freesurfer
#SBATCH --output=freesurfer_%j.out
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G

# Path to the Singularity image
SIF_FILE=/path/to/freesurfer.sif
# Path to license file
LICENSE_FILE=/path/to/license.txt
# Input and output paths
BIDS_DIR=/path/to/bids/data
OUTPUT_DIR=/path/to/output

apptainer run \
  --bind $LICENSE_FILE:/license.txt,$BIDS_DIR:/data,$OUTPUT_DIR:/output \
  $SIF_FILE \
  /data /output participant \
  --participant-label sub-01 \
  --fs-license-file /license.txt
```

### Command-Line Arguments

- Positional arguments:
  - `bids_dir`: The directory with the input BIDS dataset
  - `output_dir`: The directory where the output files should be stored
  - `analysis_level`: Processing level. Choices are `participant` and `session`.

- Optional arguments:
  - `--participant-label`: One participant label, with or without the `sub-` prefix.
  - `--session-label`: One session label, with or without the `ses-` prefix. It is accepted in participant mode for BABS session-wise jobs.
  - `--fs-license-file`: Path to the FreeSurfer license file.
  - `--skip-bids-validation`: Skip PyBIDS validation.
  - `--skip-freesurfer`: Reuse a completed `recon-all` directory and run only the remaining export steps. The directory is checked against the images it was made from; see [Reusing an existing reconstruction](#reusing-an-existing-reconstruction).
  - `--skip-nidm`: Skip NIDM output generation.
  - `--nidm-input-dir`: Directory containing existing NIDM documents to augment. Defaults to `<bids_dir>/../NIDM` (the standard BABS mount point). Per subject the app searches most-specific first: `sub-<id>/ses-<session>/nidm.ttl` and `sub-<id>_ses-<session>/nidm.ttl` (session-wise runs), then `sub-<id>/nidm.ttl`, then a root-level `nidm.ttl`, then any top-level `.ttl`/`.jsonld` file. Missing input NIDM is not an error — the app then builds `nidm.ttl` from scratch.
  - `--max-voxel-size`: Largest voxel dimension in mm accepted for a T1w/T2w input (default `2.0`). See [Input images](#input-images).
  - `--verbose`: Enable debug logging.

### Input images

The app looks for T1w and T2w images in the subject's `anat/` directory (restricted to the requested session, if any) and chooses which ones go to `recon-all`.

- **Voxel size first.** Every image is checked against `--max-voxel-size` (default `2.0` mm, applied to each of the three voxel dimensions in the header, rounded to 3 decimals) before anything is chosen, and images over the limit are set aside. A too-coarse image therefore never decides which acquisition is used. The check exists because some images labelled high resolution are thick slabs: ABIDE II UCLA_Long `acq-hires` T1w images are 1.5x1.5x4.0 mm, and `recon-all` on such an image can finish without error and still produce a meaningless reconstruction. An image whose header cannot be read ranks after every readable one; it is used only when nothing readable is left, and `recon-all` then reports the read failure.
- **Ranking.** The remaining images are ranked as the sibling ants-nidm app ranks them: no `acq-` entity before any `acq-` entity, then no other optional entities (`ce-`, `rec-`, ...) before some, then the lowest `run` (no run counts as run 0), then the file name. The best-ranked image is the one ants-nidm segments.
- **T1w.** The best-ranked image fixes the acquisition, and every run of that acquisition (same entities apart from `run`) is used; `recon-all` averages them. Other acquisitions are never mixed in. For example, ABIDE II UCLA_Long sessions hold both `sub-X_ses-N_run-1_T1w` and `sub-X_ses-N_acq-hires_run-1_T1w`; only the first is used. If every T1w is over the limit, `recon-all` is not run, the subject is marked failed and the app exits non-zero.
- **T2w.** `recon-all` takes one T2w (`-T2 ... -T2pial`): the best-ranked one within the limit. If none is within the limit, `recon-all` runs on the T1w alone.

The chosen and excluded files are logged. `processing_summary.json` records them as `input_files` and `inputs_not_used` (paths relative to the BIDS root, each excluded file with its `reason`), plus a `failure_reason` when the subject could not be processed.

### Reusing an existing reconstruction

When the subject's `freesurfer/` directory (or the staging copy of an interrupted run) already holds a finished `recon-all` run, the app does not run `recon-all` again, and with `--skip-freesurfer` it never does. Such a directory is judged by the images it was made from, not by what the selection above would choose today:

- The images are read from the `CMDARGS` line that `recon-all` writes into `scripts/recon-all.done` when it finishes (its `-i` and `-T2` arguments). If a later `recon-all` call without `-i` has rewritten that file, `input_files` in the subject's `processing_summary.json` is used instead.
- Each image must be an `anat` image of this subject (and session) and pass `--max-voxel-size`, the T2w included. Otherwise the directory is not used, the subject fails and the app exits non-zero; the reason names the directory and the image.
- A run that `recon-all` itself ended with an error (it leaves `scripts/recon-all.error`, and writes `1` to `recon-all.done`) is not finished: the subject fails rather than reusing or relocating that directory.

Remove the directory to reconstruct the subject from the inputs the current selection chooses.

### Examples

Process a single subject:
```bash
# Using Docker (for local development)
docker run -v /path/to/bids_dataset:/bids_dataset:ro \
           -v /path/to/output:/output \
           -v /path/to/freesurfer/license.txt:/license.txt \
           freesurfer-nidm-bidsapp \
           /bids_dataset /output participant \
           --participant-label 01 --fs-license-file /license.txt

# Using Apptainer (for clusters)
apptainer run \
  --bind /path/to/license.txt:/license.txt,/path/to/bids_dataset:/data,/path/to/output:/output \
  freesurfer.sif \
  /data /output participant \
  --participant-label 01 \
  --fs-license-file /license.txt
```

Process one session explicitly:

```bash
apptainer run \
  --bind /path/to/license.txt:/license.txt,/path/to/bids_dataset:/data,/path/to/output:/output \
  freesurfer.sif \
  /data /output session \
  --participant-label 01 \
  --session-label baseline \
  --fs-license-file /license.txt
```

The CLI processes one participant or participant/session pair per invocation. Use a scheduler or BABS job array for dataset-level parallelism.

## BABS compatibility

The app supports both the original BABS project layout and the configurable BIDS-study layout introduced by [PennLINC/babs PR #369](https://github.com/PennLINC/babs/pull/369). The new layout places the analysis DataLad dataset at the BABS project root and its RIA stores beneath `.babs/`:

```yaml
analysis_path: "."
input_ria_path: ".babs/input_ria"
output_ria_path: ".babs/output_ria"
```

See [`examples/babs-freesurfer-nidm-bids-study.yaml`](examples/babs-freesurfer-nidm-bids-study.yaml) for a complete template. It requires a BABS revision containing PR #369. Omit those three path settings to use the legacy `analysis/`, `input_ria/`, and `output_ria/` layout.

BABS invokes the standard positional analysis level `participant` for both subject-wise and session-wise jobs. For a session-wise job, configure `$SESSION_SELECTION_FLAG: "--session-label"`; this app honors that explicit label and only falls back to auto-detection when exactly one session is visible. If an upstream NIDM dataset is used, configure `--nidm-input-dir` explicitly in the BABS YAML.

## Outputs

### Output Directory Structure

The app writes everything for a subject under that subject's own directory, directly inside `output_dir` — there is no app-name wrapper directory. The subject directory is therefore also the top level of the per-subject zip in BABS workflows, so results land in the same layout whether the app runs standalone or through BABS.

```
<output_dir>/
├── dataset_description.json              # BIDS derivative dataset description (dataset level)
├── README                                # Auto-generated derivatives readme (dataset level)
│
├── sub-<id>/                             # single-session dataset
│   ├── anat/
│   │   ├── sub-<id>_desc-brain_T1w.nii.gz
│   │   ├── sub-<id>_desc-aparcaseg_dseg.nii.gz
│   │   └── sub-<id>_desc-wmparc_dseg.nii.gz
│   ├── stats/
│   │   ├── sub-<id>_aseg.stats
│   │   ├── sub-<id>_lh.aparc.stats
│   │   └── sub-<id>_rh.aparc.stats
│   ├── freesurfer/                       # recon-all output directory for this subject
│   │   ├── mri/    (brain.mgz, aparc.DKTatlas+aseg.mgz, wmparc.mgz, ...)
│   │   ├── surf/   (lh.white, lh.pial, rh.white, rh.pial, ...)
│   │   ├── label/
│   │   ├── stats/  (aseg.stats, lh.aparc.stats, rh.aparc.stats, ...)
│   │   └── scripts/ (recon-all.done, recon-all.log, ...)
│   ├── nidm.ttl                          # NIDM graph: input NIDM (if any) + this run's measures
│   ├── fs_cde.ttl                        # FreeSurfer CDE vocabulary (static, shipped alongside)
│   └── processing_summary.json           # Per-run processing summary with version info (not written by --skip-freesurfer)
│
└── sub-<id>/                             # multi-session dataset: same tree, one level deeper
    └── ses-<session>/
        ├── anat/    (sub-<id>_ses-<session>_desc-*.nii.gz)
        ├── stats/   (sub-<id>_ses-<session>_*.stats)
        ├── freesurfer/
        ├── nidm.ttl
        ├── fs_cde.ttl
        └── processing_summary.json
```

**Notes:**
- BIDS-organised derivative files use BIDS-compliant naming (e.g., `_desc-brain_T1w.nii.gz`, `_desc-aparcaseg_dseg.nii.gz`).
- `freesurfer/` is the FreeSurfer subject directory created by `recon-all` (the app stages `SUBJECTS_DIR` internally and moves the result here). For multi-session datasets the FreeSurfer subject ID is `sub-<id>_ses-<session>` (e.g. `sub-01_ses-baseline`).
- The NIDM output is always named `nidm.ttl`, one per subject (or per session for multi-session data). When an input NIDM document is found (see `--nidm-input-dir`), `nidm.ttl` contains that graph plus this run's measures — the input graph is preserved inside the output, never written to a separate file.
- `fs_cde.ttl` holds the shared FreeSurfer common-data-element vocabulary. It is identical across subjects and deliberately kept out of `nidm.ttl`.
- When no input NIDM document is found, the converter also leaves one intermediate `<stats-name>_nidm.jsonld` file per supported stats file (e.g. `aseg_nidm.jsonld`) in the subject directory; their content is merged into `nidm.ttl`.
- In BABS workflows each job zips only `sub-<id>/`, so `dataset_description.json` and `README` belong to the enclosing derivative dataset rather than the zip.

### FreeSurfer Output

`sub-<id>[/ses-<session>]/freesurfer/` is the subject's `recon-all` output directory. Key files copied into the BIDS-organised `anat/` and `stats/` trees include:

- Brain-extracted T1w (`brain.mgz` → `sub-<id>_desc-brain_T1w.nii.gz`)
- Cortical parcellation (`aparc.DKTatlas+aseg.mgz` → `sub-<id>_desc-aparcaseg_dseg.nii.gz`)
- White-matter parcellation (`wmparc.mgz` → `sub-<id>_desc-wmparc_dseg.nii.gz`)
- Surface meshes (`lh.white`, `rh.white`, `lh.pial`, `rh.pial`)
- Statistical measures (`aseg.stats`, `lh.aparc.stats`, `rh.aparc.stats`)

### NIDM Output

The NIDM output is a Turtle (`.ttl`) document named `nidm.ttl`, written beside the analysis results in each subject (or subject/session) directory, with the static CDE vocabulary alongside as `fs_cde.ttl`. The graph includes:

- Comprehensive version information:
  - FreeSurfer version and source (from base image)
  - BIDS app version (from setup.py)
  - Python environment and package versions
- Processing provenance
- Volume measurements for brain structures
- Cortical thickness and surface area measurements
- Standard identifiers for interoperability

## Implementation Notes

This BIDS App uses the pre-built FreeSurfer Docker image `vnmd/freesurfer_8.0.0` from Neurodesk as its base image. Neurodesk is a containerized data analysis environment for neuroimaging that provides a suite of neuroimaging tools in Docker containers. These containers are built using Neurodocker, a command-line tool that generates custom Dockerfiles for neuroimaging software.

Using the Neurodesk FreeSurfer image offers several advantages:

1. Faster build times - no need to download and install FreeSurfer during build
2. Smaller container size - uses the optimized FreeSurfer image
3. Improved reliability - uses a verified and tested FreeSurfer installation
4. Compatibility with FreeSurfer's license terms
5. Standardized environment - built using the community-supported Neurodocker tool
6. Regular maintenance - benefits from the Neurodesk project's updates and improvements

## License

This BIDS App is licensed under [MIT License](LICENSE).

## Acknowledgments

- FreeSurfer (https://surfer.nmr.mgh.harvard.edu/)
- BIDS (https://bids.neuroimaging.io/)
- NIDM (http://nidm.nidash.org/)
- Neurodesk (https://www.neurodesk.org/)
- Neurodocker (https://github.com/ReproNim/neurodocker)

## References

If you use this BIDS App in your research, please cite:

1. Fischl B. (2012). FreeSurfer. NeuroImage, 62(2), 774–781. https://doi.org/10.1016/j.neuroimage.2012.01.021
2. Gorgolewski, K. J., Auer, T., Calhoun, V. D., Craddock, R. C., Das, S., Duff, E. P., Flandin, G., Ghosh, S. S., Glatard, T., Halchenko, Y. O., Handwerker, D. A., Hanke, M., Keator, D., Li, X., Michael, Z., Maumet, C., Nichols, B. N., Nichols, T. E., Pellman, J., Poline, J. B., … Poldrack, R. A. (2016). The brain imaging data structure, a format for organizing and describing outputs of neuroimaging experiments. Scientific data, 3, 160044. https://doi.org/10.1038/sdata.2016.44
3. Maumet, C., Auer, T., Bowring, A., Chen, G., Das, S., Flandin, G., Ghosh, S., Glatard, T., Gorgolewski, K. J., Helmer, K. G., Jenkinson, M., Keator, D. B., Nichols, B. N., Poline, J. B., Reynolds, R., Sochat, V., Turner, J., & Nichols, T. E. (2016). Sharing brain mapping statistical results with the neuroimaging data model. Scientific data, 3, 160102. https://doi.org/10.1038/sdata.2016.102
4. Renton, A.I., Dao, T.T., Johnstone, T. et al. Neurodesk: an accessible, flexible and portable data analysis environment for reproducible neuroimaging. Nat Methods 21, 804–808 (2024). https://doi.org/10.1038/s41592-023-02145-x

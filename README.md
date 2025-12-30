# BIDS FreeSurfer App

A BIDS App implementation for FreeSurfer 8.0.0 that provides standardized surface reconstruction and morphometric analysis with NIDM output.

## Description

This BIDS App runs FreeSurfer's `recon-all` pipeline on structural T1w and (optionally) T2w images from a BIDS-valid dataset. It organizes outputs in a BIDS-compliant derivatives structure and provides additional NIDM format outputs for improved interoperability.

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

Fork the repository and clone it locally:

```bash
# Clone the repository
git clone https://github.com/yourusername/freesurfer-nidm_bidsapp.git
cd freesurfer-nidm_bidsapp
```

# Container Support

This BIDS App provides support for both Docker and Singularity/Apptainer, allowing you to run the application in various environments including HPC clusters.

## Building Containers

You can build the container images using these commands:

```bash
# Build Docker image (for local development)
python setup.py docker

# Build Singularity/Apptainer image on clusters
no

# Or build in a custom location
apptainer build --fakeroot /path/to/output/freesurfer-nidm-bidsapp.sif Singularity
```

Note: For cluster environments, we use the `--fakeroot` option with Apptainer as it:
1. Avoids permission issues common on shared systems
2. Doesn't require root privileges
3. Is specifically designed for HPC/cluster environments

If you encounter permission issues, you may need to:
1. Check if your user is configured for fakeroot (contact your system administrator)
2. Ensure you have proper permissions in the build directory
3. Try building in a directory where you have write permissions

## Docker Usage

```bash
# Run the container
docker run -v /path/to/license.txt:/license.txt \
  -v /path/to/bids/data:/data \
  -v /path/to/output:/output \
  freesurfer-nidm-bidsapp \
  --bids_dir /data \
  --output_dir /output
```

## Singularity/Apptainer Usage

```bash
# Run the container
apptainer run \
  --bind /path/to/license.txt:/license.txt,/path/to/bids/data:/data,/path/to/output:/output \
  /path/to/freesurfer-nidm-bidsapp.sif \
  --bids_dir /data \
  --output_dir /output
```

Note: The application files are included in the container image, so there's no need to bind the repository directory. Only the license file, input data, and output directory need to be bound.

## HPC/Cluster Usage

When running on an HPC cluster that uses Apptainer:

1. Build the container image:
   ```bash
   apptainer build --fakeroot freesurfer.sif Singularity
   ```
   Note: If you encounter permission issues, ensure your user is configured for fakeroot (contact your system administrator).

2. Create a job submission script like this:

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
  --bids_dir /data \
  --output_dir /output \
  --participant_label sub-01 sub-02  # Add your subjects here
```

Note: We no longer need to bind the repository directory since all required files are now included in the container image.

### Command-Line Arguments

- Positional arguments:
  - `bids_dir`: The directory with the input BIDS dataset
  - `output_dir`: The directory where the output files should be stored
  - `analysis_level`: Level of the analysis that will be performed. Options are: participant, group

- Optional arguments:
  - `--participant_label`: The label(s) of the participant(s) to analyze (without "sub-" prefix)
  - `--session_label`: The label(s) of the session(s) to analyze (without "ses-" prefix)
  - `--freesurfer_license`: Path to FreeSurfer license file
  - `--skip_bids_validator`: Skip BIDS validation
  - `--fs_options`: Additional options to pass to recon-all (e.g., "-parallel -openmp 4")
  - `--skip_nidm`: Skip NIDM output generation

### Examples

Process a single subject:
```bash
# Using Docker (for local development)
docker run -v /path/to/bids_dataset:/bids_dataset:ro \
           -v /path/to/output:/output \
           -v /path/to/freesurfer/license.txt:/license.txt \
           bids/freesurfer:8.0.0 \
           /bids_dataset /output participant --participant_label 01

# Using Apptainer (for clusters)
apptainer run \
  --bind /path/to/license.txt:/license.txt,/path/to/bids_dataset:/data,/path/to/output:/output \
  freesurfer.sif \
  --bids_dir /data \
  --output_dir /output \
  --participant_label 01
```

Process multiple subjects in parallel (using FreeSurfer's built-in parallelization):
```bash
# Using Docker (for local development)
docker run -v /path/to/bids_dataset:/bids_dataset:ro \
           -v /path/to/output:/output \
           -v /path/to/freesurfer/license.txt:/license.txt \
           bids/freesurfer:8.0.0 \
           /bids_dataset /output participant --fs_options="-parallel -openmp 4" \
           --participant_label 01 02 03

# Using Apptainer (for clusters)
apptainer run \
  --bind /path/to/license.txt:/license.txt,/path/to/bids_dataset:/data,/path/to/output:/output \
  freesurfer.sif \
  --bids_dir /data \
  --output_dir /output \
  --fs_options="-parallel -openmp 4" \
  --participant_label 01 02 03
```

## Outputs

### Output Directory Structure

```
<output_dir>/
└── freesurfer-nidm_bidsapp/
    ├── dataset_description.json
    ├── freesurfer/
    │   ├── dataset_description.json
    │   ├── sub-<participant_label>/
    │   │   ├── ses-<session_label>/  # Optional session directory
    │   │   │   ├── anat/
    │   │   │   │   ├── aparc+aseg.mgz
    │   │   │   │   ├── aseg.mgz
    │   │   │   │   ├── brainmask.mgz
    │   │   │   │   └── T1.mgz
    │   │   │   ├── label/
    │   │   │   ├── stats/
    │   │   │   │   ├── aseg.stats
    │   │   │   │   ├── lh.aparc.stats
    │   │   │   │   └── rh.aparc.stats
    │   │   │   ├── surf/
    │   │   │   │   ├── lh.pial
    │   │   │   │   ├── lh.white
    │   │   │   │   ├── rh.pial
    │   │   │   │   └── rh.white
    │   │   │   └── provenance.json
    │   │   └── anat/  # For single-session data
    │   │       ├── aparc+aseg.mgz
    │   │       ├── aseg.mgz
    │   │       ├── brainmask.mgz
    │   │       └── T1.mgz
    └── nidm/
        ├── dataset_description.json
        ├── sub-<participant_label>.ttl
        └── sub-<participant_label>_ses-<session_label>.ttl
```

**Note:** NIDM outputs use a flat directory structure (all .ttl files directly in `nidm/`) rather than hierarchical subject/session folders. Files are named following BIDS conventions:
- Single-session datasets: `sub-{id}.ttl` (e.g., `sub-01.ttl`)
- Multi-session datasets: `sub-{id}_ses-{session}.ttl` (e.g., `sub-01_ses-baseline.ttl`)

In BABS workflows, even multi-session datasets process one session at a time, so each job produces one .ttl file with the appropriate session label included in the filename.

### FreeSurfer Output

The FreeSurfer outputs follow standard FreeSurfer conventions but are organized in a BIDS-compliant directory structure. Key output files include:

- Segmentation volumes (`aparc+aseg.mgz`, `aseg.mgz`)
- Surface meshes (`lh.white`, `rh.white`, `lh.pial`, `rh.pial`)
- Statistical measures (`aseg.stats`, `lh.aparc.stats`, `rh.aparc.stats`)

### NIDM Output

The NIDM outputs are provided in Turtle (`.ttl`) format with files named according to BIDS conventions (e.g., `sub-01.ttl`, `sub-01_ses-baseline.ttl`). The output includes:

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
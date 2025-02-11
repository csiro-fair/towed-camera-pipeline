# Towed Camera Pipeline

A Marimba Pipeline for processing deep-sea imagery from the CSIRO Marine Resources and Industry Towed Camera (MRITC) 
platform. The Pipeline specializes in synchronizing high-resolution stereo imagery with environmental sensor data while 
preserving comprehensive spatio-temporal context.


## Overview

The Towed Camera Pipeline is designed to process deep-sea imagery collected during CSIRO marine monitoring campaigns. 
It handles data from the MRITC platform, which captures both high-resolution stereo still images and video footage 
synchronized with environmental sensor measurements including Ultra-Short Baseline (USBL) positioning data.

Key capabilities include:

- Automated synchronization of imagery with USBL sensor data
- Organization of imagery and data by deployment
- Integration of comprehensive metadata including position, depth, and camera orientation
- Generation of thumbnail and overview images for visual validation
- Preservation of precise temporal and spatial context
- Generation of FAIR-compliant datasets with embedded metadata


## Requirements

The Towed Camera Pipeline is built on the [Marimba](https://github.com/csiro-fair/marimba) framework which includes all 
necessary dependencies. No additional packages are required beyond those installed with Marimba.


## Installation

Create a new Marimba project and add the Towed Camera Pipeline:

```bash
marimba new project my-towed-camera-project
cd my-towed-camera-project
marimba new pipeline towed_camera https://github.com/csiro-fair/towed-camera-pipeline.git
```


## Configuration

### Pipeline Configuration
The Pipeline uses a configuration-free approach, with all necessary parameters derived from input data.

### Collection Configuration
Collections requires no specific configuration parameters, as all necessary information is extracted from the source 
data structure.


## Usage

### Importing

Import collections by deployment:

```bash
marimba import IN2018_V06_001 "/path/to/source/IN2018_V06_001" --operation link
```

For survey voyages, multiple deployments can be imported sequentially:

```bash
# Import multiple deployments
marimba import IN2018_V06_001 "/path/to/IN2018_V06_001" --operation link
marimba import IN2018_V06_002 "/path/to/IN2018_V06_002" --operation link
marimba import IN2018_V06_003 "/path/to/IN2018_V06_003" --operation link
```

### Source Data Structure

The Pipeline expects towed camera data organized by deployment:
```
source/
└── IN2018_V06_001/
    ├── data/                 # USBL sensor data (CSV)
    ├── stills/               # High-resolution still images
    └── video/                # Video files
```

### Processing

```bash
marimba process
```

During processing, the Towed Camera Pipeline:
1. Creates a hierarchical directory structure by deployment
2. Matches imagery timestamps with USBL sensor data
3. Generates thumbnails from still images
4. Creates deployment overview images for quality control

### Packaging

```bash
marimba package IN2018_V06 \
--operation link \
--version 1.0 \
--contact-name "Keiko Abe" \
--contact-email "keiko.abe@email.com"
```

The `--operation link` flag creates hard links instead of copying files, optimizing storage for large datasets.


## Processed Data Structure

```
IN2018_V06/                                 # Root dataset directory
├── data/                                   # Directory containing all processed data
│   └── MRITC/                              # MRITC platform data directory
│       └── IN2018_V06_*/                   # Deployment directories
│           ├── data/                       # Deployment data files (CSV)
│           ├── stills/                     # Full resolution images
│           ├── thumbnails/                 # Image thumbnails
│           ├── video/                      # Video files
│           └── IN2018_V06_*_OVERVIEW.JPG   # Deployment overview image
├── logs/                                   # Directory containing all processing logs
│   ├── pipelines/                          # Pipeline-specific logs
│   │   └── towed_camera.log                # Logs from Towed Camera Pipeline
│   ├── dataset.log                         # Dataset packaging logs
│   └── project.log                         # Overall project processing logs
├── pipelines/                              # Directory containing pipeline code
│   └── towed_camera/                       # Pipeline-specific directory
│       ├── repo/                           # Pipeline source code repository
│       │   ├── LICENSE                     # Pipeline license file
│       │   ├── README.md                   # Pipeline README file
│       │   └── towed_camera.pipeline.py    # Pipeline implementation
│       └── pipeline.yml                    # Pipeline configuration
├── ifdo.yml                                # Dataset-level iFDO metadata file
├── manifest.txt                            # File manifest with SHA256 hashes
├── map.png                                 # Spatial visualization of dataset
└── summary.md                              # Dataset summary and statistics
```


## Metadata

The Towed Camera Pipeline captures comprehensive metadata including:

### Survey Metadata
- USBL positioning data
- Depth measurements
- Collection timestamps
- Environmental parameters

### Technical Metadata
- Image acquisition parameters
- Camera configuration
- Processing parameters
- Quality metrics
- Camera orientation (pitch, roll)

### Image-Specific Data
- Precise geographic coordinates
- Temporal information
- Platform details
- Deployment context

All metadata is standardized using the iFDO schema (v2.1.0) and embedded in both image EXIF tags and dataset-level files.


## Contributors

The Towed Camera Pipeline was developed by:
- Christopher Jackett (CSIRO)
- David Webb (CSIRO)
- Franziska Althaus (CSIRO)
- Candice Untiedt (CSIRO)


## License

The Towed Camera Pipeline is distributed under the [CSIRO BSD/MIT](LICENSE) license.


## Contact

For inquiries related to this repository, please contact:

- **Christopher Jackett**  
  *Software Engineer, CSIRO*  
  Email: [chris.jackett@csiro.au](mailto:chris.jackett@csiro.au)

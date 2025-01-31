"""Marimba Pipeline for the CSIRO Towed Camera platform."""  # noqa: INP001
import os
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any
from uuid import uuid4

import pandas as pd
from ifdo.models import (
    ImageAcquisition,
    ImageCameraHousingViewport,
    ImageCaptureMode,
    ImageContext,
    ImageCreator,
    ImageData,
    ImageDeployment,
    ImageFaunaAttraction,
    ImageIllumination,
    ImageLicense,
    ImageMarineZone,
    ImageNavigation,
    ImagePI,
    ImagePixelMagnitude,
    ImageQuality,
    ImageSpectralResolution,
)

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.ifdo import iFDOMetadata
from marimba.lib import image
from marimba.main import __version__


class TowedCameraPipeline(BasePipeline):
    """
    Marimba Pipeline for the CSIRO Towed Camera platform.

    This class extends the BasePipeline to provide specific functionality for handling towed camera data. It includes
    methods for importing, processing, and packaging data from towed camera surveys. The pipeline manages various data
    types including CSV files, still images, and video files.

    Attributes:
        root_path (str | Path): Base directory path where the pipeline stores its data and configuration files.
        config (dict[str, Any] | None): Pipeline configuration dictionary.
        dry_run (bool): If True, prevents any filesystem modifications.

    Methods:
        __init__: Initialize a new TowedCameraPipeline instance.
        get_pipeline_config_schema: Get the pipeline configuration schema.
        get_collection_config_schema: Get the collection configuration schema.
        _import: Import data from the source path to the data directory.
        _create_hard_link: Create hard links with error handling.
        _process: Process the imported data, including generating thumbnails and overview images.
        _package: Package the processed data and generate metadata.
    """

    def __init__(
        self,
        root_path: str | Path,
        config: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Initialize a new Pipeline instance.

        Args:
            root_path (str | Path): Base directory path where the pipeline will store its data and configuration files.
            config (dict[str, Any] | None, optional): Pipeline configuration dictionary. If None, default configuration
             will be used. Defaults to None.
            dry_run (bool, optional): If True, prevents any filesystem modifications. Useful for validation and testing.
             Defaults to False.
        """
        super().__init__(
            root_path,
            config,
            dry_run=dry_run,
            metadata_class=iFDOMetadata,
        )

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        """
        Get the pipeline configuration schema for the Towed Camera Pipeline.

        Returns:
            dict: Configuration parameters for the pipeline
        """
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict:
        """
        Get the collection configuration schema for the Towed Camera Pipeline.

        Returns:
            dict: Configuration parameters for the collection
        """
        return {}

    def _import_data_files(self, source_dir: Path, dest_dir: Path, deployment_id: str) -> None:
        """Import CSV files with matching deployment ID."""
        if not source_dir.exists():
            return

        for source_file in source_dir.glob("*.CSV"):
            if source_file.name.endswith(f"{deployment_id}.CSV"):
                dest_file = dest_dir / source_file.name
                self._create_hard_link(source_file, dest_file)

    @staticmethod
    def _import_still_images(source_dir: Path, dest_dir: Path) -> None:
        """Import JPG files from stills directory."""
        if not source_dir.exists():
            return

        for source_file in source_dir.glob("*.JPG"):
            dest_file = dest_dir / source_file.name
            self._create_hard_link(source_file, dest_file)

    def _import_video_files(self, source_dir: Path, dest_dir: Path) -> None:
        """Import video files with specific naming pattern."""
        if not source_dir.exists():
            return

        for source_file in source_dir.iterdir():
            if source_file.is_file():
                name_without_ext, _ = source_file.name.rsplit(".", 1)
                if name_without_ext.endswith("Z") and not name_without_ext.endswith("_Z"):
                    dest_file = dest_dir / source_file.name
                    self._create_hard_link(source_file, dest_file)

    def _create_hard_link(self, source_file: Path, dest_file: Path) -> None:
        """Helper method to create hard links with error handling."""
        if not self.dry_run:
            try:
                os.link(str(source_file), str(dest_file))
                self.logger.debug(f"Created hard link: {source_file} -> {dest_file}")
            except FileExistsError:
                self.logger.warning(f"Destination file already exists: {dest_file}")
            except OSError as e:
                self.logger.exception(f"Failed to create hard link for {source_file}: {e}")

    def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: dict[str, Any],  # noqa: ARG002
        **kwargs: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """Import data from source to destination directories."""
        # Define source and destination directory mappings
        source_dirs = {
            "data": source_path / "data",
            "stills": source_path / "stills",
            "video": source_path / "video",
        }
        dest_dirs = {
            "data": data_dir / "data",
            "stills": data_dir / "stills",
            "video": data_dir / "video",
        }

        # Create destination directories
        for dest_dir in dest_dirs.values():
            dest_dir.mkdir(parents=True, exist_ok=True)

        # Process each type of data
        deployment_id = data_dir.parent.name.split("_")[-1]
        self._import_data_files(source_dirs["data"], dest_dirs["data"], deployment_id)
        self._import_still_images(source_dirs["stills"], dest_dirs["stills"])
        self._import_video_files(source_dirs["video"], dest_dirs["video"])

    def _process(
        self,
        data_dir: Path,
        config: dict[str, Any],  # noqa: ARG002
        **kwargs: dict[str, Any],  # noqa: ARG002
    ) -> None:
        # Define directories
        paths = {
            "stills": data_dir / "stills",
            "thumbs": data_dir / "thumbnails",
        }

        # Ensure thumbnail directory exists
        paths["thumbs"].mkdir(exist_ok=True)

        # Initialize thumbnail list for generating an overview image
        thumb_list = []

        # Process images from stills directory
        if paths["stills"].exists():
            for jpg in paths["stills"].glob("*.[jJ][pP][gG]"):
                # Generate thumbnail
                output_filename = f"{jpg.stem}_THUMB{jpg.suffix}"
                output_path = paths["thumbs"] / output_filename
                self.logger.info(f"Generating thumbnail image: {output_path}")

                try:
                    image.resize_fit(jpg, 300, 300, output_path)
                    # Only add SCP thumbnails to the list for overview
                    if "SCP" in jpg.name:
                        thumb_list.append(output_path)
                except Exception as e:
                    self.logger.exception(f"Error creating thumbnail for {jpg.name}: {e!s}")

        # Create an overview image if thumbnails exist
        if thumb_list:
            overview_path = data_dir / f"{data_dir.parent.name}_OVERVIEW.JPG"
            self.logger.info(f"Creating thumbnail overview image: {overview_path}")

            try:
                image.create_grid_image(thumb_list, overview_path)
            except Exception as e:
                self.logger.exception(f"Error creating overview image: {e!s}")

    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],  # noqa: ARG002
        **kwargs: dict[str, Any],  # noqa: ARG002
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:

        # Initialise an empty dictionary to store file mappings
        data_mapping: dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]] = {}

        # Find TAG CSV files
        tag_files = list((data_dir / "data").glob("*TAG*.CSV"))
        if not tag_files:
            self.logger.warning(f"No TAG CSV files found in {data_dir / 'data'} - skipping packaging collection")
            return data_mapping

        # Read the sensor data CSV file
        try:
            sensor_data_df = pd.read_csv(tag_files[0])
            sensor_data_df["FinalTime"] = pd.to_datetime(
                sensor_data_df["FinalTime"],
                format="%Y-%m-%d %H:%M:%S.%f",
            ).dt.floor("s")
        except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError, OSError) as e:
            self.logger.warning(f"Failed to process TAG CSV file {tag_files[0]}: {e!s} - skipping packaging collection")
            return data_mapping

        # Recursively gather all file paths from the data directory
        file_paths = data_dir.rglob("*")

        for file_path in file_paths:

            # Extract the deployment ID and define the relative output path
            deployment_id = str(data_dir).split("/")[-2]
            output_file_path = deployment_id / file_path.relative_to(data_dir)

            # Process only valid image files (JPGs) and videos (MP4s), excluding thumbnails and overview images
            if (
                file_path.is_file()
                and file_path.suffix.lower() in [".jpg", ".mp4"]
                and "_THUMB" not in file_path.name
                and "_OVERVIEW" not in file_path.name
            ):
                # Extract the ISO timestamp from the filename and convert it to a datetime object
                iso_timestamp = file_path.stem.split("_")[5]
                target_datetime = pd.to_datetime(iso_timestamp, format="%Y%m%dT%H%M%SZ")

                # Check file type and perform the appropriate matching
                if file_path.suffix.lower() == ".jpg":
                    # For jpgs, find the perfect match
                    matching_row = sensor_data_df.loc[sensor_data_df["FinalTime"] == target_datetime]
                elif file_path.suffix.lower() == ".mp4":
                    # For mp4s, find the closest match
                    time_diffs = abs(sensor_data_df["FinalTime"] - target_datetime)
                    matching_row = sensor_data_df.loc[time_diffs.idxmin()]
                else:
                    raise ValueError("Unsupported file type")

                if not matching_row.empty:
                    if isinstance(matching_row, pd.DataFrame):
                        first_row = matching_row.iloc[0].copy()
                    elif isinstance(matching_row, pd.Series):
                        first_row = matching_row.copy()
                    else:
                        raise ValueError(f"Unexpected type for matching_row: {type(matching_row)}")

                    # Convert any Timestamp fields in first_row directly to ISO 8601 strings
                    first_row = first_row.map(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

                    # Set the image PI and creators
                    image_pi = ImagePI(name="Alan Williams")
                    image_creators = [
                        ImageCreator(name="Alan Williams"),
                        ImageCreator(name="Christopher Jackett", uri="https://orcid.org/0000-0003-1132-1558"),
                        ImageCreator(name="Jeff Cordell"),
                        ImageCreator(name="Karl Forcey", uri="https://orcid.org/0009-0004-1780-5355"),
                        ImageCreator(name="David Webb", uri="https://orcid.org/0000-0001-5847-7002"),
                        ImageCreator(name="Franziska Althaus", uri="https://orcid.org/0000-0002-5336-4612"),
                        ImageCreator(name="Candice Untiedt", uri="https://orcid.org/0000-0003-1562-3473"),
                        ImageCreator(name="Marine National Facility", uri="https://mnf.csiro.au"),
                        ImageCreator(name="CSIRO", uri="https://www.csiro.au"),
                    ]

                    image_camera_housing_viewport = ImageCameraHousingViewport(
                        viewport_type="flatport",
                        viewport_optical_density=1.49,
                        viewport_thickness_millimeter=40,
                        viewport_extra_description=None,
                    )

                    # Create ImageContext and ImageLicense objects
                    image_context = ImageContext(
                        name="CSIRO Project OD-211438: Collaborative project between CSIRO and NIWA aimed at 1) "
                             "Recovery of deep-sea seamount ecosystems following human impacts; 2) Status of deep sea "
                             "corals in Australian and New Zealand regions",
                    )
                    image_project = ImageContext(name="IN2018_V06")
                    image_event = ImageContext(name=output_file_path.stem)
                    image_platform = ImageContext(name="MRI Deep Towed Camera System (MRITC)")
                    image_sensor = ImageContext(
                        name="Video Camera Survey (SVY) / Digital Stills Camera Port (DSP), Digital Stills Camera "
                             "Starboard (DSS)",
                    )
                    image_license = ImageLicense(
                        name="CC BY-NC 4.0",
                        uri="https://creativecommons.org/licenses/by-nc/4.0",
                    )
                    image_abstract = (
                        "High definition video and stereo stills imagery (99% systematic (~5s), ~1% human-triggered "
                        "for locations of interest or fault finding) were taken with the CSIRO MRITC platform"
                    )

                    # ruff: noqa: ERA001
                    image_data = ImageData(
                        # iFDO core
                        image_datetime=datetime.strptime(iso_timestamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc),
                        image_latitude=float(first_row["UsblLatitude"]),
                        image_longitude=float(first_row["UsblLongitude"]),
                        image_altitude_meters=-float(first_row["Pres"]),
                        image_coordinate_reference_system="EPSG:4326",
                        image_coordinate_uncertainty_meters=None,
                        image_context=image_context,
                        image_project=image_project,
                        image_event=image_event,
                        image_platform=image_platform,
                        image_sensor=image_sensor,
                        image_uuid=str(uuid4()),
                        image_pi=image_pi,
                        image_creators=image_creators,
                        image_license=image_license,
                        image_copyright="CSIRO",
                        image_abstract=image_abstract,

                        # iFDO capture (optional)
                        image_acquisition=ImageAcquisition.PHOTO,
                        image_quality=ImageQuality.PRODUCT,
                        image_deployment=ImageDeployment.SURVEY,
                        image_navigation=ImageNavigation.RECONSTRUCTED,
                        # image_scale_reference=ImageScaleReference.NONE,
                        image_illumination=ImageIllumination.ARTIFICIAL_LIGHT,
                        image_pixel_magnitude=ImagePixelMagnitude.CM,
                        image_marine_zone=ImageMarineZone.SEAFLOOR,
                        image_spectral_resolution=ImageSpectralResolution.RGB,
                        image_capture_mode=ImageCaptureMode.MIXED,
                        image_fauna_attraction=ImageFaunaAttraction.NONE,
                        # image_area_square_meter=None,
                        # image_meters_above_ground=None,
                        # image_acquisition_settings=None,
                        # image_camera_yaw_degrees=None,
                        image_camera_pitch_degrees=first_row["Pitch"],
                        image_camera_roll_degrees=first_row["Roll"],
                        image_overlap_fraction=0,
                        image_datetime_format="%Y-%m-%d %H:%M:%S.%f",
                        # image_camera_pose=None,
                        image_camera_housing_viewport=image_camera_housing_viewport,
                        # image_flatport_parameters=None,
                        # image_domeport_parameters=None,
                        # image_camera_calibration_model=None,
                        # image_photometric_calibration=None,
                        # image_objective=None,
                        image_target_environment="Benthic habitat",
                        # image_target_timescale=None,
                        # image_spatial_constraints=None,
                        # image_temporal_constraints=None,
                        # image_time_synchronization=None,
                        image_item_identification_scheme="<platform_id>_<camera_id>_<voyage_id>_<deployment_number>_<datetimestamp>_<image_id>.<ext>",
                        image_curation_protocol=f"Processed with Marimba v{__version__}",

                        # iFDO content (optional)
                        # image_entropy=0.0,
                        # image_particle_count=None,
                        # image_average_color=[0, 0, 0],
                        # image_mpeg7_colorlayout=None,
                        # image_mpeg7_colorstatistics=None,
                        # image_mpeg7_colorstructure=None,
                        # image_mpeg7_dominantcolor=None,
                        # image_mpeg7_edgehistogram=None,
                        # image_mpeg7_homogenoustexture=None,
                        # image_mpeg7_stablecolor=None,
                        # image_annotation_labels=None,
                        # image_annotation_creators=None,
                        # image_annotations=None,
                    )

                    # Add the image file, metadata (ImageData), and ancillary metadata to the data mapping
                    metadata = self._metadata_class(image_data)
                    data_mapping[file_path] = output_file_path, [metadata], first_row.to_dict()

            # For non-image files, add them without metadata
            elif file_path.is_file():
                data_mapping[file_path] = output_file_path, None, None

        # Return the constructed data mapping for all files
        return data_mapping

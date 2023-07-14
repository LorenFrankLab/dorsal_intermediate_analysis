import itertools
import os
from rec_to_nwb.processing.builder.raw_to_nwb_builder import RawToNWBBuilder
from rec_to_nwb.processing.metadata.metadata_manager import MetadataManager

from .common_abstract_classes import DataWriter
from .common_helpers import print_verbose, function_timer, no_overwrite_handler_file
from .extra_helpers import rename_nwb_file
from .file_utils import FileInfo


class NWBConverter(DataWriter):
    # Get Spyglass .nwb file paths
    from .user_settings import spyglass_nwb_path, spyglass_video_path

    _spyglass_nwb_path = spyglass_nwb_path
    _spyglass_video_path = spyglass_video_path

    def __init__(self, subject_name, dates=None, overwrite=False, verbose=False, timing=False):
        
        # Enable overwriting existing .nwb files, verbose output, and timing
        super().__init__(overwrite=overwrite, verbose=verbose, timing=timing)

        # Get file information for raw data and metadata
        self._file_info = FileInfo(subject_name, dates=dates)
        self._subject_name = self._file_info._subject_name
        self._dates = self._file_info._dates

    @function_timer
    def make_nwb_files(self):
        # Convert raw to .nwb files
        for date in self._dates:
            print(date)
            if date == "20220724":
                print("bad date")
            elif date == "20220718":
                print("bad date")
            else:
                nwb_full_name = (
                    self._file_info.search_expected_file_names(date + ".nwb")
                    .iloc[0]
                    .full_name
                )
                self._convert_rec_to_nwb(date, nwb_full_name)

    @function_timer
    def _convert_rec_to_nwb(self, date, nwb_full_name):
        # Ensure .nwb file doesn't already exist if file overwriting is disabled
        nwb_full_names_list = self._file_info.search_matching_file_names(
            ".nwb"
        ).full_name.tolist()
        if not self._overwrite and nwb_full_name in nwb_full_names_list:
            no_overwrite_handler_file(f"File '{nwb_full_name}' already exists")
            return
        print_verbose(self._verbose, f"{nwb_full_name}", color="blue")

        # Create .nwb file metadata
        metadata = self._get_nwb_metadata(date)
        # Find the reconfig .trodes file
        reconfig_full_name = self._get_reconfig_header_name(date)
        # Specify optional arguments to trodes .rec file exporter
        trodes_rec_export_args = NWBConverter._create_trodes_rec_export_args(
            {"-reconfig": reconfig_full_name}
        )
        builder = self._get_raw_to_nwb_builder(
            date, metadata, trodes_rec_export_args=trodes_rec_export_args
        )
        # Create .nwb file and create symlinks in Spyglass directories for associated files
        self._write_nwb_file(builder, nwb_full_name)

    def _get_nwb_metadata(self, date):
        # Find metadata .yml files
        yml_metadata_full_name = self._get_yml_metadata_file_name(date)
        probe_metadata_full_names_list = self._get_probe_metadata_file_names()
        # Generate and print metadata object
        metadata = NWBConverter._create_metadata_manager(
            yml_metadata_full_name, probe_metadata_full_names_list
        )
        print_verbose(self._verbose, f"{metadata}\n")
        return metadata

    def _get_raw_to_nwb_builder(self, date, metadata, trodes_rec_export_args=None):
        if trodes_rec_export_args is None:
            trodes_rec_export_args = ()
        # Create raw data to .nwb builder object
        nwb_path = self._file_info._file_name_helper._get_paths_by_file_type("nwb")[0]
        video_path = self._file_info._file_name_helper._get_paths_by_file_type("video")[
            0
        ]
        builder = NWBConverter._create_raw_to_nwb_builder(
            self._subject_name,
            FileInfo._data_path,
            [date],
            metadata,
            nwb_path,
            video_path,
            trodes_rec_export_args,
        )
        return builder

    def _write_nwb_file(self, builder, nwb_full_name):
        # Write .nwb file
        content = builder.build_nwb()
        print_verbose(self._verbose, f"{content}\n")

        # Handle all files created in the .rec to .nwb conversion
        self._nwb_converter_file_handler(builder.dates[0], nwb_full_name)
        # Delete preprocessing files
        builder.cleanup()

    def _get_reconfig_header_name(self, date):
        # Get the name of .trodesconf file for the current session
        file_name_prefix = self._file_info._file_name_helper._get_file_name_prefix(date)
        reconfig_full_name = self._file_info.search_matching_file_names(
            file_name_prefix + ".trodesconf"
        ).full_name.tolist()[0]
        return reconfig_full_name

    def _get_yml_metadata_file_name(self, date):
        # Get the name of the .yml metadata file for the current session
        file_name_prefix = self._file_info._file_name_helper._get_file_name_prefix(date)
        yml_full_name = self._file_info.search_matching_file_names(
            file_name_prefix + ".yml"
        ).full_name.tolist()[0]
        return yml_full_name

    def _get_probe_metadata_file_names(self):
        # Get the names of the .yml files for the implant probes
        probe_full_names = self._file_info.search_matching_file_names(
            "tetrode_12.5.yml"
        ).full_name.tolist()
        return probe_full_names

    @staticmethod
    def _create_trodes_rec_export_args(export_args_dict):
        # Create tuple of .rec file export arguments
        export_args = [[key, value] for key, value in export_args_dict.items()]
        export_args = tuple(itertools.chain(*export_args))
        return export_args

    @staticmethod
    def _create_metadata_manager(
        yml_metadata_full_name, probe_metadata_full_names_list
    ):
        # Create metadata object using rec_to_nwb MetadataManager
        metadata = MetadataManager(
            yml_metadata_full_name, probe_metadata_full_names_list
        )
        return metadata

    @staticmethod
    def _create_raw_to_nwb_builder(
        subject_name,
        data_path,
        dates,
        metadata,
        output_path,
        video_path,
        trodes_rec_export_args,
    ):
        # Create raw data to .nwb builder object using rec_to_nwb RawToNWBBuilder
        builder = RawToNWBBuilder(
            animal_name=subject_name,
            data_path=data_path,
            dates=dates,
            nwb_metadata=metadata,
            overwrite=True,
            output_path=output_path,
            video_path=video_path,
            trodes_rec_export_args=trodes_rec_export_args,
        )
        return builder

    def _nwb_converter_file_handler(self, date, nwb_full_name):
        # Rename .nwb file from default name
        nwb_path = self._file_info._file_name_helper._get_paths_by_file_type("nwb")[0]
        old_name = self._subject_name + date + ".nwb"
        new_name = os.path.split(nwb_full_name)[1]
        rename_nwb_file(
            os.path.join(nwb_path, old_name), os.path.join(nwb_path, new_name)
        )

        # Create symlinks of .h264 video file and newly created .nwb file
        video_files_df = self._file_info.search_matching_file_names(
            "^" + date + ".*.h264"
        )
        video_full_names_list = video_files_df[
            video_files_df.file_type == "video"
        ].full_name.tolist()
        self._create_nwb_symlink(nwb_full_name)
        for video_full_name in video_full_names_list:
            self._create_video_symlink(video_full_name)

    def _create_nwb_symlink(self, nwb_full_name):
        # Create soft copy of .nwb files in Spyglass raw .nwb file directory
        spyglass_nwb_path = NWBConverter._spyglass_nwb_path
        NWBConverter._create_symlink_target_directory(
            nwb_full_name, spyglass_nwb_path, overwrite=self._overwrite
        )
        print_verbose(self._verbose, f"Symlink {nwb_full_name} to {spyglass_nwb_path}")

    def _create_video_symlink(self, video_full_name):
        # Create soft copies of .h264 files in Spyglass video .nwb file directory
        spyglass_video_path = NWBConverter._spyglass_video_path
        NWBConverter._create_symlink_target_directory(
            video_full_name, spyglass_video_path, overwrite=self._overwrite
        )
        print_verbose(
            self._verbose, f"Symlink {video_full_name} to {spyglass_video_path}"
        )

    @staticmethod
    def _create_symlink_target_directory(
        source_full_name, destination_path, overwrite=False
    ):
        # Delete destination file if it already exists
        destination_file_name = os.path.split(source_full_name)[1]
        destination_full_name = os.path.join(destination_path, destination_file_name)
        # Either delete and overwrite or skip creating symlink if it already exists
        if destination_file_name in [
            file.name for file in os.scandir(destination_path) if file.is_file()
        ]:
            if not overwrite:
                no_overwrite_handler_file(
                    f"File '{destination_file_name}' already exists in '{destination_path}'"
                )
            else:
                os.remove(destination_full_name)
                os.symlink(source_full_name, destination_full_name)
        # Create the symlink if it doesn't exist
        else:
            os.symlink(source_full_name, destination_full_name)

        # Ensure that the symlink exists
        assert destination_file_name in [
            file.name for file in os.scandir(destination_path) if file.is_file()
        ], f"Could not find symlink for {destination_file_name} in {destination_path}"

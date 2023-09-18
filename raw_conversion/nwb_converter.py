import itertools
from rec_to_nwb.processing.builder.raw_to_nwb_builder import RawToNWBBuilder
from rec_to_nwb.processing.metadata.metadata_manager import MetadataManager

from .file_info import FileInfo
from .file_name_helper import FileNameHelper

from utils import verbose_printer
from utils.abstract_classes import DataReader

class NWBConverter(DataReader):

    def __init__(self, subject_name, dates=None):
        
        # Enable overwriting existing .nwb files, verbose output, and timing
        super().__init__(subject_name, dates=dates, verbose=False, timing=False)

        # Get file information for raw data and metadata
        self._file_name_helper = FileNameHelper(self.subject_name, dates=self.dates)
        self._file_info = FileInfo(self.subject_name, dates=self.dates)
        self._nwb_metadata = None
        self._nwb_builders = None

    @property
    def nwb_metadata(self):
        if self._nwb_metadata is None:
            self._update()
        return self._nwb_metadata
    
    @property
    def nwb_builders(self):
        if self._nwb_builders is None:
            self._update()
        return self._nwb_builders
     
    def get_reconfig_header_name(self, date):
        
        # Get the name of .trodesconf file for the current session
        self._file_name_helper.update()
        file_name_prefix = self._file_name_helper.get_file_name_prefix(date)
        reconfig_full_name = self._file_info.search_matching_file_names(file_name_prefix + ".trodesconf").iloc[0].full_name
        return reconfig_full_name

    def get_yml_metadata_file_name(self, date):
        
        # Get the name of the .yml metadata file for the current session
        self._file_name_helper.update()
        file_name_prefix = self._file_name_helper.get_file_name_prefix(date)
        yml_full_name = self._file_info.search_matching_file_names(file_name_prefix + ".yml").iloc[0].full_name
        return yml_full_name

    def get_probe_metadata_file_names(self):
        
        # Get the names of the .yml files for the implant probes
        self._file_info.update()
        probe_full_names = self._file_info.search_matching_file_names("tetrode_12.5.yml").full_name.tolist()
        return probe_full_names


    def _get_nwb_metadata(self):

        nwb_metadata = {}
        # Create .nwb metadata managers for each date
        for date in self.dates:
            nwb_metadata[date] = self._create_nwb_metadata(date)
        return nwb_metadata
    
    def _create_nwb_metadata(self, date):
        
        # Find metadata .yml files
        yml_metadata_full_name = self.get_yml_metadata_file_name(date)
        probe_metadata_full_names_list = self.get_probe_metadata_file_names()
        # Generate and print metadata object
        metadata = NWBConverter._create_metadata_manager(yml_metadata_full_name, probe_metadata_full_names_list)
        return metadata

    def _get_nwb_builders(self):

        nwb_builders = {}
        # Create .nwb file builder for each date
        for date in self.dates:
            nwb_builders[date] = self._create_nwb_builder(date)
        return nwb_builders

    def _create_nwb_builder(self, date):

        # Create .nwb file metadata
        metadata = self._nwb_metadata[date]
        # Find the reconfig .trodes file
        reconfig_full_name = self.get_reconfig_header_name(date)
        # Specify optional arguments to trodes .rec file exporter
        trodes_rec_export_args = NWBConverter._create_trodes_rec_export_args({"-reconfig": reconfig_full_name})

        if trodes_rec_export_args is None:
            trodes_rec_export_args = ()
        # Create raw data to .nwb builder object
        path_names_df = self._file_info.path_names
        nwb_path = path_names_df[path_names_df.file_type == 'nwb'].path_name.tolist()[0]
        video_path = path_names_df[path_names_df.file_type == 'video'].path_name.tolist()[0]
        builder = NWBConverter._create_raw_to_nwb_builder(self.subject_name,
                                                          self.data_path,
                                                          [date],
                                                          metadata,
                                                          nwb_path,
                                                          video_path,
                                                          trodes_rec_export_args)
        return builder




    @staticmethod
    def _create_trodes_rec_export_args(export_args_dict):
        
        # Create tuple of .rec file export arguments
        export_args = [[key, value] for key, value in export_args_dict.items()]
        export_args = tuple(itertools.chain(*export_args))
        return export_args

    @staticmethod
    def _create_metadata_manager(yml_metadata_full_name, probe_metadata_full_names_list):
        
        # Create metadata object using rec_to_nwb MetadataManager
        metadata = MetadataManager(yml_metadata_full_name, probe_metadata_full_names_list)
        return metadata

    @staticmethod
    def _create_raw_to_nwb_builder(subject_name, data_path, dates, metadata, output_path, video_path, trodes_rec_export_args):

        # Create raw data to .nwb builder object using rec_to_nwb RawToNWBBuilder
        builder = RawToNWBBuilder(animal_name=subject_name,
                                  data_path=data_path,
                                  dates=dates,
                                  nwb_metadata=metadata,
                                  overwrite=True,
                                  output_path=output_path,
                                  video_path=video_path,
                                  trodes_rec_export_args=trodes_rec_export_args)
        return builder


    def _update(self):

       # Update file name info, .nwb file metadata, and .nwb file builder objects
       self._file_name_helper.update()
       self._file_info.update()
       self._nwb_metadata = self._get_nwb_metadata()
       self._nwb_builders = self._get_nwb_builders()   

    def _print(self, dates):
        
        # Print .nwb file metadata and .nwb file builder info for the given dates
        for date in dates:
            verbose_printer.print_header(f"{self.subject_name} {date} .nwb file metadata and builder info")
            verbose_printer.print_text(f"{self._nwb_metadata[date]}")
            verbose_printer.print_newline()
            verbose_printer.print_text(f"{self._nwb_builders[date]}")
            verbose_printer.print_newline()
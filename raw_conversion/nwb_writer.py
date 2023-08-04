import os

from .file_info import FileInfo
from .file_name_helper import FileNameHelper
from .nwb_converter import NWBConverter

from utils import verbose_printer
from utils.abstract_classes import DataWriter
from utils.file_helpers import (create_symlink_in_directory, no_overwrite_handler_file, universal_file_permissions)
from utils.function_wrappers import (function_timer, prompt_password)
from utils.permissions_manager import PermissionsManager

class NWBWriter(DataWriter):

    def __init__(self, subject_name, dates=None, overwrite=False, universal_access=False, verbose=False, timing=False):
        
        # Enable overwriting existing .nwb files, verbose output, and timing
        super().__init__(subject_name, dates=dates, overwrite=overwrite, universal_access=universal_access, verbose=verbose, timing=timing)

        # Get .nwb conversion info and .nwb file permissions
        self._file_info = FileInfo(self.subject_name, dates=self.dates)
        self._nwb_converter = NWBConverter(self.subject_name, dates=self.dates)
        self._permissions_manager = PermissionsManager(self.subject_name, dates=self.dates)
        self._file_permissions = None

    @property
    def file_permissions(self):
        if self._file_permissions is None:
            self._update()
        return self._file_permissions


    @function_timer
    @prompt_password
    def make_nwb_files(self, **kwargs):
        
        # Convert raw to .nwb files
        for date in self.dates:
            self._convert_rec_to_nwb(date, pwd=kwargs['pwd'])

    @function_timer
    def _convert_rec_to_nwb(self, date, pwd=None):

        # Get .nwb file full name and .nwb file builder
        nwb_full_name = self._nwb_converter._file_info.search_expected_file_names(date + ".nwb").iloc[0].full_name
        nwb_builder = self._nwb_converter.nwb_builders[date]

        # Ensure .nwb doesn't already exist if file overwriting is disabled
        file_exists_bool, nwb_full_name = self._validate_nwb_file_existence(date)
        nwb_file_path, _ = FileNameHelper.convert_full_names_to_names(nwb_full_name)
        verbose_printer.print_header(f"{nwb_file_path}", f"Writing .nwb file")
        if not self._overwrite and file_exists_bool:
            no_overwrite_handler_file(f"File '{nwb_full_name}' already exists in'{nwb_file_path}'")
            content = ''
        else:
            # Create .nwb file
            content = self._write_nwb_file(nwb_full_name, nwb_builder, pwd=pwd)

        # Print verbose .nwb conversion output
        self._nwb_file_writer_printer(content)
        pass_bool = nwb_full_name in [file.path for file in os.scandir(nwb_file_path) if file.is_file()]
        self._nwb_file_verification_printer(pass_bool, date)

    def _write_nwb_file(self, nwb_full_name, builder, pwd=None):
        
        # Write .nwb file
        content = builder.build_nwb()
        # Make file globally readable, writable, and executable
        if self._universal_access:
            universal_file_permissions(nwb_full_name, pwd=pwd)

        # Handle .nwb and .h264 files created in the .rec to .nwb conversion
        self._nwb_converter_file_handler(builder.dates[0], nwb_full_name)
        # Delete preprocessing files
        builder.cleanup()
        return content

    def _validate_nwb_file_existence(self, date):

        # Get full path of .nwb file whose existence is being verified
        nwb_full_name = self._file_info.search_expected_file_names([date, '.nwb']).iloc[0].full_name
        nwb_full_names_list = self._file_permissions.full_name.tolist()
        # Check if expected .nwb file is in the list of all existing .nwb files
        file_exists_bool = nwb_full_name in nwb_full_names_list
        return file_exists_bool, nwb_full_name


    def _nwb_converter_file_handler(self, date, nwb_full_name):
        
        # Create symlinks of .h264 video file and newly created .nwb file
        video_files_df = self._file_info.search_matching_file_names([date, '.h264'])
        video_full_names_list = video_files_df[video_files_df.file_type == 'video'].full_name.tolist()
        self._rename_nwb_file(date, nwb_full_name)
        self._create_nwb_symlink(nwb_full_name)
        for video_full_name in video_full_names_list:
            self._create_video_symlink(video_full_name)
    
    def _rename_nwb_file(self, date, nwb_full_name):

        # Rename .nwb file and its Spyglass symlink from their default names
        nwb_file_path, _ = FileNameHelper.convert_full_names_to_names(nwb_full_name)        
        # Get old file name
        # By default, this is subject_name + date + '.nwb'
        old_file_name = self.subject_name + date + '.nwb'
        # Rename .nwb files in raw data directory
        os.rename(os.path.join(nwb_file_path, old_file_name), nwb_full_name)

    def _create_nwb_symlink(self, nwb_full_name):
        
        # Create soft copy of .nwb files in Spyglass raw .nwb file directory
        spyglass_nwb_path = NWBConverter.spyglass_nwb_path
        create_symlink_in_directory(nwb_full_name, spyglass_nwb_path, overwrite=self._overwrite)

    def _create_video_symlink(self, video_full_name):
        
        # Create soft copies of .h264 files in Spyglass video .nwb file directory
        spyglass_video_path = NWBConverter.spyglass_video_path
        create_symlink_in_directory(video_full_name, spyglass_video_path, overwrite=self._overwrite)


    def _update(self):

        # Update file and permissions info
        self._file_info.update()
        self._nwb_converter.update()
        self._permissions_manager.update()
        # Get existing .nwb file permissions
        self._file_permissions = self._permissions_manager.search_file_names('.nwb')
        self._file_permissions.reset_index(inplace=True, drop=True)


    def _print(self, dates):
        
        for date in dates:
            # Print .nwb file existence and permissions for the given date
            verbose_printer.print_header(f"{self.subject_name} {date}")
            file_exists_bool, nwb_full_name = self._validate_nwb_file_existence(date)
            if file_exists_bool:
                verbose_printer.print_pass(f"Found .nwb file {nwb_full_name}")
            else:
                verbose_printer.print_fail(f"Couldn't find .nwb file {nwb_full_name}")
            perm = self._permissions_manager.search_file_names([date, '.nwb']).iloc[0].permissions
            if perm == '777':
                verbose_printer.print_warning(f"File is globally readable, writable, and executable")
            verbose_printer.print_text(f"{self._nwb_converter._nwb_metadata[date]}")
            verbose_printer.print_newline()
            verbose_printer.print_text(f"{self._nwb_converter._nwb_builders[date]}")
            verbose_printer.print_newline()
    
    def _nwb_file_writer_printer(self, content):

        # Print verbose built .nwb file contents
        verbose_printer.verbose_print(self._verbose, f"{content}")

    def _nwb_file_verification_printer(self, pass_bool, date):
        
        if pass_bool:
            verbose_printer.print_pass(f"{self.subject_name} {date} .nwb file created")
            if self._universal_access:
                verbose_printer.print_warning(f"File is globally readable, writable, and executable")
        else:
            verbose_printer.print_fail(f"{self.subject_name} {date} .nwb file not created or not found")
        verbose_printer.print_newline()
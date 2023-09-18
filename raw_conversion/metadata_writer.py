import os

from .file_info import FileInfo
from .file_name_helper import FileNameHelper
from .metadata_converter import MetadataConverter

from utils import verbose_printer
from utils.abstract_classes import DataWriter
from utils.file_helpers import (no_overwrite_handler_file, universal_file_permissions)
from utils.function_wrappers import (function_timer, prompt_password)
from utils.permissions_manager import PermissionsManager

class MetadataWriter(DataWriter):

    def __init__(self, subject_name, dates=None, overwrite=False, universal_access=False, verbose=False, timing=False):

        # Enable overwriting existing .yml files, verbose output, and timing
        super().__init__(subject_name, dates=dates, overwrite=overwrite, universal_access=universal_access, verbose=verbose, timing=timing)

        # Get file information for raw data and metadata
        self._file_info = FileInfo(self.subject_name, dates=self.dates)
        self._metadata_converter = MetadataConverter(self.subject_name, dates=self.dates) 
        self._permissions_manager = PermissionsManager(self.subject_name, dates=self.dates)
        # Create .yml file contents from .csv metadata
        self._metadata = None
        self._file_permissions = None

    @property
    def metadata(self):
        if self._metadata is None:
            self._update()
        return self._metadata
    
    @property
    def file_permissions(self):
        if self._file_permissions is None:
            self._update()
        return self._file_permissions
  

    @function_timer
    @prompt_password
    def make_yml_files(self, **kwargs):

        self._update()
        # Create a .yml metadata file for each date
        for date in self.dates:
            self._create_yml_file_from_metadata(date, pwd=kwargs['pwd'])

    @function_timer
    def _create_yml_file_from_metadata(self, date, pwd=None):

        # Ensure .yml doesn't already exist if file overwriting is disabled
        file_exists_bool, yml_full_name = self._validate_yml_file_existence(date)
        yml_file_path, _ = FileNameHelper.convert_full_names_to_names(yml_full_name)
        verbose_printer.print_header(f"{yml_file_path}", f"Writing .yml file")
        if not self._overwrite and file_exists_bool:
            no_overwrite_handler_file(f"File '{yml_full_name}' already exists in'{yml_file_path}'")
        else:
            # Create .yml file
            self._write_yml_file(yml_full_name, date, pwd=pwd)
        
        # Print verbose metadata output
        self._yml_file_writer_printer(self._metadata[date])
        pass_bool = yml_full_name in [file.path for file in os.scandir(yml_file_path) if file.is_file()]
        self._yml_file_verification_printer(pass_bool, date)

    def _validate_yml_file_existence(self, date):

        # Get full path of .yml file whose existence is being verified
        yml_full_name = self._file_info.search_expected_file_names([date, '.yml']).iloc[0].full_name
        yml_full_names_list = self._file_permissions.full_name.tolist()
        # Check if expected .yml file is in the list of all existing .yml files
        file_exists_bool = yml_full_name in yml_full_names_list
        return file_exists_bool, yml_full_name

    def _write_yml_file(self, yml_full_name, date, pwd=None):

        # Get .yml file as plain text
        txt = self._metadata[date]
        # Write .yml file
        with open(yml_full_name, 'w') as writer:
            writer.write(txt)
        # Make file globally readable, writable, and executable
        if self._universal_access:
            universal_file_permissions(yml_full_name, pwd=pwd)

      
    def _update(self):
        
        self._file_info.update()
        self._metadata_converter.update()
        self._permissions_manager.update()
        # Get existing .yml file permissions
        self._file_permissions = self._permissions_manager.search_file_names('.yml')
        self._file_permissions.reset_index(inplace=True, drop=True)
        # Update .yml file metadata and convert to plain text
        self._metadata = {date : None for date in self.dates}
        for date in self.dates:
            if self._validate_yml_file_existence(date)[0]:
                self._metadata[date] = self._metadata_converter.metadata_to_text(date)
 

    def _print(self, dates):

        for date in dates:
            # Print .yml file existence and permissions for the given date
            verbose_printer.print_header(f"{self.subject_name} {date}")
            file_exists_bool, yml_full_name = self._validate_yml_file_existence(date)
            if file_exists_bool:
                verbose_printer.print_pass(f"Found .yml file {yml_full_name}")
            else:
                verbose_printer.print_fail(f"Couldn't find .yml file {yml_full_name}")
            perm = self._permissions_manager.search_file_names([date, '.yml']).iloc[0].permissions
            if perm == '777':
                verbose_printer.print_warning(f"File is globally readable, writable, and executable")
            verbose_printer.print_text(f"{self._metadata[date]}")
            verbose_printer.print_newline()

    def _yml_file_writer_printer(self, yml_txt):

        # Print verbose .yml file content output
        verbose_printer.verbose_print(self._verbose, f"{yml_txt}")

    def _yml_file_verification_printer(self, pass_bool, date):

        if pass_bool:
            verbose_printer.print_pass(f"{self.subject_name} {date} .yml file exists")
            if self._universal_access:
                verbose_printer.print_warning(f"File is globally readable, writable, and executable")
        else:
            verbose_printer.print_fail(f"{self.subject_name} {date} .yml file not found")
        verbose_printer.print_newline()


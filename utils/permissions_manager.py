import pandas as pd
import os

from raw_conversion.file_info import FileInfo

from utils import verbose_printer
from utils.abstract_classes import DataReader
from utils.data_helpers import parse_iterable_inputs
from utils.file_helpers import (universal_directory_permissions,
                                universal_read_only_directory,
                                restrict_directory_permissions,
                                restrict_read_only_directory)

class PermissionsManager(DataReader):

    def __init__(self, subject_name, dates=None):
        
        super().__init__(subject_name, dates=dates, verbose=False, timing=False)
        # Get top level data directory for the given subject
        self._subject_data_path = os.path.join(self._data_path, self._subject_name)
        # Instantiate file info to get information about data files
        self._file_info = FileInfo(self._subject_name, dates=self._dates) 
        self._update()
    
    @property
    def permissions(self):
        self._update_file_permissions()
        return self._permissions
    
    def search_file_names(self, match_patterns):
        
        match_patterns = parse_iterable_inputs(match_patterns)
        self._update()
        # Get the file names that match the text pattern
        file_names_df = self._permissions[self._permissions.is_file]
        for txt in match_patterns:
            file_names_df = file_names_df[file_names_df.file_name.str.contains(txt)]
        return file_names_df

    def search_path_names(self, match_patterns):

        match_patterns = parse_iterable_inputs(match_patterns)
        self._update()
        # Get the path names that match the text pattern
        path_names_df = self._permissions[~self._permissions.is_file]
        for txt in match_patterns:
            path_names_df = path_names_df[path_names_df.file_name.str.contains(txt)]
        return path_names_df


    def grant_universal_access(self):

        # Grant universal read and write access to all data files
        universal_directory_permissions(self._subject_data_path)
    
    def grant_universal_read_only_access(self):

        # Grant universal read-only access to all data files
        universal_read_only_directory(self._subject_data_path)
    
    def restrict_access(self):

        # Restrict read and write access to the current user
        restrict_directory_permissions(self._subject_data_path)
    
    def read_only_directory(self):

        # Make directory read-only only to current user
        restrict_read_only_directory(self._subject_data_path)


    def _update(self):

        # Update file name information and read, write, and execute status
        self._file_info._update()
        self._update_file_permissions()

    def _update_file_permissions(self):

        permissions_df = self._get_path_and_file_info()
        # Check permissions of each file and path
        permissions_df["permissions"] = None
        for ndx, full_name in enumerate(permissions_df.full_name.values):
            # Get last 3 digits of octal permission code
            oct_perm = oct(os.stat(full_name).st_mode)
            permissions_df.at[ndx, 'permissions'] = oct_perm[-3:]
        self._permissions = permissions_df
    
    def _get_path_and_file_info(self):

        # Initialize a dataframe of file paths and full file names
        path_names = self._file_info._file_paths.path_name.tolist()
        path_types = self._file_info._file_paths.file_type.tolist()
        full_names = self._file_info._file_names.full_name.tolist()
        file_types = self._file_info._file_names.file_type.tolist()
        file_names = self._file_info._file_names.file_name.tolist()
        # Create indicator of whether a name is a file or not
        file_ind = [True]*len(full_names)
        file_ind.extend([False]*len(path_names))
        # Merge path and file names
        file_types.extend(path_types)
        full_names.extend(path_names)
        file_names.extend(path_names)
        
        permissions_df = pd.DataFrame({'file_name' : file_names, 'full_name' : full_names, 'file_type' : file_types, 'is_file' : file_ind})
        return permissions_df

    
    def _print(self):
        
        # Print file paths and expected file names associated with subject
        verbose_printer.print_header(f"{self._subject_name} data files and permissions")
        for file_type in self._file_info._file_name_helper._file_types:
            # Get full file names of the specified file type
            ind = self._permissions.file_type == file_type
            full_names = self._permissions[ind].full_name.tolist()
            permission_codes = self._permissions[ind].permissions.tolist()
            for perm_code, full_name in zip(permission_codes, full_names):
            
                msg = perm_code + '  ' + full_name
                if perm_code == '777':
                    # Print file name that has universal read and write access
                    verbose_printer.print_fail(msg)
                elif perm_code == '444':
                    # Print file name that has universal read-only access
                    verbose_printer.print_bold(msg)
                elif perm_code == '700':
                    # Print file name that has read and write access only by current user
                    verbose_printer.print_exclusion(msg)
                elif perm_code == '400':
                    # Print file name that is read-only only by current user
                    verbose_printer.print_pass(msg)
        verbose_printer.print_newline()
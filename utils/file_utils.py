import itertools
import os
import pandas as pd
import re

from .common_abstract_classes import DataReader
from .common_helpers import (print_verbose, function_timer)
from .file_name_helpers import FileNameHelper
    
class FileInfo(DataReader):

    from .user_settings import data_path
    _data_path = data_path
    # Tuple of valid path types
    _path_types = ('sessions', 'raw', 'metadata', 'yml', 'nwb', 'video')
    # Tuple of valid file types
    _file_types = ('raw', 'metadata', 'yml', 'nwb', 'video')

    def __init__(self, subject_name, dates=None, verbose=False, timing=False):
        
        # Enable verbose output and timing
        super().__init__(verbose=verbose, timing=timing)
        # Ensure subject name is valid
        self._validate_subject_name(subject_name)
        self._subject_name = subject_name

        # Instantiate file name helper
        self._file_name_helper = FileNameHelper(self._subject_name)
        
        # Ensure list of dates is valid, or use all dates if none provided
        if dates is None:
            dates = self.get_session_dates()
        self._validate_dates(dates)
        self._dates = dates
        self._n_dates = len(self._dates)
        # Update file name helper with dates info
        self._file_name_helper._dates = self._dates
        self._file_name_helper._n_dates = self._n_dates

        # Get number of epochs for each date
        self._n_epochs = self.get_number_of_epochs()
        # Update file name helper with epoch info
        self._file_name_helper._n_epochs = self._n_epochs
        
        # Get file paths and file names, expected file names, and matching and missing file names
        self._update()
    
    @property
    def data_path(self):
        return self._data_path

    @property
    def subject_name(self):
        return self._subject_name
    
    @property
    def dates(self):
        return self._dates

    @property
    def file_paths(self):
        self._update_file_paths()
        return self._file_paths
    
    @property
    def file_names(self):
        self._update_file_names()
        return self._file_names
    
    @property
    def expected_file_names(self):
        self._update_expected_file_names()
        return self._expected_file_names

    @property
    def matching_file_names(self):
        self._update_matching_and_missing_file_names()
        return self._matching_file_names

    @property
    def missing_file_names(self):
        self._update_matching_and_missing_file_names()
        return self._missing_file_names


    def get_subject_names(self):

        # Get all folder names in the data path
        subjects_list = [folder.name for folder in os.scandir(self._data_path) if folder.is_dir()]
        return subjects_list

    def get_session_dates(self):

        # Get all folder names in subject's raw data directory
        sessions_path = self._file_name_helper._get_sessions_path()[0]
        dates = sorted([folder.name for folder in os.scandir(sessions_path) if folder.is_dir()])
        return dates
    
    def get_number_of_epochs(self, dates=None):

        # Determine number of .rec files in the raw data directory
        raw_file_names = self._get_raw_file_names()
        n_epochs = [len([name for name in file_names if re.fullmatch('^.*\\.rec$', name)]) for file_names in raw_file_names]
        # If dates are specified, return number of epochs for just those dates
        if dates is not None:
            # Ensure specified dates are valid
            dates_diff = set(dates) - set(self._dates)
            if dates_diff:
                raise ValueError(f"The following dates weren't found in the FileInfo instance: {dates_diff}")
            n_epochs = [count for (count, date) in zip(n_epochs, self._dates) if date in dates]
        
        return n_epochs


    @function_timer
    def search_file_names(self, match_pattern):
        
        self._update_file_names()
        # Get the file names that match the text pattern
        ind = self._file_names.file_name.str.contains(match_pattern)
        return self._file_names[ind].sort_values(by=['file_name'])

    @function_timer
    def search_expected_file_names(self, match_pattern):
        
        self._update_expected_file_names()
        # Get the expected file names that match the text pattern
        ind = self._expected_file_names.file_name.str.contains(match_pattern)
        return self._expected_file_names[ind].sort_values(by=['file_name'])
    
    @function_timer
    def search_matching_file_names(self, match_pattern):
        
        self._update_matching_and_missing_file_names()
        # Get the matching file names that match the text pattern
        ind = self._matching_file_names.file_name.str.contains(match_pattern)
        return self._matching_file_names[ind].sort_values(by=['file_name'])

    @function_timer
    def search_missing_file_names(self, match_pattern):
        
        self._update_matching_and_missing_file_names()
        # Get the missing file names that match the text pattern
        ind = self._missing_file_names.file_name.str.contains(match_pattern)
        return self._missing_file_names[ind].sort_values(by=['file_name'])

    @function_timer
    def verify_file_names(self):

        self._update()
        # Check if file names match expected file names, and if all expected files are present
        for file_type in self._file_types:
            self._verify_file_names_by_type(file_type)


    def _update(self):

        # Update all file information
        self._update_file_paths()
        self._update_file_names()
        self._update_expected_file_names()
        self._update_matching_and_missing_file_names()

    def _update_file_paths(self):

        # Get all file paths
        file_paths_list = [None]*len(FileInfo._path_types)
        for ndx, path_type in enumerate(FileInfo._path_types):
            file_paths_list[ndx] = self._file_name_helper._get_file_paths_dataframe_by_type(path_type)
        self._file_paths = pd.concat(file_paths_list)

    def _update_file_names(self):

        # Get file names
        file_names_list = [None]*len(FileInfo._file_types)
        for ndx, file_type in enumerate(FileInfo._file_types):
            file_names_list[ndx] = self._get_file_names_dataframe_by_type(file_type)
        self._file_names = pd.concat(file_names_list)

    def _update_expected_file_names(self):

        # Get expected file names
        expected_file_names_list = [None]*len(FileInfo._file_types)
        for ndx, file_type in enumerate(FileInfo._file_types):
            expected_file_names_list[ndx] = self._file_name_helper._get_expected_file_names_dataframe_by_type(file_type)
        self._expected_file_names = pd.concat(expected_file_names_list)

    def _update_matching_and_missing_file_names(self):

        # Get file names that match expected names and expected names with no corresponding file names
        matching_file_names_list = [None]*len(FileInfo._file_types)
        missing_file_names_list = [None]*len(FileInfo._file_types)
        for ndx, file_type in enumerate(FileInfo._file_types):
            matching_file_names_list[ndx], missing_file_names_list[ndx] = self._get_matching_and_missing_file_names_by_type(file_type)
        self._matching_file_names = pd.concat(matching_file_names_list)
        self._missing_file_names = pd.concat(missing_file_names_list)


    def _get_file_names_dataframe_by_type(self, file_type):

        # Get list of file names of the specified type    
        if file_type == 'raw':
            file_names = self._get_raw_file_names()
        elif file_type == 'metadata':
            file_names = self._get_metadata_file_names()
        elif file_type == 'yml':
            file_names = self._get_yml_file_names()
        elif file_type == 'nwb':
            file_names = self._get_nwb_file_names()
        elif file_type == 'video':
            file_names = self._get_video_file_names()
        else:
            raise NotImplementedError(f"Couldn't get file names for file type '{file_type}'")

        # Get full names for all file names and transform into a dataframe
        path_names, file_names, full_names = self._transform_file_names(file_type, file_names)
        file_names_data = zip( list(itertools.chain(*path_names)), list(itertools.chain(*file_names)), list(itertools.chain(*full_names)) )
        file_names_df = pd.DataFrame(file_names_data, columns=['path_name', 'file_name', 'full_name'])
        file_names_df['file_type'] = file_type
        return file_names_df

    def _get_raw_file_names(self):

        # Get all raw data files for the given subject and dates
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_paths_by_file_type('raw')
        assert len(file_names) == len(file_paths), \
            f"Number of dates doesn't match number of folders with raw data"
        
        for ndx, date in enumerate(self._dates):
            name_prefix = self._file_name_helper._get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*'
            file_names[ndx] = self._scan_dir_filtered_file_names(file_paths[ndx], match_pattern, full_match=True)
        return file_names

    def _get_metadata_file_names(self):

        # Get all metadata files for the given subject
        match_pattern = '.*'
        file_paths = self._file_name_helper._get_paths_by_file_type('metadata')
        file_names = [self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)]
        return file_names

    def _get_yml_file_names(self):

        # Get all .yml files for the given subject
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_paths_by_file_type('yml')
        for ndx, date in enumerate(self._dates):
            name_prefix = self._file_name_helper._get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*\\.yml$'
            file_names[ndx] = self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)
        return file_names

    def _get_nwb_file_names(self):

        # Get all .nwb files for the given subject
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_paths_by_file_type('nwb')
        for ndx, date in enumerate(self._dates):
            match_pattern = ''.join(['^', self._subject_name, '_', date, '.*\\.nwb$'])
            file_names[ndx] = self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)
        return file_names

    def _get_video_file_names(self):

        # Get all .h264 video files for the given subject
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_paths_by_file_type('video')
        for ndx, date in enumerate(self._dates):
            name_prefix = self._file_name_helper._get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*\\.h264$'
            file_names[ndx] = self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)
        return file_names


    def _get_matching_and_missing_file_names_by_type(self, file_type):

        # Get list of matching and missing file names of specified type
        file_names_dict = {'matching' : None, 'missing' : None}
        file_names_dict['matching'], file_names_dict['missing'] = self._get_matching_and_missing_file_names(file_type)

        # Get full names for all matching and missing file names and transform into dataframes
        file_names_df_dict = {'matching' : None, 'missing' : None}
        for key in file_names_df_dict.keys():
            path_names, file_names, full_names = self._transform_file_names(file_type, file_names_dict[key])
            file_names_data = zip( list(itertools.chain(*path_names)), list(itertools.chain(*file_names)), list(itertools.chain(*full_names)) )
            file_names_df_dict[key] = pd.DataFrame(file_names_data, columns=['path_name', 'file_name', 'full_name'])
            file_names_df_dict[key]['file_type'] = file_type
        return file_names_df_dict['matching'], file_names_df_dict['missing']

    def _get_matching_and_missing_file_names(self, file_type):

        # Check file names within each file path for the given file type
        path_names_list = self._file_name_helper._get_paths_by_file_type(file_type)
        matching_names = [None]*len(path_names_list)
        missing_names = [None]*len(path_names_list)
        for ndx, path_name in enumerate(path_names_list):
            # Get full file names and full expected names
            file_names = self._file_names[ (self._file_names.file_type == file_type) & \
                                           (self._file_names.path_name == path_name) ].full_name.tolist()
            expected_names = self._expected_file_names[ (self._expected_file_names.file_type == file_type) & \
                                                        (self._expected_file_names.path_name == path_name) ].full_name.tolist()
            # Compare file names
            matching_names[ndx], missing_names[ndx] = FileNameHelper._compare_file_names(file_names, expected_names)
            # Convert full names to file names
            _, matching_names[ndx] = FileNameHelper._full_names_to_file_names(matching_names[ndx])
            _, missing_names[ndx] = FileNameHelper._full_names_to_file_names(missing_names[ndx])
        return matching_names, missing_names

    def _scan_dir_filtered_file_names(self, file_path, pattern, full_match=False):

        # Find all files in specified file path matching the regular expression pattern
        file_names = [file.name for file in os.scandir(file_path) if file.is_file()]
        matching_names = FileNameHelper._file_names_regex(file_names, pattern, full_match=full_match)
        return matching_names

    def _transform_file_names(self, file_type, file_names):

        # Get full names and paths for file names of the specified type
        file_paths = self._file_name_helper._get_paths_by_file_type(file_type)
        return FileNameHelper._get_filtered_file_names_info(file_paths, file_names)


    def _validate_subject_name(self, subject_name):

        # Compare the subject name to all valid subject names
        valid_names = self.get_subject_names()
        if subject_name not in valid_names:
            raise ValueError(f"No subject named '{subject_name}' found among the names {valid_names}")

    def _validate_dates(self, dates):

        # Ensure that no duplicate dates are present
        if len(dates) != len(set(dates)):
            raise ValueError(f"Duplicate dates found in dates list {dates}")
        # Compare list of dates to all valid dates for the given subject
        valid_dates = self.get_session_dates()
        dates_diff = set(dates) - set(valid_dates)
        if dates_diff:
            raise ValueError(f"No data found for subject '{self._subject_name}' on the dates {list(dates_diff)}")


    def _verify_file_names_by_type(self, file_type):

        # Check file names within each path corresponding to the given file type
        path_names_list = self._file_name_helper._get_paths_by_file_type(file_type)
        for path_name in path_names_list:
            matching_names = self._matching_file_names[ (self._matching_file_names.file_type == file_type) & \
                                                        (self._matching_file_names.path_name == path_name) ].file_name.tolist()
            missing_names = self._missing_file_names[ (self._missing_file_names.file_type == file_type) & \
                                                      (self._missing_file_names.path_name == path_name) ].file_name.tolist()
            
            # Print verbose output for missing and matching files
            print_verbose(self._verbose,
                          f"{self._subject_name} {file_type} files",
                          f"Checking in {path_name}")
            self._file_name_comparison_printer(sorted(matching_names), sorted(missing_names))
    
    def _file_name_comparison_printer(self, matching_file_names, missing_file_names):

        # Print verbose output for number of files expected and actually found
        print_verbose(self._verbose,
                      f"{len(matching_file_names)} actual files match",
                      f"{len(missing_file_names)} expected files missing")
        # Print expected file names that correspond to an actual file
        sep = '\n'
        if matching_file_names:
            print_verbose(self._verbose,
                          f"{sep.join(matching_file_names)}",
                          color='green')
        # Print expected file names that don't correspond to an actual file
        if missing_file_names:
            print_verbose(self._verbose,
                          f"{sep.join(missing_file_names)}",
                          color='red')
        print_verbose(self._verbose,
                      f"\n")

    def _file_not_found_handler(self, missing_names):
        
        # Print a warning if expected raw data files are missing
        print_verbose(self._verbose,
                      f"No files named {missing_names}")
    
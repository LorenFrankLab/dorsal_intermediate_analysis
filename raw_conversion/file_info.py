import itertools
import os
import pandas as pd

from .file_name_helper import FileNameHelper

from utils import verbose_printer
from utils.abstract_classes import DataReader
from utils.data_helpers import parse_iterable_inputs
from utils.function_wrappers import function_timer
    
class FileInfo(DataReader):

    def __init__(self, subject_name, dates=None, verbose=False, timing=False):
        
        super().__init__(subject_name, dates=dates, verbose=verbose, timing=timing)

        # Instantiate file name helper
        self._file_name_helper = FileNameHelper(self._subject_name, dates=self._dates)
        # Get file paths and file names, expected file names, and matching and missing file names
        self._update()

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


    def search_file_names(self, match_patterns):

        match_patterns = parse_iterable_inputs(match_patterns)
        self._update_file_names()
        # Get the file names that match the text pattern
        file_names_df = self._file_names
        for txt in match_patterns:
            file_names_df = file_names_df[file_names_df.file_name.str.contains(txt)]
        return file_names_df

    def search_expected_file_names(self, match_patterns):
        
        match_patterns = parse_iterable_inputs(match_patterns)
        self._update_expected_file_names()
        # Get the expected file names that match the text pattern
        expected_file_names_df = self._expected_file_names
        for txt in match_patterns:
            expected_file_names_df = expected_file_names_df[expected_file_names_df.file_name.str.contains(txt)]
        return expected_file_names_df
    
    def search_matching_file_names(self, match_patterns):
        
        match_patterns = parse_iterable_inputs(match_patterns)
        self._update_matching_and_missing_file_names()
        # Get the matching file names that match the text pattern
        matching_file_names_df = self._matching_file_names
        for txt in match_patterns:
            matching_file_names_df = matching_file_names_df[matching_file_names_df.file_name.str.contains(txt)]
        return matching_file_names_df

    def search_missing_file_names(self, match_patterns):
        
        match_patterns = parse_iterable_inputs(match_patterns)
        self._update_matching_and_missing_file_names()
        # Get the missing file names that match the text pattern
        missing_file_names_df = self._missing_file_names
        for txt in match_patterns:
            missing_file_names_df = missing_file_names_df[missing_file_names_df.file_name.str.contains(txt)]
        return missing_file_names_df

    @function_timer
    def verify_file_names(self):

        self._update()
        # Check if file names match expected file names, and if all expected files are present
        for file_type in FileNameHelper._file_types:
            self._verify_file_names_by_type(file_type)

    def _verify_file_names_by_type(self, file_type):

        # Check file names within each path corresponding to the given file type
        path_names_list = self._file_name_helper._get_file_paths_by_type(file_type)
        for ndx, path_name in enumerate(path_names_list):
            if file_type == 'raw':
                date = self._dates[ndx]
            else:
                date = 'all dates'
            verbose_printer.print_header(f"Checking {self._subject_name} {file_type} files {date}", f"in path {path_name}")  
            matching_names = self._matching_file_names[ (self._matching_file_names.file_type == file_type) & \
                                                        (self._matching_file_names.path_name == path_name) ].file_name.tolist()
            missing_names = self._missing_file_names[ (self._missing_file_names.file_type == file_type) & \
                                                      (self._missing_file_names.path_name == path_name) ].file_name.tolist()
            

            # Print verbose output for missing and matching files
            self._file_name_comparison_printer(sorted(matching_names), sorted(missing_names))
            # Print status of matching and missing files check
            pass_bool = not missing_names
            self._file_name_verification_printer(pass_bool, file_type, date)


    def _update(self):

        # Update all file information
        self._update_file_paths()
        self._update_file_names()
        self._update_expected_file_names()
        self._update_matching_and_missing_file_names()

    def _update_file_paths(self):

        # Get all file paths
        self._file_name_helper._update_file_paths()
        self._file_paths = self._file_name_helper._file_paths

    def _update_file_names(self):

        # Get file names
        file_names_list = [None]*len(FileNameHelper._file_types)
        for ndx, file_type in enumerate(FileNameHelper._file_types):
            file_names_list[ndx] = self._get_file_names_dataframe_by_type(file_type)
        self._file_names = pd.concat(file_names_list)
        self._file_names.reset_index(inplace=True, drop=True)

    def _update_expected_file_names(self):

        # Get expected file names
        self._file_name_helper._update_expected_file_names
        self._expected_file_names = self._file_name_helper._expected_file_names

    def _update_matching_and_missing_file_names(self):

        # Get file names that match expected names and expected names with no corresponding file names
        matching_file_names_list = [None]*len(FileNameHelper._file_types)
        missing_file_names_list = [None]*len(FileNameHelper._file_types)
        for ndx, file_type in enumerate(FileNameHelper._file_types):
            matching_file_names_list[ndx], missing_file_names_list[ndx] = self._get_matching_and_missing_file_names_by_type(file_type)
        self._matching_file_names = pd.concat(matching_file_names_list)
        self._matching_file_names.reset_index(inplace=True, drop=True)
        self._missing_file_names = pd.concat(missing_file_names_list)
        self._missing_file_names.reset_index(inplace=True, drop=True)


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
        file_paths = self._file_name_helper._get_file_paths_by_type('raw')
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
        file_paths = self._file_name_helper._get_file_paths_by_type('metadata')
        file_names = [self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)]
        return file_names

    def _get_yml_file_names(self):

        # Get all .yml files for the given subject
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_file_paths_by_type('yml')
        for ndx, date in enumerate(self._dates):
            name_prefix = self._file_name_helper._get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*\\.yml$'
            file_names[ndx] = self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)
        return file_names

    def _get_nwb_file_names(self):

        # Get all .nwb files for the given subject
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_file_paths_by_type('nwb')
        for ndx, date in enumerate(self._dates):
            match_pattern = ''.join(['^', self._subject_name, '_', date, '.*\\.nwb$'])
            file_names[ndx] = self._scan_dir_filtered_file_names(file_paths[0], match_pattern, full_match=True)
        return file_names

    def _get_video_file_names(self):

        # Get all .h264 video files for the given subject
        file_names = [None]*self._n_dates
        file_paths = self._file_name_helper._get_file_paths_by_type('video')
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
        path_names_list = self._file_name_helper._get_file_paths_by_type(file_type)
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
        file_paths = self._file_name_helper._get_file_paths_by_type(file_type)
        return FileNameHelper._get_filtered_file_names_info(file_paths, file_names)


    def _print(self, dates):

        # Print list of all file names for the given date
        for date in dates:
            verbose_printer.print_header(f"{self._subject_name} {date}")
            for file_type in FileNameHelper._file_types:
                # Get full file names of the specified file type for the given date
                full_names = self.search_file_names(date)
                full_names = full_names[full_names.file_type == file_type].full_name.tolist()
                for full_name in full_names:
                    # Print expected file name that is missing
                    if full_name in self._missing_file_names.full_name.values:
                        verbose_printer.verbose_print(True, full_name, color='red')
                    else:
                        # Print expected file name that is matching
                        if full_name in self._matching_file_names.full_name.values:
                            verbose_printer.verbose_print(True, full_name, color='green')
                        # Print actual file name that isn't expected
                        else:
                            verbose_printer.verbose_print(True, full_name)
            verbose_printer.print_newline()

    def _file_name_comparison_printer(self, matching_file_names, missing_file_names):

        matching_file_names, missing_file_names = parse_iterable_inputs(matching_file_names, missing_file_names)
        # Print verbose output for number of files expected and actually found
        verbose_printer.print_text(f"{len(matching_file_names)} actual files match", f"{len(missing_file_names)} expected files missing")
        # Print expected file names that correspond to an actual file
        sep = '\n'
        if matching_file_names:
            verbose_printer.verbose_print(self._verbose, f"{sep.join(matching_file_names)}", color='green')
        # Print expected file names that don't correspond to an actual file
        if missing_file_names:
            verbose_printer.verbose_print(self._verbose, f"{sep.join(missing_file_names)}", modifier='bold', color='red')

    def _file_name_verification_printer(self, pass_bool, file_type, date):

        if pass_bool:
            verbose_printer.print_pass(f"{self._subject_name} {file_type} files {date} all files found")
        else:
            verbose_printer.print_fail(f"{self._subject_name} {file_type} files {date} some files missing")
        verbose_printer.print_newline()
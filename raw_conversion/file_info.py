import pandas as pd

from .file_name_helper import FileNameHelper

from utils import verbose_printer
from utils.abstract_classes import DataReader
from utils.data_helpers import parse_iterable_inputs, parse_iterable_outputs
from utils.function_wrappers import function_timer
    
class FileInfo(DataReader):

    def __init__(self, subject_name, dates=None, verbose=False, timing=False):
        
        super().__init__(subject_name, dates=dates, verbose=verbose, timing=timing)

        # Instantiate file name helper
        self._file_name_helper = FileNameHelper(self.subject_name, dates=self.dates)
        self._path_names = None
        self._file_names = None
        self._expected_file_names = None
        self._matching_file_names = None
        self._missing_file_names = None

    @property
    def path_names(self):
        if self._path_names is None:
            self._update()
        return self._path_names
    
    @property
    def file_names(self):
        if self._file_names is None:
            self._update()
        return self._file_names
    
    @property
    def expected_file_names(self):
        if self._expected_file_names is None:
            self._update()
        return self._expected_file_names

    @property
    def matching_file_names(self):
        if self._matching_file_names is None:
            self._update()
        return self._matching_file_names

    @property
    def missing_file_names(self):
        if self._missing_file_names is None:
            self._update()
        return self._missing_file_names


    @function_timer
    def verify_file_names(self):

        self._update()
        # Check if file names match expected file names, and if all expected files are present
        for file_type in FileNameHelper.file_types:
            self._verify_file_names_by_type(file_type)

    def _verify_file_names_by_type(self, file_type):

        # Check file names within each path corresponding to the given file type
        path_names = self._get_path_names_by_type(file_type)
        path_names = parse_iterable_inputs(path_names)
        for ndx, path_name in enumerate(path_names):
            date = self.dates[ndx] if file_type == 'raw' else 'all dates'
            verbose_printer.print_header(f"Checking {self.subject_name} {file_type} files {date}", f"in path {path_name}")
            # Get the names of matching and missing files
            matching_ind = (self._matching_file_names.file_type == file_type) & (self._matching_file_names.path_name == path_name)
            missing_ind = (self._missing_file_names.file_type == file_type) & (self._missing_file_names.path_name == path_name)
            matching_names = self._matching_file_names[matching_ind].file_name.tolist()
            missing_names = self._missing_file_names[missing_ind].file_name.tolist()
            
            # Print verbose output for missing and matching files
            self._file_name_comparison_printer(sorted(matching_names), sorted(missing_names))
            # Print status of matching and missing files check
            pass_bool = not missing_names
            self._file_name_verification_printer(pass_bool, file_type, date)

    def search_path_names(self, match_patterns):

        match_patterns = parse_iterable_inputs(match_patterns)
        self._update_path_names()
        # Get the path names that match the text pattern
        path_names_df = self._path_names
        for txt in match_patterns:
            path_names_df = path_names_df[path_names_df.file_name.str.contains(txt)]
        return path_names_df

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

    def _search_file_names_by_type(self, names_type, match_patterns):

        match_patterns = parse_iterable_inputs(match_patterns)
        if names_type == 'file_names':
            self._update_file_names()
            names_df = self._file_names
        elif names_type == 'expected_file_names':
            self._update_expected_file_names()
            names_df = self._expected_file_names
        elif names_type == 'matching_file_names':
            self._update_matching_and_missing_file_names()
            names_df = self._matching_file_names
        elif names_type == 'missing_file_names':
            self._update_matching_and_missing_file_names()
            names_df = self._missing_file_names
        else:
            raise NotImplementedError(f"Couldn't search file names in the category '{names_type}'")

        # Get the file names that match the text pattern
        for txt in match_patterns:
            names_df = names_df[names_df.file_name.str.contains(txt)]
        return names_df

    def _get_path_names_by_type(self, file_type):

        path_names_df = self._path_names
        path_names = path_names_df[path_names_df.file_type == file_type]['path_name'].tolist()
        # Package path names into a list of lists if more than one path name
        path_names = [[name] for name in path_names]
        path_names = parse_iterable_outputs(path_names, chain_args=True)
        return path_names

    def _get_file_names_by_type(self, file_type):

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
        path_names = self._get_path_names_by_type(file_type)
        file_names_df = self._file_name_helper.get_file_name_info(path_names, file_names)
        file_names_df['file_type'] = file_type
        return file_names_df

    def _get_raw_file_names(self):

        # Get all raw data files for the given subject and dates
        file_names = [None]*self._n_dates
        path_names = parse_iterable_inputs(self._get_path_names_by_type('raw'))
        assert len(file_names) == len(path_names), \
            f"Number of dates doesn't match number of folders with raw data"
        
        for ndx, date in enumerate(self.dates):
            name_prefix = self._file_name_helper.get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*'
            file_names[ndx] = FileNameHelper.filter_dir_file_names(path_names[ndx], match_pattern, full_match=True)
        return file_names

    def _get_metadata_file_names(self):

        # Get all metadata files for the given subject
        match_pattern = '.*'
        path_names = self._get_path_names_by_type('metadata')
        file_names = [FileNameHelper.filter_dir_file_names(path_names, match_pattern, full_match=True)]
        return file_names

    def _get_yml_file_names(self):

        # Get all .yml files for the given subject
        file_names = [None]*self._n_dates
        path_names = self._get_path_names_by_type('yml')
        for ndx, date in enumerate(self.dates):
            name_prefix = self._file_name_helper.get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*\\.yml$'
            file_names[ndx] = FileNameHelper.filter_dir_file_names(path_names, match_pattern, full_match=True)
        return file_names

    def _get_nwb_file_names(self):

        # Get all .nwb files for the given subject
        file_names = [None]*self._n_dates
        path_names = self._get_path_names_by_type('nwb')
        for ndx, date in enumerate(self.dates):
            match_pattern = ''.join(['^', self.subject_name, '_', date, '.*\\.nwb$'])
            file_names[ndx] = FileNameHelper.filter_dir_file_names(path_names, match_pattern, full_match=True)
        return file_names

    def _get_video_file_names(self):

        # Get all .h264 video files for the given subject
        file_names = [None]*self._n_dates
        path_names = self._get_path_names_by_type('video')
        for ndx, date in enumerate(self.dates):
            name_prefix = self._file_name_helper.get_file_name_prefix(date)
            match_pattern = '^' + name_prefix + '.*\\.h264$'
            file_names[ndx] = FileNameHelper.filter_dir_file_names(path_names, match_pattern, full_match=True)
        return file_names


    def _get_matching_and_missing_file_names_by_type(self, file_type):

        # Get list of matching and missing file names of specified type
        file_names_dict = {'matching' : None, 'missing' : None}
        file_names_dict['matching'], file_names_dict['missing'] = self._get_matching_and_missing_file_names(file_type)

        # Get full names for all matching and missing file names and transform into dataframes
        file_names_df_dict = {'matching' : None, 'missing' : None}
        for key, file_names in file_names_dict.items():
            path_names = self._get_path_names_by_type(file_type)
            file_names_df = self._file_name_helper.get_file_name_info(path_names, file_names)
            file_names_df['file_type'] = file_type
            file_names_df_dict[key] = file_names_df
        return file_names_df_dict['matching'], file_names_df_dict['missing']

    def _get_matching_and_missing_file_names(self, file_type):

        # Check file names within each file path for the given file type
        path_names = parse_iterable_inputs(self._get_path_names_by_type(file_type))
        matching_names = [None]*len(path_names)
        missing_names = [None]*len(path_names)
        for ndx, path_name in enumerate(path_names):
            # Get full file names and full expected names
            names_ind = (self._file_names.file_type == file_type) & (self._file_names.path_name == path_name)
            expected_names_ind =  (self._expected_file_names.file_type == file_type) & (self._expected_file_names.path_name == path_name)
            full_names = self._file_names[names_ind].full_name.tolist()
            expected_full_names = self._expected_file_names[expected_names_ind].full_name.tolist()
            # Compare full file names
            matching_names[ndx], missing_names[ndx] = FileNameHelper.compare_file_names(full_names, expected_full_names)
            # Convert full names to file names
            _, matching_names[ndx] = FileNameHelper.convert_full_names_to_names(matching_names[ndx])
            _, missing_names[ndx] = FileNameHelper.convert_full_names_to_names(missing_names[ndx])
        return matching_names, missing_names


    def _update(self):

        # Update all file information
        self._update_path_names()
        self._update_file_names()
        self._update_expected_file_names()
        self._update_matching_and_missing_file_names()

    def _update_path_names(self):

        # Get all file paths
        self._file_name_helper.update()
        self._path_names = self._file_name_helper.path_names

    def _update_file_names(self):

        # Get file names
        file_names_list = [None]*len(FileNameHelper.file_types)
        for ndx, file_type in enumerate(FileNameHelper.file_types):
            file_names_list[ndx] = self._get_file_names_by_type(file_type)
        self._file_names = pd.concat(file_names_list)
        self._file_names.reset_index(inplace=True, drop=True)

    def _update_expected_file_names(self):

        # Get expected file names
        self._file_name_helper.update()
        self._expected_file_names = self._file_name_helper.expected_file_names

    def _update_matching_and_missing_file_names(self):

        # Get file names that match expected names and expected names with no corresponding file names
        matching_file_names_list = [None]*len(FileNameHelper.file_types)
        missing_file_names_list = [None]*len(FileNameHelper.file_types)
        for ndx, file_type in enumerate(FileNameHelper.file_types):
            matching_file_names_list[ndx], missing_file_names_list[ndx] = self._get_matching_and_missing_file_names_by_type(file_type)
        self._matching_file_names = pd.concat(matching_file_names_list)
        self._matching_file_names.reset_index(inplace=True, drop=True)
        self._missing_file_names = pd.concat(missing_file_names_list)
        self._missing_file_names.reset_index(inplace=True, drop=True)


    def _print(self, dates):

        # Print list of all files names not associated with a date
        verbose_printer.print_header(f"{self.subject_name} all dates")
        for file_type in FileNameHelper.file_types:
            self._colored_file_name_printer(file_type, '^(?!.*\d{8}).*$')
        verbose_printer.print_newline()
        # Print list of all file names for the given date
        for date in dates:
            verbose_printer.print_header(f"{self.subject_name} {date}")
            for file_type in FileNameHelper.file_types:
                self._colored_file_name_printer(file_type, date)
            verbose_printer.print_newline()

    def _colored_file_name_printer(self, file_type, match_pattern):

        # Get full file names of the specified file type for the given regex pattern
        full_names = self.search_file_names(match_pattern)
        full_names = set(full_names[full_names.file_type == file_type].full_name.tolist())
        expected_full_names = self.search_expected_file_names(match_pattern)
        expected_full_names = set(expected_full_names[expected_full_names.file_type == file_type].full_name.tolist())
        missing_full_names = self._missing_file_names.full_name.values.tolist()
        matching_full_names = self._matching_file_names.full_name.values.tolist()

        # Print both expected and unexpected file names
        full_names = full_names | expected_full_names
        for full_name in full_names:
            # Print expected file name that is missing
            if full_name in missing_full_names:
                verbose_printer.verbose_print(True, full_name, color='red')
            else:
                # Print expected file name that is matching
                if full_name in matching_full_names:
                    verbose_printer.verbose_print(True, full_name, color='green')
                # Print actual file name that isn't expected
                else:
                    verbose_printer.verbose_print(True, full_name)

    def _file_name_comparison_printer(self, matching_file_names, missing_file_names):

        matching_file_names, missing_file_names = parse_iterable_inputs(matching_file_names, missing_file_names)
        # Print verbose output for number of files expected and actually found
        verbose_printer.print_text(f"{len(matching_file_names)} actual files match", f"{len(missing_file_names)} expected files missing")
        sep = '\n'
        if matching_file_names:
            # Print expected file names that correspond to an actual file
            verbose_printer.verbose_print(self._verbose, f"{sep.join(matching_file_names)}", color='green')
        if missing_file_names:
            # Print expected file names that don't correspond to an actual file
            verbose_printer.verbose_print(self._verbose, f"{sep.join(missing_file_names)}", modifier='bold', color='red')

    def _file_name_verification_printer(self, pass_bool, file_type, date):

        if pass_bool:
            verbose_printer.print_pass(f"{self.subject_name} {file_type} files {date} all files found")
        else:
            verbose_printer.print_fail(f"{self.subject_name} {file_type} files {date} some files missing")
        verbose_printer.print_newline()
from abc import ABC, abstractmethod
from inspect import signature
import os
import re

from utils.data_helpers import parse_iterable_inputs

class DataReader(ABC):

    from config import data_path, spyglass_nwb_path, spyglass_video_path
    data_path = data_path
    spyglass_nwb_path = spyglass_nwb_path
    spyglass_video_path = spyglass_video_path

    def __init__(self, subject_name, dates=None, verbose=False, timing=False):

        # Store subject name, location of raw data for all recording sessions, and session dates
        self._validate_subject_name(subject_name)
        self._subject_name = subject_name
        # By default, the sessions path is data_path/subject_name/raw
        self._sessions_path = os.path.join(DataReader.data_path, self._subject_name, 'raw')
        # Ensure provided dates are valid, or use all dates if none provided
        if dates is None:
            dates = self.get_session_dates()
        self._validate_dates(dates)
        self._dates = parse_iterable_inputs(dates)
        self._n_dates = len(self._dates)
        # Get number of epochs per date
        self._n_epochs = self.get_number_of_epochs()
        
        # Enable verbose output and timing decorated functions
        self._verbose = verbose
        self._timing = timing

    @property
    def sessions_path(self):
        return self._sessions_path

    @property
    def subject_name(self):
        return self._subject_name

    @property
    def dates(self):
        return self._dates
    
    @property
    def n_dates(self):
        return self._n_dates
    
    @property
    def n_epochs(self):
        return self._n_epochs

    @property
    def verbose(self):
        return self._verbose
    
    @verbose.setter
    def verbose(self, value):
        self._verbose = value
    
    @property
    def timing(self):
        return self._timing
    
    @timing.setter
    def timing(self, value):
        self._timing = value


    def update(self):

        # Check that subject name and dates are still valid
        self._validate_subject_name(self._subject_name)
        self._validate_dates(self._dates)
        self._update()

    def print(self, dates=None):
        
        if 'dates' in signature(self._print).parameters.keys():
            # Print for the given dates
            if dates is None:
                dates = self._dates
            self._validate_dates(dates)
            self._print(dates)
        else:
            # Print without specifying dates
            self._print()


    def get_subject_names(self):

        # Get all folder names in the data path
        subjects_list = [folder.name for folder in os.scandir(self.data_path) if folder.is_dir()]
        return subjects_list
    
    def _validate_subject_name(self, subject_name):

        # Compare the subject name to all valid subject names
        valid_names = self.get_subject_names()
        if subject_name not in valid_names:
            raise ValueError(f"No subject named '{subject_name}' found among the names {valid_names}")
    
    def get_session_dates(self):

        # Get all folder names in subject's raw data directory
        dates = sorted([folder.name for folder in os.scandir(self._sessions_path) if folder.is_dir()])
        return dates
    
    def _validate_dates(self, dates):

        dates = parse_iterable_inputs(dates)
        # Ensure that no duplicate dates are present
        if len(dates) != len(set(dates)):
            raise ValueError(f"Duplicate dates found in dates list {dates}")
        # Compare list of dates to all valid dates for the given subject
        valid_dates = self.get_session_dates()
        dates_diff = set(dates) - set(valid_dates)
        if dates_diff:
            raise ValueError(f"No data found for subject '{self._subject_name}' on the dates {list(dates_diff)}")
    
    def get_number_of_epochs(self):

        # Determine number of .rec files in the raw data directory
        n_epochs = [None]*len(self._dates)
        for ndx, date in enumerate(self._dates):
            # By default, the raw data path is data_path/subject_name/raw/date
            raw_path = os.path.join(self._sessions_path, date)
            raw_file_names = [file.name for file in os.scandir(raw_path) if file.is_file()]
            n_epochs[ndx] = len([name for name in raw_file_names if re.fullmatch('^.*\\.rec$', name)])
        return n_epochs


    @abstractmethod
    def _update(self):
        pass

    @abstractmethod
    def _print(self, *args):
        pass


class DataWriter(DataReader):

    def __init__(self, subject_name, dates=None, overwrite=False, universal_access=False, verbose=False, timing=False):

        super().__init__(subject_name, dates=dates, verbose=verbose, timing=timing)
        # Enable overwriting existing data and letting anyone access the created data
        self._overwrite = overwrite
        self._universal_access = universal_access
    
    @property
    def overwrite(self):
        return self._overwrite
    
    @overwrite.setter
    def overwrite(self, value):
        self._overwrite = value
    
    @property
    def universal_access(self):
        return self._universal_access
    
    @universal_access.setter
    def universal_access(self, value):
        self._universal_access = value
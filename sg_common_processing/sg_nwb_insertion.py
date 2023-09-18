import os
import numpy as np
import re
import warnings
warnings.filterwarnings('ignore')

from spyglass.common import Nwbfile
from spyglass.data_import import insert_sessions

from utils.common_helpers import (print_verbose, function_timer, no_overwrite_handler_table)
from sg_common_utils.sg_helpers import (sql_or_query, _parse_iterable_inputs)
from sg_common_utils.preprocessing_info import PreprocessingInfo

class InsertionProcessor():

    # Get Spyglass .nwb file paths
    from utils.user_settings import spyglass_nwb_path
    _spyglass_nwb_path = spyglass_nwb_path

    def __init__(self, subject_name, dates_list=None, overwrite=False, verbose=False, timing=False):

        # Enable overwriting existing .nwb files
        self._overwrite = overwrite
        # Enable verbose output and timing
        self._verbose = verbose
        self._timing = timing

        # Get preprocessing analyses info
        self._preprocessing_info = PreprocessingInfo(subject_name, dates_list=dates_list)
        # Get list of already inserted files
        self._inserted_nwb_file_names = self._preprocessing_info.insertion['Nwbfile'].fetch('nwb_file_name')

    @property
    def overwrite(self):
        return self._overwrite
    
    @overwrite.setter
    def overwrite(self, value):
        self._overwrite = value

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


    @function_timer
    def insert_nwb_files(self):

        # Insert each .nwb file into the database
        for nwb_file_name in self._preprocessing_info._nwb_file_names:
            print_verbose(self._verbose,
                          f"{nwb_file_name}",
                          color='blue')
            # Either delete existing .nwb file and reinsert or skip insertion if .nwb file is already inserted
            if nwb_file_name in self._inserted_nwb_file_names:
                if not self._overwrite:
                    no_overwrite_handler_table(f"Entry '{nwb_file_name}' already exists in 'Nwbfile' table")
                else:
                    InsertionProcessor._depopulate_nwb_file_table(nwb_file_name)
                    InsertionProcessor._populate_nwb_file_table(nwb_file_name)
            else:
                InsertionProcessor._populate_nwb_file_table(nwb_file_name)
    
    @function_timer
    def delete_nwb_files(self):

        # Delete each .nwb file from the database
        for nwb_file_name in self._preprocessing_info._nwb_file_names:
            print_verbose(self._verbose,
                          f"{nwb_file_name}",
                          color='blue')
            InsertionProcessor._depopulate_nwb_file_table(nwb_file_name)


    @staticmethod
    def _depopulate_nwb_file_table(nwb_file_names):
        
        nwb_file_names = _parse_iterable_inputs(nwb_file_names)
        # Delete .nwb file from Nwbfile table and remove ephys .nwb file from Spyglass directory
        (Nwbfile & sql_or_query(nwb_file_names)).delete()
        Nwbfile().cleanup(delete_files=True)

    @staticmethod
    def _populate_nwb_file_table(nwb_file_name):
        
        # Add .nwb file to Nwbfile table
        nwb_file_name = nwb_file_name.split('_.nwb')[0] + '.nwb'
        insert_sessions(nwb_file_name)

    

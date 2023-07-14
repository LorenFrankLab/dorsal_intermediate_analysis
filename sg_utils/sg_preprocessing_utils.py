from sg_utils.sg_abstract_classes import TableTools
from sg_utils.sg_helpers import (sql_or_query,
                                 _parse_table_attribute_values, _table_to_string)
from utils.common_abstract_classes import DataReader
from utils.common_helpers import (print_verbose, function_timer,
                                  _nested_dict_len)
from utils.file_utils import FileInfo


from spyglass.common import (Nwbfile,
                             LFPSelection, LFP, LFPBandSelection, LFPBand)
from spyglass.spikesorting import (SpikeSorterParameters)

class PreprocessingInfo(DataReader):

    # Tuple of valid preprocessing analyses
    _preprocessing_tables = {'insertion' : {'Nwbfile' : Nwbfile},
                             'lfp' : {'LFPSelection' : LFPSelection,
                                      'LFP' : LFP,
                                      'LFPBandSelection' : LFPBandSelection,
                                      'LFPBand' : LFPBand},
                             'ripples' : (),
                             'spikesorting' : (),
                             'curation' : (),
                             'decoding' : ()}
    _preprocessing_types = _preprocessing_tables.keys()
    
    def __init__(self, subject_name, dates=None, verbose=False, timing=False):
    
        # Enable verbose output and timing
        super().__init__(verbose=verbose, timing=timing)

        # Get file information for selected animal and dates
        self._file_info = FileInfo(subject_name, dates=dates)       
        self._subject_name = self._file_info._subject_name
        self._dates = self._file_info._dates
        self._n_dates = len(self._dates)
        
        # Get .nwb file information
        nwb_file_names = self._file_info.search_matching_file_names('.*\\.nwb$')['file_name'].tolist()
        self._validate_nwb_file_names()
        self._nwb_file_names = PreprocessingInfo._nwb_file_names_to_table_names(nwb_file_names)
        self._n_nwb_files = len(self._nwb_file_names)
        # Initailize dictionary of table queries for preprocessing analyses
        self._preprocessing_queries = {key: None for key in PreprocessingInfo._preprocessing_types}

    
    @property
    def nwb_file_names(self):
        return self._nwb_file_names
        
    @property
    def insertion(self):
        self._update_insertion_info()
        return self._preprocessing_queries['insertion']


    @function_timer
    def validate_preprocessing(self):

        self._update()
        # Check if each preprocessing analysis has been conducted for each .nwb file
        for preprocessing_type in self._preprocessing_types():
            self._validate_preprocessing_by_type(preprocessing_type)
    
    @function_timer
    def validate_insertion(self):

        self._update_insertion_info()
        # Check if each .nwb file has been inserted into the database
        self._validate_preprocessing_by_type('insertion')
    

    def _update(self):

        self._file_info._update()
        # Update all preprocessing information
        self._update_insertion_info()
        self._update_lfp_info()
        #self._update_ripples_info()
        #self._update_spikesorting_info()
        #self._update_curation_info()
        #self._update_decoding_info()

    def _update_insertion_info(self):

        # Query Nwbfile table for .nwb file names
        nwb_file_key = sql_or_query('nwb_file_name', self._nwb_file_names)
        query_dict = {'Nwbfile' : nwb_file_key}
        self._query_tables('insertion', query_dict)
    
    def _update_lfp_info(self):

        # Query LFPSelection, LFP, LFPBandSelection, and LFPBand tables for .nwb file names
        nwb_file_key = sql_or_query('nwb_file_name', self._nwb_file_names)
        query_dict = {'LFPSelection' : nwb_file_key,
                      'LFP' : nwb_file_key,
                      'LFPBandSelection' : nwb_file_key,
                      'LFPBand' : nwb_file_key}
        self._query_tables('lfp', query_dict)


    def _validate_preprocessing_by_type(self, preprocessing_type):

        for (key, query) in self._preprocessing_queries[preprocessing_type].items():
            # Define attributes to validate in each table
            if key == 'Nwbfile':
                attribute_names = ['nwb_file_name']
                attribute_values = [self._nwb_file_names]
            elif key == 'LFPSelection':
                attribute_names = ['nwb_file_name']
                attribute_values = [self._nwb_file_names]
            elif key == 'LFP':
                attribute_names =  ['nwb_file_name', 'interval_list_name']
                attribute_values = [self._nwb_file_names, ['lfp valid times']]
            elif key == 'LFPBandSelection':
                attribute_names = ['nwb_file_name', 'target_interval_list_name', 'filter_name']
                attribute_values = [self._nwb_file_names, ['raw data valid times'], ['Theta 5-11 Hz', 'Ripple 150-250 Hz']]
            elif key == 'LFPBand':
                attribute_names = ['nwb_file_name', 'target_interval_list_name', 'filter_name']
                attribute_values = [self._nwb_file_names, ['raw data valid times'], ['Theta 5-11 Hz', 'Ripple 150-250 Hz']]
            else:
                raise NotImplementedError(f"Couldn't get validate preprocessing analyses for table '{key}'")

            # Check for specified entries in the table
            attribute_values = _parse_table_attribute_values(query, attribute_names, attribute_values=attribute_values)
            query_ind = TableTools._create_table_entries_indicator(query, attribute_names, attribute_values)
            print_verbose(self._verbose,
                          f"\n{self._subject_name}: {key}",
                          color='blue')
            self._preprocessing_validation_printer(query_ind)

    def _query_tables(self, preprocessing_type, query_dict):

        # Initialize an empty dictionary of preprocessing tables
        self._preprocessing_queries[preprocessing_type] = {}
        # Get all tables corresponding to the type of preprocessing
        preprocessing_tables = PreprocessingInfo._preprocessing_tables[preprocessing_type]
        # Query each preprocessing table
        for (key, table) in preprocessing_tables.items():
            query = table & query_dict[key]
            self._preprocessing_queries[preprocessing_type][_table_to_string(table)] = query


    def _validate_nwb_file_names(self):

        # Get names of missing .nwb files for the given dates
        missing_nwb_file_names = self._file_info.search_missing_file_names('.*\\.nwb$')['file_name'].tolist()
        # Raise an error if expected .nwb files aren't found
        if missing_nwb_file_names:
            raise RuntimeError(f"Couldn't find the following .nwb files {missing_nwb_file_names}")

    def _preprocessing_validation_printer(self, query_ind):

        # Print verbose output for number of analyses expected and actually found
        print_verbose(self._verbose,
                      f"{TableTools._table_ind_count(query_ind)} preprocessing analyses found",
                      f"{_nested_dict_len(query_ind)-TableTools._table_ind_count(query_ind)} preprocessing analyses missing\n")
        # Print indicator for expected entries in preprocessing table
        self._print_validation_ind(query_ind)
    
    def _print_validation_ind(self, query_ind, tab_depth=1):

        # Recursively traverse and print the values in a nested dictionary
        tab = '  '*tab_depth
        for key, value in query_ind.items():
            if isinstance(value, dict):
                print_verbose(self._verbose,
                              f"{tab}{key}")
                self._print_validation_ind(value, tab_depth=tab_depth+1)
            else:
                color = 'green' if value else 'red'
                print_verbose(self._verbose,
                              f"{tab}{key}",
                              color=color)
    
    @staticmethod
    def _nwb_file_names_to_table_names(nwb_file_names):

        # Convert .nwb file names to table entry convention
        return [name.split('.nwb')[0] + '_.nwb' for name in nwb_file_names]
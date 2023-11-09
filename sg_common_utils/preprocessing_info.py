from .sg_abstract_classes import TableReader, TableTools
from .sg_helpers import (get_primary_key,
                         sql_or_query,
                         get_table_name)
from .sg_tables import (Nwbfile,
                        LFPBandSelection, LFPBand, LFPElectrodeGroup, LFPSelection, LFP)

from utils import verbose_printer
from utils.data_containers import NestedDict
from utils.function_wrappers import function_timer



from spyglass.spikesorting import (SpikeSorterParameters)

class PreprocessingInfo(TableReader):

    # Tuple of valid preprocessing analyses
    _preprocessing_tables = {'insertion' : {'Nwbfile' : Nwbfile},
                             'lfp' : {'LFPElectrodeGroup' : LFPElectrodeGroup,
                                      'LFPSelection' : LFPSelection,
                                      'LFP' : LFP,
                                      'LFPBandSelection' : LFPBandSelection,
                                      'LFPBand' : LFPBand}}
                             #'ripples' : (),
                             #'spikesorting' : (),
                             #'curation' : (),
                             #'decoding' : ()}
    _preprocessing_types = _preprocessing_tables.keys()
    _table_tuples = NestedDict(_preprocessing_tables).deep_items()
    _tables = dict((key_list[-1], table) for key_list, table in _table_tuples)
    
    def __init__(self, subject_names=None, nwb_file_names=None, verbose=False, timing=False):
    
        # Enable verbose output and timing
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, verbose=verbose, timing=timing)
        
        # Initailize dictionary of table queries for preprocessing analyses
        self._preprocessing_queries = {key: None for key in PreprocessingInfo._preprocessing_types}
        
    @property
    def insertion(self):
        if self._preprocessing_queries['insertion'] is None:
            self._update_insertion_info()
        return self._preprocessing_queries['insertion']
    
    @property
    def lfp(self):
        if self._preprocessing_queries['lfp'] is None:
            self._update_lfp_info()
        return self._preprocessing_queries['lfp']
    
    @property
    def ripples(self):
        if self._preprocessing_queries['ripples'] is None:
            self._update_ripples_info()
        return self._preprocessing_queries['ripples']
    
    @property
    def spikesorting(self):
        if self._preprocessing_queries['spikesorting'] is None:
            self._update_spikesorting_info()
        return self._preprocessing_queries['spikesorting']
    
    @property
    def curation(self):
        if self._preprocessing_queries['curation'] is None:
            self._update_curation_info()
        return self._preprocessing_queries['curation']
    
    @property
    def decoding(self):
        if self._preprocessing_queries['decoding'] is None:
            self._update_decoding_info()
        return self._preprocessing_queries['decoding']


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
    
    def _validate_preprocessing_by_type(self, preprocessing_type):

        # Check each table associated with a type of preprocessing for the expected entries
        query_ind = self._create_table_entries_indicator_by_type(preprocessing_type)
        verbose_printer.print_header(f"{', '.join(self.subject_names)} {preprocessing_type}")
        self._preprocessing_validation_printer(query_ind)


    def _create_table_entries_indicator_by_type(self, preprocessing_type):

        query_ind = {}
        for (key, query) in self._preprocessing_queries[preprocessing_type].items():
            # Define attributes to validate in each table
            if key == 'Nwbfile':
                attribute_names = ['nwb_file_name']
                attribute_values = [self.nwb_file_names]
            elif key == 'LFPElectrodeGroup':
                attribute_names = ['nwb_file_name', 'group_name', 'electrode_list']
                attribute_values = [self.nwb_file_names]
            elif key == 'LFPSelection':
                attribute_names = ['nwb_file_name']
                attribute_values = [self.nwb_file_names]
            elif key == 'LFP':
                attribute_names =  ['nwb_file_name', 'interval_list_name']
                attribute_values = [self.nwb_file_names, ['lfp valid times']]
            elif key == 'LFPBandSelection':
                attribute_names = ['nwb_file_name', 'target_interval_list_name', 'filter_name']
                attribute_values = [self.nwb_file_names, ['raw data valid times'], ['Theta 5-11 Hz', 'Ripple 150-250 Hz']]
            elif key == 'LFPBand':
                attribute_names = ['nwb_file_name', 'target_interval_list_name', 'filter_name']
                attribute_values = [self.nwb_file_names, ['raw data valid times'], ['Theta 5-11 Hz', 'Ripple 150-250 Hz']]
            else:
                raise NotImplementedError(f"Couldn't validate preprocessing analyses for table '{key}'")

            # Check for specified entries in the table
            query_ind[key] = TableTools.create_table_entries_indicator(query, attribute_names, attribute_values)
        return query_ind


    def _update(self):

        # Update all preprocessing information
        self._update_insertion_info()
        self._update_lfp_info()
        #self._update_ripples_info()
        #self._update_spikesorting_info()
        #self._update_curation_info()
        #self._update_decoding_info()

    def _update_query_tables(self, preprocessing_type, query_dict):

        # Initialize an empty dictionary of preprocessing tables
        self._preprocessing_queries[preprocessing_type] = {}
        # Get all tables corresponding to the type of preprocessing
        preprocessing_tables = PreprocessingInfo._preprocessing_tables[preprocessing_type]
        # Query each preprocessing table
        for (key, table) in preprocessing_tables.items():
            query = table & query_dict[key]
            self._preprocessing_queries[preprocessing_type][get_table_name(table)] = query

    def _update_insertion_info(self):

        # Query Nwbfile table for .nwb file names
        nwb_file_key = sql_or_query('nwb_file_name', self.nwb_file_names)
        query_dict = {'Nwbfile' : nwb_file_key}
        self._update_query_tables('insertion', query_dict)
    
    def _update_lfp_info(self):

        # Query LFPSelection, LFP, LFPBandSelection, and LFPBand tables for .nwb file names
        nwb_file_key = sql_or_query('nwb_file_name', self.nwb_file_names)
        query_dict = {'LFPSelection' : nwb_file_key,
                      'LFP' : nwb_file_key,
                      'LFPBandSelection' : nwb_file_key,
                      'LFPBand' : nwb_file_key}
        self._update_query_tables('lfp', query_dict)

    def _update_ripples_info():

        raise NotImplementedError('Not implemented')
    
    def _update_spikesorting_info():

        raise NotImplementedError('Not implemented')
    
    def _update_curation_info():

        raise NotImplementedError('Not implemented')
    
    def _update_decoding_info():

        raise NotImplementedError('Not implemented')


    def _print(self):
        
        # Print list of all entries for the given tables associated with preprocessing
        verbose_printer.print_header(f"{', '.join(self.subject_names)} all entries")
        for preprocessing_type in PreprocessingInfo._preprocessing_types:
            query_ind = self._create_table_entries_indicator_by_type(preprocessing_type)
            for table_name, ind in query_ind.items():
                verbose_printer.print_newline()
                verbose_printer.print_bold(f"{table_name} table")
                query = self._preprocessing_queries[preprocessing_type][table_name]
                #query = TableTools.query_table(query, 'nwb_file_name', nwb_file_name)
                # Get all entries in the preprocessing table associated with the .nwb file
                query_df = TableTools.fetch_as_dataframe(query, attribute_names=get_primary_key(query))

                # Get all existing entry names and expected entry names
                entry_names = [[ " ".join([ str(query_df[col_name][idx]) for col_name in query_df.columns ]) for idx in query_df.index ]]
                if [] in entry_names:
                    entry_names.remove([])
                expected_entry_names = list(ind.deep_keys())
                for expected_name in expected_entry_names:
                    if expected_name in entry_names:
                        continue
                    else:
                        entry_names.append(expected_name)

                for name in entry_names:
                    # Print expected entry
                    if name in expected_entry_names:
                        if ind[name]:
                            # Print expected entry that is matching
                            verbose_printer.verbose_print(True, str(name), color='green')
                        else:
                            # Print expected entry that is missing
                            verbose_printer.verbose_print(True, str(name), color='red')
                    else:
                        # Print entry that isn't expected
                        verbose_printer.verbose_print(True, str(name))

    def _preprocessing_validation_printer(self, query_ind):

        # Print verbose output for number of analyses expected and actually found
        ind_count = sum([TableTools.table_ind_count(ind) for ind in query_ind.values()])
        ind_length = sum(len(ind) for ind in query_ind.values())
        verbose_printer.print_text(f"{ind_count} preprocessing analyses found",
                                   f"{ind_length-ind_count} preprocessing analyses missing")
        # Print indicator for expected entries in preprocessing table
        for ind_name, ind in query_ind.items():
            verbose_printer.print_newline()
            verbose_printer.verbose_print(self._verbose, f"{ind_name} table", modifier='bold')
            self._validation_indicator_printer(ind)
        verbose_printer.print_newline()
    
    def _validation_indicator_printer(self, query_ind):

        # Print all of the deepest items in the nested dictionary
        for key, value in query_ind.deep_items():
            # Print the deepest key
            if value:
                # Print preprocessing analysis that corresponds to a table entry
                verbose_printer.verbose_print(self._verbose, f"{key}", color='green')
            else:
                # Print preprocessing analysis that doesn't have a corresponding table entry
                verbose_printer.verbose_print(self._verbose, f"{key}", modifier='bold', color='red')
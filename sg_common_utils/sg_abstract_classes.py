from abc import ABC, abstractmethod
from inspect import signature

from .sg_helpers import (fetch,
                         fetch_as_dataframe,
                         multi_table_fetch,
                         parse_table_attribute_values,
                         query_by_key,
                         query_table,
                         query_to_dataframe,
                         sql_combinatoric_query)
from .sg_tables import (IntervalList, Session, Subject, TaskEpoch)

from utils.data_containers import NestedDict
from utils.data_helpers import parse_iterable_inputs


class TableReader(ABC):
        
    from config import spyglass_nwb_path, spyglass_video_path
    spyglass_nwb_path = spyglass_nwb_path
    spyglass_video_path = spyglass_video_path

    # Initialize the tables that will be queried
    _tables = None

    def __init__(self, subject_names=None, nwb_file_names=None, verbose=False, timing=False):

        if subject_names is None and nwb_file_names is None:
            raise ValueError(f"Either subject names or .nwb file names must be specified")
        # Store subject name and names of .nwb file
        if subject_names is None:
            subject_names = self._get_subject_names_from_nwb_files(nwb_file_names)
        self._validate_subject_names(subject_names)
        self._subject_names = parse_iterable_inputs(subject_names)
        # Ensure provided dates are valid, or use all dates if none provided
        if nwb_file_names is None:
            nwb_file_names = self.get_nwb_file_names()
        self._validate_nwb_file_names(nwb_file_names)
        self._nwb_file_names = parse_iterable_inputs(nwb_file_names)
        self._n_files = len(self._nwb_file_names)
        # Get names of epochs for each session
        self._epochs = self.get_epoch_ids()
        # Get task names for each epoch of each session
        self._tasks = self.get_tasks()
        # Query appropriate tables for the given .nwb file names
        self._queries = self.get_queries()
        
        # Enable verbose output and timing decorated functions
        self._verbose = verbose
        self._timing = timing

    @property
    def subject_names(self):
        return self._subject_names

    @property
    def nwb_file_names(self):
        return self._nwb_file_names
    
    @property
    def epochs(self):
        return self._epochs

    @property
    def tasks(self):
        return self._tasks

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
    
    @property
    def tables(self):
        return self._tables
        
    @property
    def queries(self):
        return self._queries

    def update(self):

        # Check that subject name and dates are still valid
        self._validate_subject_names(self._subject_names)
        self._validate_nwb_file_names(self._nwb_file_names)
        self._update()
    
    def print(self, nwb_file_names=None):
        
        if 'nwb_file_names' in signature(self._print).parameters.keys():
            # Print for the given .nwb files
            if nwb_file_names is None:
                nwb_file_names = self._nwb_file_names
            self._validate_nwb_file_names(nwb_file_names)
            self._print(nwb_file_names)
        else:
            # Print without specifying .nwb file names
            self._print()

    def get_subject_names(self):

        # Get all subject names from the Subject table
        subjects = Subject.fetch('subject_id')
        return subjects
    
    def _get_subject_names_from_nwb_files(self, nwb_file_names):

        # Get subject names from the given .nwb files
        subject_names = []
        for nwb_file_name in nwb_file_names:
            subject_names.append( (Session & {'nwb_file_name' : nwb_file_name}).fetch1('subject_id') )
        return list(set(subject_names))

    def _validate_subject_names(self, subject_names):

        subject_names = parse_iterable_inputs(subject_names)
        # Compare the subject name to all valid subject names
        valid_names = self.get_subject_names()
        for name in subject_names:
            if name not in valid_names:
                raise ValueError(f"No subject named '{name}' found among the names {valid_names}")
    
    def get_nwb_file_names(self):

        # Get all inserted .nwb file names corresponding to the subject
        nwb_file_names = []
        for name in self._subject_names:
            nwb_file_names.extend( list((Session & {'subject_id' : name}).fetch('nwb_file_name')) )
        return list(nwb_file_names)
    
    def _validate_nwb_file_names(self, nwb_file_names):

        nwb_file_names = parse_iterable_inputs(nwb_file_names)
        # Ensure that no duplicate .nwb file names are present
        if len(nwb_file_names) != len(set(nwb_file_names)):
            raise ValueError(f"Duplicate .nwb file names found in file names list {nwb_file_names}")
        # Compare list of file names to all valid file names for the given subject
        valid_file_names = self.get_nwb_file_names()
        file_names_diff = set(nwb_file_names) - set(valid_file_names)
        if file_names_diff:
            raise ValueError(f"The following .nwb files not found for the subject '{self.subject_names}:' {list(file_names_diff)}")

    def get_epoch_ids(self):

        # Determine number of epochs in each .nwb file
        epoch_ids = [None]*len(self._nwb_file_names)
        for ndx, nwb_file_name in enumerate(self._nwb_file_names):
            epoch_ids[ndx] = (TaskEpoch & {'nwb_file_name' : nwb_file_name}).fetch('epoch')
        return epoch_ids
    
    def get_tasks(self):

        # Determine the task for each epoch
        task_names = [None]*len(self._nwb_file_names)
        for ndx, nwb_file_name in enumerate(self._nwb_file_names):
            # Get task names for the given .nwb file
            task_names[ndx] = (TaskEpoch & {'nwb_file_name' : nwb_file_name}).fetch('task_name', order_by='epoch')
        return task_names
    
    def get_queries(self):

        # Query each table for entries matching the .nwb file names
        queries = {key: None for key in self._tables.keys()}
        for key, table in self._tables.items():
            if self.nwb_file_names:
                queries[key] = query_table(table, 'nwb_file_name', self.nwb_file_names)
            else:
                queries[key] = None
        return queries

    @abstractmethod
    def _update(self):
        pass

    @abstractmethod
    def _print(self, *args):
        pass


class TableWriter(TableReader):

    def __init__(self, subject_names=None, nwb_file_names=None, interval_list_names=None, overwrite=False, verbose=False, timing=False):

        # Enable verbose output and timing
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, verbose=verbose, timing=timing)

        # Ensure provided interval list names are valid, or use all use interval lists if none provided
        if interval_list_names is None:
            self._interval_list_names = self._create_interval_list_names_dict(interval_list_names)
        self._validate_interval_list_names(interval_list_names)
        self._interval_list_names = self._create_interval_list_names_dict(interval_list_names)
        for nwb_file_name in self.nwb_file_names:
            self._interval_list_names[nwb_file_name] = parse_iterable_inputs(self._interval_list_names[nwb_file_name])
        
        # Enable overwriting existing data
        self._overwrite = overwrite
    
    @property
    def interval_list_names(self):
        return self._interval_list_names
    
    @property
    def overwrite(self):
        return self._overwrite
    
    @overwrite.setter
    def overwrite(self, value):
        self._overwrite = value

    def validate_table_entry(self, query_name, key):

        # Check if key corresponds to an entry in the query table
        query = query_by_key(self.queries[query_name], key=key)
        entry_exists_bool = (query == True)
        return entry_exists_bool
    
    def get_interval_list_names(self, nwb_file_name):

        # Get all interval list names for an inserted .nwb files
        query = query_table(IntervalList, 'nwb_file_name', nwb_file_name)
        interval_list_names = fetch(query, 'interval_list_name')
        return interval_list_names
    
    def _validate_interval_list_names(self, interval_list_names):

        interval_list_names = self._create_interval_list_names_dict(interval_list_names)
        # Ensure that keys of interval list names dictionary are valid .nwb file names
        self._validate_nwb_file_names(interval_list_names.keys())
        for nwb_file_name in self.nwb_file_names:
            # Ensure that no duplicate interval list names are present
            interval_names = parse_iterable_inputs(interval_list_names[nwb_file_name])
            if len(interval_names) != len(set(interval_names)):
                raise ValueError(f"Duplicate interval list names found for .nwb file {nwb_file_name}")
            
            # Compare list of file names to all valid file names for the given subject
            valid_interval_names = self.get_interval_list_names(nwb_file_name)
            interval_names_diff = set(interval_names) - set(valid_interval_names)
            if interval_names_diff:
                raise ValueError(f"The following interval list names not found for .nwb file '{nwb_file_name}': {list(interval_names_diff)}")
    
    def _create_interval_list_names_dict(self, interval_list_names):

        # Get all interval list names for all .nwb files if no interval list names are specified
        if interval_list_names is None:
            interval_list_names = {nwb_file_name : self.get_interval_list_names(nwb_file_name) for nwb_file_name in self._nwb_file_names}
        # Use same interval list names for all .nwb files if no .nwb files are specified as keys
        interval_list_names = parse_iterable_inputs(interval_list_names)
        if not isinstance(interval_list_names, dict):
            interval_list_names = {nwb_file_name : interval_list_names for nwb_file_name in self.nwb_file_names}
        # Get all interval list names for any .nwb file names not specified
        for nwb_file_name in self._nwb_file_names:
            if nwb_file_name not in interval_list_names.keys():
                interval_list_names[nwb_file_name] = self.get_interval_list_names(nwb_file_name)
        return interval_list_names
    


class TableTools(TableReader):

    def __init__(self, subject_names=None, nwb_file_names=None, verbose=False, timing=False):
        
        # Enable verbose output and timing
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, verbose=verbose, timing=timing)

    def query_by_nwb_file(self, table, attribute_names, attribute_values, dataframe=False):

        attribute_names, attribute_values = parse_table_attribute_values(table, attribute_names, attribute_values=attribute_values)
        # Query tables using the given attribute names and values
        queries = {nwb_file_name : None for nwb_file_name in self._nwb_file_names}
        for nwb_file_name in self._nwb_file_names:
            # Restrict the table to entries corresponding to the .nwb file
            nwb_query = query_table(table, 'nwb_file_name', nwb_file_name)
            # Query the table for the given attribute names and values
            query = query_table(nwb_query, attribute_names, attribute_values)
            if dataframe:
                query = query_to_dataframe(query)
            queries[nwb_file_name] = query
        return queries

    def fetch_by_nwb_file(self, table, attribute_names, sort_attribute_names=None, dataframe=True):

        attribute_names, sort_attribute_names = parse_iterable_inputs(attribute_names, sort_attribute_names)
        # Fetch specified attributes from table
        fetches = {nwb_file_name : None for nwb_file_name in self._nwb_file_names}
        for nwb_file_name in self._nwb_file_names:
            # Restrict the table to entries corresponding to the .nwb file
            nwb_query = query_table(table, 'nwb_file_name', nwb_file_name)
            # Fetch the data in the specified columns of the table
            if dataframe:
                fetch_data = fetch_as_dataframe(nwb_query, attribute_names, sort_attribute_names=sort_attribute_names)
            else:
                fetch_data = fetch(nwb_query, attribute_names, sort_attribute_names=sort_attribute_names)
            fetches[nwb_file_name] = fetch_data
        return fetches
    
    def multi_table_fetch_by_nwb_file(self, queries_dict, conformer_attributes_list, shaper_attribute_dict):

        # Get conformed data for each .nwb file
        fetches = {nwb_file_name : None for nwb_file_name in self._nwb_file_names}
        for nwb_file_name in self._nwb_file_names:
            # Restrict each table to entries corresponding to just the current .nwb file
            nwb_queries = {key : query_table(table, 'nwb_file_name', nwb_file_name) for key, table in queries_dict.items()}
            # Fetch conformed data
            data_df = multi_table_fetch(nwb_queries, shaper_attribute_dict, conformer_attributes_list)
            fetches[nwb_file_name] = data_df
        return fetches


    @staticmethod
    def create_table_entries_indicator(table, attribute_names, attribute_values):

        # Create indicator for table entries corresponding to all combinations of keys
        table_entries_ind = NestedDict()
        attribute_names, attribute_values = parse_table_attribute_values(table, attribute_names, attribute_values=attribute_values)
        queries = sql_combinatoric_query(attribute_names, attribute_values)
        for key in queries:
            # Query table and check if entry exists or not
            query = table & key
            ind = True if query else False
            table_entries_ind[list(key.values())] = ind
        return table_entries_ind

    @staticmethod
    def table_ind_count(nested_dict_ind):

        # Count the number of table entries found
        return sum(list(nested_dict_ind.deep_values()))
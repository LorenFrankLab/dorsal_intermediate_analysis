from sg_utils.sg_helpers import (create_key, conformed_fetch, get_shared_primary_key, sql_combinatoric_query, sql_multi_query, sql_or_query,
                                 _parse_iterable_inputs, _parse_restriction_key, _parse_table_attribute_values)
from utils.common_abstract_classes import DataReader
from utils.common_helpers import (_nested_dict_set, _sort_dataframe_numeric_string, _sort_data_numeric_string)

from spyglass.common import Nwbfile

class TableTools(DataReader):

    # Initialize the tables that will be queried
    _tables = None

    def __init__(self, nwb_file_names, verbose=False, timing=True):
        
        # Enable verbose output and timing
        super().__init__(verbose=verbose, timing=timing)
        # Validate .nwb file names
        TableTools._validate_nwb_file_names(nwb_file_names)
        self._nwb_file_names = nwb_file_names
        # Query appropriate tables for the given .nwb file names
        self._queries = self._get_queries()
    
    @property
    def nwb_file_names(self):
        return self._nwb_file_names
    
    @property
    def tables(self):
        return self._tables
    
    @property
    def queries(self):
        return self._queries
    

    def _get_queries(self):

        # Query each table for entries matching the .nwb file names
        queries = {key: None for key in self._tables.keys()}
        for key, table in self._tables.items():
            queries[key] = TableTools._query_table(table, 'nwb_file_name', self.nwb_file_names)
        return queries
        
    def _query_by_key(self, table, key=None, **kwargs):

        # Create a query key from keyword arguments if no key is specified
        if key is None:
            key = create_key(**kwargs)
        # Create queries from the key
        query = table
        if key:
            attribute_names, attribute_values = _parse_restriction_key(key)
            query = query & sql_multi_query(attribute_names, attribute_values)
        return query
    

    def _query_by_nwb_file(self, table, attribute_names, attribute_values, dataframe=True):

        attribute_names = _parse_iterable_inputs(attribute_names)
        attribute_values = _parse_iterable_inputs(attribute_values)
        # Query tables using the given attribute names and values
        queries = {nwb_file_name : None for nwb_file_name in self._nwb_file_names}
        for nwb_file_name in self._nwb_file_names:
            # Restrict the table to entries corresponding to the .nwb file
            nwb_query = TableTools._query_table(table, 'nwb_file_name', nwb_file_name)
            # Query the table for the given attribute names and values
            query = TableTools._query_table(nwb_query, attribute_names, attribute_values)
            if dataframe:
                query = TableTools._query_to_dataframe(query)
            queries[nwb_file_name] = query
        return queries

    def _fetch_by_nwb_file(self, table, attribute_names, sort_attribute_names=None, dataframe=True):

        attribute_names = _parse_iterable_inputs(attribute_names)
        sort_attribute_names = _parse_iterable_inputs(sort_attribute_names)
        # Fetch specified attributes from table
        fetches = {nwb_file_name : None for nwb_file_name in self._nwb_file_names}
        for nwb_file_name in self._nwb_file_names:
            # Restrict the table to entries corresponding to the .nwb file
            nwb_query = TableTools._query_table(table, 'nwb_file_name', nwb_file_name)
            # Fetch the data in the specified columns of the table
            if dataframe:
                fetch = TableTools._fetch_as_dataframe(nwb_query, attribute_names, sort_attribute_names=sort_attribute_names)
            else:
                fetch = TableTools._fetch_multiple(nwb_query, attribute_names, sort_attribute_names=sort_attribute_names)
            fetches[nwb_file_name] = fetch
        return fetches
    
    def _conform_by_nwb_file(self, queries_dict, fetch_attributes_dict, shaper_attribute_dict):

        # Get conformed data for each .nwb file
        fetches = {nwb_file_name : None for nwb_file_name in self._nwb_file_names}
        for nwb_file_name in self._nwb_file_names:
            # Restrict each table to entries corresponding to just the current .nwb file
            nwb_queries = {key : TableTools._query_table(table, 'nwb_file_name', nwb_file_name) for key, table in queries_dict.items()}
            # Fetch conformed data
            data_df = conformed_fetch(nwb_queries, fetch_attributes_dict, shaper_attribute_dict)
            # Reorder multiindex so that table names are above .nwb file names
            fetches[nwb_file_name] = data_df
        return fetches


    @staticmethod
    def _validate_nwb_file_names(nwb_file_names):

        # Ensure .nwb file names are contained within an iterable data structure
        nwb_file_names = _parse_iterable_inputs(nwb_file_names)
        # Ensure that no duplicate .nwb file names are present
        if len(nwb_file_names) != len(set(nwb_file_names)):
            raise ValueError(f"Duplicate .nwb file names found in file names list {nwb_file_names}")
        nwb_file_names = set(nwb_file_names)
        
        # Check that each .nwb file name is present in the Nwbfile table
        key = sql_or_query('nwb_file_name', nwb_file_names)
        inserted_nwb_file_names = set((Nwbfile & key).fetch('nwb_file_name'))
        missing_nwb_file_names = nwb_file_names.difference(inserted_nwb_file_names)
        # Ensure that no .nwb files are missing from the Nwbfile table
        if missing_nwb_file_names:
            raise ValueError(f"Could not find the following .nwb file names in the Nwbfile table: {missing_nwb_file_names}")
    
    @staticmethod
    def _parse_dataframe_transformation(table_df, dtypes=None, transform_mappings=None):

        # Get all table columns and indices available for transforming and converting
        table_indices = set(table_df.index.names)
        table_columns = set(table_df.columns)
        if not(len(table_indices) == 1 and list(table_indices)[0] is None):
            table_columns = set(table_df.columns).union(table_indices)
        # Ensure keys of data type conversion are attribute names of the table
        if dtypes is not None:
            dtypes_keys_diff = set(dtypes.keys()) - table_columns
            if dtypes_keys_diff:
                raise ValueError(f"The following keys of 'dtypes' aren't valid attribute names: {dtypes_keys_diff}")
        else:
            dtypes = {attribute_name : None for attribute_name in table_columns}
        # Ensure keys of transform mappings are attribute names of the table
        if transform_mappings is not None:
            mappings_keys_diff = set(transform_mappings.keys()) - table_columns
            if mappings_keys_diff:
                raise ValueError(f"The following keys of 'transform_mappings' aren't valid attribute names: {mappings_keys_diff}")
        else:
            transform_mappings = {attribute_name : None for attribute_name in table_columns}
        return dtypes, transform_mappings
    

    @staticmethod
    def _query_table(table, attribute_name, attribute_values):

        attribute_values = _parse_iterable_inputs(attribute_values)
        # Query the table using the attribute name and attribute values
        key = sql_or_query(attribute_name, attribute_values)
        return table & key

    @staticmethod
    def _query_table_serial(table, attribute_names, attribute_values):

        # Ensure the number of attribute values matches the number of attribute names
        if len(attribute_names) != len(attribute_values):
            raise ValueError(f"Number of attribute names doesn't match number of containers for attribute values")
        # Serially query the table using the attribute names and values for those attributes
        query = table
        for name, values in zip(attribute_names, attribute_values):
            query = TableTools._query_table(query, name, values)
        return query
    
    @staticmethod
    def _query_to_dataframe(table, reset_index=False):

        # Convert table to a dataframe
        table_df = table.fetch(format='frame')
        if reset_index:
            table_df = table_df.reset_index()
        return table_df


    @staticmethod
    def _fetch(table, attribute_name, sort_attribute_names=None):

        sort_attribute_names = _parse_iterable_inputs(sort_attribute_names)
        # Fetch data corresponding to the attribute name from the table
        if sort_attribute_names is not None:
            # Sort data with numeric strings converted to numbers
            data_df = TableTools._fetch_as_dataframe(table, attribute_name, sort_attribute_names=sort_attribute_names)
            data = data_df[attribute_name].values.tolist()
        else:
            data = list(table.fetch(attribute_name))
        return data
    
    @staticmethod
    def _fetch_multiple(table, attribute_names, sort_attribute_names=None):

        attribute_names = _parse_iterable_inputs(attribute_names)
        sort_attribute_names = _parse_iterable_inputs(sort_attribute_names)
        # Fetch multiple columns of data specified by attribute names
        return {attribute_name : TableTools._fetch(table, attribute_name, sort_attribute_names=sort_attribute_names) for attribute_name in attribute_names}
    
    @staticmethod
    def _fetch_conformed_data(conformer_table, conformer_attribute_name, shaper_table, shaper_attribute_name):

        # Reshape a column of data from the conformer table to match the column of data from the shaper table
        shaper_data = _sort_data_numeric_string(list(set( shaper_table.fetch(shaper_attribute_name) )))
        shaper_data = [[item] for item in shaper_data]
        conformer_data = [None]*len(shaper_data)
        # Fetch and reshape from the conformer table restricted to each value of the shaper attribute
        for ndx, item in enumerate(shaper_data):
            # Get shared primary attribute names across conformer and shaper tables
            shared_attribute_names = get_shared_primary_key([conformer_table, shaper_table])
            # Serially query the conformer table using the key determined by the shaper attribute value
            conformer_query = conformer_table
            for shared_attribute_name in shared_attribute_names:
                shared_data= list( (shaper_table & {shaper_attribute_name : item[0]}).fetch(shared_attribute_name) )
                conformer_query = conformer_query & sql_or_query(shared_attribute_name, shared_data)
            # Fetch the column of data from the conformer table
            conformer_data[ndx] = list(conformer_query.fetch(conformer_attribute_name))
        return conformer_data, shaper_data

    @staticmethod
    def _fetch_as_dataframe(table, attribute_names, sort_attribute_names=None, index_names=None, reset_index=True):

        attribute_names = _parse_iterable_inputs(attribute_names)
        sort_attribute_names = _parse_iterable_inputs(sort_attribute_names)
        # Convert table to a dataframe
        table_df = table.fetch(format='frame')
        # Store index column names to recreate index after dropping unused columns
        index_columns = table_df.index.names
        table_df.reset_index(inplace=True)
        # Sort dataframe columns
        if sort_attribute_names:
            table_df = _sort_dataframe_numeric_string(table_df, sort_attribute_names)
        
        # Drop columns that aren't being fetched
        drop_columns = set(table_df.columns) - set(attribute_names)
        table_df = table_df.drop(columns=drop_columns)
        # Recreate dataframe index
        if not reset_index or index_names is not None:
            if index_names is None:
                # Use the remaining original indices to recreate an index
                index_names = list(set(index_columns) - drop_columns)
            # Create a new index using the specified column names
            table_df.set_index(index_names, inplace=True)
        return table_df
    
    
    @staticmethod
    def _create_table_entries_indicator(table, attribute_names, attribute_values):

        # Create indicator for table entries corresponding to all combinations of keys
        table_entries_ind = {}
        attribute_values = _parse_table_attribute_values(table, attribute_names, attribute_values=attribute_values)
        queries = sql_combinatoric_query(attribute_names, attribute_values)
        for key in queries:
            # Query table and check if entry exists or not
            query = table & key
            ind = True if query else False
            _nested_dict_set(table_entries_ind, list(key.values()), ind) 
        return table_entries_ind

    @staticmethod
    def _table_ind_count(ind):

        # Count the number of table entries found
        return sum([TableTools._table_ind_count(val) if isinstance(val, dict) else val for val in ind.values()])
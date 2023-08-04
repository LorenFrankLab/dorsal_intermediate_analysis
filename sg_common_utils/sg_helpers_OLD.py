import itertools
import numpy as np
import pandas as pd

from utils.data_helpers import (flatten_dataframe,
                                iterable_to_string,
                                parse_iterable_inputs,
                                parse_iterable_outputs,
                                sort_dataframe)

def get_table_name(table):

    # Convert a variable whose value is a table to a string literal
    return table.__name__


def get_key(table):

    # Get the attribute names of a table
    return list(table.heading.attributes.keys())

def get_primary_key(table):

    # Get primary attribute names of a table
    return table.primary_key

def get_secondary_key(table):

    # Get secondary attribute names of a table
    return list( set(get_key(table)) - set(get_primary_key(table)) )

def get_shared_key(tables):

    # Get attribute names shared by all tables
    keys = [None]*len(tables)
    for ndx, table in enumerate(tables):
        keys[ndx] = set(get_key(table))
    return list(set.intersection(*keys))

def get_shared_primary_key(tables):
    
    # Get primary attribute names shared by all tables
    keys = [None]*len(tables)
    for ndx, table in enumerate(tables):
        keys[ndx] = set(get_primary_key(table))
    return list(set.intersection(*keys))

def get_shared_secondary_key(tables):

    # Get secondary attribute names shared by all tables
    keys = [None]*len(tables)
    for ndx, table in enumerate(tables):
        keys[ndx] = set(get_secondary_key(table))
    return list(set.intersection(*keys))


def parse_restriction_key(restriction_dict):

    attribute_names = [None]*len(restriction_dict)
    attribute_values = [None]*len(restriction_dict)
    # Parse each attribute name-value pair
    for ndx, (name, values) in enumerate(restriction_dict.items()):
        values = parse_iterable_inputs(values)
        attribute_names[ndx] = name
        attribute_values[ndx] = values
    return attribute_names, attribute_values

def parse_table_attribute_values(table, attribute_names, attribute_values=None):

    attribute_names, attribute_values = parse_iterable_inputs(attribute_names, attribute_values)
    # Get all possible attribute values for any attribute names with no values specified
    if attribute_values is None:
        attribute_values = [None]*len(attribute_names)
    for ndx, (name, values) in enumerate(zip(attribute_names, attribute_values)):
        # If no attribute values are specified, get all possible values for that attribute
        if values is None:
            attribute_values[ndx] = np.unique(table.fetch(name))
    # Package attribute values into a 1-item list if a single attribute name is given
    if len(attribute_names) == 1 and len(attribute_values) > 1 and all([isinstance(value, str) for value in attribute_values]):
        attribute_values = [attribute_values]
    return attribute_names, attribute_values


def sql_or_query(attribute_name, attribute_values):

    attribute_values = parse_iterable_inputs(attribute_values)
    # Enclose each attribute value in double quotes
    attribute_values = ['\"' + str(value) + '\"' for value in attribute_values]
    # Create OR query from iterable of attribute values
    delimiter = attribute_name + ' = '
    separator = ' OR ' + delimiter
    return iterable_to_string(attribute_values, prefix=delimiter, separator=separator)

def sql_multi_query(attribute_names, attribute_values):

    attribute_names, attribute_values = parse_iterable_inputs(attribute_names, attribute_values)
    # Create a list of serial queries from the given attribute names and values
    queries = [None]*len(attribute_names)
    for ndx, (name, values) in enumerate(zip(attribute_names, attribute_values)):
        queries[ndx] = sql_or_query(name, values)
    return queries

def sql_combinatoric_query(attribute_names, attribute_values):

    # Create each possible combination of queries from the given attribute names and values
    query_product = list(itertools.product(*attribute_values))
    queries = [None]*len(query_product)
    for ndx, query_values in enumerate(query_product):
        queries[ndx] = {name : value for (name, value) in zip(attribute_names, query_values)}
    return queries

def create_key(**kwargs):

    # Create a query key using the provided keyword arguments
    query_key = {key : value for key, value in kwargs.items()}
    return query_key


def query_table(table, attribute_names, attribute_values=None):

    attribute_names, attribute_values = parse_table_attribute_values(table, attribute_names, attribute_values=attribute_values)
    # Ensure the number of attribute values matches the number of attribute names
    if len(attribute_names) != len(attribute_values):
        raise ValueError(f"Number of attribute names doesn't match number of containers for attribute values")
    # Serially query the table using the attribute names and values for those attributes
    query = table
    for name, values in zip(attribute_names, attribute_values):
        return table & sql_or_query(name, values)
    return query

def query_by_key(table, key=None, **kwargs):

    # Create a query key from keyword arguments if no key is specified
    if key is None:
        key = create_key(**kwargs)
    # Create queries from the key
    query = table
    if key:
        attribute_names, attribute_values = parse_restriction_key(key)
        query = query & sql_multi_query(attribute_names, attribute_values)
    return query

def query_to_dataframe(table):

    # Convert table to a dataframe
    table_df = flatten_dataframe(table.fetch(format='frame'))
    return table_df


def fetch(table, attribute_names, sort_attribute_names=None):

    attribute_names, sort_attribute_names = parse_iterable_inputs(attribute_names, sort_attribute_names)
    # Fetch data corresponding to the attribute name from the table
    fetches = {name : None for name in attribute_names}
    for attribute_name in attribute_names:
        if sort_attribute_names is not None:
            # Sort data with numeric strings sorted as floats
            data_df = fetch_as_dataframe(table, attribute_name, sort_attribute_names=sort_attribute_names)
            data = data_df[attribute_name].values.tolist()
        else:
            data = list(table.fetch(attribute_name))
        fetches[attribute_name] = data
        
    # Unpack data out of dictionary if only one attribute was fetched
    if len(attribute_names) == 1:
        fetches = fetches[attribute_names[0]]
    return fetches

def fetch_as_dataframe(table, attribute_names=None, sort_attribute_names=None):

    if attribute_names is None:
        # Fetch all columns
        attribute_names = get_key(table)
    attribute_names, sort_attribute_names = parse_iterable_inputs(attribute_names, sort_attribute_names)
    # Convert table to a dataframe
    table_df = flatten_dataframe(table.fetch(format='frame'))
    if not table_df.empty and sort_attribute_names is not None:
        # Sort the dataframe
        table_df = sort_dataframe(table_df, sort_attribute_names)
    # Drop columns that aren't being fetched
    drop_columns = set(table_df.columns)-set(attribute_names)
    table_df = table_df.drop(columns=drop_columns)
    table_df = table_df[attribute_names]
    return table_df

def fetch_conformed_data(fetch_table, fetch_attribute_names, shaper_table, shaper_attribute_name, dataframe=True, flatten=True):

    fetch_attribute_names = parse_iterable_inputs(fetch_attribute_names)
    # Create dictionary of the conformed and shaper tables
    fetch_name = get_table_name(fetch_table)
    shaper_name = get_table_name(shaper_table)
    tables_dict = {fetch_name : fetch_table, shaper_name : shaper_table}
    # Create dictionary of the shaper and conformed attributes to fetch
    shaper_dict = {shaper_name : shaper_attribute_name}
    fetch_dict = {fetch_name : fetch_attribute_names}
    # Fetch conformed data as a dataframe
    data_df = conformed_fetch(tables_dict, fetch_dict, shaper_dict, flatten=flatten)
    
    if not dataframe:
        # Unpack shaper data into a dictionary
        data_dict = {shaper_name : {shaper_attribute_name : data_df.index.get_level_values(1)}}
        # Unpack conformed data into a dictionary
        for name in fetch_attribute_names:
            data_dict[fetch_name][name] = data_df[(fetch_name, name)].values.tolist()
        return data_dict
    else:
        return data_df

def conformed_fetch(tables_dict, fetch_attributes_dict, shaper_attribute_dict, sort_attributes_dict=None, flatten=True):

    if sort_attributes_dict is None:
        # Use shaper table and attribute to sort data if no sort table and attributes are specified
        sort_attributes_dict = shaper_attribute_dict
    _validate_conformed_fetch(tables_dict, fetch_attributes_dict, shaper_attribute_dict, sort_attributes_dict)
    
    # Get shaper table and shaper attribute name
    shaper_table_name = list(shaper_attribute_dict.keys())[0]
    shaper_attribute_name = parse_iterable_inputs(*shaper_attribute_dict.values())[0]
    shaper_table = tables_dict[shaper_table_name]
    # Get sort table and sort attribute names
    sort_table_name = list(sort_attributes_dict.keys())[0]
    sort_attribute_names = parse_iterable_inputs(*sort_attributes_dict.values())
    sort_table = tables_dict[sort_table_name]
    # Ensure conformer tables are compatible with the shaper table
    for table in tables_dict.values():
        _validate_table_conformability(table, shaper_table, sort_table)
    
    # Get fetch table attribute names
    fetch_attributes_dict = {key : parse_iterable_inputs(value) for (key, value) in fetch_attributes_dict.items()}
    fetch_attribute_names = list( itertools.chain(*parse_iterable_inputs(fetch_attributes_dict.values())) )
    # Initialize dictionary from which conformed data dataframe will be created
    column_names = list( itertools.chain(*[[(key, item) for item in value] for (key, value) in fetch_attributes_dict.items()]) )
    data_dict = {'index' : None,
                 'columns' : column_names,
                 'data' : [None]*(len(fetch_attribute_names)),
                 'index_names' : ['shaper_table_name', shaper_attribute_name],
                 'column_names' : ['table_name', 'attribute_name']}

    # Fetch columns from tables and resize to conform to shaper data size
    for table_name, table in tables_dict.items():
        if table_name not in fetch_attributes_dict.keys():
            continue
        for attribute_name in fetch_attributes_dict[table_name]:
            shaper_data, conformer_data = _fetch_conformed_data(shaper_table,
                                                                shaper_attribute_name,
                                                                table,
                                                                attribute_name,
                                                                sort_table,
                                                                sort_attribute_names)
            # Get index of data array corresponding to current conformed data
            idx = [col[0] == table_name and col[1] == attribute_name for col in column_names].index(True)
            data_dict['data'][idx] = conformer_data
    
    # Transpose data
    data_dict['data'] = list( zip(*data_dict['data']) )
    # Store shaper data
    data_dict['index'] = [(shaper_table_name, item) for item in shaper_data]
    # Store data in a data frame with table names as labels and fetched data as columns
    data_df = pd.DataFrame.from_dict(data_dict, orient='tight')
    if flatten:
        data_df = flatten_dataframe(data_df)
    return data_df

def _fetch_conformed_data(shaper_table, shaper_attribute_name, conformer_table, conformer_attribute_name, sort_table, sort_attribute_names):

    # Sort shaper data according to the sort attributes
    shaper_data = sort_table.fetch(shaper_attribute_name)
    sort_df = fetch_as_dataframe(sort_table, attribute_names=sort_attribute_names)
    sort_df['shaper'] = shaper_data
    shaper_data = sort_dataframe(sort_df, sort_attribute_names)['shaper'].values
    _, sort_idx = np.unique(shaper_data, return_index=True)
    shaper_data = shaper_data[np.sort(sort_idx)]
    
    conformer_data = [None]*len(shaper_data)
    # Fetch and reshape from the conformer table restricted to each value of the shaper attribute
    for ndx, item in enumerate(shaper_data):
        # Get shared primary attribute names across conformer and shaper tables
        shared_attribute_names = get_shared_primary_key([conformer_table, shaper_table])
        # Serially query the conformer table using the key determined by the shaper attribute value
        conformer_query = conformer_table
        for shared_attribute_name in shared_attribute_names:
            shared_data= list( (shaper_table & {shaper_attribute_name : item}).fetch(shared_attribute_name) )
            conformer_query = conformer_query & sql_or_query(shared_attribute_name, shared_data)
        # Fetch the column of data from the conformer table
        conformer_data[ndx] = list(conformer_query.fetch(conformer_attribute_name))
    
    return shaper_data, conformer_data 

def _validate_conformed_fetch(tables_dict, fetch_attributes_dict, shaper_attribute_dict, sort_attributes_dict):

    # Ensure dictionary keys for tables contains all tables to fetch from
    if any([key not in tables_dict.keys() for key in fetch_attributes_dict.keys()]):
        raise ValueError(f"The keys of {tables_dict} must contain the keys of {fetch_attributes_dict}")
    # Ensure only one shaper attribute is specified
    if len(shaper_attribute_dict) != 1:
        raise ValueError(f"The shaper attribute dictionary {shaper_attribute_dict} must have only 1 key-value pair")
    # Ensure the key of shaper attribute is also a key of tables
    if not(list(shaper_attribute_dict.keys())[0] in tables_dict.keys()):
        raise ValueError(f"The key of {shaper_attribute_dict} must be a key in {tables_dict}")
    if sort_attributes_dict is not None:
        # Ensure only one sort table is specified
        if len(sort_attributes_dict) != 1:
            raise ValueError(f"The sort attribute dictionary {sort_attributes_dict} must have only 1 key-value pair")
        # Ensure the key of sort attribute is also a key for tables
        if not list(sort_attributes_dict.keys())[0] in tables_dict.keys():
            raise ValueError(f"The key of {sort_attributes_dict} must be a key in {tables_dict}")
        sort_table_name = list(sort_attributes_dict.keys())[0]
        # Ensure the shaper attribute name is in the key of sort table
        if not parse_iterable_inputs(*shaper_attribute_dict.values())[0] in get_key(tables_dict[sort_table_name]):
            raise ValueError(f"The value of {shaper_attribute_dict} must be in the key of table '{sort_table_name}'")

def _validate_table_conformability(conformer_table, shaper_table, sort_table):

    # Ensure that conformer table primary attributes are a subset of shaper table primary attributes up to possibly one exception
    conformer_primary_key = set(get_primary_key(conformer_table))
    shaper_primary_key = set(get_primary_key(shaper_table))
    primary_key_diff = conformer_primary_key - shaper_primary_key
    if len(primary_key_diff) > 1:
        raise ValueError(f"'{get_table_name(conformer_table)}' cannot be shaped by '{get_table_name(shaper_table)}'")
    # Ensure that the sort and shaper table attributes have nonzero intersection
    sort_key = set(get_key(sort_table))
    shaper_key = set(get_key(shaper_table))
    key_intersect = sort_key & shaper_key
    if not key_intersect:
        raise ValueError(f"'{get_table_name(shaper_table)}' data cannot be sorted by '{get_table_name(sort_table)}'")


def cross_fetch(tables_dict, source_attribute_dict, target_attributes_list, sort_attributes_dict, flatten=True):

    if sort_attributes_dict is None:
        # Use source table and attribute to sort data if no sort table and attributes are specified
        sort_attributes_dict = source_attribute_dict
    _validate_cross_fetch(tables_dict, source_attribute_dict, target_attributes_list, sort_attributes_dict)

    # Get source table and source attribute name
    source_table_name = list(source_attribute_dict.keys())[0]
    source_attribute_name = parse_iterable_inputs(*source_attribute_dict.values())[0]
    source_table = tables_dict[source_table_name]
    # Get sort table and sort attribute names
    sort_table_name = list(sort_attributes_dict.keys())[0]
    sort_attribute_names = parse_iterable_inputs(*sort_attributes_dict.values())
    sort_table = tables_dict[sort_table_name]
    # Get target tables and target attribute names
    target_attribute_names = [parse_iterable_inputs(*target.values()) for target in target_attributes_list]
    target_attribute_names = list(itertools.chain(*target_attribute_names))
    n_attributes = [len( parse_iterable_inputs(list(target.values())[0]) ) for target in target_attributes_list]
    target_table_names = [[list(target.keys())[0]]*n_values for target, n_values in zip(target_attributes_list, n_attributes)]
    target_table_names = list(itertools.chain(*target_table_names))
    target_tables = [tables_dict[name] for name in target_table_names]
    # Ensure target tables are compatible with source table
    _validate_table_crossability(source_table, target_tables, sort_table)

    # Initialize dictionary from which cross-fetched data dataframe will be created
    column_names = [(table, attribute) for (table, attribute) in zip(target_table_names, target_attribute_names)]
    data_dict = {'index' : None,
                 'columns' : column_names,
                 'data' : [None]*(len(target_attribute_names)),
                 'index_names' : ['source_table_name', source_table_name],
                 'column_names' : ['table_name', 'attribute_name']}
    
    # Fetch columns from tables sequentially from source table through all target tables
    source_data, target_data = _fetch_cross_data(source_table,
                                                 source_attribute_name,
                                                 target_tables[0],
                                                 target_attribute_names[0],
                                                 sort_table=sort_table,
                                                 sort_attribute_names=sort_attribute_names)
    idx = [col[0] == target_table_names[0] and col[1] == target_attribute_names[0] for col in column_names].index(True)
    data_dict['data'][idx] = target_data
    data_dict['index'] = [(source_table_name, item) for item in source_data]
    
    for ndx, (table_name, attribute_name) in enumerate(zip(target_table_names, target_attribute_names)):
        if ndx == 0:
            continue
        # Cross-fetch data using previous target table as the source table
        _, target_data = _fetch_cross_data(target_tables[ndx-1],
                                           target_attribute_names[ndx-1],
                                           target_tables[ndx],
                                           target_attribute_names[ndx],
                                           source_data=target_data)
        # Get index of data array corresponding to current target data
        idx = [col[0] == table_name and col[1] == attribute_name for col in column_names].index(True)
        data_dict['data'][idx] = target_data 
    
    # Transpose data
    data_dict['data'] = list( zip(*data_dict['data']) )
    # Store data in a data frame with table names as labels and fetched data as columns
    data_df = pd.DataFrame.from_dict(data_dict, orient='tight')
    if flatten:
        data_df = flatten_dataframe(data_df)
    return data_df

def _fetch_cross_data(source_table, source_attribute_name, target_table, target_attribute_name, sort_table=None, sort_attribute_names=None, source_data=None):
    
    if source_data is not None:
    # Ensure that either source data is specified or sort table and attributes, but not both
        if sort_table is not None and sort_attribute_names is not None:
            raise ValueError(f"A source dataset and a sort table and attributes to sort by can't both be specified")
    else:
        if sort_table is None and sort_attribute_names is None:
            # Get sorted source data if no sort attributes specified
            sort_table = source_table
            sort_attribute_names = source_attribute_name
        elif (sort_table is None and sort_attribute_names is not None) \
            or (sort_table is not None and sort_attribute_names is None):
            # Both source table and source attributes must be specified
            raise ValueError(f"A sort table and attributes to sort by must both be specified")
        # Get source data and sort according to sort table attributes
        source_data, _ = _fetch_conformed_data(source_table,
                                               source_attribute_name,
                                               source_table,
                                               source_attribute_name,
                                               sort_table,
                                               sort_attribute_names)
    
    # Fetch and reshape from the target table restricted to each value of the source data
    _, target_data = _fetch_conformed_data(source_table,
                                           source_attribute_name,
                                           target_table,
                                           target_attribute_name,
                                           sort_table,
                                           sort_attribute_names)


    #target_data = [None]*len(source_data)
    ## Fetch and reshape from the target table restricted to each value of the source data
    #for ndx, item in enumerate(source_data):
    #    target_query = target_table & sql_or_query(source_attribute_name, item)
    #    target_data[ndx] = list(target_query.fetch(target_attribute_name))
    source_data = parse_iterable_outputs(source_data)
    return source_data, target_data


def _fetch_cross_data_OLD(source_table, source_attribute_name, target_table, target_attribute_name, sort_table=None, sort_attribute_names=None, source_data=None):
    
    # Ensure that either source data is specified or sort table and attributes, but not both
    if sort_table is not None and sort_attribute_names is not None and source_data is not None:
        raise ValueError(f"A source dataset and a sort table and attributes to sort by can't both be specified")
    # Get sorted source data if not already specified
    if source_data is None:
        # Get source data and sort according to sort table attributes
        if sort_table is not None and sort_attribute_names is not None:
            source_data = sort_table.fetch(source_attribute_name)
            sort_df = fetch_as_dataframe(sort_table, attribute_names=sort_attribute_names)
            sort_df['source'] = source_data
            source_data = sort_dataframe(sort_df, sort_attribute_names)['source'].values
        elif sort_table is None and sort_attribute_names is None:
            # Get sorted source data if no sort attributes specified
            source_data = fetch(source_table, source_attribute_name)
        else:
            # Both source table and source attributes must be specified
            raise ValueError(f"A sort table and attributes to sort by must both be specified")
        _, sort_idx = np.unique(source_data, return_index=True)
        source_data = source_data[np.sort(sort_idx)]

    target_data = [None]*len(source_data)
    # Fetch and reshape from the target table restricted to each value of the source data
    for ndx, item in enumerate(source_data):
        target_query = target_table & sql_or_query(source_attribute_name, item)
        target_data[ndx] = list(target_query.fetch(target_attribute_name))
    return source_data, target_data

def _validate_cross_fetch(tables_dict, source_attribute_dict, target_attributes_list, sort_attributes_dict):

    for ndx, target_dict in enumerate(target_attributes_list):       
        # Ensure list of target tables contains dictionaries specifying only one table
        if len(target_dict) != 1:
            raise ValueError(f"Each target table must have only 1 key-value pair")
        # All but last target table in list of target table must fetch only 1 attribute
        if ndx < len(target_attributes_list)-1:
            if len( list(target_dict.values())[0] ) != 1:
                raise ValueError(f"The value of {target_dict} must be a 1-item iterable")
        # Ensure dictionary keys for tables contains all tables to fetch from
        if any([key not in tables_dict.keys() for key in target_dict.keys()]):
            raise ValueError(f"The keys of {tables_dict} must contain the keys of {target_dict}")
    # Ensure list of target tables doesn't contain duplicate tables
    target_table_names = [list(target_dict.keys())[0]]
    if len(target_table_names) != len(set(target_table_names)):
        raise ValueError(f"Each target table can only be specified once")
    # Ensure only one source table is specified
    if len(source_attribute_dict) != 1:
        raise ValueError(f"The source attribute dictionary {source_attribute_dict} must have only 1 key-value pair")
    # Ensure the key of source attribute is also a key of tables
    if not(list(source_attribute_dict.keys())[0] in tables_dict.keys()):
        raise ValueError(f"The key of {source_attribute_dict} must be a key in {tables_dict}")
    if sort_attributes_dict is not None:
        # Ensure only one sort table is specified
        if len(sort_attributes_dict) != 1:
            raise ValueError(f"The sort attribute dictionary {sort_attributes_dict} must have only 1 key-value pair")
        # Ensure the key of sort attribute is also a key for tables
        if not list(sort_attributes_dict.keys())[0] in tables_dict.keys():
            raise ValueError(f"The key of {sort_attributes_dict} must be a key in {tables_dict}")
        sort_table_name = list(sort_attributes_dict.keys())[0]
        # Ensure the source attribute is in the key of sort table
        if not parse_iterable_inputs(*source_attribute_dict.values())[0] in get_key(tables_dict[sort_table_name]):
            raise ValueError(f"The value of {source_attribute_dict} must be in the key of the table '{sort_table_name}'")

def _validate_table_crossability(source_table, target_tables, sort_table):

    # Check crossability of each table to its next target
    for ndx, target_table in enumerate(target_tables):
        if ndx == 0:
            previous_target_table = source_table
        else:
            previous_target_table = target_tables[ndx-1]        
        # Ensure that previous target and current target table attributes have nonzero intersection
        previous_target_key = set(get_key(previous_target_table))
        target_key = set(get_key(target_table))
        key_intersect = target_key & previous_target_key
        if not key_intersect:
            raise ValueError(f"'{get_table_name(target_table)}' cannot be the target of '{get_table_name(previous_target_table)}'")
    # Ensure that the sort and source table attributes have nonzero intersection
    sort_key = set(get_key(sort_table))
    source_key = set(get_key(source_table))
    key_intersect = sort_key & source_key
    if not key_intersect:
        raise ValueError(f"'{get_table_name(source_table)}' data cannot be sorted by '{get_table_name(sort_table)}'")
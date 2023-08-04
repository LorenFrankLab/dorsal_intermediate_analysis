import itertools
import numpy as np
import pandas as pd

from utils.data_helpers import (flatten_dataframe,
                                iterable_to_string,
                                parse_iterable_inputs,
                                parse_iterable_outputs,
                                sort_dataframe,
                                unique_data,
                                unique_dataframe)

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
    if sort_attribute_names is None:
        sort_attribute_names = attribute_names
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


def multi_table_fetch(tables_dict, shaper_attribute_dict, conformer_attributes_list, sort_attributes_dict=None, flatten=True):

    if sort_attributes_dict is None:
        # Use shaper table and attribute to sort data if no sort table and attributes are specified
        sort_attributes_dict = shaper_attribute_dict
    _validate_multiplexed_fetch(tables_dict, shaper_attribute_dict, conformer_attributes_list, sort_attributes_dict, chain_fetches=False)
    
    # Get shaper, conformer, and sort tables and attribute names
    shaper = _parse_multiplex_fetch_arguments(tables_dict, [shaper_attribute_dict])
    conformer = _parse_multiplex_fetch_arguments(tables_dict, conformer_attributes_list)
    sorter = _parse_multiplex_fetch_arguments(tables_dict, [sort_attributes_dict])
    # Ensure conformer tables are compatible with the shaper table
    _validate_multiplexed_tables(shaper['tables'], conformer['tables'], sorter['tables'], primary_key_intersect=False, chain_fetches=False)    
    data_df = _multiplex_fetch_as_dataframe(shaper, conformer, sorter, cross_fetches=False, chain_fetches=False, flatten=flatten)
    return data_df

def cross_fetch(tables_dict, shaper_attribute_dict, conformer_attributes_list, sort_attributes_dict=None, flatten=True):

    if sort_attributes_dict is None:
        # Use shaper table and attribute to sort data if no sort table and attributes are specified
        sort_attributes_dict = shaper_attribute_dict
    _validate_multiplexed_fetch(tables_dict, shaper_attribute_dict, conformer_attributes_list, sort_attributes_dict, chain_fetches=False)
    
    # Get shaper, conformer, and sort tables and attribute names
    shaper = _parse_multiplex_fetch_arguments(tables_dict, [shaper_attribute_dict])
    conformer = _parse_multiplex_fetch_arguments(tables_dict, conformer_attributes_list)
    sorter = _parse_multiplex_fetch_arguments(tables_dict, [sort_attributes_dict])
    # Ensure conformer tables are compatible with the shaper table
    _validate_multiplexed_tables(shaper['tables'], conformer['tables'], sorter['tables'], primary_key_intersect=True, chain_fetches=False)    
    data_df = _multiplex_fetch_as_dataframe(shaper, conformer, sorter, cross_fetches=True, chain_fetches=False, flatten=flatten)
    return data_df

def _multiplex_fetch_as_dataframe(origin, fetches, sorter, cross_fetches=False, chain_fetches=False, flatten=True):

    # Initialize dictionary from which conformed data dataframe will be created
    column_names = [(table, attribute) for (table, attribute) in zip(fetches['table_names'], fetches['attribute_names'])]
    data_dict = {'index' : None,
                 'columns' : column_names,
                 'data' : [None]*len(fetches['attribute_names']),
                 'index_names' : ['shaper_table_name', origin['attribute_names']],
                 'column_names' : ['table_name', 'attribute_name']}
    data = _multiplex_fetch([origin['tables']]+fetches['tables'],
                            [origin['attribute_names']]+fetches['attribute_names'],
                            sort_table=sorter['tables'],
                            sort_attribute_names=sorter['attribute_names'],
                            cross_fetches=cross_fetches,
                            chain_fetches=chain_fetches)
    data_dict['index'] = data[origin['attribute_names']].values.tolist()
    data.drop(columns=origin['attribute_names'], inplace=True)
    data_dict['data'] = data
    return data_dict

def _multiplex_fetch(fetch_tables, fetch_attribute_names, sort_table=None, sort_attribute_names=None, cross_fetches=False, chain_fetches=False):

    if sort_table is None and sort_attribute_names is not None \
        or sort_table is not None and sort_attribute_names is None:
        # Both source table and source attributes must be specified
        raise ValueError(f"A sort table and attributes to sort by must both be specified")
    if sort_table is None and sort_attribute_names is None:
        sort_table = fetch_tables[0]
        sort_attribute_names = fetch_attribute_names[0]
    
    if not cross_fetches and not chain_fetches:
        join_table = fetch_tables[0].proj(fetch_attribute_names[0])*sort_table.proj(sort_attribute_names)
        for table, attribute_name in zip(fetch_tables[1:], fetch_attribute_names[1:]):
            if attribute_name not in set(get_secondary_key(table)) & set(get_secondary_key(join_table)):
                join_table = join_table*table.proj(attribute_name)
    fetch_data = fetch_as_dataframe(join_table, fetch_attribute_names, sort_attribute_names=sort_attribute_names)
    return fetch_data

    fetch_data = [None]*len(origin_data)
    # Fetch and reshape from the target table restricted to each value of the source data 
    if not chain_fetches:
        if cross_fetches:
            # Get shared primary attribute names across origin and fetch tables
            shared_attribute_names = get_shared_primary_key([origin_table, fetch_table])
            # Serially query the fetch table based on shared attribute values
            fetch_query = fetch_table
            for shared_attribute_name in shared_attribute_names:
                shared_query = origin_table & sql_or_query(origin_attribute_name, origin_data)
                shared_data = fetch(shared_query, shared_attribute_name)
                fetch_query = fetch_query & sql_or_query(shared_attribute_name, shared_data)
        else:
            # Fetch and reshape from the target table restricted to each value of the source data
            fetch_query = fetch_table & sql_or_query(origin_attribute_name, origin_data)
    else:
        # Restrict fetch table using first fetch attribute and fetch specified fetch attributes
        fetch_query = fetch_table & sql_or_query(fetch_attribute_names[0], origin_data)
    
    attribute_names = list(set( parse_iterable_inputs(fetch_attribute_names)+parse_iterable_inputs(origin_attribute_name) ))
    fetch_df = fetch_as_dataframe(fetch_query, attribute_names)
    for ndx, item in enumerate(origin_data):
        fetch_data[ndx] = fetch_df[fetch_df[origin_attribute_name] == item][fetch_attribute_names].values.tolist()
        
    # Unpack fetched and origin data
    origin_data = list(itertools.chain( *[[item]*len(fetch_values) for item, fetch_values in zip(origin_data, fetch_data)] ))
    fetch_data = list(itertools.chain(*fetch_data))
    return origin_data, fetch_data

def _get_multiplex_data_index(sort_table=None, sort_attribute_names=None, origin_data=None):

    # Ensure that either source data is specified or sort table and attributes, but not both
    if sort_table is not None and sort_attribute_names is not None and origin_data is not None:
        raise ValueError(f"A source dataset and a sort table and attributes to sort by can't both be specified")
    # Get sorted source data if not already specified
    if origin_data is None:
        # Get source data and sort according to sort table attributes
        if sort_table is not None and sort_attribute_names is not None:
            attribute_names = list(set( parse_iterable_inputs(sort_attribute_names)+parse_iterable_inputs(origin_attribute_name) ))
            sort_df = fetch_as_dataframe(sort_table, attribute_names=attribute_names)
            origin_df = sort_dataframe(sort_df, sort_attribute_names)

        elif sort_table is None and sort_attribute_names is None:
            # Both source table and source attributes must be specified
            raise ValueError(f"A sort table and attributes to sort by must both be specified")
        origin_data, _ = unique_dataframe(origin_df, origin_attribute_name)
        origin_data = origin_data[origin_attribute_name].values

def _parse_multiplex_fetch_arguments(tables_dict, dict_list):

    # Get dictionary of tables, table names, and fetch attributes
    fetch_attribute_names = [parse_iterable_inputs(*fetch.values()) for fetch in dict_list]
    fetch_attribute_names = list(itertools.chain(*fetch_attribute_names))
    n_attributes = [len( parse_iterable_inputs(list(fetch.values())[0]) ) for fetch in dict_list]
    fetch_table_names = [[list(fetch.keys())[0]]*n_values for fetch, n_values in zip(dict_list, n_attributes)]
    fetch_table_names = list(itertools.chain(*fetch_table_names))
    fetch_tables = [tables_dict[name] for name in fetch_table_names]
    fetch_dict = {'tables' : parse_iterable_outputs(fetch_tables),
                  'table_names' : parse_iterable_outputs(fetch_table_names),
                  'attribute_names' : parse_iterable_outputs(fetch_attribute_names)}
    return fetch_dict

def _validate_multiplexed_fetch(tables_dict, origin_dict, fetch_list, sort_dict, chain_fetches=False):

    for ndx, fetch_dict in enumerate(fetch_list):
        # Ensure list of fetch tables contains dictionaries specifying only one table
        if len(fetch_dict) != 1:
            raise ValueError(f"Each table to fetch from in {fetch_list} must have only one table specified")
        if chain_fetches:
            # All but last table in list of tables to fetch from must fetch exactly 2 attributes
            if ndx < len(fetch_list)-1:
                if len( list(fetch_dict.values())[0] ) != 2:
                    raise ValueError(f"Exactly 2 attributes to be chain-fetched must be specified in {fetch_dict}")
            # Last table in list of tables must fetch at least 2 attributes
            if ndx == len(fetch_list)-1:
                if len( list(fetch_dict.values())[0] ) < 2:
                    raise ValueError(f"At least 2 attributes to be chain-fetched must be specified in {fetch_dict}")
        # Ensure dictionary keys for tables contains all tables to fetch from
        if any([key not in tables_dict.keys() for key in fetch_dict.keys()]):
            raise ValueError(f"The tables specified in {tables_dict.keys()} must contain all tables in {fetch_dict}")
    
    # Ensure only one origin table is specified
    if len(origin_dict) != 1:
        raise ValueError(f"Only one origin table can be specified in {origin_dict}")
    # Ensure dictionary keys for tables contains the origin table
    if list(origin_dict.keys())[0] not in tables_dict.keys():
        raise ValueError(f"The tables specified in {tables_dict.keys()} must contain the table in {origin_dict}")
    
    if sort_dict is not None:
        # Ensure only one sort table is specified
        if len(sort_dict) != 1:
            raise ValueError(f"Only one sort table can be specified in {sort_dict}")
        # Ensure dictionary keys for tables contains the sort table
        if not list(sort_dict.keys())[0] in tables_dict.keys():
            raise ValueError(f"The tables specified in {tables_dict.keys()} must contain the table in {sort_dict}")
        sort_table_name = list(sort_dict.keys())[0]
        origin_attribute_name = parse_iterable_inputs(*origin_dict.values())[0] 
        # Ensure the origin attribute is also an attribute of the sort table
        if not origin_attribute_name in get_key(tables_dict[sort_table_name]):
            raise ValueError(f"The attribute specified in {origin_dict} must be an attribute of the table '{sort_table_name}'")
        
        # Ensure that the origin attribute has enough entries in the sort table to be uniquely sorted
        origin_table_name = list(origin_dict.keys())[0]
        origin_values = set(fetch(tables_dict[origin_table_name], origin_attribute_name))
        sort_origin_values = set(fetch(tables_dict[sort_table_name], origin_attribute_name))
        if origin_values-sort_origin_values:
            raise ValueError(f"The attribute specified in {origin_dict} can't be sorted by {sort_dict}")

def _validate_multiplexed_tables(origin_table, fetch_table_list, sort_table, primary_key_intersect=False, chain_fetches=False):

    # Check if each table is compatible with each other in a multiplexed fetch
    for ndx, fetch_table in enumerate(fetch_table_list):
        if ndx == 0 or not chain_fetches:
            # Check compatibility between each fetch table and origin table if fetches aren't chained
            previous_fetch_table = origin_table
        else:
            # Check compatibility of each fetch table with previous fetch table if fetches are chained
            previous_fetch_table = fetch_table_list[ndx-1]

        # Ensure that previous fetch table and current fetch table keys have nonzero intersection
        previous_fetch_key = set(get_key(previous_fetch_table))
        fetch_key = set(get_key(fetch_table))
        key_intersect = fetch_key & previous_fetch_key
        if not key_intersect:
            raise ValueError(f"'{fetch_key}' doesn't have any overlapping keys with '{previous_fetch_key}'")
        
        if primary_key_intersect:
            # Ensure that previous and current fetch table primary keys overlap up to possibly 1 exception
            previous_fetch_primary_key = set(get_primary_key(previous_fetch_table))
            fetch_primary_key = set(get_primary_key(fetch_table))
            primary_key_diff = fetch_primary_key - previous_fetch_primary_key
            if len(primary_key_diff) > 1:
                raise ValueError(f"'{fetch_primary_key}' primary key differs by more than one attribute from '{previous_fetch_primary_key}'")

    # Ensure that the sort and origin table keys have nonzero intersection
    sort_key = set(get_key(sort_table))
    origin_key = set(get_key(origin_table))
    sort_key_intersect = sort_key & origin_key
    if not sort_key_intersect:
        raise ValueError(f"'{origin_key}' doesn't have any overlapping keys with sort table '{sort_key}'")




def _fetch_conformed_data(shaper_table, shaper_attribute_name, conformer_table, conformer_attribute_name, sort_table=None, sort_attribute_names=None, shaper_data=None):

    if shaper_data is not None:
    # Ensure that either shaper data is specified or sort table and attributes, but not both
        if sort_table is not None and sort_attribute_names is not None:
            raise ValueError(f"A shaper dataset and a sort table and attributes to sort by can't both be specified")
    else:
        if sort_table is None and sort_attribute_names is None:
            # Get sorted source data if no sort attributes specified
            sort_table = shaper_table
            sort_attribute_names = shaper_attribute_name
        elif (sort_table is None and sort_attribute_names is not None) \
            or (sort_table is not None and sort_attribute_names is None):
            # Both source table and source attributes must be specified
            raise ValueError(f"A sort table and attributes to sort by must both be specified")

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


def _validate_table_conformability(shaper_table, conformer_tables, sort_table):

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


def cross_fetch_OLD(tables_dict, source_attribute_dict, target_attributes_list, sort_attributes_dict, flatten=True):

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
            source_data = unique_dataframe(sort_df, sort_attribute_names)['source'].values
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


    

def _validate_conformed_fetch(tables_dict, shaper_attribute_dict, fetch_attributes_dict, sort_attributes_dict):

    # Ensure dictionary keys for tables contains all tables to fetch from
    if any([key not in tables_dict.keys() for key in fetch_attributes_dict.keys()]):
        raise ValueError(f"The tables in {tables_dict} must contain all tables in {fetch_attributes_dict}")
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



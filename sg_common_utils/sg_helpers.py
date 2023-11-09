import itertools
import numpy as np

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
        query = query & sql_or_query(name, values)
    return query

def query_by_key(table, key=None, **kwargs):

    # Create a query key from keyword arguments if no key is specified
    if key is None:
        key = create_key(**kwargs)
    # Create queries from the key
    query = table
    if key:
        attribute_names, attribute_values = parse_restriction_key(key)
        query = query_table(query, attribute_names, attribute_values)
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


def multi_table_fetch(tables_dict, shaper_attribute_dict, conformer_attributes_list, sort_attributes_dict=None):

    if sort_attributes_dict is None:
        # Use shaper table and attribute to sort data if no sort table and attributes are specified
        sort_attributes_dict = shaper_attribute_dict
    _validate_multiplexed_fetch(tables_dict, shaper_attribute_dict, conformer_attributes_list, sort_attributes_dict)
    
    # Get shaper, conformer, and sort tables and attribute names
    shaper = _parse_multiplex_fetch_arguments(tables_dict, [shaper_attribute_dict])
    conformer = _parse_multiplex_fetch_arguments(tables_dict, conformer_attributes_list)
    sorter = _parse_multiplex_fetch_arguments(tables_dict, [sort_attributes_dict])
    # Ensure conformer tables are compatible with the shaper table
    _validate_multiplexed_tables(shaper['tables'], conformer['tables'], sorter['tables'])
    data_df = _multiplex_fetch_as_dataframe(shaper, conformer, sorter)
    return data_df

def _multiplex_fetch_as_dataframe(origin, fetches, sorter):

    # Fetch data
    data_df = _multiplex_fetch([origin['tables']]+fetches['tables'],
                               [origin['attribute_names']]+fetches['attribute_names'],
                               sort_table=sorter['tables'],
                               sort_attribute_names=sorter['attribute_names'])
    # Remove any duplicate columns
    data_df = data_df.loc[:,~data_df.columns.duplicated()].copy()
    data_df.set_index(origin['attribute_names'], inplace=True)
    return data_df

def _multiplex_fetch(fetch_tables, fetch_attribute_names, sort_table=None, sort_attribute_names=None):

    if sort_table is None and sort_attribute_names is not None \
        or sort_table is not None and sort_attribute_names is None:
        # Both source table and source attributes must be specified
        raise ValueError(f"A sort table and attributes to sort by must both be specified")
    if sort_table is None and sort_attribute_names is None:
        sort_table = fetch_tables[0]
        sort_attribute_names = fetch_attribute_names[0]
    
    # Merge all fetch and sort tables
    join_table = fetch_tables[0].proj(fetch_attribute_names[0])*sort_table.proj(sort_attribute_names)
    for table, attribute_name in zip(fetch_tables[1:], fetch_attribute_names[1:]):
        if attribute_name not in set(get_secondary_key(join_table)):
            # Merge tables if attribute name is not in the secondary key of the join table
            join_table = join_table*table.proj(attribute_name)
    fetch_data = fetch_as_dataframe(join_table, fetch_attribute_names, sort_attribute_names=sort_attribute_names)
    return fetch_data

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

def _validate_multiplexed_fetch(tables_dict, origin_dict, fetch_list, sort_dict):

    for fetch_dict in fetch_list:
        # Ensure list of fetch tables contains dictionaries specifying only one table
        if len(fetch_dict) != 1:
            raise ValueError(f"Each table to fetch from in {fetch_list} must have only one table specified")
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

def _validate_multiplexed_tables(origin_table, fetch_table_list, sort_table):

    # Check if each table is compatible with each other in a multiplexed fetch
    for ndx, fetch_table in enumerate(fetch_table_list):
        if ndx == 0:
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

    # Ensure that the sort and origin table keys have nonzero intersection
    sort_key = set(get_key(sort_table))
    origin_key = set(get_key(origin_table))
    sort_key_intersect = sort_key & origin_key
    if not sort_key_intersect:
        raise ValueError(f"'{origin_key}' doesn't have any overlapping keys with sort table '{sort_key}'")
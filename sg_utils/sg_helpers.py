import itertools
import numpy as np
import pandas as pd

from utils.common_helpers import _convert_data_datatype, _parse_iterable_inputs, _sort_data_numeric_string

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
        keys[ndx] = set(get_primary_key(table))
    return list(set.intersection(*keys))


def sql_or_query(attribute_name, attribute_values):

    attribute_values = _parse_iterable_inputs(attribute_values)
    # Enclose each attribute value in double quotes
    attribute_values = ['\"' + str(value) + '\"' for value in attribute_values]
    # Create OR query from iterable of attribute values
    delimiter = attribute_name + ' = '
    separator = ' OR ' + delimiter
    return _iterable_to_string(attribute_values, prefix=delimiter, separator=separator)

def sql_multi_query(attribute_names, attribute_values):

    attribute_names = _parse_iterable_inputs(attribute_names)
    attribute_values = _parse_iterable_inputs(attribute_values)
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


def conformed_fetch(tables_dict, fetch_attributes_dict, shaper_attribute_dict, convert_shaper_data=True):

    _validate_conformed_fetch(tables_dict, fetch_attributes_dict, shaper_attribute_dict)
    # Get shaper table and shaper attribute name
    shaper_table_name = list(shaper_attribute_dict.keys())[0]
    shaper_attribute_name = list(shaper_attribute_dict.values())[0]
    shaper_table = tables_dict[shaper_table_name]
    # Ensure conformer tables are compatible with the shaper table
    for table in tables_dict.values():
        _validate_table_conformability(table, shaper_table)
    # Get fetch table attribute names
    fetch_attributes_dict = {key : _parse_iterable_inputs(value) for (key, value) in fetch_attributes_dict.items()}
    fetch_attribute_names = list( itertools.chain(*fetch_attributes_dict.values()) )
    # Initialize dictionary from which conformed data dataframe will be created
    column_names = list( itertools.chain(*[[(key, item) for item in value] for (key, value) in fetch_attributes_dict.items()]) )
    data_dict = {'index' : None,
                 'columns' : column_names,
                 'data' : [None]*(len(fetch_attribute_names)),
                 'index_names' : ['shaper_table_name', shaper_attribute_name],
                 'column_names' : ['table_name', 'attribute_name']}

    # Fetch columns from tables and resize to conform to shaper data size
    for table_name, table in tables_dict.items():
        for attribute_name in fetch_attributes_dict[table_name]:
            conformer_data, shaper_data = _fetch_conformed_data(table, attribute_name, shaper_table, shaper_attribute_name, convert_shaper_data)
            # Get index of data array corresponding to current conformed data
            idx = [col[0] == table_name and col[1] == attribute_name for col in column_names].index(True)
            data_dict['data'][idx] = conformer_data
    # Transpose data
    data_dict['data'] = list( zip(*data_dict['data']) )
    # Store shaper data
    data_dict['index'] = [(shaper_table_name, item) for item in shaper_data]
    # Store data in a data frame with table names as labels and fetched data as columns
    data_df = pd.DataFrame.from_dict(data_dict, orient='tight')
    return data_df


def _validate_conformed_fetch(tables_dict, fetch_attributes_dict, shaper_attribute_dict):

    # Ensure number of tables matches number of sets of attributes to fetch
    if len(tables_dict) != len(fetch_attributes_dict):
        raise ValueError(f"Number of tables must match number of set of attributes to fetch")
    # Ensure dictionary keys for tables and fetch attributes match
    if tables_dict.keys() != fetch_attributes_dict.keys():
        raise ValueError(f"The keys of {tables_dict} must match the keys of {fetch_attributes_dict}")
    # Ensure only one shaper attribute is specified
    if len(shaper_attribute_dict) != 1:
        raise ValueError(f"The shaper attribute dictionary {shaper_attribute_dict} must have only 1 key-value pair")
    # Ensure the key of shaper attribute is also a key for both tables and fetch attributes
    if not(list(shaper_attribute_dict.keys())[0] in tables_dict.keys() and \
        list(shaper_attribute_dict.keys())[0] in fetch_attributes_dict.keys()):
        raise ValueError(f"The key of {shaper_attribute_dict} must be a key in both {tables_dict} and {fetch_attributes_dict}")

def _validate_table_conformability(conformer_table, shaper_table):

    # Ensure that conformer table primary attributes are a subset of shaper table primary attributes up to possibly one exception
    conformer_primary_key = set(get_primary_key(conformer_table))
    shaper_primary_key = set(get_primary_key(shaper_table))
    primary_key_diff = conformer_primary_key - shaper_primary_key
    if len(primary_key_diff) > 1:
        raise ValueError(f"{conformer_table} cannot be shaped by {shaper_table}")

def _parse_restriction_key(restriction_dict):

    attribute_names = []
    attribute_values = []
    # Parse each attribute name-value pair
    for name, values in restriction_dict.items():
        values = _parse_iterable_inputs(values)
        attribute_names.append(name)
        attribute_values.append(values)
    return attribute_names, attribute_values

def _parse_table_attribute_values(table, attribute_names, attribute_values=None):

    # Get all possible attribute values for any attribute names with no values specified
    if attribute_values is None:
        attribute_values = [None]*len(attribute_names)
    for ndx, (name, values) in enumerate(zip(attribute_names, attribute_values)):
        # If no attribute values are specified, get all possible values for that attribute
        if values is None:
            attribute_values[ndx] = np.unique(table.fetch(name))
    return attribute_values


def _fetch_conformed_data(conformer_table, conformer_attribute_name, shaper_table, shaper_attribute_name, convert_shaper_data):

    # Reshape a column of data from the conformer table to match the column of data from the shaper table
    shaper_data = _sort_data_numeric_string( list(set( shaper_table.fetch(shaper_attribute_name) )) )
    if convert_shaper_data and all([type(item) == str and item.isnumeric() for item in shaper_data]):
        # Convert shaper data to integers if all data are numeric strings
        shaper_data = list( _convert_data_datatype(shaper_data, dtype=int) )
    shaper_data = [item for item in shaper_data]
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
    return conformer_data, shaper_data


def _iterable_to_string(values, prefix="", separator="", terminator=""):

    # Join each string with a separator and attach prefix and terminator
    return prefix + separator.join(values) + terminator

def _table_to_string(table):

    # Convert a variable whose value is a table to a string literal
    return table.__name__
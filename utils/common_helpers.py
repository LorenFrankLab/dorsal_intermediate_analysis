from collections.abc import Iterable
from functools import wraps
import numpy as np
import pandas as pd
from time import time

def print_verbose(verbose, *args, color=None):
    
    # Create prefix and suffix to display colored text
    if not color:
        color_tuple = ('', '')
    elif color == 'red':
        color_tuple = ('\x1b[031m', '\x1b[0m')
    elif color == 'green':
        color_tuple = ('\x1b[032m', '\x1b[0m')
    elif color == 'blue':
        color_tuple = ('\x1b[034m', '\x1b[0m')
    elif color == 'magenta':
        color_tuple = ('\x1b[035m', '\x1b[0m')
    elif color == 'yellow':
        color_tuple = ('\x1b[033m', '\x1b[0m')
    else:
        raise ValueError(f"{color} is not an allowed color")

    # Print each string if verbose is true
    if verbose:
        for string in args:
            print(color_tuple[0] + string + color_tuple[1])

def function_timer(function_name):
    
    # Wrap the called function with timing code
    @wraps(function_name)
    def wrapper(*args, **kwargs):
        try:
            timing = kwargs['timing']
        except:
            timing = getattr(args[0], 'timing')
        else:
            timing = False
        
        # Compute and print elapsed time in seconds
        if timing:
            t0 = time()
            out = function_name(*args, **kwargs)
            t1 = time()
            t_elapsed = t1-t0
            print_verbose(True,
                          f"Time elapsed: {t_elapsed:.6f} seconds",
                          color='magenta')
        else:
            out = function_name(*args, **kwargs)

        return out
    return wrapper

def no_overwrite_handler_file(message):
    
    # Print message if file already exists
    print_verbose(True,
                  message,
                  color='yellow')

def no_overwrite_handler_table(message):
    
    # Print message if table entry already exists
    print_verbose(True,
                  message,
                  color='yellow')


def _sort_dataframe_numeric_string_OLD(data_df, sort_columns):

    # Store index column names to recreate index after sorting
    index_columns = data_df.index.names
    if not(len(index_columns) == 1 and index_columns[0] is None):
        data_df.reset_index(inplace=True)
    # Sort the dataframe using the specified columns
    numeric_columns = set()
    for column in sort_columns:
        # Convert a sort column to integers if all values are numeric strings
        if column in data_df.columns:
            if all([type(item) == str and item.isnumeric() for item in data_df[column].values]):
                data_df[column] = data_df[column].astype(int)
                numeric_columns.add(column)
    data_df.sort_values(by=sort_columns, inplace=True)
    # Convert numeric string columns back to strings
    for column in numeric_columns:
        data_df[column] = data_df[column].astype(str)
    # Recreate dataframe index
    if not(len(index_columns) == 1 and index_columns[0] is None):
        data_df.set_index(index_columns, inplace=True)
    else:
        data_df.reset_index(drop=True, inplace=True)
    return data_df

def _sort_dataframe_numeric_string(data_df, sort_columns):

    data_df = data_df.sort_values(by=sort_columns)   
    return data_df 

def _convert_dataframe_datatypes(data_df, dtypes=None):

    # Store index column names to recreate index after converting
    index_columns = data_df.index.names
    if not(len(index_columns) == 1 and index_columns[0] is None):
        data_df.reset_index(inplace=True)
    # Convert each specified column to the new datatype
    if dtypes is not None:
        for attribute_name, dtype in dtypes.items():
            fetch_data = data_df[attribute_name].values
            new_data = _convert_data_datatype(fetch_data, dtype=dtype)
            # Overwrite old data in the dataframe
            data_df[attribute_name] = new_data
    # Recreate dataframe index
    if not(len(index_columns) == 1 and index_columns[0] is None):
        data_df.set_index(index_columns, inplace=True)
    else:
        data_df.reset_index(drop=True, inplace=True)
    return data_df

def _transform_dataframe_values(data_df, transform_mappings=None):

    # Store index column names to recreate index after transforming
    index_columns = data_df.index.names
    if not(len(index_columns) == 1 and index_columns[0] is None):
        data_df.reset_index(inplace=True)
    # Transform each specified column according to the mappings
    if transform_mappings is not None:
        for attribute_name, mappings in transform_mappings.items():
            fetch_data = data_df[attribute_name].values
            new_data = _transform_data_values(fetch_data, transform_mappings=mappings)
            # Overwrite old data in the dataframe
            data_df[attribute_name] = new_data
    # Recreate dataframe index
    if not(len(index_columns) == 1 and index_columns[0] is None):
        data_df.set_index(index_columns, inplace=True)
    else:
        data_df.reset_index(drop=True, inplace=True)
    return data_df

def _sort_data_numeric_string(data, convert_data=False):

    # Sort data treating numeric strings as integers
    if all([type(item) == str and item.isnumeric() for item in data]):
        data = sorted(data, key=int)
        # Convert data to integers
        if convert_data:
            data = _convert_data_datatype(data, dtype=int)
    else:
        data = sorted(data)
    return data

def _convert_data_datatype(data, dtype=None):

    # Convert data to specified data type
    if dtype is not None:
        data = _nested_convert_datatype(data, dtype)
    return data

def _transform_data_values(data, transform_mappings=None):

    # Transform data according to the specified mapping
    if transform_mappings is not None:
        data = _nested_transform_data(data, transform_mappings)
    return data


def _nested_convert_datatype(data, dtype):

    if all([isinstance(item, str) or not isinstance(item, Iterable) for item in data]):
        # Convert data if all items in data are strings or aren't iterables
        return list(map(dtype, data))
    else:
        # Recursively convert data if data contains nonstring iterables
        return [_nested_convert_datatype(item, dtype) for item in data]

def _nested_transform_data(data, transform_mappings):

    if all([isinstance(item, str) or not isinstance(item, Iterable) for item in data]):
        data = np.array(data, dtype=object)
        # Transform data if data are items in data are strings or aren't iterables
        for mapping in transform_mappings:
            ind = (data == mapping[0])
            data[ind] = mapping[1]
        return data
    else:
        # Recursively transform data if data contains nonstring iterables
        return [_nested_transform_data(item, transform_mappings) for item in data]

def _parse_iterable_inputs(*args):

    # If data is None return None
    if len(args) == 1 and args[0] is None:
        return None
    # If data is not iterable or a string iterable, package data into a list
    data = [None]*len(args)
    for ndx, arg in enumerate(args):
        if isinstance(arg, str) or not isinstance(arg, Iterable):
            data[ndx] = [arg]
        else:
            data[ndx] = arg
    if len(data) == 1:
        return data[0]
    else:
        return data

def _remove_dataframe_multiindex(dataframe):

    # Get existing index and columns
    rows = dataframe.index
    columns = dataframe.columns
    
    # Determine if dataframe has multiindex columns
    col_multiindex_ind = isinstance(dataframe.columns, pd.MultiIndex)
    # Unstack columns into multiindex
    while col_multiindex_ind:
        dataframe = dataframe.stack(level=0)
        col_multiindex_ind = isinstance(dataframe.columns, pd.MultiIndex)
    # Unstack last remaining column
    dataframe.stack(level=0)
    
    # Determine if dataframe has multiindex rows
    row_multiindex_ind = isinstance(dataframe.index, pd.MultiIndex)
    # Reset index to remove multiindex
    if row_multiindex_ind:
        dataframe.reset_index(inplace=True)
    
    return dataframe, rows, columns

def _reinstate_dataframe_multiindex(dataframe, index, columns):

    pass
    # Create


def _nested_dict(keys):

    # Create empty nested dictionary from list of keys
    nested_dict = temp_dict = {}
    for key in keys:
        temp_dict[key] = {}
        temp_dict = temp_dict[key]
    return nested_dict

def _nested_dict_get(nested_dict, keys):

    # Access a value in a nested dictionary using an iterable of keys
    for key in keys:
        nested_dict = nested_dict[key]
    return nested_dict

def _nested_dict_set(nested_dict, keys, value):
    
    # Set a value in a nested dictionary using an iterable of keys
    for key in keys[:-1]:
        nested_dict = nested_dict.setdefault(key, {})
    nested_dict[keys[-1]] = value

def _nested_dict_len(nested_dict):

    # Count the total number of non-dictionary values in a nested dictionary
    return sum([_nested_dict_len(val) if isinstance(val, dict) else 1 for val in nested_dict.values()])
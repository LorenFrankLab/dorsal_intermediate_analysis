from collections.abc import Iterable
import copy
from itertools import chain
import numpy as np

def parse_iterable_inputs(*args):

    # Place any string or non-iterable literal into a 1-item list containing just that literal
    out = [None]*len(args)
    for ndx, arg in enumerate(args):
        if arg is None:
            continue
        # If data is not iterable or is a string, package data into a list
        if isinstance(arg, str) or not isinstance(arg, Iterable):
            out[ndx] = [arg]
        else:
            out[ndx] = arg
    
    # Unpack output if just one argument was given
    if len(args) == 1:
        return out[0]
    else:
        return out

def parse_iterable_outputs(*args, chain_args=False):

    # Unpack any 1-item nested iterables containing just a literal
    out = [None]*len(args)
    for ndx, arg in enumerate(args):
        if arg is None:
            continue
        # Chain iterables together
        if chain_args:
            arg = list(chain(*arg))
        while not isinstance(arg, str) and isinstance(arg, Iterable):
            if len(arg) != 1:
                break
            else:
                arg = arg[0]
        
        # If data is a 1-item non-string iterable, unpack
        if len(arg) == 1 and not isinstance(arg, str) and isinstance(arg, Iterable):
            out[ndx] = arg[0]
        else:
            out[ndx] = arg
    
    # Unpack output if just one argument was given
    if len(args) == 1:
        return out[0]
    else:
        return out


def sort_data(data, parse_numeric_strings=True):

    # Sort data treating numeric strings as integers
    data = _nested_sort_data(data, parse_numeric_strings=parse_numeric_strings)
    return data

def unique_data(data, parse_numeric_strings=True, sort=False):

    # Get unique data values while retaining order
    data = _nested_unique_data(data, parse_numeric_strings=parse_numeric_strings, sort=sort)
    return data

def convert_data_datatype(data, dtype=None):

    # Convert data to specified data type
    if dtype is not None:
        data = _nested_convert_datatype(data, dtype)
    return data

def transform_data_values(data, transform_mappings=None):

    # Transform data according to the specified mapping
    if transform_mappings is not None:
        data = _nested_transform_data(data, transform_mappings)
    return data

def _nested_sort_data(data, parse_numeric_strings):

    # Check if original data is a non-string iterable data container
    pack_bool = not isinstance(data, str) and isinstance(data, Iterable)
    data = parse_iterable_inputs(data)
    if all([isinstance(item, str) or not isinstance(item, Iterable) for item in data]):
        # Sort data if all items in data aren't iterables or are strings
        data = np.array(data, dtype=object)
        # Convert any numerical string items to integers
        if parse_numeric_strings:
            ind = np.array(string_is_number(data))
            old_data = data[ind]
            data[ind] = old_data.astype(float)
        
        # Sort data and use sorted indices to convert back to numeric strings
        sort_idx = np.argsort(data)
        data.sort()
        # Convert back to numeric strings
        if parse_numeric_strings:
            data[ind] = old_data[sort_idx[ind]]
        out = list(data)
        # Unpack non-string iterable data that contains only 1 item
        if len(data) == 1 and not pack_bool:
            out = out[0]
        return out
    else:
        # Recursively sort data if data contains nonstring iterables
        return [_nested_sort_data(item, parse_numeric_strings) for item in data]

def _nested_unique_data(data, parse_numeric_strings, sort):

    # Check if original data is a non-string iterable data container
    pack_bool = not isinstance(data, str) and isinstance(data, Iterable)
    data = np.array(parse_iterable_inputs(data))
    if all([isinstance(item, str) or not isinstance(item, Iterable) for item in data]):
        # Get unique data values while retaining order of first occurrence
        _, sort_idx = np.unique(data, return_index=True)
        out = data[np.sort(sort_idx)]
        if sort:
            out = _nested_sort_data(data, parse_numeric_strings)
        # Unpack non-string iterable data that contains only 1 item
        if len(data) == 1 and not pack_bool:
            out = out[0]
        return out
    else:
        # Recursively get unique data values if data contains nonstring iterables
        return [_nested_unique_data(item, parse_numeric_strings, sort) for item in data]

def _nested_convert_datatype(data, dtype):

    # Check if original data is a non-string iterable data container
    pack_bool = not isinstance(data, str) and isinstance(data, Iterable)
    data = parse_iterable_inputs(data)
    if all([isinstance(item, str) or not isinstance(item, Iterable) for item in data]):
        # Convert data if all items in data aren't iterables or are strings
        out = list(map(dtype, data))
        # Unpack non-string iterable data that contains only 1 item
        if len(data) == 1 and not pack_bool:
            out = out[0]
        return out
    else:
        # Recursively convert data if data contains nonstring iterables
        return [_nested_convert_datatype(item, dtype) for item in data]

def _nested_transform_data(data, transform_mappings):

    # Check if original data is a non-string iterable data container
    pack_bool = not isinstance(data, str) and isinstance(data, Iterable)
    data = parse_iterable_inputs(data)
    if all([isinstance(item, str) or not isinstance(item, Iterable) for item in data]):
        data = np.array(data, dtype=object)
        # Transform data if items in data are strings or aren't iterables
        for mapping in transform_mappings:
            ind = (data == mapping[0])
            data[ind] = mapping[1]
        out = list(data)
        if len(data) == 1 and not pack_bool:
            out = out[0]
        return out
    else:
        # Recursively transform data if data contains nonstring iterables
        return [_nested_transform_data(item, transform_mappings) for item in data]


def sort_dataframe(data_df, sort_columns, parse_numeric_strings=True, retain_index=False):

    sort_columns = parse_iterable_inputs(sort_columns)
    _validate_dataframe_sort(data_df, sort_columns)
    # Sort the dataframe using the specified columns
    old_data = {}
    for col_name in sort_columns:
        if parse_numeric_strings:
            # Convert a sort column to float if all values are numeric strings
            ind = np.array(string_is_number(data_df[col_name].values))
            old_data[col_name] = copy.deepcopy(data_df[col_name].values)
            data_df[col_name].values[ind] = data_df[col_name][ind].astype(float)
    # Sort data using the specified columns
    data_df.sort_values(by=sort_columns, inplace=True)
    sort_idx = data_df.index.values
    
    # Convert numeric string data back to strings
    if parse_numeric_strings:
        for col_name in old_data.keys():
            data_df[col_name] = old_data[col_name][sort_idx]
    # Reorder index if index is not to be retained
    if not retain_index:
        data_df.reset_index(inplace=True, drop=True)
    return data_df

def unique_dataframe(data_df, uniquify_column, retain_index=False):

    uniquify_column = parse_iterable_inputs(uniquify_column)[0]
    _validate_dataframe_uniquify(data_df, uniquify_column)
    #old_data = data_df[uniquify_column].values
    #new_data = _nested_unique_data(old_data, False, False)
    dropped_data_df = copy.deepcopy(data_df)
    unique_data_df = copy.deepcopy(data_df)
    unique_data_df.drop_duplicates(subset=uniquify_column, inplace=True)
    # Create dataframe of dropped values
    for idx in unique_data_df.index.values:
        dropped_data_df.drop(index=idx)

    # Reorder index if index is not to be retained
    if not retain_index:
        dropped_data_df.reset_index(inplace=True, drop=True)
        unique_data_df.reset_index(inplace=True, drop=True)
    return unique_data_df, dropped_data_df

def convert_dataframe_datatypes(data_df, dtypes=None):

    _validate_dataframe_convert(data_df, dtypes)
    # Convert each specified column to the new datatype
    if dtypes is not None:
        for col_name, dtype in dtypes.items():
            old_data = data_df[col_name].values
            new_data = _nested_convert_datatype(old_data, dtype=dtype)
            # Overwrite old data in the dataframe
            data_df[col_name] = new_data
    return data_df

def transform_dataframe_values(data_df, transform_mappings=None):

    _validate_dataframe_transform(data_df, transform_mappings)
    # Transform each specified column according to the mappings
    if transform_mappings is not None:
        for col_name, mappings in transform_mappings.items():
            old_data = data_df[col_name].values
            new_data = _nested_transform_data(old_data, transform_mappings=mappings)
            # Overwrite old data in the dataframe
            data_df[col_name] = new_data
    return data_df

def _validate_dataframe_sort(table_df, sort_columns):
    
    # Get all table column names available for transforming and converting
    table_columns = set(table_df.columns)
    # Sort columns are attribute names of the table
    if sort_columns is not None:
        sort_columns_diff = set(sort_columns)-table_columns
        if sort_columns_diff:
            raise ValueError(f"The following columns aren't valid column names: {sort_columns_diff}")

def _validate_dataframe_uniquify(table_df, uniquify_column):

    # Get all columns names available for uniquifying
    table_columns = set(table_df.columns)
    # Ensure only 1 uniquify column is specified
    if len(parse_iterable_inputs(uniquify_column)) > 1:
        raise ValueError(f"Only 1 column can be specified for uniquifying dataframe values")
    if uniquify_column not in table_columns:
        raise ValueError(f"The column to uniquify '{uniquify_column}' isn't a valid column name")


def _validate_dataframe_convert(table_df, dtypes):

    # Get all table column names available for transforming and converting
    table_columns = set(table_df.columns)
    # Ensure keys of data type conversion are attribute names of the table
    if dtypes is not None:
        dtypes_keys_diff = set(dtypes.keys())-table_columns
        if dtypes_keys_diff:
            raise ValueError(f"The following keys of 'dtypes' aren't valid column names: {dtypes_keys_diff}")

def _validate_dataframe_transform(table_df, transform_mappings):

    # Get all table column names available for transforming and converting
    table_columns = set(table_df.columns)
    # Ensure keys of transform mappings are attribute names of the table
    if transform_mappings is not None:
        mappings_keys_diff = set(transform_mappings.keys())-table_columns
        if mappings_keys_diff:
            raise ValueError(f"The following keys of 'transform_mappings' aren't valid column names: {mappings_keys_diff}")


def flatten_dataframe(data_df):

    # Remove multiindex rows from dataframe\
    data_df = _flatten_dataframe_columns(data_df)
    data_df = _flatten_dataframe_index(data_df)
    return data_df

def _flatten_dataframe_columns(data_df):

    # Remove column labels
    data_df.columns = data_df.columns.to_flat_index()
    if len(data_df.columns) != 0:
        data_df = _rename_dataframe_flattened_column(data_df)
    return data_df

def _flatten_dataframe_index(data_df):

    # Remove multiindex rows from dataframe
    n_index_levels = data_df.index.nlevels
    for ndx in range(n_index_levels):
        data_df = _convert_dataframe_index_to_column(data_df)
        # Drop index level that was converted to a column
        if ndx < n_index_levels-1:
            data_df.index = data_df.index.droplevel(level=0)
        else:
            data_df.reset_index(inplace=True, drop=True)
    return data_df

def _rename_dataframe_flattened_column(data_df, level=-1):

    n_column_levels = max([len(name) if not isinstance(name, str) else 1 for name in data_df.columns.values])
    # Get the preferred name for each column
    if n_column_levels == 1:
        preferred_names = list(data_df.columns.values)
    else:
        preferred_names = [col_name[level] for col_name in data_df.columns.values]
    # Rename columns prioritizing using the label found at the specified level
    new_names = [None]*len(data_df.columns)
    for ndx, col_name in enumerate(data_df.columns.values):
        # Avoid renaming the column if its preferred name matches another preferred name
        if  preferred_names.count(preferred_names[ndx]) > 1:
            new_names[ndx] = col_name
            continue
        # Use deepest level label if preferred column name is empty
        if preferred_names[ndx] == '':
            new_names[ndx] = col_name[-1]
        else:
            new_names[ndx] = preferred_names[ndx]

    # Rename the columns
    rename_mapping = {old_name : new_name for old_name, new_name in zip(data_df.columns.values, new_names)}
    data_df.rename(columns=rename_mapping, inplace=True)
    return data_df
        
def _convert_dataframe_index_to_column(data_df, level=0):

    n_column_levels = data_df.columns.nlevels
    # Take the highest-level index and make it a column
    col = data_df.index.get_level_values(level=level)
    # Place new column name at the deepest level of a multiindex column
    col_name = ['']*(n_column_levels-1)
    col_name.append(col.name)
    col_name = tuple(col_name)
    if len(col_name) == 1:
        col_name = col_name[0]
    data_df[col_name] = col.values
    
    return data_df


def string_is_number(strings):

    strings = parse_iterable_inputs(strings)
    numeric_ind = [None]*len(strings)
    for ndx, txt in enumerate(strings):
        # Check if input is a string
        if not isinstance(txt, str):
            numeric_ind[ndx] = False
            continue
        # Check if string is numeric up to decimal, minus, and plus sign characters
        numeric_ind[ndx] = txt.replace('.', '').replace('-', '').replace('−', '').replace('+', '').isnumeric()
    return numeric_ind

def iterable_to_string(values, prefix="", separator="", terminator=""):

    # Join each string with a separator and attach prefix and terminator
    return prefix + separator.join(values) + terminator
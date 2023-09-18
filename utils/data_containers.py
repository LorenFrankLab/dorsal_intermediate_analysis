from collections.abc import MutableMapping
from textwrap import indent

from utils.data_helpers import parse_iterable_inputs

class NestedDict(MutableMapping):

    def __init__(self, *args, **kwargs):

        self.store = dict()
        self.update(dict(*args, **kwargs))
    
    def __getitem__(self, key):
        key = list(parse_iterable_inputs(key))
        if key in self.keys():
            return NestedDict._nested_dict_get_item(self.store, key)
        else:
            raise KeyError(f"Key '{key}' not found.")
    
    def __setitem__(self, key, value):
        key = parse_iterable_inputs(key)
        key_length = len(key)
        for idx in range(1, key_length):
            prefix_key = list(key[:idx])
            if prefix_key in self.keys() and not isinstance(self[prefix_key], dict):
                raise KeyError(f"Key {key} cannot be set")
        NestedDict._nested_dict_set_item(self.store, key, value)
    
    def __delitem__(self, key):
        if key in self.keys():
            return NestedDict._nested_dict_del_item(self.store, key)
        else:
            raise KeyError(f"Key '{key}' not found.")
    
    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return NestedDict._nested_dict_len(self.store)
    
    def __str__(self):
        return NestedDict._nested_dict_print(self.store)


    def keys(self):
        # Get all combinations of keys leading to any value
        return NestedDict._nested_dict_items(self.store)[0]
    
    def values(self):
        # Get all values in the nested dictionary
        return NestedDict._nested_dict_items(self.store)[1]

    def items(self):
        # Get all key-value pairs corresponding to any value in the nested dictionary
        return tuple(zip(*NestedDict._nested_dict_items(self.store)))
    
    def deep_keys(self):
        # Get all combinations of keys leading to a non-dictionary value
        return NestedDict._nested_dict_deep_items(self.store)[0]
    
    def deep_values(self):
        # Get all non-dictionary values in the nested dictionary
        return NestedDict._nested_dict_deep_items(self.store)[1]
    
    def deep_items(self):
        # Get all key-value pairs corresponding to a non-dictionary value
        return tuple(zip(*NestedDict._nested_dict_deep_items(self.store)))
    

    @staticmethod
    def from_dict(store_dict):
        nested_dict = NestedDict()
        nested_dict.store = store_dict
        return nested_dict

    @staticmethod
    def _nested_dict_get_item(store_dict, key):
        
        key = parse_iterable_inputs(key)
        # Use an iterable of keys to sequentially index into the nested dictionary
        prefix_key = key[0]
        if len(key) == 1:
            return store_dict[prefix_key]
        else:
            return NestedDict._nested_dict_get_item(store_dict[prefix_key], key[1:])

    @staticmethod
    def _nested_dict_set_item(store_dict, key, value):

        # Iterate through keys and assign the value while creating new dictionaries as needed
        prefix_key = key[0]
        if prefix_key not in store_dict.keys():
            store_dict[prefix_key] = {}
        if len(key) == 1:
            store_dict[prefix_key] = value
        else:
            NestedDict._nested_dict_set_item(store_dict[prefix_key], key[1:], value)
    
    @staticmethod
    def _nested_dict_del_item(store_dict, key):

        # Use an iterable of keys to sequentially index into the nested dictionary and delete the item
        prefix_key = key[0]
        if len(key) == 1:
            del store_dict[prefix_key]
        else:
            NestedDict._nested_dict_del_item(store_dict[prefix_key], key[1:])

    @staticmethod
    def _nested_dict_len(store_dict):

        # Count the total number of non-dictionary values in a nested dictionary
        return sum([NestedDict._nested_dict_len(val) if isinstance(val, dict) else 1 for val in store_dict.values()])
    
    @staticmethod
    def _nested_dict_items(store_dict, prefix_keys=None, keys_list=None, values_list=None):

        if prefix_keys is None:
            prefix_keys = []
        if keys_list is None:
            keys_list = []
        if values_list is None:
            values_list = []
        
        # Get all combinations of keys leading to a non-dictionary value
        for key, val in store_dict.items():
            new_prefix = prefix_keys + [key]
            keys_list.append(new_prefix)
            values_list.append(val)
            if isinstance(val, dict):
                keys_list, values_list = NestedDict._nested_dict_items(val, new_prefix, keys_list, values_list)
        return keys_list, values_list

    @staticmethod
    def _nested_dict_deep_items(store_dict, prefix_keys=None, keys_list=None, values_list=None):

        if prefix_keys is None:
            prefix_keys = []
        if keys_list is None:
            keys_list = []
        if values_list is None:
            values_list = []
        
        # Get all combinations of keys leading to a non-dictionary value
        for key, val in store_dict.items():
            new_prefix = prefix_keys + [key]
            if isinstance(val, dict):
                keys_list, values_list = NestedDict._nested_dict_deep_items(val, new_prefix, keys_list, values_list)
            else:
                keys_list.append(new_prefix)
                values_list.append(val)
        return keys_list, values_list
    
    @staticmethod
    def _nested_dict_print(store_dict, tab_depth=0):

        txt = ''
        # Recursively traverse and print the values in the nested dictionary
        tab_char = '  '
        for key, val in store_dict.items():
            txt += tab_char*tab_depth + str(key) + ' :\n'
            if isinstance(val, dict):
                txt += NestedDict._nested_dict_print(val, tab_depth=tab_depth+1)
            else:
                txt += tab_char*(tab_depth+1) + str(val) + '\n'
        return txt
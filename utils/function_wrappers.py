from collections.abc import Iterable
import copy
from functools import wraps
from inspect import signature
from time import time

from utils import verbose_printer
from utils.file_helpers import _prompt_unix_password

def wrapperdecorator(decorator_name):

    # Wraps a decorator and allows it to use positional and keyword arguments
    @wraps(decorator_name)
    def wrapper(*args, **kwargs):
        # Run the decorated function if no other arguments given
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            return decorator_name(args[0])
        # Otherwise, call decorator with the provided arguments
        else:
            return lambda function_name: decorator_name(function_name, *args, **kwargs)
    return wrapper

@wrapperdecorator
def function_timer(function_name, unit='seconds'):
    
    # Wrap the called function with timing code
    @wraps(function_name)
    def wrapper(*args, **kwargs):
        try:
            timing = kwargs['timing']
        except:
            timing = getattr(args[0], 'timing')
        else:
            timing = False
        
        # Determine time units and scale factor
        if unit is 'seconds':
            scale = 1.0
        elif unit is 'minutes':
            scale = 1/60
        elif unit is 'hours':
            scale = 1/60/60
        else:
            raise ValueError(f"Allowed units of timing are 'seconds', 'minutes', and 'hours'")

        # Compute and print elapsed time in seconds
        if timing:
            t0 = time()
            out = function_name(*args, **kwargs)
            t1 = time()
            t_elapsed = scale*(t1-t0)
            verbose_printer.print_timer(f"Time elapsed: {t_elapsed:.6f} {unit}\n")
        else:
            out = function_name(*args, **kwargs)
        return out
    return wrapper

@wrapperdecorator
def parse_iterable_input(function_name, *args, unpack_output=False):

    iterable_arg_names = args if args else None
    # Pack non-iterable and string arguments into lists and unpack after executing code
    @wraps(function_name)
    def wrapper(*args, **kwargs):
        nonlocal iterable_arg_names
        function_arg_names = list(signature(function_name).parameters.keys())
        # Treat all function arguments as iterable arguments if no iterable argument names provided
        if not iterable_arg_names:
            iterable_arg_names = copy.deepcopy(function_arg_names)
        # Don't allow packaging of the 'self' argument
        if 'self' in iterable_arg_names:
            iterable_arg_names.remove('self')
        # Ensure provided names of iterable arguments are all argument names in wrapped function
        if any([item not in function_arg_names for item in iterable_arg_names]):
            raise ValueError(f"Provided iterable argument names are not argument names of the wrapped function")
        # If arguments provided are None, return None
        if len(args) == 1 and args[0] is None:
            args_tmp = None
        if len(kwargs) == 1 and list(kwargs.values())[0] is None:
            kwargs = None
        
        # Appropriately package iterable arguments into lists
        args_tmp = [item for item in args]
        kwargs_tmp = {key : value for key, value in kwargs.items()}
        unity_length_ind = [None]*len(iterable_arg_names)
        for ndx, arg_name in enumerate(function_arg_names):
            # Pass if argument is not in the list of iterable argument names
            if iterable_arg_names and arg_name not in iterable_arg_names:
                continue
            # Get value corresponding to argument name
            if ndx < len(args):
                arg = args[ndx]
            else:
                arg = kwargs[arg_name]
            # If data is not iterable or a string iterable, package data into a list
            if isinstance(arg, str) or not isinstance(arg, Iterable):
                arg = [arg]
            # Determine if argument is an iterable containing only 1 item
            if len(arg) == 1:
                idx = iterable_arg_names.index(arg_name)
                unity_length_ind[idx] = True

            if ndx < len(args):
                args_tmp[ndx] = arg
            else:
                kwargs_tmp[arg_name] = arg
        
        # Pass packaged positional and keyword arguments to wrapped function
        args = args_tmp
        kwargs = kwargs_tmp
        out = function_name(*args, **kwargs)

        # Unpack output if all iterable input arguments have only 1 item
        if unpack_output:
            if all(unity_length_ind):
                if len(out) == 1:
                    # Unpack a single returned value
                    out = out[0]
                else:
                    # Unpack multiple returned values each containing only 1 item
                    out = tuple(item[0] for item in function_name(*args, **kwargs))
        
        return out
    return wrapper

def prompt_password(function_name):

    @wraps(function_name)
    def wrapper(self, *args, **kwargs):

        # Add unix password to keyword arguments passed to the wrapped function
        if self._universal_access:
            pwd = _prompt_unix_password()
            kwargs['pwd'] = pwd
        else:
            kwargs['pwd'] = None
        out = function_name(self, *args, **kwargs)
        return out
    return wrapper

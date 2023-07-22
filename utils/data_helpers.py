from collections.abc import Iterable


def parse_iterable_inputs(*args):

    # Place any string arguments into a 1-item list containing just that string
    out = [None]*len(args)
    for ndx, arg in enumerate(args):
        # If data is not iterable or a string iterable, package data into a list
        if isinstance(arg, str) or not isinstance(arg, Iterable):
            out[ndx] = [arg]
        else:
            out[ndx] = arg
    
    if len(args) == 1:
        return out[0]
    else:
        return out
from utils.data_helpers import parse_iterable_inputs

def verbose_print(print_bool, *args, modifier=None, color=None, highlight=None):
    
    code_dict = _get_ansi_escape_codes()
    # Ensure modifiers, text colors, and highlight colors are valid
    if modifier and modifier not in code_dict['modifier'].keys():
        raise ValueError(f"Text modifier '{modifier}' not found in allowed modifiers {code_dict['modifier'].keys()}")
    if color and color not in code_dict['foreground_color'].keys():
        raise ValueError(f"Text color '{color}' not found in allowed text colors {code_dict['foreground_color'].keys()}")
    if highlight and highlight not in code_dict['background_color'].keys():
        raise ValueError(f"Highlight color '{highlight}' not found in allowed highlight colors {code_dict['background_color'].keys()}")

    escape_sequence = '\x1b['
    # Create escape sequence to display modified text
    if modifier:
        escape_sequence += code_dict['modifier'][modifier] + ';'
    # Create escape sequence to display colored foreground
    if color:
        escape_sequence += code_dict['foreground_color'][color] + ';'
    # Create escape sequence to display colored background
    if highlight:
        escape_sequence += code_dict['background_color'][highlight] + ';'
    # Remove appended semicolon if any ANSI escape codes were specified
    if any([modifier, color, highlight]):
        escape_sequence = escape_sequence[:-1]
    # Close escape sequences
    escape_sequence += 'm'

    # Print each string
    if print_bool:
        for string in args:
            print(escape_sequence + string + '\x1b[0m')

def print_text(*args):
    verbose_print(True, *args)

def print_bold(*args):
    verbose_print(True, *args, modifier='bold')

def print_iterable(iterable, prefix=''):
    iterable = parse_iterable_inputs(iterable)
    for item in iterable:
        print_text(prefix + item)

def print_header(*args):
    verbose_print(True, *args, modifier='bold', color='blue')

def print_pass(*args):
    verbose_print(True, *args, modifier='bold', highlight='green')

def print_fail(*args):
    verbose_print(True, *args, modifier='bold', highlight='red')

def print_error(*args):
    verbose_print(True, *args, modifier='bold', color='red')

def print_warning(*args):
    verbose_print(True, *args, modifier='bold', highlight='yellow')

def print_exclusion(*args):
    verbose_print(True, *args, modifier='bold', color='yellow')

def print_timer(*args):
    verbose_print(True, *args, color='magenta')

def print_newline():
    verbose_print(True, f"\n")

def _get_ansi_escape_codes():

    # Create dictionaries of ANSI codes for foreground and background text colors and text modifiers
    fg_color_code_dict = {'black' : '0',
                          'red' : '31',
                          'green' : '32',
                          'yellow' : '33',
                          'blue' : '34',
                          'magenta' : '35',
                          'cyan' : '36',
                          'white' : '37'}
    bg_color_code_dict = {'black' : '40',
                          'red' : '41',
                          'green' : '42',
                          'yellow' : '43',
                          'blue' : '44',
                          'magenta' : '45',
                          'cyan' : '46',
                          'white' : '47'}
    modifier_code_dict = {'bold' : '1',
                          'italics' : '3',
                          'underline' : '4',
                          'strikethrough' : '9'}

    code_dict = {'foreground_color' : fg_color_code_dict,
                 'background_color' : bg_color_code_dict,
                 'modifier' : modifier_code_dict}
    return code_dict
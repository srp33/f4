import datetime
import gzip
import fastnumbers
import mmap
from operator import itemgetter
import zstandard

def open_read_file(file_path, file_extension=""):
    the_file = open(file_path + file_extension, 'rb')
    return mmap.mmap(the_file.fileno(), 0, prot=mmap.PROT_READ)

def read_str_from_file(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return the_file.read().rstrip()

def read_int_from_file(file_path, file_extension=""):
    return fastnumbers.fast_int(read_str_from_file(file_path, file_extension))

def write_str_to_file(file_path, the_string):
    with open(file_path, 'wb') as the_file:
        the_file.write(the_string)

#def is_missing_value(value):
#    return value == b"" or value == b"NA"

def get_column_start_coords(column_sizes):
    # Calculate the position where each column starts.
    column_start_coords = []
    cumulative_position = 0
    for column_size in column_sizes:
        column_start_coords.append(str(cumulative_position).encode())
        cumulative_position += column_size
    column_start_coords.append(str(cumulative_position).encode())

    return column_start_coords

def build_string_map(the_list):
    # Find maximum length of value.
    max_value_length = get_max_string_length(the_list)

    column_items = format_column_items(the_list, max_value_length)
    return b"\n".join(column_items), max_value_length

def get_max_string_length(the_list):
    return max([len(x) for x in set(the_list)])

def format_column_items(the_list, max_value_length, suffix=""):
    formatter = "{:<" + str(max_value_length) + "}" + suffix
    return [formatter.format(value.decode()).encode() for value in the_list]

def print_message(message, verbose=False):
    if verbose:
        print(f"{message} - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}")

def decode_string(x):
    return x.decode()

def do_nothing(x):
     return(x)

def get_conversion_function(column_type):
    if column_type == "c": # column name
        return do_nothing
    elif column_type == "s":
        return decode_string
    elif column_type == "f":
        return fastnumbers.fast_float
    else:
        return fastnumbers.fast_int

def sort_first_column(list_of_lists):
    list_of_lists.sort(key=itemgetter(0))

def sort_first_two_columns(list_of_lists):
    list_of_lists.sort(key=itemgetter(0, 1))

def reverse_string(s):
    return s[::-1]

def get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    else:
        return open(file_path, 'rb')

def format_string_as_fixed_width(x, size):
    return x + b" " * (size - len(x))

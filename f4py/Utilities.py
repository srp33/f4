import datetime
import mmap

def open_read_file(file_path, file_extension=""):
    the_file = open(file_path + file_extension, 'rb')
    return mmap.mmap(the_file.fileno(), 0, prot=mmap.PROT_READ)

def read_str_from_file(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return the_file.read().rstrip()

def read_int_from_file(file_path, file_extension=""):
    return int(read_str_from_file(file_path, file_extension))

def write_str_to_file(file_path, file_extension, the_string):
    with open(file_path + file_extension, 'wb') as the_file:
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

def print_message(message, verbose):
    if verbose:
        print(f"{message} - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}")

import fastnumbers
from Helper import *

def convert_delimited_file_to_f4(in_file_path, f4_file_path, in_file_delimiter="\t"):
    if type(in_file_delimiter) != str:
        raise Exception("The in_file_delimiter value must be a string.")
    if in_file_delimiter not in ("\t"):
        raise Exception("Invalid delimiter. Must be \t.")

    in_file_delimiter = in_file_delimiter.encode()

    column_size_dict = {}
    column_start_coords = []

    # Initialize a dictionary that will hold the column index as key and width of the column as value.
    with open(in_file_path, 'rb') as my_file:
        # Remove any leading or trailing white space around the column names.
        column_names = [x.strip() for x in my_file.readline().rstrip(b"\n").split(in_file_delimiter)]

        for i in range(len(column_names)):
            column_size_dict[i] = 0

    num_cols = len(column_names)

    # Iterate through the lines to find the max width of each column.
    with open(in_file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere.
        my_file.readline()

        num_rows = 0
        for line in my_file:
            num_rows += 1
            line_items = line.rstrip(b"\n").split(in_file_delimiter)

            if len(line_items) != num_cols:
                raise Exception(f"The number of elements in row {num_rows} was different from the number of column names.")

            for i in range(len(line_items)):
                column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

    # Calculate the length of the first line (and thus all the other lines).
    line_length = sum([column_size_dict[i] for i in range(len(column_names))]) + 1

    # Save value that indicates line length.
    __write_string_to_file(f4_file_path, ".ll", str(line_length).encode())

    # Calculate the position where each column starts.
    cumulative_position = 0
    for i in range(len(column_names)):
        column_size = column_size_dict[i]
        column_start_coords.append(str(cumulative_position).encode())
        cumulative_position += column_size
    column_start_coords.append(str(cumulative_position).encode())

    # Build a map of the column names and save this to a file.
    column_names_string, max_col_name_length = __build_string_map([x for x in column_names])
    __write_string_to_file(f4_file_path, ".cn", column_names_string)
    __write_string_to_file(f4_file_path, ".mcnl", str(max_col_name_length).encode())

    # Calculate the column coordinates and max length of these coordinates.
    column_coords_string, max_column_coord_length = __build_string_map(column_start_coords)

    # Save column coordinates.
    __write_string_to_file(f4_file_path, ".cc", column_coords_string)

    # Save value that indicates maximum length of column coords string.
    __write_string_to_file(f4_file_path, ".mccl", str(max_column_coord_length).encode())

    # Save number of rows and cols.
    __write_string_to_file(f4_file_path, ".nrow", str(num_rows).encode())
    __write_string_to_file(f4_file_path, ".ncol", str(len(column_names)).encode())

    # Save the data to output file.
    with open(in_file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        with open(f4_file_path, 'wb') as out_file:
            out_lines = []
            chunk_size = 1000

            for line in my_file:
                line_items = line.rstrip(b"\n").split(in_file_delimiter)

                line_out = b""
                for i in sorted(column_size_dict.keys()):
                    line_out += __format_string_as_fixed_width(line_items[i], column_size_dict[i])

                out_lines.append(line_out)

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

    __parse_and_save_column_types(f4_file_path, line_length, num_rows, num_cols, max_column_coord_length, max_col_name_length)

#####################################################
# Private functions
#####################################################

def __parse_and_save_column_types(file_path, line_length, num_rows, num_cols, mccl, mcnl):
    data_handle = open_read_file(file_path)
    cc_handle = open_read_file(file_path, ".cc")
    col_coords = parse_data_coords(range(num_cols), cc_handle, mccl)

    column_types = [__parse_column_type(data_handle, num_rows, line_length, [col_coords[i]]) for i in range(num_cols)]

    # Save the column types and max length of these types.
    column_types_string, max_column_types_length = __build_string_map(column_types)
    __write_string_to_file(file_path, ".ct", column_types_string)
    __write_string_to_file(file_path, ".mctl", str(max_column_types_length).encode())

    data_handle.close()
    cc_handle.close()

def __parse_column_type(data_handle, num_rows, line_length, col_coords):
    has_non_number = False
    has_non_float = False
    has_non_int = False
    num_non_missing_values = 0
    unique_values = set()

    for row_index in range(num_rows):
        value = next(parse_data_values(row_index, line_length, col_coords, data_handle)).rstrip()

        if is_missing_value(value):
            continue

        num_non_missing_values += 1
        unique_values.add(value)

        is_float = fastnumbers.isfloat(value)
        is_int = fastnumbers.isint(value)

        if not has_non_number and not is_float and not is_int:
            has_non_number = True
        else:
            if not has_non_float and not is_float:
                has_non_float = True
            if not has_non_int and not is_int:
                has_non_int = True

    if has_non_number:
        column_type = b"d" #Discrete
    elif has_non_int:
        column_type = b"f" #Float
    else:
        column_type = b"i" #Int

    # Are all values unique (is this a unique identifier)?
    if len(unique_values) == num_non_missing_values:
        column_type += b"u"

    return column_type

def __format_string_as_fixed_width(x, size):
    formatted = "{:<" + str(size) + "}"
    return formatted.format(x.decode()).encode()

def __build_string_map(the_list):
    # Find maximum length of value.
    max_value_length = __get_max_string_length(the_list)

    # Build output string.
    output = ""
    formatter = "{:<" + str(max_value_length) + "}\n"
    for value in the_list:
        output += formatter.format(value.decode())

    return output.encode(), max_value_length

def __get_max_string_length(the_list):
    return max([len(x) for x in set(the_list)])

def __write_string_to_file(file_path, file_extension, the_string):
    with open(file_path + file_extension, 'wb') as the_file:
        the_file.write(the_string)

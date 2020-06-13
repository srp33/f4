import fastnumbers
from DataSetHelper import *

def convert_delimited_file_to_f4(in_file_path, f4_file_path, in_file_delimiter=b"\t"):
    if not isinstance(in_file_delimiter, bytes):
        in_file_delimiter = in_file_delimiter.encode()

    column_size_dict = {}
    column_start_coords = []

    # Initialize a dictionary with the column index as key and width of the column as value
    with open(in_file_path, 'rb') as my_file:
        column_names = my_file.readline().rstrip(b"\n").split(in_file_delimiter)

        for i in range(len(column_names)):
            column_size_dict[i] = 0

    # Iterate through the lines to find the max width of each column
    with open(in_file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        num_rows = 0
        for line in my_file:
            num_rows += 1
            line_items = line.rstrip(b"\n").split(in_file_delimiter)

            for i in range(len(line_items)):
                column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

    # Calculate the length of the first line (and thus all the other lines)
    line_length = sum([column_size_dict[i] for i in range(len(column_names))])

    # Save value that indicates line length
    write_string_to_file(f4_file_path, ".ll", str(line_length + 1).encode())

    # Calculate the position where each column starts
    cumulative_position = 0
    for i in range(len(column_names)):
        column_size = column_size_dict[i]
        column_start_coords.append(str(cumulative_position).encode())
        cumulative_position += column_size
    column_start_coords.append(str(cumulative_position).encode())

    # Build a map of the column names and save this to a file
    column_names_string, max_col_name_length = build_string_map([x for x in column_names])
    write_string_to_file(f4_file_path, ".cn", column_names_string)
    write_string_to_file(f4_file_path, ".mcnl", max_col_name_length)

    # Calculate the column coordinates and max length of these coordinates
    column_coords_string, max_column_coord_length = build_string_map(column_start_coords)

    # Save column coordinates
    write_string_to_file(f4_file_path, ".cc", column_coords_string)

    # Save value that indicates maximum length of column coords string
    write_string_to_file(f4_file_path, ".mccl", max_column_coord_length)

    # Save number of rows and cols
    write_string_to_file(f4_file_path, ".nrow", str(num_rows).encode())
    write_string_to_file(f4_file_path, ".ncol", str(len(column_names)).encode())

    # Save the data to output file
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
                    line_out += format_string(line_items[i], column_size_dict[i])

                out_lines.append(line_out)

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

    parse_and_save_column_types(f4_file_path)

def parse_and_save_column_types(file_path):
    # Initialize
    data_handle = open_read_file(file_path)
    ll = read_int_from_file(file_path, ".ll")
    cc_handle = open_read_file(file_path, ".cc")
    mccl = read_int_from_file(file_path, ".mccl")
    cn_handle = open_read_file(file_path, ".cn")
    mcnl = read_int_from_file(file_path, ".mcnl")
    num_rows = read_int_from_file(file_path, ".nrow")
    num_cols = read_int_from_file(file_path, ".ncol")
    col_coords = list(parse_data_coords(range(num_cols), cc_handle, mccl))

    # Find column type for each column
    column_types = []
    for col_index in range(num_cols):
        column_name = parse_meta_value(cn_handle, mcnl, col_index)
        column_values = [x.rstrip() for x in parse_column_values(data_handle, num_rows, col_coords, ll, 0, col_index)]
        column_type = parse_column_type(column_name, column_values)

        if col_index > 0 and col_index % 100 == 0:
            print("Finding column type - {}".format(col_index))

        column_types.append(column_type)

    # Save the column types and max length of these types
    column_types_string, max_column_types_length = build_string_map(column_types)
    write_string_to_file(file_path, ".ct", column_types_string)
    write_string_to_file(file_path, ".mctl", max_column_types_length)

    data_handle.close()
    cc_handle.close()
    cn_handle.close()

def parse_column_type(name, values):
    non_missing_values = [x for x in values if x != b"" and x != b"NA"]
    unique_values = set(non_missing_values)

    has_non_number = False
    for x in unique_values:
        if not fastnumbers.isfloat(x):
            has_non_number = True
            break

    if has_non_number:
        if len(unique_values) == len(non_missing_values):
            return b"i" #ID
        else:
            return b"d" #Discrete

    return b"n" # Numeric

def format_string(x, size):
    formatted = "{:<" + str(size) + "}"
    return formatted.format(x.decode()).encode()

def get_max_string_length(the_list):
    return max([len(x) for x in set(the_list)])

def build_string_map(the_list):
    # Find maximum length of value
    max_value_length = get_max_string_length(the_list)

    # Build output string
    output = ""
    formatter = "{:<" + str(max_value_length) + "}\n"
    for value in the_list:
        output += formatter.format(value.decode())

    return output.encode(), str(max_value_length).encode()

def parse_column_values(data_handle, data_num_rows, cc, ll, row_start_index, col_index):
    col_coords = [cc[col_index]]
    for row_index in range(row_start_index, data_num_rows):
        yield next(parse_data_values(row_index, ll, col_coords, data_handle))

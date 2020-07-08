import datetime
import fastnumbers
import glob
import gzip
from joblib import Parallel, delayed
from Helper import *
import os
import shutil
import sys
import tempfile

def convert_delimited_file_to_f4(in_file_path, f4_file_path, in_file_delimiter="\t", num_processes=1, lines_per_chunk=1000, tmp_dir_path=None):
    if type(in_file_delimiter) != str:
        raise Exception("The in_file_delimiter value must be a string.")
    if in_file_delimiter not in ("\t"):
        raise Exception("Invalid delimiter. Must be \t.")

    __print_message(f"Parsing {in_file_path}")

    in_file_delimiter = in_file_delimiter.encode()

    # Get column info. Remove any leading or trailing white space around the column names.
    in_file = __get_delimited_file_handle(in_file_path)
    column_names = [x.strip() for x in in_file.readline().rstrip(b"\n").split(in_file_delimiter)]
    num_cols = len(column_names)
    in_file.close()

    if num_cols == 0:
        raise Exception(f"No data was detected in {in_file_path}.")

    # Open file again. Ignore the header line because we already saved column names.
    in_file = __get_delimited_file_handle(in_file_path)
    in_file.readline()

    # Iterate through the lines to find the max width of each column.
    __print_message(f"Finding max width of each column")
    chunk_results = Parallel(n_jobs=num_processes)(delayed(__parse_lines_chunk)(lines_chunk, chunk_number, in_file_delimiter, num_cols) for lines_chunk, chunk_number in __generate_lines_chunks(in_file, lines_per_chunk))
    in_file.close()

    # Summarize the values across the chunks.
    column_sizes = __merge_column_sizes([x[0] for x in chunk_results])
    column_types = __merge_column_types([x[1] for x in chunk_results])
    num_rows = sum([x[2] for x in chunk_results])
    __print_message(f"Got here1")

    if num_rows == 0:
        raise Exception(f"A header rows but no data rows were detected in {in_file_path}")

    # Calculate the length of the first line (and thus all the other lines).
    line_length = sum(column_sizes) + 1

    # Save value that indicates line length.
    __write_string_to_file(f4_file_path, ".ll", str(line_length).encode())

    # Calculate the position where each column starts.
    column_start_coords = []
    cumulative_position = 0
    for column_size in column_sizes:
        column_start_coords.append(str(cumulative_position).encode())
        cumulative_position += column_size
    column_start_coords.append(str(cumulative_position).encode())
    __print_message(f"Got here2")

    # Calculate and save the column coordinates and max length of these coordinates.
    column_coords_string, max_column_coord_length = __build_string_map(column_start_coords)
    __write_string_to_file(f4_file_path, ".cc", column_coords_string)
    __write_string_to_file(f4_file_path, ".mccl", str(max_column_coord_length).encode())

    # Build a map of the column names and save this to a file.
    column_names_string, max_col_name_length = __build_string_map(column_names)
    __write_string_to_file(f4_file_path, ".cn", column_names_string)
    __write_string_to_file(f4_file_path, ".mcnl", str(max_col_name_length).encode())

    # Save number of rows and cols.
    __write_string_to_file(f4_file_path, ".nrow", str(num_rows).encode())
    __write_string_to_file(f4_file_path, ".ncol", str(len(column_names)).encode())

    # Build a map of the column types and save this to a file.
    column_types_string, max_col_type_length = __build_string_map(column_types)
    __write_string_to_file(f4_file_path, ".ct", column_types_string)
    __write_string_to_file(f4_file_path, ".mctl", str(max_col_type_length).encode())

    __print_message(f"Got here3")
    # Save the data to output file. Ignore the header line.
    in_file = __get_delimited_file_handle(in_file_path)
    in_file.readline()

    # Figure out where temp files will be stored.
    if tmp_dir_path:
        if not os.path.exists(tmp_dir_path):
            os.mkdir(tmp_dir_path)
    else:
        tmp_dir_path = tempfile.mkdtemp()
    if not tmp_dir_path.endswith("/"):
        tmp_dir_path += "/"

    # Parse chunks of the input file and save to temp files.
    __print_message(f"Parsing chunks of the input file and saving to temp files")
    max_line_sizes = Parallel(n_jobs=num_processes)(delayed(__save_lines_temp)(lines_chunk, in_file_delimiter, column_sizes, tmp_dir_path, chunk_number) for lines_chunk, chunk_number in __generate_lines_chunks(in_file, lines_per_chunk))
    in_file.close()

    # Merge the file chunks. This dictionary enables us to sort them properly.
    __print_message(f"Merging the file chunks")
    chunk_file_paths = {int(os.path.basename(file_path)): file_path for file_path in glob.glob(f"{tmp_dir_path}/*")}
    with open(f4_file_path, "wb") as f4_file:
        out_lines = []

        for chunk_number, chunk_file_path in chunk_file_paths.items():
            with open(chunk_file_path, "rb") as chunk_file:
                for line in chunk_file:
                    out_lines.append(line)

                    if len(out_lines) % lines_per_chunk == 0:
                        f4_file.write(b"".join(out_lines))
                        out_lines = []

            os.remove(chunk_file_path)

        if len(out_lines) > 0:
            f4_file.write(b"\n".join(out_lines) + b"\n")

    # Remove the temp directory if it was generated by the code (not the user).
    if tmp_dir_path:
        try:
            os.rmdir(tmp_dir_path)
        except:
            # Don't throw an exception if we can't delete the directory.
            pass

    __print_message(f"Done saving to {f4_file_path}")

#####################################################
# Private functions
#####################################################

def __get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    else:
        return open(file_path, 'rb')

def __merge_column_sizes(all_column_sizes):
    max_column_sizes = []

    for col_i in range(len(all_column_sizes[0])):
        max_column_sizes.append(max([all_column_sizes[row_i][col_i] for row_i in range(len(all_column_sizes))]))

    return max_column_sizes

def __merge_column_types(all_column_types):
    column_types = []

    for col_i in range(len(all_column_types[0])):
        column_types.append(__infer_type_from_list([all_column_types[row_i][col_i] for row_i in range(len(all_column_types))]))

    return column_types

def __generate_lines_chunks(file_handle, lines_per_chunk):
    lines_chunk = []
    chunk_number = 0

    for line in file_handle:
        lines_chunk.append(line)

        if len(lines_chunk) == lines_per_chunk:
            chunk_number += 1
            yield lines_chunk, chunk_number
            lines_chunk = []

    if len(lines_chunk) > 0:
        chunk_number += 1
        yield lines_chunk, chunk_number

def __parse_lines_chunk(lines_chunk, chunk_number, delimiter, num_cols):
    column_sizes = [0 for x in range(num_cols)]
    column_types = [None for x in range(num_cols)]

    print(chunk_number)

    for line in lines_chunk:
        line_items = line.rstrip(b"\n").split(delimiter)

        if len(line_items) != num_cols:
            raise Exception(f"The number of elements in row {num_rows} was different from the number of column names.")

        for i in range(len(line_items)):
            column_sizes[i] = max([column_sizes[i], len(line_items[i])])
            column_types[i] = __infer_type_from_list([column_types[i], __infer_type(line_items[i])])

    return column_sizes, column_types, len(lines_chunk)

def __save_lines_temp(lines_chunk, delimiter, column_sizes, tmp_dir_path, chunk_number):
    max_line_size = 0

    with open(f"{tmp_dir_path}{chunk_number}", 'wb') as tmp_file:
        for line in lines_chunk:
            __print_message(f"Saving to {tmp_dir_path}{chunk_number}")
            line_items = line.rstrip(b"\n").split(delimiter)

            line_out = b""
            for i in range(len(column_sizes)):
                line_out += __format_string_as_fixed_width(line_items[i], column_sizes[i])

            max_line_size = max([max_line_size, len(line_out)])
            tmp_file.write(line_out + b"\n")

    return max_line_size

def __infer_type(value):
    if not value or is_missing_value(value):
        return None
    if fastnumbers.isint(value):
        return b"i"
    if fastnumbers.isfloat(value):
        return b"f"
    return b"c"

def __infer_type_from_list(types):
    # Remove any None or missing values. Convert to set so only contains unique values and is faster.
    types = [x for x in types if x and not is_missing_value(x)]

    if len(types) == 0:
        return None
    if len(types) == 1:
        return types[0]

    types = set(types)

    if b"c" in types:
        return b"c" # If any value is non-numeric, then we infer categorical.
    if b"f" in types:
        return b"f" # If any value if a float in a numeric column, then we infer float.
    return b"i"

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

def __print_message(message):
    print(f"{message} - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}")

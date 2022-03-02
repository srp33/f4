import fastnumbers
import gzip
from joblib import Parallel, delayed
from f4py.Utilities import *
import math
import os
import sys
import tempfile
import zstandard

class Builder:
    # TODO:
    #   documentation: Specify None for compression_level if you do not want compression.
    def __init__(self, delimited_file_path, f4_file_path, delimiter="\t", compression_level=22, tmp_dir_path=None, verbose=False):
        if type(delimiter) != str:
            raise Exception("The delimiter value must be a string.")
        if delimiter not in ("\t"):
            raise Exception("Invalid delimiter. Must be \t.")

        self.__delimited_file_path = delimited_file_path
        self.__f4_file_path = f4_file_path
        self.__delimiter = delimiter.encode()

        self.__compression_level = compression_level

        # Figure out where temp files will be stored.
        if tmp_dir_path:
            self.tmp_dir_path = tmp_dir_path
            if not os.path.exists(tmp_dir_path):
                os.makedirs(tmp_dir_path)
        else:
            self.tmp_dir_path = tempfile.mkdtemp()

        if not self.tmp_dir_path.endswith("/"):
            self.tmp_dir_path += "/"

        self.__verbose = verbose

    def build(self, num_processes=1, num_cols_per_chunk=None, num_rows_per_save=10):
        self._print_message(f"Converting from {self.__delimited_file_path}")

        # Get column names. Remove any leading or trailing white space around the column names.
        in_file = _get_delimited_file_handle(self.__delimited_file_path)
        column_names = [x.strip() for x in in_file.readline().rstrip(b"\n").split(self.__delimiter)]
        num_cols = len(column_names)
        in_file.close()

        if num_cols == 0:
            raise Exception(f"No data was detected in {self.__delimited_file_path}.")

        column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)

        # Iterate through the lines to find the max width of each column.
        self._print_message(f"Finding max width of each column in {self.__delimited_file_path}")
        chunk_results = Parallel(n_jobs=num_processes)(delayed(self._parse_columns_chunk)(column_chunk[0], column_chunk[1]) for column_chunk in column_chunk_indices)

        # Summarize the column sizes and types across the chunks.
        column_sizes = []
        column_types = []
        for chunk_tuple in chunk_results:
            for i, size in sorted(chunk_tuple[0].items()):
                column_sizes.append(size)
            for i, the_type in sorted(chunk_tuple[1].items()):
                column_types.append(the_type)

        num_rows = chunk_results[0][2]

        if num_rows == 0:
            raise Exception(f"A header rows but no data rows were detected in {self.__delimited_file_path}")

        # Calculate and save the column coordinates and max length of these coordinates.
        column_start_coords = get_column_start_coords(column_sizes)
        column_coords_string, max_column_coord_length = build_string_map(column_start_coords)
        write_string_to_file(self.__f4_file_path, ".cc", column_coords_string)
        write_string_to_file(self.__f4_file_path, ".mccl", str(max_column_coord_length).encode())

        # Build a map of the column names and save this to a file.
        column_names_string, max_col_name_length = build_string_map(column_names)
        write_string_to_file(self.__f4_file_path, ".cn", column_names_string)
        write_string_to_file(self.__f4_file_path, ".mcnl", str(max_col_name_length).encode())

        # Save number of rows and cols.
        write_string_to_file(self.__f4_file_path, ".nrow", str(num_rows).encode())
        write_string_to_file(self.__f4_file_path, ".ncol", str(len(column_names)).encode())

        # Build a map of the column types and save this to a file.
        column_types_string, max_col_type_length = build_string_map(column_types)
        write_string_to_file(self.__f4_file_path, ".ct", column_types_string)
        #write_string_to_file(self.__f4_file_path, ".mctl", str(max_col_type_length).encode())

        self._print_message(f"Parsing chunks of {self.__delimited_file_path} and saving to temp files")
        row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)
        max_line_sizes = Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(column_sizes, i, row_chunk[0], row_chunk[1], num_rows_per_save) for i, row_chunk in enumerate(row_chunk_indices))

        # Find and save the line length.
        line_length = max(max_line_sizes)
        write_string_to_file(self.__f4_file_path, ".ll", str(line_length).encode())

        # Indicate compression level.
        write_string_to_file(self.__f4_file_path, ".cmp", str(self.__compression_level).encode())

        # Merge the file chunks. This dictionary enables us to sort them properly.
        self._print_message(f"Merging the file chunks for {self.__delimited_file_path}")
        self._merge_chunk_files(num_processes, line_length, num_rows_per_save)

        # Remove the temp directory if it was generated by the code (not the user).
        if self.tmp_dir_path:
            try:
                os.rmdir(self.tmp_dir_path)
            except:
                # Don't throw an exception if we can't delete the directory.
                pass

        self._print_message(f"Done converting {self.__delimited_file_path} to {self.__f4_file_path}")

    #####################################################
    # Non-public functions
    #####################################################

    def _parse_columns_chunk(self, start_index, end_index):
        in_file = _get_delimited_file_handle(self.__delimited_file_path)

        # Ignore the header line because we don't need column names here.
        in_file.readline()

        # Initialize the column sizes and types.
        column_sizes_dict = {}
        column_types_dict = {}
        for i in range(start_index, end_index):
            column_sizes_dict[i] = 0
            column_types_dict[i] = None

        # Loop through the file for the specified columns.
        num_rows = 0
        for line in in_file:
            line_items = line.rstrip(b"\n").split(self.__delimiter)
            for i in range(start_index, end_index):
                column_sizes_dict[i] = max([column_sizes_dict[i], len(line_items[i])])
                column_types_dict[i] = _infer_type_from_list([column_types_dict[i], _infer_type(line_items[i])])

            num_rows += 1

        in_file.close()

        return column_sizes_dict, column_types_dict, num_rows

    def _save_rows_chunk(self, column_sizes, chunk_number, start_index, end_index, num_rows_per_save):
        max_line_size = 0

        compressor = None
        if self.__compression_level:
            compressor = zstandard.ZstdCompressor(level=self.__compression_level)

        # Save the data to output file. Ignore the header line.
        in_file = _get_delimited_file_handle(self.__delimited_file_path)
        in_file.readline()

        with open(f"{self.tmp_dir_path}{chunk_number}", 'wb') as chunk_file:
            with open(f"{self.tmp_dir_path}{chunk_number}_linesizes", 'wb') as size_file:
                out_lines = []
                out_line_sizes = []

                line_index = -1
                for line in in_file:
                    # Check whether we should process the specified line.
                    line_index += 1
                    if line_index < start_index:
                        continue
                    if line_index == end_index:
                        break

                    # Parse the data from the input file.
                    line_items = line.rstrip(b"\n").split(self.__delimiter)

                    # Format the columns using fixed widths.
                    out_items = [_format_string_as_fixed_width(line_items[i], size) for i, size in enumerate(column_sizes)]
                    out_line = b"".join(out_items)

                    if self.__compression_level:
                        out_line = compressor.compress(out_line)
                    else:
                        # We add a newline character when the data are not compressed.
                        # This makes the file more readable (doesn't matter when the data are compressed).
                        out_line += b"\n"

                    line_size = len(out_line)
                    max_line_size = max([max_line_size, line_size])

                    out_lines.append(out_line)
                    out_line_sizes.append((f"{line_size}\n").encode())

                    if len(out_lines) % num_rows_per_save == 0:
                        chunk_file.write(b"".join(out_lines))
                        size_file.write(b"".join(out_line_sizes))
                        out_lines = []
                        out_line_sizes = []

                if len(out_lines) > 0:
                    chunk_file.write(b"".join(out_lines))
                    size_file.write(b"".join(out_line_sizes))

        in_file.close()

        return max_line_size

    def _merge_chunk_files(self, num_processes, line_length, num_rows_per_save=10):
        with open(self.__f4_file_path, "wb") as f4_file:
            out_lines = []

            for i in range(num_processes):
                chunk_file_path = f"{self.tmp_dir_path}{i}"
                if not os.path.exists(chunk_file_path):
                    continue
                chunk_file = open_read_file(chunk_file_path)

                size_file_path = f"{self.tmp_dir_path}{i}_linesizes"

                with open(size_file_path, 'rb') as size_file:
                    position = 0
                    for size_line in size_file:
                        size = int(size_line.rstrip(b"\n"))

                        out_line = chunk_file[position:(position + size)]
                        out_line = _format_string_as_fixed_width(out_line, line_length)
                        out_lines.append(out_line)
                        position += size

                        if len(out_lines) % num_rows_per_save == 0:
                            f4_file.write(b"".join(out_lines))
                            out_lines = []

                chunk_file.close()
                os.remove(chunk_file_path)

            if len(out_lines) > 0:
                f4_file.write(b"".join(out_lines))

    def _print_message(self, message):
        print_message(message, self.__verbose)

#####################################################
# Class functions
#####################################################

def _get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    else:
        return open(file_path, 'rb')

def _generate_chunk_ranges(num_cols, num_cols_per_chunk):
    if num_cols_per_chunk:
        last_end_index = 0

        while last_end_index != num_cols:
            last_start_index = last_end_index
            last_end_index = min([last_start_index + num_cols_per_chunk, num_cols])
            yield [last_start_index, last_end_index]
    else:
        yield [0, num_cols]

def _infer_type(value):
    if not value or is_missing_value(value):
        return None
    if fastnumbers.isint(value):
        return b"i"
    if fastnumbers.isfloat(value):
        return b"f"
    return b"c"

def _infer_type_from_list(types):
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

def _format_string_as_fixed_width(x, size):
    return x + b" " * (size - len(x))

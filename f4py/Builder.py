import f4py
import fastnumbers
import gzip
from joblib import Parallel, delayed
import math
import os
import tempfile
import zstandard

class Builder:
    def __init__(self, verbose=False):
        self.__verbose = verbose

    def convert_delimited_file(self, delimited_file_path, f4_file_path, delimiter="\t", compression_level=22, num_processes=1, num_cols_per_chunk=None, num_rows_per_save=10, tmp_dir_path=None):
        if type(delimiter) != str:
            raise Exception("The delimiter value must be a string.")

        if delimiter not in ("\t"):
            raise Exception("Invalid delimiter. Must be \t.")

        delimiter = delimiter.encode()

        tmp_dir_path = self._prepare_tmp_dir(tmp_dir_path)

        self._print_message(f"Converting from {delimited_file_path}")

        # Get column names. Remove any leading or trailing white space around the column names.
        in_file = _get_delimited_file_handle(delimited_file_path)
        column_names = [x.strip() for x in in_file.readline().rstrip(b"\n").split(delimiter)]
        num_cols = len(column_names)
        in_file.close()

        if num_cols == 0:
            raise Exception(f"No data was detected in {delimited_file_path}.")

        column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)

        # Iterate through the lines to summarize each column.
        self._print_message(f"Summarizing each column in {delimited_file_path}")
        chunk_results = Parallel(n_jobs=num_processes)(delayed(self._parse_columns_chunk)(delimited_file_path, delimiter, column_chunk[0], column_chunk[1]) for column_chunk in column_chunk_indices)

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
            raise Exception(f"A header row but no data rows were detected in {delimited_file_path}")

        self._print_message(f"Converting {delimited_file_path} to {f4_file_path}")
        line_length = self._convert_delimited_file_in_chunks(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, num_rows, num_processes, num_rows_per_save, tmp_dir_path)

        self._print_message(f"Saving meta files for {f4_file_path}")
        self._save_meta_files(f4_file_path, column_sizes, line_length, column_names, column_types, compression_level, num_rows)

        self._remove_tmp_dir(tmp_dir_path)
        self._print_message(f"Done converting {delimited_file_path} to {f4_file_path}")

    #####################################################
    # Non-public functions
    #####################################################

    def _save_meta_files(self, f4_file_path, column_sizes, line_length, column_names=None, column_types=None, compression_level=None, num_rows=None):
        # Calculate and save the column coordinates and max length of these coordinates.
        column_start_coords = f4py.get_column_start_coords(column_sizes)
        column_coords_string, max_column_coord_length = f4py.build_string_map(column_start_coords)
        f4py.write_string_to_file(f4_file_path, ".cc", column_coords_string)
        f4py.write_string_to_file(f4_file_path, ".mccl", str(max_column_coord_length).encode())

        # Find and save the line length.
        f4py.write_string_to_file(f4_file_path, ".ll", str(line_length).encode())

        if column_names:
            column_name_index_dict = {}
            for i, column_name in enumerate(column_names):
                column_name_index_dict[column_name] = i

            # Build an index of the column names and save this to a file.
            sorted_column_names = sorted(column_names)
            values_positions = [[x, column_name_index_dict[x]] for x in sorted_column_names]
            f4py.IdentifierIndexer(f"{f4_file_path}.cn", None).build(values_positions)

            if column_types:
                # Build a map of the column types and save this to a file.
                column_types_string, max_col_type_length = f4py.build_string_map(column_types)
                f4py.write_string_to_file(f4_file_path, ".ct", column_types_string)
                #f4py.write_string_to_file(f4_file_path, ".mctl", str(max_col_type_length).encode())

        # Indicate compression level.
        f4py.write_string_to_file(f4_file_path, ".cmp", str(compression_level).encode())

        if num_rows:
            # Save number of rows and columns.
            f4py.write_string_to_file(f4_file_path, ".nrow", str(num_rows).encode())
            f4py.write_string_to_file(f4_file_path, ".ncol", str(len(column_names)).encode())

    def _prepare_tmp_dir(self, tmp_dir_path):
        # Figure out where temp files will be stored and create directory, if needed.
        if tmp_dir_path:
            if not os.path.exists(tmp_dir_path):
                os.makedirs(tmp_dir_path)
        else:
            tmp_dir_path = tempfile.mkdtemp()

        if not tmp_dir_path.endswith("/"):
            tmp_dir_path += "/"

        return tmp_dir_path

    def _remove_tmp_dir(self, tmp_dir_path):
        # Remove the temp directory if it was generated by the code (not the user).
        if tmp_dir_path:
            try:
                os.rmdir(tmp_dir_path)
            except:
                # Don't throw an exception if we can't delete the directory.
                pass

    def _parse_columns_chunk(self, delimited_file_path, delimiter, start_index, end_index):
        in_file = _get_delimited_file_handle(delimited_file_path)

        # Ignore the header line because we don't need column names here.
        in_file.readline()

        # Initialize the column sizes and types.
        column_sizes_dict = {}
        column_types_dict = {}
        for i in range(start_index, end_index):
            column_sizes_dict[i] = 0
            column_types_dict[i] = {b"i": 0, b"f": 0, b"s": 0, "unique_s": set()}

        # Loop through the file for the specified columns.
        num_rows = 0
        for line in in_file:
            line_items = line.rstrip(b"\n").split(delimiter)
            for i in range(start_index, end_index):
                column_sizes_dict[i] = max([column_sizes_dict[i], len(line_items[i])])

                inferred_type = _infer_type(line_items[i])
                if inferred_type:
                    if inferred_type == b"s":
                        column_types_dict[i]["unique_s"].add(line_items[i])

                    column_types_dict[i][inferred_type] += 1

            num_rows += 1

            if num_rows % 100000 == 0:
                self._print_message(f"Processed line of {delimited_file_path} for columns {start_index} - {end_index - 1}")

        in_file.close()

        for i in range(start_index, end_index):
            column_types_dict[i] = _infer_type_for_column(column_types_dict[i], num_rows)

        return column_sizes_dict, column_types_dict, num_rows

    def _convert_delimited_file_in_chunks(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, num_rows, num_processes, num_rows_per_save, tmp_dir_path):
        self._print_message(f"Parsing chunks of {delimited_file_path} and saving to temp files")
        row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)
        max_line_sizes = Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(delimited_file_path, delimiter, compression_level, column_sizes, i, row_chunk[0], row_chunk[1], num_rows_per_save, tmp_dir_path) for i, row_chunk in enumerate(row_chunk_indices))

        # Find and save the line length.
        line_length = max(max_line_sizes)

        # Merge the file chunks. This dictionary enables us to sort them properly.
        self._print_message(f"Merging the file chunks for {delimited_file_path}")
        self._merge_chunk_files(f4_file_path, num_processes, line_length, num_rows_per_save, tmp_dir_path)

        return line_length

    def _save_rows_chunk(self, delimited_file_path, delimiter, compression_level, column_sizes, chunk_number, start_index, end_index, num_rows_per_save, tmp_dir_path):
        max_line_size = 0

        compressor = None
        if compression_level:
            compressor = zstandard.ZstdCompressor(level=compression_level)

        # Save the data to output file. Ignore the header line.
        in_file = _get_delimited_file_handle(delimited_file_path)
        in_file.readline()

        with open(f"{tmp_dir_path}{chunk_number}", 'wb') as chunk_file:
            with open(f"{tmp_dir_path}{chunk_number}_linesizes", 'wb') as size_file:
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
                    line_items = line.rstrip(b"\n").split(delimiter)

                    # Format the columns using fixed widths.
                    out_items = [_format_string_as_fixed_width(line_items[i], size) for i, size in enumerate(column_sizes)]
                    out_line = b"".join(out_items)

                    if compression_level:
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

    def _merge_chunk_files(self, f4_file_path, num_processes, line_length, num_rows_per_save, tmp_dir_path):
        with open(f4_file_path, "wb") as f4_file:
            out_lines = []

            for i in range(num_processes):
                chunk_file_path = f"{tmp_dir_path}{i}"
                if not os.path.exists(chunk_file_path):
                    continue
                chunk_file = f4py.open_read_file(chunk_file_path)

                size_file_path = f"{tmp_dir_path}{i}_linesizes"

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
        f4py.print_message(message, self.__verbose)

#####################################################
# Class functions (non-public)
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
    if not value or f4py.is_missing_value(value):
        return None
    if fastnumbers.isint(value):
        return b"i"
    if fastnumbers.isfloat(value):
        return b"f"
    return b"s"

def _infer_type_for_column(types_dict, num_rows):
    if len(types_dict) == 0:
        return None
    if len(types_dict) == 1:
        return list(types_dict.keys())[0]

    if types_dict[b"s"] > 0:
        if len(types_dict["unique_s"]) == num_rows:
            return b"u"
        return b"c"
    elif types_dict[b"f"] > 0:
        return b"f"

    return b"i"

def _format_string_as_fixed_width(x, size):
    return x + b" " * (size - len(x))

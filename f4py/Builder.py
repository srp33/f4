import f4py
import fastnumbers
from joblib import Parallel, delayed
import math
import os
import pickle
import shutil
import tempfile
import zstandard

class Builder:
    def __init__(self, verbose=False):
        self.__verbose = verbose

    def convert_delimited_file(self, delimited_file_path, f4_file_path, index_columns=[], delimiter="\t", compression_level=1, num_processes=1, num_cols_per_chunk=None, num_rows_per_save=100, tmp_dir_path=None, cache_dir_path=None):
        if type(delimiter) != str:
            raise Exception("The delimiter value must be a string.")

        if delimiter not in ("\t"):
            raise Exception("Invalid delimiter. Must be \t.")

        delimiter = delimiter.encode()

        self._print_message(f"Converting from {delimited_file_path}")

        tmp_dir_path2 = self._prepare_tmp_dir(tmp_dir_path)

        # Get column names. Remove any leading or trailing white space around the column names.
        with f4py.get_delimited_file_handle(delimited_file_path) as in_file:
            column_names = [x.strip() for x in in_file.readline().rstrip(b"\n").split(delimiter)]
            num_cols = len(column_names)

        if num_cols == 0:
            raise Exception(f"No data was detected in {delimited_file_path}.")

        tmp_chunk_results_file_path = None
        if cache_dir_path:
            # Make sure there is a backslash at the end
            cache_dir_path = cache_dir_path.rstrip("/") + "/"
            os.makedirs(cache_dir_path, exist_ok=True)
            tmp_chunk_results_file_path = f"{cache_dir_path}chunk_results"

        if tmp_dir_path and cache_dir_path and tmp_dir_path == cache_dir_path:
            raise Exception("tmp_dir_path and cache_dir_path cannot point to the same location.")

        if tmp_chunk_results_file_path and os.path.exists(tmp_chunk_results_file_path):
            self._print_message(f"Retrieving cached chunk results from {tmp_chunk_results_file_path}")
            chunk_results = pickle.loads(f4py.read_str_from_file(tmp_chunk_results_file_path))
        else:
            column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)

            # Iterate through the lines to summarize each column.
            self._print_message(f"Summarizing each column in {delimited_file_path}")
            chunk_results = Parallel(n_jobs=num_processes)(delayed(self._parse_columns_chunk)(delimited_file_path, delimiter, column_chunk[0], column_chunk[1], compression_level != None) for column_chunk in column_chunk_indices)

            if tmp_chunk_results_file_path:
                self._print_message(f"Saving cached chunk results to {tmp_chunk_results_file_path}")
                f4py.write_str_to_file(tmp_chunk_results_file_path, pickle.dumps(chunk_results))

        # Summarize the column sizes and types across the chunks.
        column_sizes = []
        column_types = []
        compression_training_set = set()

        for chunk_tuple in chunk_results:
            for i, size in sorted(chunk_tuple[0].items()):
                column_sizes.append(size)
            for i, the_type in sorted(chunk_tuple[1].items()):
                column_types.append(the_type)

            compression_training_set = compression_training_set | chunk_tuple[4]

        # When each chunk was processed, we went through all rows, so we can get these numbers from one chunk.
        num_rows = chunk_results[0][2]
        total_num_chars = chunk_results[0][3]

        if num_rows == 0:
            raise Exception(f"A header row but no data rows were detected in {delimited_file_path}")

        # Check whether we have enough data to train a compression dictionary.
        if compression_level != None:
            if total_num_chars > 100000 and len(compression_training_set) > 0:
                f4py.CompressionHelper._save_training_dict(compression_training_set, f4_file_path, compression_level, num_processes)

            f4py.CompressionHelper._save_level_file(f4_file_path, compression_level)

        line_length = self._create_output_file(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, num_rows, num_processes, total_num_chars, num_rows_per_save, tmp_dir_path2)

        self._print_message(f"Saving meta files for {f4_file_path}")
        self._save_meta_files(f4_file_path, column_sizes, line_length, column_names, column_types, num_rows)

        self._remove_tmp_dir(tmp_dir_path2)
        self._print_message(f"Done converting {delimited_file_path} to {f4_file_path}")

        if index_columns:
            f4py.IndexHelper.build_indexes(f4_file_path, index_columns)

    #####################################################
    # Non-public functions
    #####################################################

    def _save_meta_files(self, f4_file_path, column_sizes, line_length, column_names=None, column_types=None, num_rows=None):
        # Calculate and save the column coordinates and max length of these coordinates.
        column_start_coords = f4py.get_column_start_coords(column_sizes)
        column_coords_string, max_column_coord_length = f4py.build_string_map(column_start_coords)
        f4py.write_str_to_file(f4_file_path + ".cc", column_coords_string)
        f4py.write_str_to_file(f4_file_path + ".mccl", str(max_column_coord_length).encode())

        # Find and save the line length.
        f4py.write_str_to_file(f4_file_path + ".ll", str(line_length).encode())

        if column_names:
            column_name_index_dict = {}
            for i, column_name in enumerate(column_names):
                column_name_index_dict[column_name] = i

            # Build an index of the column names and save this to a file.
            sorted_column_names = sorted(column_names)
            values_positions = [[x.decode(), column_name_index_dict[x]] for x in sorted_column_names]
            f4py.IndexHelper._customize_values_positions(values_positions, ["c"], f4py.sort_first_column, f4py.do_nothing)
            f4py.IndexHelper._save_index(values_positions, f"{f4_file_path}.cn")

            if column_types:
                # Build a map of the column types and save this to a file.
                column_types_string, max_col_type_length = f4py.build_string_map(column_types)
                f4py.write_str_to_file(f4_file_path + ".ct", column_types_string)

        if num_rows:
            # Save number of rows and columns.
            f4py.write_str_to_file(f4_file_path + ".nrow", str(num_rows).encode())
            f4py.write_str_to_file(f4_file_path + ".ncol", str(len(column_names)).encode())

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
                shutil.rmtree(tmp_dir_path)
                self._print_message(f"Removed {tmp_dir_path} directory")
            except Exception as e:
                # Don't throw an exception if we can't delete the directory.
                self._print_message(f"Warning: {tmp_dir_path} directory could not be removed")
                print(e)
                pass

    def _parse_columns_chunk(self, delimited_file_path, delimiter, start_index, end_index, need_compression):
        compression_training_set = set()

        with f4py.get_delimited_file_handle(delimited_file_path) as in_file:
            # Ignore the header line because we don't need column names here.
            in_file.readline()

            # Initialize the column sizes and types.
            column_sizes_dict = {}
            column_types_dict = {}
            for i in range(start_index, end_index):
                column_sizes_dict[i] = 0
                #column_types_dict[i] = {b"i": 0, b"f": 0, b"s": 0, "unique_s": set()}
                column_types_dict[i] = {b"i": 0, b"f": 0, b"s": 0}

            # Loop through the file for the specified columns.
            num_rows = 0
            num_chars = 0
            for line in in_file:
                line = line.rstrip(b"\n")
                num_chars += len(line)

                line_items = line.split(delimiter)
                for i in range(start_index, end_index):
                    column_sizes_dict[i] = max([column_sizes_dict[i], len(line_items[i])])

                    inferred_type = _infer_type(line_items[i])

                    if inferred_type == b"s":
                    #    column_types_dict[i]["unique_s"].add(line_items[i])
                        compression_training_set.add(line_items[i])

                    column_types_dict[i][inferred_type] += 1

                num_rows += 1

                if num_rows % 100000 == 0:
                    self._print_message(f"Processed line {num_rows} of {delimited_file_path} for columns {start_index} - {end_index - 1}")

        for i in range(start_index, end_index):
            column_types_dict[i] = _infer_type_for_column(column_types_dict[i], num_rows)

        return column_sizes_dict, column_types_dict, num_rows, num_chars, compression_training_set

    def _create_output_file(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, num_rows, num_processes, total_num_chars, num_rows_per_save, tmp_dir_path):
        self._print_message(f"Parsing chunks of {delimited_file_path} and saving to temp directory ({tmp_dir_path})")

        if num_processes == 1:
            line_length = self._save_rows_chunk(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, 0, 0, num_rows, total_num_chars, num_rows_per_save, tmp_dir_path)
        else:
            row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)

            # Find the line length.
            max_line_sizes = Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, i, row_chunk[0], row_chunk[1], total_num_chars, num_rows_per_save, tmp_dir_path) for i, row_chunk in enumerate(row_chunk_indices))
            line_length = max(max_line_sizes)

        # Merge the file chunks. This dictionary enables us to sort them properly.
        self._print_message(f"Merging the file chunks for {delimited_file_path}")
        self._merge_chunk_files(f4_file_path, num_processes, line_length, num_rows_per_save, tmp_dir_path)

        return line_length

    def _save_rows_chunk(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, chunk_number, start_index, end_index, total_num_chars, num_rows_per_save, tmp_dir_path):
        max_line_size = 0
        compressor = f4py.CompressionHelper._get_compressor(f4_file_path, compression_level)

        # Save the data to output file. Ignore the header line.
        with f4py.get_delimited_file_handle(delimited_file_path) as in_file:
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

                        if compressor:
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
                            self._print_message(f"Processed chunk of {delimited_file_path} at line {line_index} (start_index = {start_index}, end_index = {end_index})")
                            chunk_file.write(b"".join(out_lines))
                            size_file.write(b"".join(out_line_sizes))
                            out_lines = []
                            out_line_sizes = []

                    if len(out_lines) > 0:
                        chunk_file.write(b"".join(out_lines))
                        size_file.write(b"".join(out_line_sizes))

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
                        size = fastnumbers.fast_int(size_line.rstrip(b"\n"))

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
    #if not value or f4py.is_missing_value(value):
    #    return None
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
#        if len(types_dict["unique_s"]) == num_rows:
#            return b"u"
        return b"s"
    elif types_dict[b"f"] > 0:
        return b"f"

    return b"i"

def _format_string_as_fixed_width(x, size):
    return x + b" " * (size - len(x))

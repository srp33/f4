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

    def convert_delimited_file(self, delimited_file_path, f4_file_path, index_columns=[], delimiter="\t", compression_level=1, build_compression_dictionary=True, num_processes=1, num_cols_per_chunk=None, num_rows_per_save=100, tmp_dir_path=None, cache_dir_path=None):
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

        #tmp_chunk_results_file_path = None
        #if cache_dir_path:
        #    # Make sure there is a backslash at the end
        #    cache_dir_path = cache_dir_path.rstrip("/") + "/"
        #    os.makedirs(cache_dir_path, exist_ok=True)
        #    tmp_chunk_results_file_path = f"{cache_dir_path}chunk_results"

        #if tmp_dir_path and cache_dir_path and tmp_dir_path == cache_dir_path:
        #    raise Exception("tmp_dir_path and cache_dir_path cannot point to the same location.")

        #if tmp_chunk_results_file_path and os.path.exists(tmp_chunk_results_file_path):
        #    self._print_message(f"Retrieving cached chunk results from {tmp_chunk_results_file_path}")
        #    chunk_results = pickle.loads(f4py.read_str_from_file(tmp_chunk_results_file_path))
        #else:

        # Iterate through the lines to summarize each column.
        self._print_message(f"Summarizing each column in {delimited_file_path}")
        if num_processes == 1:
            chunk_results = [self._parse_columns_chunk(delimited_file_path, delimiter, 0, num_cols, build_compression_dictionary)]
        else:
            column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)
            chunk_results = Parallel(n_jobs=num_processes)(delayed(self._parse_columns_chunk)(delimited_file_path, delimiter, column_chunk[0], column_chunk[1], build_compression_dictionary) for column_chunk in column_chunk_indices)

        #    if tmp_chunk_results_file_path:
        #        self._print_message(f"Saving cached chunk results to {tmp_chunk_results_file_path}")
        #        f4py.write_str_to_file(tmp_chunk_results_file_path, pickle.dumps(chunk_results))

        ## Summarize the column sizes and types across the chunks.
        column_sizes = []
        column_types = []
        compression_dicts = {}

        for chunk_tuple in chunk_results:
            for i, size in sorted(chunk_tuple[0].items()):
                column_sizes.append(size)

            for i, the_type in sorted(chunk_tuple[1].items()):
                column_types.append(the_type)

            if len(chunk_tuple) > 0:
                # This merges the dictionaries
                compression_dicts = {**compression_dicts, **chunk_tuple[2]}

        # When each chunk was processed, we went through all rows, so we can get these numbers from one chunk.
        num_rows = chunk_results[0][3]
        #total_num_chars = chunk_results[0][4]

        if num_rows == 0:
            raise Exception(f"A header row but no data rows were detected in {delimited_file_path}")

        ## Check whether we have enough data to train a compression dictionary.
        #if compression_level != None:
        #    if total_num_chars > 100000 and len(compression_training_set) > 0:
        #        f4py.CompressionHelper._save_training_dict(compression_training_set, f4_file_path, compression_level, num_processes)

        #    f4py.CompressionHelper._save_level_file(f4_file_path, compression_level)

        nocomp_total_bits = 0
        encode_total_bits = 0
        new_total_bits = 0

        for col_index in range(len(column_sizes)):
        #for col_index in range(1, 2):
            column_size = column_sizes[col_index]
            #print("column size:")
            #print(column_size)

            column_type = column_types[col_index]
            #print("column type:")
            #print(column_type)

            #column_type_values = column_types_values[col_index]

            num_unique = len(compression_dicts[col_index])
            #print("num_unique:")
            #print(num_unique)

            nocomp_num_bits = column_sizes[col_index] * 8
            nocomp_total_bits += nocomp_num_bits
            #print("nocomp_num_bits")
            #print(nocomp_num_bits)

            if num_unique <= 256:
                encode_num_bits = 8
            elif num_unique <= 65536:
                encode_num_bits = 16
            elif num_unique <= 16777216:
                encode_num_bits = 24
            else:
                encode_num_bits = 32
            #print("encode_num_bits")
            #print(encode_num_bits)
            encode_total_bits += encode_num_bits

            # TODO: Try 2-grams and 4-grams.
            ngram_size = 3
            ngrams = set()
            max_value_length = 0

            for value in compression_dicts[col_index]:
                if len(value) > max_value_length:
                    max_value_length = len(value)
                for i in range(0, len(value), ngram_size):
                    ngrams.add(value[i:(i+ngram_size)])

            ngrams = sorted(list(ngrams))

            # TODO: Try using n-grams with b"i"

            if num_unique <= 2:
                new_num_bits = 1
            # This is a rough estimate for now.
            elif num_unique < (num_rows * 0.05) or max_value_length <= ngram_size:
                new_num_bits = math.ceil(math.log2(num_unique))
            elif column_type == b"i":
                #1000000 = 56 bits
                #10 + 00 + 00 + 00 = 4 x 7 = 28 bits
                #100 + 000 + 000 = 3 x 10 = 30 bits
                #1000 + 0000 = 2 x 14 = 28 bits
                #int2ba = 24

                #10000000 = 64 bits
                #10 + 00 + 00 + 00 = 4 x 7 = 28 bits
                #100 + 000 + 000 = 3 x 10 = 30 bits
                #1000 + 0000 = 2 x 14 = 28 bits
                #int2ba = 27

                #100000000 = 72 bits
                #10 + 00 + 00 + 00 + 00 = 5 x 7 = 35 bits
                #100 + 000 + 000 = 3 x 10 = 30 bits
                #1000 + 0000 + 0000 = 3 x 14 = 42 bits
                #int2ba = 30

                # This is a conservative approximation
                max_int = int("9" * column_size)
                from bitarray.util import int2ba
                new_num_bits = len(int2ba(max_int))
                #print("new_num_bits")
                #print(new_num_bits)
            else:
# Before this change:
#14040000000.0
#4100000000.0
#2960000000
                from bitarray.util import int2ba

                max_ngram_index_length = len(int2ba(len(ngrams) - 1))

                #ngram_bitarray_index_dict = {}
                #for i, ngram in enumerate(ngrams):
                #    ngram_bitarray_index_dict[ngram] = f"{int2ba(i).to01():0>{max_ngram_index_length}}"

                max_num_ngrams = math.ceil(max_value_length / ngram_size)
                new_num_bits = max_num_ngrams * max_ngram_index_length

#                print(ngrams)
#                print(len(ngrams))
#                print(max_ngram_index_length)
#                print(max_num_ngrams)

                #print(col_index)
                #print(ngrams)
                #print(len(ngrams))
                #print(max_value_length)
                #print(max_ngram_index_length)
                #print(ngram_bitarray_index_dict)
                #print(max_num_ngrams)
                #print(new_num_bits)

            new_total_bits += new_num_bits

        print(num_rows * nocomp_total_bits / 8)
        print(num_rows * encode_total_bits / 8)
        print(num_rows * math.ceil(new_total_bits / 8))
        import sys
        sys.exit()


        #line_length = self._create_output_file(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, num_rows, num_processes, num_rows_per_save, tmp_dir_path2)
        self._create_output_file(delimited_file_path, f4_file_path, delimiter, compression_level, column_types, compression_dicts, num_rows, num_processes, num_rows_per_save, tmp_dir_path2)

        self._print_message(f"Saving meta files for {f4_file_path}")
        #self._save_meta_files(f4_file_path, column_sizes, line_length, column_names, column_types, compression_dicts, num_rows)
        self._save_meta_files(f4_file_path, column_names, column_types, compression_dicts, num_rows)

        self._remove_tmp_dir(tmp_dir_path2)
        self._print_message(f"Done converting {delimited_file_path} to {f4_file_path}")

        if index_columns:
            f4py.IndexHelper.build_indexes(f4_file_path, index_columns)

    #####################################################
    # Non-public functions
    #####################################################

    #TODO: Currently, this function is used in IndexHelper as well. Consider splitting it out.
    #def _save_meta_files(self, f4_file_path, column_sizes, line_length, column_names=None, column_types=None, compression_dicts=None, num_rows=None):
    def _save_meta_files(self, f4_file_path, column_names, column_types, compression_dicts, num_rows):
        column_sizes = []
        for column_index, compression_dict in sorted(compression_dicts.items()):
            column_sizes.append(len(list(compression_dict.values())[0])) # All values should have the same length

        # Calculate and save the column coordinates and max length of these coordinates.
        column_start_coords = f4py.get_column_start_coords(column_sizes)
        column_coords_string, max_column_coord_length = f4py.build_string_map(column_start_coords)
        f4py.write_str_to_file(f4_file_path + ".cc", column_coords_string)
        f4py.write_str_to_file(f4_file_path + ".mccl", str(max_column_coord_length).encode())

        # Find and save the line length.
        line_length = sum(column_sizes) + 1
        f4py.write_str_to_file(f4_file_path + ".ll", str(line_length).encode())

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

        #TODO: switch keys and values and save as 3-column f4 file...
        import pickle
        f4py.write_str_to_file(f4_file_path + ".cmpd", pickle.dumps(compression_dicts))
        #TODO
        #f4py.write_str_to_file(f4_file_path + ".cmpd", f4py.CompressionHelper.build_table(compression_dicts))
        #f4py.CompressionHelper._build_table(compression_dicts)

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

    def _parse_columns_chunk(self, delimited_file_path, delimiter, start_index, end_index, build_compression_dictionary):
        #compression_training_set = set()

        with f4py.get_delimited_file_handle(delimited_file_path) as in_file:
            # Ignore the header line because we don't need column names here.
            in_file.readline()

            # Initialize the column sizes and types.
            column_sizes_dict = {}
            column_types_values_dict = {}
            column_types_dict = {}
            for i in range(start_index, end_index):
                column_sizes_dict[i] = 0
                column_types_values_dict[i] = {b"i": set(), b"f": set(), b"s": set()}

            # Loop through the file for the specified columns.
            num_rows = 0
            #num_chars = 0
            for line in in_file:
                line = line.rstrip(b"\n")
                #num_chars += len(line)

                line_items = line.split(delimiter)
                for i in range(start_index, end_index):
                    column_sizes_dict[i] = max([column_sizes_dict[i], len(line_items[i])])

                    inferred_type = _infer_type(line_items[i])

                    column_types_values_dict[i][inferred_type].add(line_items[i])

                    #if build_compression_dictionary and inferred_type == b"s":
                        #compression_training_set.add(line_items[i])

                num_rows += 1

                if num_rows % 100000 == 0:
                    self._print_message(f"Processed line {num_rows} of {delimited_file_path} for columns {start_index} - {end_index - 1}")

        compression_dict = {}

        for i in range(start_index, end_index):
            column_type = _infer_type_for_column(column_types_values_dict[i])

            unique_values = column_types_values_dict[i][b"s"] | column_types_values_dict[i][b"f"] | column_types_values_dict[i][b"i"]
            # TODO: Not sure if we should sort these. Might have to get fancy.
            unique_values = list(unique_values)

            compression_characters = f4py.CompressionHelper.get_compression_characters(len(unique_values))
            compression_dict[i] = {value: compression_characters[i] for i, value in enumerate(unique_values)}

            column_types_dict[i] = column_type

            # Not sure if this is helpful, but intended to reduce memory usage.
            column_types_values_dict[i] = None

        #TODO
        return column_sizes_dict, column_types_dict, compression_dict, num_rows#, num_chars
        #return column_types_dict, compression_dict, num_rows#, num_chars

    #def _create_output_file(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, num_rows, num_processes, num_rows_per_save, tmp_dir_path):
    def _create_output_file(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_types, compression_dicts, num_rows, num_processes, num_rows_per_save, tmp_dir_path):
        self._print_message(f"Parsing chunks of {delimited_file_path} and saving to temp directory ({tmp_dir_path})")

        if num_processes == 1:
            #line_length = self._save_rows_chunk(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, 0, 0, num_rows, num_rows_per_save, tmp_dir_path)
            self._save_rows_chunk(delimited_file_path, f4_file_path, delimiter, compression_level, column_types, compression_dicts, 0, 0, num_rows, num_rows_per_save, tmp_dir_path)
        else:
            row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)

            # Find the line length.
            #max_line_sizes = Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, i, row_chunk[0], row_chunk[1], num_rows_per_save, tmp_dir_path) for i, row_chunk in enumerate(row_chunk_indices))
            #line_length = max(max_line_sizes)
            Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(delimited_file_path, f4_file_path, delimiter, compression_level, column_types, compression_dicts, i, row_chunk[0], row_chunk[1], num_rows_per_save, tmp_dir_path) for i, row_chunk in enumerate(row_chunk_indices))

        # Merge the file chunks. This dictionary enables us to sort them properly.
        self._print_message(f"Merging the file chunks for {delimited_file_path}")
        #self._merge_chunk_files(f4_file_path, num_processes, line_length, num_rows_per_save, tmp_dir_path)
        self._merge_chunk_files(f4_file_path, num_processes, num_rows_per_save, tmp_dir_path)

#        return line_length

    #def _save_rows_chunk(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, chunk_number, start_index, end_index, num_rows_per_save, tmp_dir_path):
    def _save_rows_chunk(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_types, compression_dicts, chunk_number, start_index, end_index, num_rows_per_save, tmp_dir_path):
        #max_line_size = 0
        #column_sizes = []
        compressor = f4py.CompressionHelper._get_compressor(f4_file_path, compression_level)

        # Save the data to output file. Ignore the header line.
        with f4py.get_delimited_file_handle(delimited_file_path) as in_file:
            in_file.readline()

            with open(f"{tmp_dir_path}{chunk_number}", 'wb') as chunk_file:
#                with open(f"{tmp_dir_path}{chunk_number}_linesizes", 'wb') as size_file:
                out_lines = []
                #out_line_sizes = []

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

                    # Replace values with compressed versions and update column sizes.
                    for column_index, compression_dict in compression_dicts.items():
                        line_items[column_index] = compression_dict[line_items[column_index]]
                        #column_sizes[column_index] = len(line_items[column_index])

                    # Format the column sizes using fixed widths.
                    #out_items = [f4py._format_string_as_fixed_width(line_items[i], size) for i, size in enumerate(column_sizes)]
                    #out_line = b"".join(out_items)
                    out_line = (b"".join(line_items))

                    #TODO
                    #if compressor:
                    #    out_line = compressor.compress(out_line)
                    #else:
                    #    # We add a newline character when the data are not compressed.
                    #    # This makes the file more readable (doesn't matter when the data are compressed).
                    #    out_line += b"\n"

                    #TODO: Does it work if we not include a newline character after the last line?
                    out_line += b"\n"
                    #line_size = len(out_line)

                    #line_size = len(out_line)
                    #max_line_size = max([max_line_size, line_size])
                    #if line_index == 307 or line_index == 306:
                    #    print(out_line)
                    #    print(line_size)

                    out_lines.append(out_line)
                    #out_line_sizes.append((f"{line_size}\n").encode())

                    if len(out_lines) % num_rows_per_save == 0:
                        self._print_message(f"Processed chunk of {delimited_file_path} at line {line_index} (start_index = {start_index}, end_index = {end_index})")
                        chunk_file.write(b"".join(out_lines))
                        #size_file.write(b"".join(out_line_sizes))
                        out_lines = []
                        #out_line_sizes = []

                if len(out_lines) > 0:
                    chunk_file.write(b"".join(out_lines))
                    #size_file.write(b"".join(out_line_sizes))

        #return max_line_size

    #def _merge_chunk_files(self, f4_file_path, num_processes, line_length, num_rows_per_save, tmp_dir_path):
    def _merge_chunk_files(self, f4_file_path, num_processes, num_rows_per_save, tmp_dir_path):
        with open(f4_file_path, "wb") as f4_file:
            out_lines = []

            for i in range(num_processes):
                chunk_file_path = f"{tmp_dir_path}{i}"

                if not os.path.exists(chunk_file_path):
                    continue

#                with f4py.open_read_file(chunk_file_path) as chunk_file:
                with open(chunk_file_path, "rb") as chunk_file:
                    for line in chunk_file:
                        out_lines.append(line)

                        if len(out_lines) % num_rows_per_save == 0:
                            f4_file.write(b"".join(out_lines))
                            out_lines = []

                    #size_file_path = f"{tmp_dir_path}{i}_linesizes"

#                    with open(size_file_path, 'rb') as size_file:
#                    position = 0

#                        for size_line in size_file:
                            #TODO
                            #size = fastnumbers.fast_int(size_line.rstrip(b"\n"))
#                            print(line_length)

                            #out_line = chunk_file[position:(position + size)]
#                            out_line = chunk_file[position:(position + line_length - 1)]
                            #out_line = f4py._format_string_as_fixed_width(out_line, line_length)
#                            out_lines.append(out_line)
                            #position += size
#                            position += line_length

#                            if len(out_lines) % num_rows_per_save == 0:
#                                f4_file.write(b"".join(out_lines))
#                                out_lines = []

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

def _infer_type_for_column(types_dict):
    if len(types_dict) == 0:
        return None
    #if len(types_dict) == 1:
    #    return list(types_dict.keys())[0]

    if len(types_dict[b"s"]) > 0:
        return b"s"
    elif len(types_dict[b"f"]) > 0:
        return b"f"

    return b"i"

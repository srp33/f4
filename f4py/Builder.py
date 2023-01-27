import f4py
import fastnumbers
from joblib import Parallel, delayed
import math
import os
import pickle
import shutil
import tempfile
#TODO: Keep?
#import zstandard

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

        # Iterate through the lines to summarize each column.
        self._print_message(f"Summarizing each column in {delimited_file_path}")
        if num_processes == 1:
            chunk_results = [self._parse_columns_chunk(delimited_file_path, delimiter, 0, num_cols, build_compression_dictionary)]
        else:
            column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)
            chunk_results = Parallel(n_jobs=num_processes)(delayed(self._parse_columns_chunk)(delimited_file_path, delimiter, column_chunk[0], column_chunk[1], build_compression_dictionary) for column_chunk in column_chunk_indices)

        # Summarize the column sizes and types across the chunks.
        column_sizes = []
        column_types = []
        column_compression_dicts = {}

        for chunk_tuple in chunk_results:
            for i, size in sorted(chunk_tuple[0].items()):
                column_sizes.append(size)

            for i, the_type in sorted(chunk_tuple[1].items()):
                column_types.append(the_type)

            if len(chunk_tuple) > 0:
                # This merges the dictionaries
                column_compression_dicts = {**column_compression_dicts, **chunk_tuple[2]}

        # When each chunk was processed, we went through all rows, so we can get these numbers from just the first chunk.
        num_rows = chunk_results[0][3]

        if num_rows == 0:
            raise Exception(f"A header row but no data rows were detected in {delimited_file_path}")

        ## Check whether we have enough data to train a compression dictionary.
        #if compression_level != None:
        #    if total_num_chars > 100000 and len(compression_training_set) > 0:
        #        f4py.CompressionHelper._save_training_dict(compression_training_set, f4_file_path, compression_level, num_processes)

        #    f4py.CompressionHelper._save_level_file(f4_file_path, compression_level)

        line_length = self._save_output_file(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, column_compression_dicts, num_rows, num_processes, num_rows_per_save, tmp_dir_path2)

        self._print_message(f"Saving meta files for {f4_file_path}")
        self._save_meta_files(f4_file_path, column_sizes, line_length, column_names, column_types, column_compression_dicts, num_rows)

        self._remove_tmp_dir(tmp_dir_path2)
        self._print_message(f"Done converting {delimited_file_path} to {f4_file_path}")

        if index_columns:
            f4py.IndexHelper.build_indexes(f4_file_path, index_columns)

    #####################################################
    # Non-public functions
    #####################################################

    #TODO: Currently, this function is used in IndexHelper as well. Consider splitting it out.
    def _save_meta_files(self, f4_file_path, column_sizes, line_length, column_names=None, column_types=None, column_compression_dicts=None, num_rows=None):
        #column_sizes = []
        #for column_index, compression_dict in sorted(compression_dicts.items()):
        #    column_sizes.append(len(list(compression_dict.values())[0])) # All values should have the same length

        # Calculate and save the column coordinates and max length of these coordinates.
        column_start_coords = f4py.get_column_start_coords(column_sizes)
        column_coords_string, max_column_coord_length = f4py.build_string_map(column_start_coords)
        f4py.write_str_to_file(f4_file_path + ".cc", column_coords_string)
        f4py.write_str_to_file(f4_file_path + ".mccl", str(max_column_coord_length).encode())

        # Find and save the line length.
        #line_length = sum(column_sizes) + 1
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
            f4py.write_str_to_file(f4_file_path + ".mctl", str(max_col_type_length).encode())

        if column_compression_dicts:
            self._save_compression_dict(f4_file_path, column_compression_dicts)

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
            column_types_values_dict = {} # TODO: This dictionary could get really large. Could modify the code to use sqlitedict or https://stackoverflow.com/questions/47233562/key-value-store-in-python-for-possibly-100-gb-of-data-without-client-server.
            column_types_dict = {}
            for i in range(start_index, end_index):
                column_sizes_dict[i] = 0
                column_types_values_dict[i] = {b"i": set(), b"f": set(), b"s": set()}

            # Loop through the file for the specified columns.
            num_rows = 0
            for line in in_file:
                line = line.rstrip(b"\n")

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

        column_compression_dicts = {}

        for i in range(start_index, end_index):
            column_types_dict[i] = _infer_type_for_column(column_types_values_dict[i])

            unique_values = list(column_types_values_dict[i][b"s"] | column_types_values_dict[i][b"i"] | column_types_values_dict[i][b"f"])
            unique_values = sorted(unique_values)

            use_categorical_compression = (len(unique_values) / num_rows) <= 0.1
            column_compression_dicts[i] = {}
            column_compression_dicts[i]["map"] = {}

            if use_categorical_compression:
                column_compression_dicts[i]["compression_type"] = b"c"
                num_bytes = f4py.get_bigram_size(len(unique_values))

                for j, value in _enumerate_for_compression(unique_values):
                    #column_compression_dicts[i]["map"][value] = int2ba(j, length = length).to01()
                    column_compression_dicts[i]["map"][value] = j.to_bytes(length = num_bytes, byteorder = "big")

                column_sizes_dict[i] = num_bytes
            else:
                column_compression_dicts[i]["compression_type"] = column_types_dict[i]
                bigrams = _find_unique_bigrams(unique_values)
                num_bytes = f4py.get_bigram_size(len(bigrams))

                for j, gram in _enumerate_for_compression(bigrams):
                    #column_compression_dicts[i]["map"][gram] = int2ba(j, length = length).to01()
                    column_compression_dicts[i]["map"][gram] = j.to_bytes(length = num_bytes, byteorder = "big")

                column_sizes_dict[i] = 0
                for unique_value in unique_values:
                    compressed_length = len(f4py.compress_using_2_grams(unique_value, column_compression_dicts[i]["map"]))
                    column_sizes_dict[i] = max(column_sizes_dict[i], compressed_length)

        return column_sizes_dict, column_types_dict, column_compression_dicts, num_rows

    def _save_output_file(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, num_rows, num_processes, num_rows_per_save, tmp_dir_path):
        self._print_message(f"Parsing chunks of {delimited_file_path} and saving to temp directory ({tmp_dir_path})")

        if num_processes == 1:
            line_length = self._save_rows_chunk(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, 0, 0, num_rows, num_rows_per_save, tmp_dir_path)
        else:
            row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)

            # Find the line length.
            max_line_sizes = Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, i, row_chunk[0], row_chunk[1], num_rows_per_save, tmp_dir_path) for i, row_chunk in enumerate(row_chunk_indices))
            line_length = max(max_line_sizes)
            Parallel(n_jobs=num_processes)(delayed(self._save_rows_chunk)(delimited_file_path, f4_file_path, delimiter, compression_level, column_types, compression_dicts, i, row_chunk[0], row_chunk[1], num_rows_per_save, tmp_dir_path) for i, row_chunk in enumerate(row_chunk_indices))

        # Merge the file chunks. This dictionary enables us to sort them properly.
        self._print_message(f"Merging the file chunks for {delimited_file_path}")
        self._merge_chunk_files(f4_file_path, num_processes, num_rows_per_save, tmp_dir_path)

        return line_length

    def _save_rows_chunk(self, delimited_file_path, f4_file_path, delimiter, compression_level, column_sizes, column_types, compression_dicts, chunk_number, start_index, end_index, num_rows_per_save, tmp_dir_path):
        max_line_size = 0
        #column_sizes = []
        #compressor = f4py.CompressionHelper._get_compressor(f4_file_path, compression_level)

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
#                    for column_index, compression_dict in compression_dicts.items():
#                        line_items[column_index] = compression_dict[line_items[column_index]]
                        #column_sizes[column_index] = len(line_items[column_index])

                    # Format the column sizes using fixed widths.
                    # out_items = [f4py.format_string_as_fixed_width(line_items[i], size) for i, size in enumerate(column_sizes)]
                    # out_line = b"".join(out_items)

                    #TODO: Make a shared int/float compression dict?
                    out_items = []

                    for i, size in enumerate(column_sizes):
                        compressed_value = f4py.compress_using_2_grams(line_items[i], compression_dicts[i]["map"])

                        out_items.append(f4py.format_string_as_fixed_width(compressed_value, size))
                    out_line = b"".join(out_items)

                    #TODO
                    #if compressor:
                    #    out_line = compressor.compress(out_line)
                    #else:
                    #    # We add a newline character when the data are not compressed.
                    #    # This makes the file more readable (doesn't matter when the data are compressed).
                    #    out_line += b"\n"

                    out_line += b"\n"
                    line_size = len(out_line)

                    max_line_size = max([max_line_size, line_size])

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

        return max_line_size

    def _merge_chunk_files(self, f4_file_path, num_processes, num_rows_per_save, tmp_dir_path):
        with open(f4_file_path, "wb") as f4_file:
            out_lines = []

            for i in range(num_processes):
                chunk_file_path = f"{tmp_dir_path}{i}"

                if not os.path.exists(chunk_file_path):
                    continue

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

    def _save_compression_dict(self, f4_file_path, column_compression_dicts):
        # To enable decompressing the data, we need to switch the keys and values.
        for column_index, compression_dict in column_compression_dicts.items():
            decompression_map = {}
            for value, compressed_value in compression_dict["map"].items():
                decompression_map[f4py.convert_bytes_to_int(compressed_value)] = value
                #decompression_map[compressed_value] = value

            column_compression_dicts[column_index]["map"] = decompression_map

        with open(f"{f4_file_path}.cmpr", "wb") as cmpr_file:
            cmpr_file.write(f4py.serialize(column_compression_dicts))

                #cmpr_file.write((f"{column_index}\t").encode() + f4py.serialize(decompression_dict) + b"\n")

        # column_indices = []
        # values = []
        # compressed_values = []

        # for i, column_compression_dict in column_compression_dicts.items():
        #     for value, compressed_value in column_compression_dict["map"].items():
        #         column_indices.append(str(i).encode())
        #         values.append(value)
        #         compressed_values.append(compressed_value)

        # mcil = f4py.get_max_string_length(column_indices)
        # mvl = f4py.get_max_string_length(values)
        # mcvl = f4py.get_max_string_length(compressed_values)

        # column_indices = f4py.format_column_items(column_indices, mcil)
        # values = f4py.format_column_items(values, mvl)
        # print(compressed_values)
        # print(mcvl)
        # import sys
        # sys.exit()
        #compressed_values = f4py.format_column_items(compressed_values, mcvl)

        # table = b""
        # for i in range(len(column_indices)):
        #     #table += (f"{column_indices[i].decode()}{values[i].decode()}{compressed_values[i].decode()}").encode()
        #     table += column_indices[i] + values[i] + compressed_values[i]

        #     if i != (len(column_indices) - 1):
        #         table += b"\n"

        # f4py.write_str_to_file(f4_file_path + ".cmpr", table)

        # column_start_coords = f4py.get_column_start_coords([mcil, mvl, mcvl])
        # column_coords_string, max_column_coord_length = f4py.build_string_map(column_start_coords)
        # f4py.write_str_to_file(f4_file_path + ".cmpr.cc", column_coords_string)
        # f4py.write_str_to_file(f4_file_path + ".cmpr.mccl", str(max_column_coord_length).encode())
        # #TODO: Remove the following line?
        # f4py.write_str_to_file(f4_file_path + ".cmpr.ll", str(mcil + mvl + mcvl + 1).encode())

        # # Save compression type information
        # compression_types = []
        # for i, column_compression_dict in column_compression_dicts.items():
        #     compression_types.append(column_compression_dict["compression_type"])

        # mctl = f4py.get_max_string_length(compression_types)
        # compression_types = f4py.format_column_items(compression_types, mctl)
        # table = b"\n".join(compression_types)

        # f4py.write_str_to_file(f4_file_path + ".cmprtype", table)
        # f4py.write_str_to_file(f4_file_path + ".cmprtype.ll", str(mctl + 1).encode())

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

    if len(types_dict[b"s"]) > 0:
        return b"s"
    elif len(types_dict[b"f"]) > 0:
        return b"f"

    return b"i"

def _find_unique_bigrams(values):
    grams = set()

    for value in values:
        for start_i in range(0, len(value), 2):
            end_i = (start_i + 2)
            grams.add(value[start_i:end_i])

    return sorted(list(grams))

# We skip the space character because it causes a problem when we parse from a file.
def _enumerate_for_compression(values):
    ints = []
    capacity = len(values)
    length = f4py.get_bigram_size(capacity)

    i = 0
    while len(ints) < capacity:
        if b' ' not in i.to_bytes(length = length, byteorder = "big"):
            ints.append(i)
        
        i += 1

    for i in ints:
        yield i, values.pop(0)
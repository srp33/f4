import f4py
import fastnumbers
import glob
from itertools import chain
from joblib import Parallel, delayed
import math
import os
import sys
import zstandard

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
    """
    def __init__(self, data_file_path, fixed_file_extensions=["", ".cc", ".ct"], stats_file_extensions=[".ll", ".mccl", ".nrow", ".ncol"]):
        #TODO: expand this out for the other parameters.
        if not isinstance(data_file_path, str):
            raise Exception("You must specify data_file_path as an str value.")

        self.data_file_path = data_file_path

        # Cache file handles in a dictionary.
        self.__file_handles = {}
        for ext in fixed_file_extensions:
            self.__file_handles[ext] = self.set_file_handle(ext)

        # Cache statistics in a dictionary.
        self.__stats = {}
        for ext in stats_file_extensions:
            self.__stats[ext] = f4py.read_int_from_file(data_file_path, ext)

    def __enter__(self):
        return self

    def __exit__(self, the_type, value, traceback):
        for handle in self.__file_handles.values():
            handle.close()

    def query_and_save(self, fltr, select_columns, out_file_path=None, out_file_type="tsv", num_processes=1, lines_per_chunk=10):
        """
        Query the data file using zero or more filters.

        This function accepts filtering criteria, identifies matching rows,
        and saves the output (for select columns) to an output file.

        Args:
            fltr (BaseFilter): A filter.
            select_columns (list): A list of strings that indicate the names of columns that should be selected. If this is an empty list, all columns will be selected.
            out_file_path(str): A path to a file that will store the output data. If None is specified, the data will be directed to standard output.
            out_file_type (str): The output file type. Currently, the only supported value is tsv.
        """
        if not fltr:
            raise Exception("A filter must be specified.")

        if not isinstance(fltr, f4py.NoFilter):
            raise Exception("An object that inherits from NoFilter must be specified.")

        if out_file_type != "tsv":
            raise Exception("The only out_file_type currently supported is tsv.")

        if select_columns:
            if not isinstance(select_columns, list):
                raise Exception("You must specify select_column as a list.")
        else:
            select_columns = []

        # Store column indices and types in dictionaries so we only have to retrieve
        # each once, even if we use the same column in multiple filters.
        select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = self._get_column_meta(fltr.get_column_name_set(), select_columns)

        fltr.check_types(column_type_dict)

        has_index = len(glob.glob(self.data_file_path + ".idx_*")) > 0

        if has_index:
#TODO: Remove this stuff if we don't need it after testing on huge files.
#            sub_filters = fltr.get_sub_filters()

#            if num_processes == 1 or len(sub_filters) == 1:
            keep_row_indices = sorted(fltr.filter_indexed_column_values(self.data_file_path, self.get_num_rows(), num_processes))
#            else:
#                fltr_results_dict = {}

##                for f in sub_filters:
##                    fltr_results_dict[str(f)] = f.filter_indexed_column_values(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_processes)

                # This is a parallelization of the above code.
                # At least in some cases, it slows things down more than it speeds things up.
#                fltr_results = Parallel(n_jobs = num_processes)(delayed(f.filter_indexed_column_values)(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_processes) for f in sub_filters)
#                for i in range(len(sub_filters)):
#                    fltr_results_dict[str(sub_filters[i])] = fltr_results[i]
#
#                keep_row_indices = sorted(fltr.filter_indexed_column_values_parallel(fltr_results_dict))
        else:
            if num_processes == 1:
                row_indices = set(range(self.get_num_rows()))
                keep_row_indices = sorted(fltr.filter_column_values(self.data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict))
            else:
                # Loop through the rows in parallel and find matching row indices.
                keep_row_indices = sorted(chain.from_iterable(Parallel(n_jobs = num_processes)(delayed(fltr.filter_column_values)(self.data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict) for row_indices in self._generate_row_chunks(num_processes))))

        select_column_coords = [column_coords_dict[name] for name in select_columns]
        decompressor = f4py.get_decompressor(decompression_type, decompressor)

        if out_file_path:
            # Write output (in chunks)
            with open(out_file_path, 'wb') as out_file:
                # Header line
                out_file.write(b"\t".join(select_columns) + b"\n")
                
                out_lines = []
                for row_index in keep_row_indices:
                    #out_values = self.__parse_values_for_output(decompression_type, decompressor, bigram_size_dict, row_index, select_column_coords, select_columns)
                    out_values = self.__parse_row_values(row_index, select_column_coords, decompression_type, decompressor, bigram_size_dict, select_columns)
                    out_lines.append(b"\t".join(out_values))

                    if len(out_lines) % lines_per_chunk == 0:
                        out_file.write(b"\n".join(out_lines) + b"\n")
                        out_lines = []

                if len(out_lines) > 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
        else:
            sys.stdout.buffer.write(b"\t".join(select_columns) + b"\n")

            for row_index in keep_row_indices:
                out_values = self.__parse_row_values(row_index, select_column_coords, decompression_type, decompressor, bigram_size_dict, select_columns)
                sys.stdout.buffer.write(b"\t".join(out_values))

                if row_index != keep_row_indices[-1]:
                    sys.stdout.buffer.write(b"\n")

    def head(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        if not select_columns:
            select_columns = []
        self.query_and_save(f4py.HeadFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def tail(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        if not select_columns:
            select_columns = []
        self.query_and_save(f4py.TailFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def get_num_rows(self):
        return self.__stats[".nrow"]

    def get_num_cols(self):
        return self.__stats[".ncol"]

    def get_column_type(self, column_index):
        return next(self.__parse_data_values(column_index, 2, [[0, 1]], self.__file_handles[".ct"])).decode()

    def get_column_type_from_name(self, column_name):
        try:
            with f4py.IndexSearcher._get_index_parser(f"{self.data_file_path}.cn") as index_parser:
                return self.get_column_type(self._get_column_index_from_name(index_parser, column_name))
        except:
            raise Exception(f"A column with the name {column_name} does not exist.")

    def _get_column_index_from_name(self, index_parser, column_name):
        position = f4py.IndexSearcher._get_identifier_row_index(index_parser, column_name.encode(), self.get_num_cols())

        if position < 0:
            raise Exception(f"Could not retrieve index because column named {column_name} was not found.")

        return position

    def get_file_handle(self, ext):
        return self.__file_handles[ext]

    def set_file_handle(self, ext):
        if ext not in self.__file_handles:
            self.__file_handles[ext] = f4py.open_read_file(self.data_file_path, ext)

        return self.get_file_handle(ext)

    def get_stat(self, ext):
        return self.__stats[ext]

    ##############################################
    # Non-public functions
    ##############################################

    def _get_column_meta(self, filter_column_set, select_columns):
        column_type_dict = {}
        column_coords_dict = {}
        column_index_name_dict = {}
        #column_name_index_dict = {} #TODO

        if len(select_columns) == 0:
            with f4py.Parser(self.data_file_path + ".cn", fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as cn_parser:
                coords = cn_parser._parse_data_coords([0, 1])

                for row_index in range(self.get_num_cols()):
                    values = cn_parser.__parse_row_values(row_index, coords)
                    column_name = values[0]
                    column_index = fastnumbers.fast_int(values[1])

                    column_index_name_dict[column_index] = column_name
                    #column_name_index_dict[column_name] = column_index

                    if column_name in filter_column_set:
                        column_type_dict[column_name] = row_index

                all_coords = self._parse_data_coords(range(self.get_num_cols()))
                for row_index in range(self.get_num_cols()):
                    column_coords_dict[column_index_name_dict[row_index]] = all_coords[row_index]

            all_columns = [x[1] for x in sorted(column_index_name_dict.items())]
            select_columns = all_columns
        else:
            with f4py.IndexSearcher._get_index_parser(f"{self.data_file_path}.cn") as index_parser:
                select_columns = [name.encode() for name in select_columns]
                all_columns = list(filter_column_set | set(select_columns))

                column_name_index_dict = {}
                for column_name in all_columns:
                    column_index = self._get_column_index_from_name(index_parser, column_name.decode())
                    column_name_index_dict[column_name] = column_index
                    column_index_name_dict[column_index] = column_name

                for column_name in filter_column_set:
                    column_type_dict[column_name] = self.get_column_type(column_name_index_dict[column_name])

            all_column_indices = [column_name_index_dict[name] for name in all_columns]
            all_coords = self._parse_data_coords(all_column_indices)

            for i, column_name in enumerate(all_columns):
                column_coords_dict[column_name] = all_coords[i]

        decompression_type = None
        decompressor = None
        bigram_size_dict = {}
        decompressor_file_path = f"{self.data_file_path}.cmpr"

        if os.path.exists(decompressor_file_path):
            decompression_text = f4py.read_str_from_file(decompressor_file_path)

            if decompression_text == b"z":
                decompression_type = "zstd"
                # decompressor = zstandard.ZstdDecompressor()
            else:
                decompression_type = "dictionary"
                decompressor = self.__get_decompression_dict(decompressor_file_path, column_index_name_dict)
                # if len(decompression_dict) > 0:
                #    select_compression_dict = self.__invert_decompression_dict(decompression_dict, select_columns)

                for column_name in all_columns:
                    bigram_size_dict[column_name] = f4py.get_bigram_size(len(decompressor[column_name]["map"]))

        return select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict

    def _generate_row_chunks(self, num_processes):
        rows_per_chunk = math.ceil(self.get_num_rows() / num_processes)

        row_indices = set()

        for row_index in range(self.get_num_rows()):
            row_indices.add(row_index)

            if len(row_indices) == rows_per_chunk:
                yield row_indices
                row_indices = set()

        if len(row_indices) > 0:
            yield row_indices

    def _parse_data_coords(self, indices):
        data_coords = []
        out_dict = {}
        mccl = self.__stats[".mccl"] + 1

        for index in indices:
            start_pos = index * mccl
            next_start_pos = start_pos + mccl
            further_next_start_pos = next_start_pos + mccl

            # See if we already have cached the start position.
            if index in out_dict:
                data_start_pos = out_dict[index]
            # If not, retrieve the start position from the cc file and then cache it.
            else:
                data_start_pos = fastnumbers.fast_int(self.__file_handles[".cc"][start_pos:next_start_pos].rstrip(b" "))
                out_dict[index] = data_start_pos

            # See if we already have cached the end position.
            if (index + 1) in out_dict:
                data_end_pos = out_dict[index + 1]
            # If not, retrieve the end position from the cc file and then cache it.
            else:
                data_end_pos = fastnumbers.fast_int(self.__file_handles[".cc"][next_start_pos:further_next_start_pos].rstrip(b" "))
                out_dict[index + 1] = data_end_pos

            data_coords.append([data_start_pos, data_end_pos])

        return data_coords

    def __parse_data_value(self, start_element, segment_length, coords, str_like_object):
        start_pos = start_element * segment_length

        return str_like_object[(start_pos + coords[0]):(start_pos + coords[1])]

    def __parse_data_values(self, start_element, segment_length, data_coords, str_like_object):
        start_pos = start_element * segment_length

        for coords in data_coords:
            yield str_like_object[(start_pos + coords[0]):(start_pos + coords[1])].rstrip(b" ")

    def _parse_row_value(self, row_index, column_coords, line_length, file_handle, decompression_type=None, decompressor=None, bigram_size_dict=None, column_name=None):
        if decompression_type == "zstd":
            line = self.__parse_data_value(row_index, line_length, [0, line_length], file_handle)
            line = decompressor.decompress(line)
            return self.__parse_data_value(0, 0, column_coords, line).rstrip(b" ")
        else:
            value = self.__parse_data_value(row_index, line_length, column_coords, file_handle).rstrip(b" ")

            if decompression_type == "dictionary":
                value = f4py.decompress(value, decompressor[column_name], bigram_size_dict[column_name])

            return value

    def __parse_row_values(self, row_index, column_coords, decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
        if decompression_type == "zstd":
            line_length = self.__stats[".ll"]
            line = self.__parse_data_value(row_index, line_length, [0, line_length], self.__file_handles[""])
            line = decompressor.decompress(line)

            return list(self.__parse_data_values(0, 0, column_coords, line))
        else:
            values = list(self.__parse_data_values(row_index, self.__stats[".ll"], column_coords, self.__file_handles[""]))

            if decompression_type == "dictionary":
                values = [f4py.decompress(values.pop(0), decompressor[column_name], bigram_size_dict[column_name]) for column_name in column_names]

            return values

    def __get_decompression_dict(self, file_path, column_index_name_dict):
        with open(file_path, "rb") as cmpr_file:
            return f4py.deserialize(cmpr_file.read())


    #     compression_dict = {}
    #     with open(file_path, "rb") as cmpr_file:
    #         for line in cmpr_file:
    #             line_items = line.rstrip(b"\n").split(b"\t")
    #             column_index = fastnumbers.fast_int(line_items[0])

    #             if column_index in column_index_name_dict:
    #                 column_name = column_index_name_dict[column_index]
    #                 compression_dict[column_name] = f4py.deserialize(line_items[1])

    #     #for column_index in column_index_name_dict.keys():
    #     #     compression_dict[column_index_name_dict[column_index]] = {}
    #     #     compression_dict[column_index_name_dict[column_index]]["map"] = {}

    #     # with Parser(file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as parser:
    #     #     coords = parser._parse_data_coords([0, 1, 2])
    #     #     num_rows = fastnumbers.fast_int((len(parser.get_file_handle("")) + 1) / parser.get_stat(".ll"))

    #     #     # Use a set for performance reasons
    #     #     column_indices_set = set(column_index_name_dict.keys())

    #     #     with Parser(f"{self.data_file_path}.cmpr", fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as parser:
    #     #         for row_index in range(num_rows):
    #     #             values = parser.__parse_row_values(row_index, coords)
    #     #             column_index = fastnumbers.fast_int(values[0])

    #     #             if column_index in column_indices_set:
    #     #                 compressed_value = f4py.convert_bytes_to_int(values[2])

    #     #                 compression_dict[column_index_name_dict[column_index]]["map"][compressed_value] = values[1]

    #     # # # We need column names as keys rather than indices.
    #     # # compression_dict2 = {}
    #     # # for i, column_name in enumerate(column_names):
    #     # #     compression_dict2[column_name] = compression_dict[column_indices[i]]

    #     # with Parser(f"{self.data_file_path}.cmprtype", fixed_file_extensions=[""], stats_file_extensions=[".ll"]) as parser:
    #     #     coords = [[0, 1]]

    #     #     for column_index in column_index_name_dict.keys():
    #     #         compression_dict[column_index_name_dict[column_index]]["compression_type"] = parser.__parse_row_values(column_index, coords)[0]

    #     return compression_dict

    # def __invert_decompression_dict(self, decompression_dict, select_columns):
    #     inverted_dict = {}
    #
    #     for select_column in select_columns:
    #         inverted_dict[select_column] = {"compression_type": decompression_dict[select_column]["compression_type"]}
    #         inverted_dict[select_column]["map"] = {}
    #
    #         for compressed_value, value in decompression_dict[select_column]["map"].items():
    #             inverted_dict[select_column]["map"][value] = compressed_value
    #
    #     return inverted_dict
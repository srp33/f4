import f4py
import fastnumbers
import glob
from itertools import chain
from joblib import Parallel, delayed
import math
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
        self.data_file_path = data_file_path

        self.compression_level = f4py.read_str_from_file(data_file_path, ".cmp")
        self.__decompressor = None
        if self.compression_level != b"None":
            self.__decompressor = zstandard.ZstdDecompressor()

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

        if not isinstance(fltr, f4py.BaseFilter):
            raise Exception("An object that inherits from BaseFilter must be specified.")

        if out_file_type != "tsv":
            raise Exception("The only out_file_type currently supported is tsv.")

        if select_columns:
            if not isinstance(select_columns, list):
                raise Exception("You must specify select_column as a list.")
        else:
            select_columns = []

        # Store column indicies and types in dictionaries so we only have to retrieve
        # each once, even if we use the same column in multiple filters.
        select_columns, column_index_dict, column_type_dict, column_coords_dict = self._get_column_meta(fltr, select_columns)

        fltr.check_types(column_index_dict, column_type_dict)

        has_index = len(glob.glob(self.data_file_path + ".idx_*")) > 0

        if has_index:
#            sub_filters = fltr.get_sub_filters()

#            if num_processes == 1 or len(sub_filters) == 1:
            keep_row_indices = sorted(fltr.filter_indexed_column_values(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_processes))
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
                keep_row_indices = sorted(fltr.filter_column_values(self.data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict))
            else:
                # Loop through the rows in parallel and find matching row indices.
                keep_row_indices = sorted(chain.from_iterable(Parallel(n_jobs = num_processes)(delayed(fltr.filter_column_values)(self.data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict) for row_indices in self._generate_row_chunks(num_processes))))

        # Get the coords for each column to select
        select_column_coords = self._parse_data_coords([column_index_dict[x] for x in select_columns])

        if out_file_path:
            # Write output (in chunks)
            with open(out_file_path, 'wb') as out_file:
                # Header line
                out_file.write(b"\t".join(select_columns) + b"\n")

                out_lines = []
                for row_index in keep_row_indices:
                    out_lines.append(b"\t".join([x for x in self.__parse_row_values(row_index, select_column_coords)]))

                    if len(out_lines) % lines_per_chunk == 0:
                        out_file.write(b"\n".join(out_lines) + b"\n")
                        out_lines = []

                if len(out_lines) > 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
        else:
            sys.stdout.buffer.write(b"\t".join(select_columns) + b"\n")

            for row_index in keep_row_indices:
                sys.stdout.buffer.write(b"\t".join([x for x in self.__parse_row_values(row_index, select_column_coords)]))

    def head(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        self.query_and_save(f4py.HeadFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def tail(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        self.query_and_save(f4py.TailFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def get_num_rows(self):
        return self.__stats[".nrow"]

    def get_num_cols(self):
        return self.__stats[".ncol"]

    def get_column_type(self, column_index):
        return next(self.__parse_data_values(column_index, 2, [[0, 1]], self.__file_handles[".ct"])).decode()

    def get_column_type_from_name(self, column_name):
        """
        Find the type of a specified column.

        Args:
            column_name (str): Name of the column.
        Returns:
            A character indicating the data type for the specified column.
            The character will be one of the following:
                * c (categorical)
                * f (float)
                * i (integer)
        """

        try:
            return self.get_column_type(self.get_column_index_from_name(column_name))
        except:
            raise Exception(f"A column with the name {column_name} does not exist.")

    def get_column_index_from_name(self, column_name):
        position = list(f4py.IdentifierIndexer(f"{self.data_file_path}.cn", None).filter(f4py.StringEqualsFilter("NotNeeded", column_name), self.get_num_cols()))

        if len(position) == 0:
            raise Exception(f"Could not retrieve index because column named {column_name} was not found.")

        return position[0]

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

    def _get_column_meta(self, fltr, select_columns):
        if len(select_columns) == 0:
            with f4py.Parser(self.data_file_path + ".cn", fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as cn_parser:
                line_length = cn_parser.get_stat(".ll")
                coords = cn_parser._parse_data_coords([0, 1])

                # They are not in sorted order in the file, so we must put them in a dict and sort it.
                column_index_dict = {}
                for row_index in range(self.get_num_cols()):
                    values = cn_parser.__parse_row_values(row_index, coords)

                    column_index_dict[fastnumbers.fast_int(values[1])] = values[0]

                select_columns = []
                for index, name in sorted(column_index_dict.items()):
                    select_columns.append(name)

                column_index_dict = {name: index for index, name in enumerate(select_columns)}
        else:
            select_columns = [x.encode() for x in select_columns]
            column_index_dict = {name: self.get_column_index_from_name(name.decode()) for name in fltr.get_column_name_set() | set(select_columns)}

        type_columns = fltr.get_column_name_set() | set(select_columns)
        filter_column_type_dict = {}
        for column_name in type_columns:
            column_index = column_index_dict[column_name]
            filter_column_type_dict[column_index] = self.get_column_type(column_index)

        column_indices = list(column_index_dict.values())
        column_coords = self._parse_data_coords(column_indices)
        column_coords_dict = {}
        for i in range(len(column_indices)):
            column_coords_dict[column_indices[i]] = column_coords[i]

        return select_columns, column_index_dict, filter_column_type_dict, column_coords_dict

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
                data_start_pos = fastnumbers.fast_int(self.__file_handles[".cc"][start_pos:next_start_pos].rstrip())
                out_dict[index] = data_start_pos

            # See if we already have cached the end position.
            if (index + 1) in out_dict:
                data_end_pos = out_dict[index + 1]
            # If not, retrieve the end position from the cc file and then cache it.
            else:
                data_end_pos = fastnumbers.fast_int(self.__file_handles[".cc"][next_start_pos:further_next_start_pos].rstrip())
                out_dict[index + 1] = data_end_pos

            data_coords.append([data_start_pos, data_end_pos])

        return data_coords

    def __parse_data_value(self, start_element, segment_length, coords, str_like_object):
        start_pos = start_element * segment_length

        return str_like_object[(start_pos + coords[0]):(start_pos + coords[1])]

    def __parse_data_values(self, start_element, segment_length, data_coords, str_like_object):
        start_pos = start_element * segment_length

        for coords in data_coords:
            yield str_like_object[(start_pos + coords[0]):(start_pos + coords[1])].rstrip()

    def _parse_row_value(self, row_index, column_coords, line_length, file_handle):
        if self.__decompressor:
            line = self.__parse_data_value(row_index, line_length, [0, line_length], file_handle)
            line = self.__decompressor.decompress(line)

            return self.__parse_data_value(0, 0, column_coords, line).rstrip()

        return self.__parse_data_value(row_index, line_length, column_coords, file_handle).rstrip()

    def __parse_row_values(self, row_index, column_coords):
        if self.__decompressor:
            line_length = self.__stats[".ll"]
            line = self.__parse_data_value(row_index, line_length, [0, line_length], self.__file_handles[""])
            line = self.__decompressor.decompress(line)

            return list(self.__parse_data_values(0, 0, column_coords, line))

        return list(self.__parse_data_values(row_index, self.__stats[".ll"], column_coords, self.__file_handles[""]))

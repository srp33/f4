import atexit
import f4py
#from f4py.Filters import *
#from f4py.Utilities import *
from itertools import chain
from joblib import Parallel, delayed
import math
import zstandard

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
    """

    #def __init__(self, data_file_path, is_index=False):
    def __init__(self, data_file_path, fixed_file_extensions=["", ".cc", ".cn", ".ct"], stats_file_extensions=[".ll", ".mccl", ".mcnl", ".nrow", ".ncol"]):
        self.data_file_path = data_file_path
        #self.is_index = is_index

        self.compression_level = f4py.read_str_from_file(data_file_path, ".cmp")
        #if is_index:
        #    cmp_file_path = f"{data_file_path}.idx.cmp"
        #else:
        #    cmp_file_path = f"{data_file_path}.cmp"
        self.__decompressor = None
        #if read_str_from_file(cmp_file_path) != b"None":
        #    self.__decompressor = zstandard.ZstdDecompressor()

        self.__file_handles = {}
        self.__stats = {}

        # Cache file handles in a dictionary.
        for ext in fixed_file_extensions:
            ext2 = ext
            #if is_index:
            #    ext2 = f".idx{ext}"
            self.__file_handles[ext] = f4py.open_read_file(data_file_path, ext2)

        # Cache statistics in a dictionary.
        for ext in stats_file_extensions:
            ext2 = ext
            #if is_index:
            #    ext2 = f".idx{ext}"
            self.__stats[ext] = f4py.read_int_from_file(data_file_path, ext2)

        atexit.register(self.close)

    def query_and_save(self, fltr, select_columns, out_file_path, out_file_type="tsv", num_processes=1, lines_per_chunk=10):
        """
        Query the data file using zero or more filters.

        This function accepts filtering criteria, identifies matching rows,
        and saves the output (for select columns) to an output file.

        Args:
            fltr (BaseFilter): A filter.
            select_columns (list): A list of strings that indicate the names of columns that should be selected. If this is an empty list, all columns will be selected.
            out_file_path(str): A path to a file that will store the output data.
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

        # self.__decompressor is not None

        if num_processes == 1:
            # This is a non-parallelized version of the code.
            row_indices = set(range(self.get_num_rows()))
            keep_row_indices = _process_rows(self, fltr, row_indices, column_index_dict, column_type_dict, column_coords_dict)
            #TODO
            #keep_row_indices = self._process_rows(self.data_file_path, fltr, row_indices, column_index_dict, column_type_dict, column_coords_dict)
        else:
            # Loop through the rows in parallel and find matching row indices.
            #keep_row_indices = chain.from_iterable(Parallel(n_jobs=num_processes)(delayed(self._process_rows)(self.data_file_path, fltr, row_indices, column_index_dict, column_type_dict, column_coords_dict) for row_indices in self._generate_row_chunks(num_processes)))
            keep_row_indices = chain.from_iterable(Parallel(n_jobs=num_processes)(delayed(_process_rows)(self.data_file_path, fltr, row_indices, column_index_dict, column_type_dict, column_coords_dict) for row_indices in self._generate_row_chunks(num_processes)))

        # Get the coords for each column to select
        select_column_coords = self._parse_data_coords([column_index_dict[x] for x in select_columns])

        # Write output file (in chunks)
        with open(out_file_path, 'wb') as out_file:
            # Header line
            out_file.write(b"\t".join(select_columns) + b"\n")

            out_lines = []
            for row_index in keep_row_indices:
                out_lines.append(b"\t".join([x.rstrip() for x in self._parse_row_values(row_index, select_column_coords)]))

                if len(out_lines) % lines_per_chunk == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

    def get_num_rows(self):
        return self.__stats[".nrow"]

    def get_num_cols(self):
        return self.__stats[".ncol"]

    def get_column_type(self, column_index):
        return next(self._parse_data_values(column_index, 2, [[0, 1]], self.__file_handles[".ct"])).decode()

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

        # TODO: Rework this when you implement binary search on disk in get_column_meta
        cn_file_handle = self.__file_handles[".cn"]
        all_column_names = [x.rstrip(b" ") for x in cn_file_handle[:].rstrip(b"\n").split(b"\n")]

        try:
            return self.get_column_type(all_column_names.index(column_name.encode()))
        except:
            raise Exception(f"A column with the name {column_name} does not exist.")

    def get_file_handle(self, ext):
        return self.__file_handles[ext]

    def get_stat(self, ext):
        return self.__stats[ext]

    def parse_data_value(self, start_element, segment_length, coords, str_like_object):
        start_pos = start_element * segment_length

        return str_like_object[(start_pos + coords[0]):(start_pos + coords[1])]

    def close(self):
        for handle in self.__file_handles.values():
            handle.close()

    ##############################################
    # Non-public functions
    ##############################################

    def _get_column_meta(self, fltr, select_columns):
        cn_file_handle = self.__file_handles[".cn"]
        #mcnl = self.__stats[".mcnl"]
        #col_coords = [[0, mcnl]]
        all_column_names = [x.rstrip(b" ") for x in cn_file_handle[:].rstrip(b"\n").split(b"\n")]

        if len(select_columns) == 0:
            select_columns = all_column_names
            column_index_dict = {name: index for index, name in enumerate(all_column_names)}
        else:
            select_columns = [x.encode() for x in select_columns]
            column_index_dict = {name: all_column_names.index(name) for name in fltr.get_column_name_set() | set(select_columns)}

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

#    def _get_column_meta(self, fltr, select_columns):
#        cn_file_handle = self.__file_handles[".cn"]
#        # TODO: Rework this code to do a binary search on disk
#        #mcnl = self.__stats[".mcnl"]
#        #col_coords = [[0, mcnl]]
#        all_column_names = [x.rstrip(b" ") for x in cn_file_handle[:].rstrip(b"\n").split(b"\n")]
#
#        if len(select_columns) == 0:
#            select_columns = all_column_names
#            column_index_dict = {name: index for index, name in enumerate(all_column_names)}
#        else:
#            select_columns = [x.encode() for x in select_columns]
#            column_index_dict = {name: all_column_names.index(name) for name in fltr.get_column_name_set() | set(select_columns)}
#
#        filter_column_type_dict = {}
#        for column_name in fltr.get_column_name_set():
#            column_index = column_index_dict[column_name]
#            filter_column_type_dict[column_index] = self.get_column_type(column_index)
#
#        column_indices = list(column_index_dict.values())
#        column_coords = self._parse_data_coords(column_indices)
#        column_coords_dict = {}
#        for i in range(len(column_indices)):
#            column_coords_dict[column_indices[i]] = column_coords[i]
#
#        return select_columns, column_index_dict, filter_column_type_dict, column_coords_dict

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
                data_start_pos = int(self.__file_handles[".cc"][start_pos:next_start_pos].rstrip())
                out_dict[index] = data_start_pos

            # See if we already have cached the end position.
            if (index + 1) in out_dict:
                data_end_pos = out_dict[index + 1]
            # If not, retrieve the end position from the cc file and then cache it.
            else:
                data_end_pos = int(self.__file_handles[".cc"][next_start_pos:further_next_start_pos].rstrip())
                out_dict[index + 1] = data_end_pos

            data_coords.append([data_start_pos, data_end_pos])

        return data_coords

    def _parse_data_values(self, start_element, segment_length, data_coords, str_like_object):
        start_pos = start_element * segment_length

        for coords in data_coords:
            yield str_like_object[(start_pos + coords[0]):(start_pos + coords[1])]

    def _parse_row_values(self, row_index, column_coords):
        return list(self._parse_data_values(row_index, self.__stats[".ll"], column_coords, self.__file_handles[""]))

#    def _process_rows2(self, fltr, row_indices, column_index_dict, column_coords_dict, is_compressed):
#        line_length = self.get_stat(".ll")
#        coords = column_coords_dict[column_index_dict[fltr.column_name]]
#        data_file_handle = self.get_file_handle("")

#        return NonCompressedIndexRetriever().filter(fltr, row_indices, column_index_dict, column_coords_dict):
#        return row_indices

#        passing_row_indices = set()
#        for i in row_indices:
#            if fltr.passes(self.parse_data_value(i, line_length, coords, data_file_handle).rstrip()):
#                passing_row_indices.add(i)
#
#        return passing_row_indices


        #is_index = os.path.exists(f"{self.data_file_path}.idx_{fltr.column_name}")

#        return row_indices

        #parser = Parser(data_file_path, is_index=is_index)
        #passing_row_indices = sorted(parser.filter_column_values(fltr, row_indices, column_index_dict, column_coords_dict))
        #parser.close()

        #return passing_row_indices

#    def _filter_column_values(self, fltr, row_indices, column_index_dict, column_coords_dict):
#        line_length = self.get_stat(".ll")
#        coords = column_coords_dict[column_index_dict[fltr.column_name]]
#        data_file_handle = self.get_file_handle("")
#
#        passing_row_indices = set()
#        for i in row_indices:
#            if fltr.passes(self.parse_data_value(i, line_length, coords, data_file_handle).rstrip()):
#                passing_row_indices.add(i)
#
#        return passing_row_indices


#####################################################
# Class functions
#####################################################

def _process_rows(parser, fltr, row_indices, column_index_dict, column_type_dict, column_coords_dict):
#    is_index = os.path.exists(f"{data_file_path}.idx")

    #parser = Parser(data_file_path, is_index=is_index)
    passing_row_indices = sorted(fltr.filter_column_values(parser, row_indices, column_index_dict, column_type_dict, column_coords_dict))
    #parser.close()

    return passing_row_indices

class NonCompressedIndexRetriever:
#    def __init__(self):
#        self.column_index_dict = column_index_dict
#        self.column_coords_dict = column_coords_dict

    def filter(self, fltr, row_indices, column_index_dict, column_coords_dict):
#        line_length = self.get_stat(".ll")
#        coords = column_coords_dict[column_index_dict[fltr.column_name]]
#        data_file_handle = self.get_file_handle("")

        passing_row_indices = set()
        for i in row_indices:
            if fltr.passes(self.parse_data_value(i, line_length, coords, data_file_handle).rstrip()):
                passing_row_indices.add(i)

        return passing_row_indices

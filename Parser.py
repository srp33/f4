import atexit
import fastnumbers
from Helper import *
from itertools import chain
from joblib import Parallel, delayed
import math
import operator
import os
import re
import zstandard

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
    """

    def __init__(self, data_file_path, is_index=False):
        self.data_file_path = data_file_path
        self.is_index = is_index

        if is_index:
            cmp_file_path = f"{data_file_path}.idx.cmp"
        else:
            cmp_file_path = f"{data_file_path}.cmp"
        self.__decompressor = None
        if read_str_from_file(cmp_file_path) == b"True":
            self.__decompressor = zstandard.ZstdDecompressor()

        self.__file_handles = {}
        self.__stats = {}

        # Cache file handles in a dictionary.
        for ext in ["", ".cc", ".cn", ".ct"]:
            ext2 = ext
            if is_index:
                ext2 = f".idx{ext}"
            self.__file_handles[ext] = open_read_file(data_file_path, ext2)

        # Cache statistics in a dictionary.
        for ext in [".ll", ".mccl", ".mcnl", ".nrow", ".ncol"]:
            ext2 = ext
            if is_index:
                ext2 = f".idx{ext}"
            self.__stats[ext] = read_int_from_file(data_file_path, ext2)

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
        if not isinstance(fltr, BaseFilter):
            raise Exception("An object that inherits from BaseFilter must be specified.")

        # Check whether filter types are valid for the types of the columns specified.
        filter_column_type_dict = {name: self._get_column_type(name) for name in fltr.get_column_name_set()}
        fltr.check_types(self, filter_column_type_dict)

        # Loop through the rows in parallel and find matching row indices.
        keep_row_indices = sorted(chain.from_iterable(Parallel(n_jobs=num_processes)(delayed(_process_rows)(self.data_file_path, fltr, row_indices) for row_indices in self._generate_row_chunks(num_processes))))

        # By default, select all columns.
        if not select_columns or len(select_columns) == 0:
            column_names = self.get_column_names()
            select_column_indices = range(len(column_names))
        else:
            column_names = [x.encode() for x in select_columns]
            select_column_indices = self.get_column_indices(column_names)

        # Get the coords for each column to select
        select_column_coords = self._parse_data_coords(select_column_indices)

        #data_file_handle = self.__file_handles[""]
        #line_length = self.__stats[".ll"]

        # Write output file (in chunks)
        with open(out_file_path, 'wb') as out_file:
            # Header line
            out_file.write(b"\t".join(column_names) + b"\n")

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

#    def get_column_values(self, column_coords):
#        data_file_handle = self.__file_handles[""]
#        line_length = self.__stats[".ll"]
#
#        for row_index in range(self.get_num_rows()):
#            yield next(self.__parse_data_values(row_index, line_length, column_coords, data_file_handle)).rstrip()

#    def get_cell_value(self, row_index, column_coords):
#        return next(self.__parse_data_values(row_index, self.__stats[".ll"], column_coords, self.__file_handles[""])).rstrip()

    def get_column_indices(self, query_column_names):
        query_column_names_set = set(query_column_names)
        mcnl = self.__stats[".mcnl"]
        col_coords = [[0, mcnl]]
        matching_column_dict = {}
        cn_file_handle = self.__file_handles[".cn"]

        for col_index in range(self.__stats[".ncol"]):
            column_name = next(self._parse_data_values(col_index, mcnl + 1, col_coords, cn_file_handle)).rstrip()

            if column_name in query_column_names_set:
                matching_column_dict[column_name] = col_index

        unmatched_column_names = query_column_names_set - set(matching_column_dict.keys())
        if len(unmatched_column_names) > 0:
            raise Exception("The following column name(s) could not be found for {}: {}.".format(self.data_file_path, ", ".join(sorted([x.decode() for x in unmatched_column_names]))))

        # This makes sure the indices are returned in the specified order.
        return [matching_column_dict[column_name] for column_name in query_column_names]

    def get_column_type_from_index(self, column_index):
        return next(self._parse_data_values(column_index, 2, [[0, 1]], self.__file_handles[".ct"])).decode()

    def get_column_names(self):
        column_names = []
        with open(self.data_file_path + ".cn", 'rb') as the_file:
            for line in the_file:
                column_names.append(line.rstrip())

        return column_names

    def close(self):
        for handle in self.__file_handles.values():
            handle.close()

    ##############################################
    # Non-public functions
    ##############################################

    def _get_column_type(self, column_name):
        #TODO: Remove this documentation?
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

        return self.get_column_type_from_index(self.get_column_indices([column_name])[0])

    def _generate_row_chunks(self, num_processes):
        rows_per_chunk = math.ceil(self.__stats[".nrow"] / num_processes)

        row_indices = []

        for row_index in range(self.__stats[".nrow"]):
            row_indices.append(row_index)

            if len(row_indices) == rows_per_chunk:
                yield row_indices
                row_indices = []

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
        if self.__decompressor:
            # Parse and then decompress the entire row.
            row = next(self._parse_data_values(row_index, self.__stats[".ll"], [[0, self.__stats[".ll"]]], self.__file_handles[""]))
            row = self.__decompressor.decompress(row)

            # Retrieve the desired columns from that row.
            return [x for x in self._parse_data_values(0, 0, column_coords, row)]
        else:
            return list(self._parse_data_values(row_index, self.__stats[".ll"], column_coords, self.__file_handles[""]))

    def _parse_row_dict(self, row_index, column_name_coords_dict):
        row_dict = {}

        if self.__decompressor:
            # Parse and then decompress the entire row.
            row = self.__decompressor.decompress(next(self._parse_data_values(row_index, self.__stats[".ll"], [[0, self.__stats[".ll"]]], self.__file_handles[""])))

            for column_name, column_coords in column_name_coords_dict.items():
                row_dict[column_name] = next(self._parse_data_values(0, 0, column_coords, row)).rstrip(b" ")
        else:
            for column_name, column_coords in column_name_coords_dict.items():
                row_dict[column_name] = next(self._parse_data_values(row_index, self.__stats[".ll"], column_coords, self.__file_handles[""])).rstrip(b" ")

        return row_dict

#####################################################
# Class functions
#####################################################

def _process_rows(data_file_path, fltr, row_indices):
    is_index = os.path.exists(f"{data_file_path}.idx")
    parser = Parser(data_file_path, is_index=is_index)

    filter_column_names = list(fltr.get_column_name_set())
    filter_column_indices = parser.get_column_indices(filter_column_names)

    filter_column_coords_dict = {filter_column_names[i]: parser._parse_data_coords([filter_column_indices[i]]) for i in range(len(filter_column_indices))}

    passing_row_indices = []
    for row_index in row_indices:
        row_value_dict = parser._parse_row_dict(row_index, filter_column_coords_dict)
        if fltr.passes(parser, row_value_dict):
            passing_row_indices.append(row_index)

    parser.close()

    return passing_row_indices

#####################################################
# Classes used for filtering.
#####################################################

"""
This is a base class for all filters used in this package. It provides common class functions.
"""
class BaseFilter:
    def get_column_name_set(self):
        raise Exception("This function must be implemented by classes that inherit this class.")

    def check_types(self, parser, column_type_dict):
        pass

    def passes(self, parser, row_value_dict):
        raise Exception("This function must be implemented by classes that inherit this class.")

"""
This class is used to indicate when no filtering should be performed.
"""
class KeepAll(BaseFilter):
    def get_column_name_set(self):
        return set()

    def passes(self, parser, row_value_dict):
        return True

class InFilter(BaseFilter):
    """
    This class is used to construct a filter that identifies rows container any of a list of values in a particular column. It can be used on any column type.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        values_list (list): A non-empty list of strings that indicates which values should be matched in the specified column. All values will be evaluated as strings. Values of other types will be ignored. Missing values (empty string or 'NA') are allowed.
        negate (bool): Whether to use negation. In other words, this will match rows that do not contain the specified values in the specified column. Default: False.
    """
    def __init__(self, column_name, values_list, negate=False):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        if not values_list or len([x for x in values_list if type(x) == str]) == 0:
            raise Exception("The values_list argument must contain at least one string value.")

        self.__column_name = column_name.encode()
        self.__values_set = set([x.encode() for x in values_list if type(x) == str])
        self.__negate = negate

    def get_column_name_set(self):
        return set([self.__column_name])

    def passes(self, parser, row_value_dict):
        #value = parser.get_cell_value(row_index, column_coords_dict[self.__column_name])
        value = row_value_dict[self.__column_name]

        if self.__negate and value not in self.__values_set:
            return True
        elif not self.__negate and value in self.__values_set:
            return True
        return False

class LikeFilter(BaseFilter):
    """
    This class is used to construct regular-expression based filters for querying any column type in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        regular_expression (str): Values in the specified column will be compared against this regular expression. Matches will be retained. Can be a raw string. May not be an empty string. Missing values will not be evaluated.
        negate (bool): Whether to use negation. In other words, this will match rows that do not contain the specified values in the specified column. Default: False.
    """
    def __init__(self, column_name, regular_expression, negate=False):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        if type(regular_expression) != str:
            raise Exception("The regular expression must be a string.")

        self.__column_name = column_name.encode()
        self.__regular_expression = re.compile(regular_expression)
        self.__negate = negate

    def get_column_name_set(self):
        return set([self.__column_name])

    def passes(self, parser, row_value_dict):
        #value = parser.get_cell_value(row_index, column_coords_dict[self.__column_name])
        value = row_value_dict[self.__column_name]

        if is_missing_value(value):
            return False

        if self.__negate and not self.__regular_expression.search(value.decode()):
            return True
        elif not self.__negate and self.__regular_expression.search(value.decode()):
            return True
        return False

class NumericFilter(BaseFilter):
    """
    This class is used to construct filters for querying based on a numeric column in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        operator (operator): The comparison operator to use.
        query_value (float or int): A numeric value to use for comparison.
    """
    def __init__(self, column_name, oper, query_value):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        q_type = type(query_value)
        if not q_type == float and not q_type == int:
            raise Exception("The query_value value must be a float or an integer.")

        self.__column_name = column_name.encode()
        self.__operator = oper
        self.__query_value = query_value

    def get_column_name_set(self):
        return set([self.__column_name])

    def check_types(self, parser, column_type_dict):
        if column_type_dict[self.__column_name] == "c":
            raise Exception(f"A numeric filter may only be used with numeric columns, but {self.__column_name.decode()} is not numeric (float or integer).")

    def passes(self, parser, row_value_dict):
        #value = parser.get_cell_value(row_index, column_coords_dict[self.__column_name])
        value = row_value_dict[self.__column_name]

        if is_missing_value(value):
            return False

        return self.__operator(fastnumbers.float(value), self.__query_value)

class AndFilter(BaseFilter):
    """
    This class is used to construct a filter with multiple sub-filters that must all evaluate to True.

    Args:
        *args (list): A variable number of filters that should be evaluated. At least two filters must be specified.
    """
    def __init__(self, *args):
        if len(args) < 2:
            raise Exception("At least two filters must be passed to this function.")

        self.__filters = args

    def get_column_name_set(self):
        column_name_set = set()

        for fltr in self.__filters:
            column_name_set = column_name_set | fltr.get_column_name_set()

        return column_name_set

    def check_types(self, parser, column_type_dict):
        for fltr in self.__filters:
            fltr.check_types(parser, column_type_dict)

    def passes(self, parser, row_value_dict):
        for fltr in self.__filters:
            if not fltr.passes(parser, row_value_dict):
                return False

        return True

class OrFilter(BaseFilter):
    """
    This class is used to construct a filter with multiple sub-filters. At least one must evaluate to True.

    Args:
        *args (list): A variable number of filters that should be evaluated. At least two filters must be specified.
    """
    def __init__(self, *args):
        if len(args) < 2:
            raise Exception("At least two filters must be passed to this function.")

        self.__filters = args

    def get_column_name_set(self):
        column_name_set = set()

        for fltr in self.__filters:
            column_name_set = column_name_set | fltr.get_column_name_set()

        return column_name_set

    def check_types(self, parser, column_type_dict):
        for fltr in self.__filters:
            fltr.check_types(parser, column_type_dict)

    def passes(self, parser, row_value_dict):
        for fltr in self.__filters:
            if fltr.passes(parser, row_value_dict):
                return True

        return False

import atexit
import fastnumbers
from Helper import *
from itertools import chain
from joblib import Parallel, delayed
import operator
import os
import re

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
        num_rows (int): The number of rows in the dataset.
        num_cols (int): The number of columns in the dataset.
    """

    def __init__(self, data_file_path):
        #TODO: Remove this when everything is in a single file.
        #for file_extension in ("", ".cc", ".cn", ".ct", ".ll", ".mccl", ".mcnl", ".mctl", ".ncol", ".nrow"):
        #    file_path = f"{data_file_path}{file_extension}"
        #    if not os.path.exists(file_path):
        #        raise Exception(f"A file named {file_path} does not exist.")

        self.data_file_path = data_file_path
        self.num_rows = read_int_from_file(self.data_file_path, ".nrow")
        self.num_columns = read_int_from_file(self.data_file_path, ".ncol")

        self.__data_handle = open_read_file(self.data_file_path)
        self.__ll = read_int_from_file(self.data_file_path, ".ll")
        self.__cc_handle = open_read_file(self.data_file_path, ".cc")
        self.__mccl = read_int_from_file(self.data_file_path, ".mccl")
        self.__cn_handle = open_read_file(self.data_file_path, ".cn")
        self.__mcnl = read_int_from_file(self.data_file_path, ".mcnl")
        self.__ct_handle = open_read_file(self.data_file_path, ".ct")
        #self.__mctl = read_int_from_file(self.data_file_path, ".mctl")

        atexit.register(self.close)

    def query_and_save(self, fltr, select_columns, out_file_path, out_file_type="tsv", num_processes=1, lines_per_chunk=1000):
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

        # Loop through the rows in parallel and find matching row indices.
        keep_row_indices = sorted(chain.from_iterable(Parallel(n_jobs=num_processes)(delayed(process_rows)(self.data_file_path, fltr, row_indices) for row_indices in self.__generate_row_chunks(lines_per_chunk))))

        # By default, select all columns.
        if not select_columns or len(select_columns) == 0:
            column_names = self.__get_column_names()
            select_column_indices = range(len(column_names))
        else:
            column_names = [x.encode() for x in select_columns]
            select_column_indices = self.get_column_indices(column_names)

        # Get the coords for each column to select
        select_column_coords = self.__parse_data_coords(select_column_indices)

        # Write output file (in chunks)
        with open(out_file_path, 'wb') as out_file:
            # Header line
            out_file.write(b"\t".join(column_names) + b"\n")

            out_lines = []
            for row_index in keep_row_indices:
                out_lines.append(b"\t".join([x.rstrip() for x in self.__parse_data_values(row_index, self.__ll, select_column_coords, self.__data_handle)]))

                if len(out_lines) % lines_per_chunk == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

    def get_cell_value(self, row_index, column_index):
        return next(self.__parse_data_values(row_index, self.__ll,
            self.__parse_data_coords([column_index]), self.__data_handle)).rstrip()

    def get_column_type(self, column_name):
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

        return self.get_column_type_from_index(self.get_column_indices([column_name.encode()])[0])

    def get_column_indices(self, query_column_names):
        query_column_names_set = set(query_column_names)
        col_coords = [[0, self.__mcnl]]
        matching_column_dict = {}

        for col_index in range(self.num_columns):
            column_name = next(self.__parse_data_values(col_index, self.__mcnl + 1, col_coords, self.__cn_handle)).rstrip()

            if column_name in query_column_names_set:
                matching_column_dict[column_name] = col_index

        unmatched_column_names = query_column_names_set - set(matching_column_dict.keys())
        if len(unmatched_column_names) > 0:
            raise Exception("The following column name(s) could not be found for {}: {}.".format(self.data_file_path, ", ".join(sorted([x.decode() for x in unmatched_column_names]))))

        return [matching_column_dict[column_name] for column_name in query_column_names]

    def get_column_type_from_index(self, column_index):
        #return next(self.__parse_data_values(column_index, self.__mctl + 1, [[0, self.__mctl]], self.__ct_handle)).decode()
        return next(self.__parse_data_values(column_index, 2, [[0, 1]], self.__ct_handle)).decode()

    ##############################################
    # Private functions.
    ##############################################

    def close(self):
        self.__data_handle.close()
        self.__cc_handle.close()
        self.__cn_handle.close()
        self.__ct_handle.close()

    def __get_column_names(self):
        column_names = []
        with open(self.data_file_path + ".cn", 'rb') as the_file:
            for line in the_file:
                column_names.append(line.rstrip())

        return column_names

    def __generate_row_chunks(self, rows_per_chunk):
        row_indices = []
        for row_index in range(self.num_rows):
            row_indices.append(row_index)

            if len(row_indices) == rows_per_chunk:
                yield row_indices
                row_indices = []

        if len(row_indices) > 0:
            yield row_indices

    def __parse_data_coords(self, line_indices):
        data_coords = []
        out_dict = {}

        for index in line_indices:
            start_pos = index * (self.__mccl + 1)
            next_start_pos = start_pos + self.__mccl + 1
            further_next_start_pos = next_start_pos + self.__mccl + 1

            # See if we already have cached the start position.
            if index in out_dict:
                data_start_pos = out_dict[index]
            # If not, retrieve the start position from the cc file and then cache it.
            else:
                data_start_pos = int(self.__cc_handle[start_pos:next_start_pos].rstrip())
                out_dict[index] = data_start_pos

            # See if we already have cached the end position.
            if (index + 1) in out_dict:
                data_end_pos = out_dict[index + 1]
            # If not, retrieve the end position from the cc file and then cache it.
            else:
                data_end_pos = int(self.__cc_handle[next_start_pos:further_next_start_pos].rstrip())
                out_dict[index + 1] = data_end_pos

            data_coords.append([data_start_pos, data_end_pos])

        return data_coords

    def __parse_data_values(self, start_offset, segment_length, data_coords, str_like_object):
        start_pos = start_offset * segment_length

        for coords in data_coords:
            yield str_like_object[(start_pos + coords[0]):(start_pos + coords[1])]

#TODO: Modify this to yield passing row indices
def process_rows(data_file_path, fltr, row_indices):
    parser = Parser(data_file_path)

    filter_column_names = list(fltr.get_column_name_set())
    filter_column_indices = parser.get_column_indices(filter_column_names)
    column_index_dict = {filter_column_names[i]: filter_column_indices[i] for i in range(len(filter_column_indices))}
    column_type_dict = {filter_column_names[i]: parser.get_column_type_from_index(filter_column_indices[i]) for i in range(len(filter_column_indices))}

    passing_row_indices = []
    for row_index in row_indices:
        if fltr.passes(parser, row_index, column_index_dict, column_type_dict):
            passing_row_indices.append(row_index)

    parser.close()

    return passing_row_indices

"""
This is a base class for all filters used in this package. It provides common class functions.
"""
class BaseFilter:
    def get_column_name_set(self):
        raise Exception("This function must be implemented by classes that inherit this class.")

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
        raise Exception("This function must be implemented by classes that inherit this class.")

"""
This class is used to indicate when no filtering should be performed.
"""
class KeepAll(BaseFilter):
    def get_column_name_set(self):
        return set()

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
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

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
        value = parser.get_cell_value(row_index, column_index_dict[self.__column_name])

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

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
        value = parser.get_cell_value(row_index, column_index_dict[self.__column_name])

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

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
        column_type = column_type_dict[self.__column_name]
        if column_type == "c":
            raise Exception(f"A numeric filter may only be used with numeric columns, but {self.__column_name.decode()} is not a float or integer column.")

        value = parser.get_cell_value(row_index, column_index_dict[self.__column_name])

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

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
        for fltr in self.__filters:
            if not fltr.passes(parser, row_index, column_index_dict, column_type_dict):
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

    def passes(self, parser, row_index, column_index_dict, column_type_dict):
        for fltr in self.__filters:
            if fltr.passes(parser, row_index, column_index_dict, column_type_dict):
                return True

        return False

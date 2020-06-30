import atexit
import fastnumbers
from itertools import islice
import operator
import os
import re
from Helper import *
from InFilter import *
from NumericFilter import *
from LikeFilter import *

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
        for file_extension in ("", ".cc", ".cn", ".ct", ".ll", ".mccl", ".mcnl", ".mctl", ".ncol", ".nrow"):
            file_path = f"{data_file_path}{file_extension}"
            if not os.path.exists(file_path):
                raise Exception(f"A file named {file_path} does not exist.")

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
        self.__mctl = read_int_from_file(self.data_file_path, ".mctl")

        atexit.register(self.__cleanup)

    def query_and_save(self, filters, select_columns, out_file_path, out_file_type="tsv", num_processes=1, lines_per_chunk=1000):
        """
        Query the data file using zero or more filters.

        This function accepts filtering criteria, identifies matching rows,
        and saves the output (for select columns) to an output file.

        Args:
            filters (list): A list of filter objects. This list may be empty; if so, no filtering will occur.
            select_columns (list): A list of strings that indicate the names of columns that should be selected. If this is an empty list, all columns will be selected.
            out_file_path(str): A path to a file that will store the output data.
            out_file_type (str): The output file type. Currently, the only supported value is tsv.
        """
        # Get column indices for column names to be filtered.
        filter_column_indices = []
        if len(filters) > 0:
            filter_column_indices = self.__find_column_indices([x.column_name for x in filters])

        # Start with keeping all row indices.
        keep_row_indices = range(self.num_rows)

        # Find rows that match the filtering criteria.
        for fltr in filters:
            filter_column_index = filter_column_indices.pop(0)
            filter_column_type = self.__get_column_type_from_index(filter_column_index)

            if type(fltr) is InFilter:
                keep_row_indices = self.__filter_rows_in(keep_row_indices, filter_column_index, fltr.values_set, fltr.negate)
            elif type(fltr) is NumericFilter:
                if filter_column_type == "c":
                    raise Exception(f"A NumericFilter may only be used with numeric columns, but {fltr.column_name.decode()} is not a float or integer column.")

                keep_row_indices = self.__filter_rows_numeric(keep_row_indices, filter_column_index, fltr.operator, fltr.query_value)
            elif type(fltr) is LikeFilter:
                keep_row_indices = self.__filter_rows_regex(keep_row_indices, filter_column_index, fltr.regular_expression, fltr.negate)
            else:
                raise Exception(f"An object of type {type(fltr)} may not be used as a filter.")

        # By default, select all columns.
        if not select_columns or len(select_columns) == 0:
            column_names = self.__get_column_names()
            select_column_indices = range(len(column_names))
        else:
            column_names = [x.encode() for x in select_columns]
            select_column_indices = self.__find_column_indices(column_names)

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

        return self.__get_column_type_from_index(self.__find_column_indices([column_name.encode()])[0])

    ##############################################
    # Private functions.
    ##############################################

    def __cleanup(self):
        self.__data_handle.close()
        self.__cc_handle.close()
        self.__cn_handle.close()
        self.__ct_handle.close()

    def __get_column_type_from_index(self, column_index):
        return next(self.__parse_data_values(column_index, self.__mctl + 1, [[0, self.__mctl]], self.__ct_handle)).decode()

    def __filter_rows_in(self, row_indices, column_index, values_set, negate):
        query_col_coords = self.__parse_data_coords([column_index])

        for row_index in row_indices:
            value = next(self.__parse_data_values(row_index, self.__ll, query_col_coords, self.__data_handle)).rstrip()

            if negate and value not in values_set:
                yield row_index
            elif not negate and value in values_set:
                yield row_index

    def __filter_rows_numeric(self, row_indices, column_index, oper, query_value):
        query_col_coords = self.__parse_data_coords([column_index])

        for row_index in row_indices:
            value = next(self.__parse_data_values(row_index, self.__ll, query_col_coords, self.__data_handle)).rstrip()
            if is_missing_value(value):
                continue

            if oper(fastnumbers.float(value), query_value):
                yield row_index

    def __filter_rows_regex(self, row_indices, column_index, regular_expression, negate):
        query_col_coords = self.__parse_data_coords([column_index])

        for row_index in row_indices:
            value = next(self.__parse_data_values(row_index, self.__ll, query_col_coords, self.__data_handle)).rstrip()
            if is_missing_value(value):
                continue

            if negate and not regular_expression.search(value.decode()):
                yield row_index
            elif not negate and regular_expression.search(value.decode()):
                yield row_index

    def __find_column_indices(self, query_column_names):
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

    def __get_column_names(self):
        column_names = []
        with open(self.data_file_path + ".cn", 'rb') as the_file:
            for line in the_file:
                column_names.append(line.rstrip())

        return column_names

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

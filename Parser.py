import fastnumbers
from itertools import islice
import operator
import os
from Helper import *
from DiscreteFilter import *
from NumericFilter import *

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
    """

    def __init__(self, data_file_path):
        for file_extension in ("", ".cc", ".cn", ".ct", ".ll", ".mccl", ".mcnl", ".mctl", ".ncol", ".nrow"):
            file_path = f"{data_file_path}{file_extension}"
            if not os.path.exists(file_path):
                raise Exception(f"A file named {file_path} does not exist.")

        self.data_file_path = data_file_path
        self.__num_rows = None
        self.__num_columns = None
        self.__num_datapoints = None

    @property
    def num_rows(self) -> int:
        """Return the number of rows in the dataset."""
        if self.__num_rows == None:
            self.__num_rows = read_int_from_file(self.data_file_path, ".nrow")
        return self.__num_rows

    @property
    def num_columns(self) -> int:
        """Return the number of columns in the dataset."""
        if self.__num_columns == None:
            self.__num_columns = read_int_from_file(self.data_file_path, ".ncol")
        return self.__num_columns

    def query_and_save(self, filters, select_columns, out_file_path, out_file_type="tsv"):
        """
        Query the data file using zero or more filters.

        This function accepts filtering criteria, identifies matching rows,
        and saves the output (for select columns) to an output file.

        Args:
            filters (list): A list of DiscreteFilter and/or NumericFilter objects. This list may be empty; if so, no filtering will occur.
            select_columns (list): A list of strings that indicate the names of columns that should be selected. If this is an empty list, all columns will be selected.
            out_file_path(str): A path to a file that will store the output data.
            out_file_type (str): The output file type. Currently, the only supported value is tsv.
        """
        # Prepare to parse data.
        data_handle = open_read_file(self.data_file_path)
        ll = read_int_from_file(self.data_file_path, ".ll")
        cc_handle = open_read_file(self.data_file_path, ".cc")
        mccl = read_int_from_file(self.data_file_path, ".mccl")
        cn_handle = open_read_file(self.data_file_path, ".cn")
        mcnl = read_int_from_file(self.data_file_path, ".mcnl")

        # Get column indices for column names to be filtered.
        filter_column_indices = []
        if len(filters) > 0:
            filter_column_indices = self.__find_column_indices([x.column_name for x in filters], cn_handle, mcnl)

        # Start with keeping all row indices.
        keep_row_indices = range(self.num_rows)

        # Find rows that match the filtering criteria.
        for fltr in filters:
            filter_column_index = filter_column_indices.pop(0)

            if type(fltr) is DiscreteFilter:
                keep_row_indices = self.__filter_rows_discrete(keep_row_indices, filter_column_index, fltr.values_set, data_handle, ll, cc_handle, mccl, cn_handle, mcnl)
            elif type(fltr) is NumericFilter:
                keep_row_indices = self.__filter_rows_numeric(keep_row_indices, filter_column_index, fltr.operator, fltr.query_value, data_handle, ll, cc_handle, mccl, cn_handle, mcnl)
            else:
                raise Exception("An object of type {} may not be used as a filter.".format(type(fltr)))

        # By default, select all columns.
        if not select_columns or len(select_columns) == 0:
            column_names = self.__get_column_names()
            select_column_indices = range(len(column_names))
        else:
            column_names = [x.encode() for x in select_columns]
            select_column_indices = self.__find_column_indices(column_names, cn_handle, mcnl)

        # Get the coords for each column to select
        select_column_coords = parse_data_coords(select_column_indices, cc_handle, mccl)

        # Write output file (in chunks)
        with open(out_file_path, 'wb') as out_file:
            # Header line
            out_file.write(b"\t".join(column_names) + b"\n")

            out_lines = []
            chunk_size = 1000

            for row_index in keep_row_indices:
                out_lines.append(b"\t".join([x.rstrip() for x in parse_data_values(row_index, ll, select_column_coords, data_handle)]))

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

        data_handle.close()
        cc_handle.close()

    def get_column_type(self, column_name):
        """
        Find the type of a specified column.

        Args:
            column_name (str): The column name.
        Returns:
            A character indicating the data type for the specified column.
            The character will be one of the following:
                * d (discrete or categorical)
                * f (float)
                * i (integer)
        """
        return self.__get_column_type_info(column_name, 0)

    def does_column_have_unique_values(self, column_name):
        """
        Indicate whether a specified column contains all unique values.

        Args:
            column_name (str): The column name.
        Returns:
            A Boolean value indicating whether all the values are unique.
        """
        return self.__get_column_type_info(column_name, 1) == "u"

    ##############################################
    # Private functions.
    ##############################################

    def __filter_rows_discrete(self, row_indices, column_index, values_set, data_handle, ll, cc_handle, mccl, cn_handle, mcnl):
        query_col_coords = parse_data_coords([column_index], cc_handle, mccl)

        for row_index in row_indices:
            if next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip() in values_set:
                yield row_index

    def __filter_rows_numeric(self, row_indices, column_index, oper, query_value, data_handle, ll, cc_handle, mccl, cn_handle, mcnl):
        query_col_coords = parse_data_coords([column_index], cc_handle, mccl)

        for row_index in row_indices:
            value = next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip()
            if is_missing_value(value):
                continue

            if oper(fastnumbers.float(value), query_value):
                yield row_index

    def __find_column_index(self, query_column_name, cn_handle, mcnl):
        col_coords = [[0, mcnl]]

        for col_index in range(self.num_columns):
            column_name = next(parse_data_values(col_index, mcnl + 1, col_coords, cn_handle)).rstrip()

            if query_column_name == column_name:
                return col_index

        raise Exception(f"A column named {query_column_name.decode()} could not be found for {self.data_file_path}.")

    def __find_column_indices(self, query_column_names, cn_handle, mcnl):
        query_column_names = set(query_column_names)
        col_coords = [[0, mcnl]]
        matching_column_names = set()
        matching_column_indices = []

        for col_index in range(self.num_columns):
            column_name = next(parse_data_values(col_index, mcnl + 1, col_coords, cn_handle)).rstrip()

            if column_name in query_column_names:
                matching_column_names.add(column_name)
                matching_column_indices.append(col_index)

        unmatched_column_names = query_column_names - matching_column_names
        if len(unmatched_column_names) > 0:
            raise Exception(f"The following column name(s) could not be found for {self.data_file_path}: {sorted(list(unmatched_column_names))}.")

        return matching_column_indices

    def __get_column_names(self):
        return [x.rstrip(b" ") for x in read_strings_from_file(self.data_file_path, ".cn")]

    def __get_column_type_info(self, column_name, parse_index):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        cn_handle = open_read_file(self.data_file_path, ".cn")
        mcnl = read_int_from_file(self.data_file_path, ".mcnl")
        ct_handle = open_read_file(self.data_file_path, ".ct")
        mctl = read_int_from_file(self.data_file_path, ".mctl")

        column_index = self.__find_column_index(column_name.encode(), cn_handle, mcnl)
        column_type = next(parse_data_values(column_index, mctl + 1, [[0, mctl]], ct_handle))

        ct_handle.close()
        cn_handle.close()

        return column_type.decode()[parse_index]

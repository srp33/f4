import fastnumbers
from itertools import islice
import operator
import os
from DataSetHelper import *
from DiscreteFilter import *
from NumericFilter import *

class DataSetParser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Atrributes:
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

        # Find rows that match the filtering criteria.
        keep_row_indices = range(self.num_rows)
        for fltr in filters:
            if type(fltr) is DiscreteFilter:
                keep_row_indices = self.__filter_rows_discrete(keep_row_indices, fltr, data_handle, ll, cc_handle, mccl, cn_handle, mcnl)
            elif type(fltr) is NumericFilter:
                keep_row_indices = self.__filter_rows_numeric(keep_row_indices, fltr, data_handle, ll, cc_handle, mccl, cn_handle, mcnl)
            else:
                raise Exception("An object of type {} may not be used as a filter.".format(type(fltr)))

        # Read all column names.
        column_names = self.__get_column_names()

        # By default, select all columns.
        if not select_columns or len(select_columns) == 0:
            select_column_indices = range(len(column_names))
        else:
            # This enables faster lookups.
            select_columns_set = set([x.encode() for x in select_columns])

            # Make sure select_columns are valid.
            nonexistent_columns = select_columns_set - set(column_names)
            if len(nonexistent_columns) > 0:
                raise Exception("Invalid select_columns value(s): {}".format(",".join(sorted(list(nonexistent_columns)))))

            # Pair column names with positions.
            select_columns_dict = { column_name: i for i, column_name in enumerate(column_names) if column_name in select_columns_set }

            # Specific the indices and names of columns we want to select (in the user-specified order).
            select_column_indices = [select_columns_dict[column_name.encode()] for column_name in select_columns]
            column_names = [x.encode() for x in select_columns]

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
        return self.__get_column_type_info(column_name, 0)

    def does_column_have_unique_values(self, column_name):
        return self.__get_column_type_info(column_name, 1) == "u"

    ##############################################
    # Private functions.
    ##############################################

    def __filter_rows_discrete(self, row_indices, the_filter, data_handle, ll, cc_handle, mccl, cn_handle, mcnl):
        column_index = self.__find_column_index(the_filter.column_name, cn_handle, mcnl)
        query_col_coords = parse_data_coords([column_index], cc_handle, mccl)

        for row_index in row_indices:
            if next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip() in the_filter.values_set:
                yield row_index

    def __filter_rows_numeric(self, row_indices, the_filter, data_handle, ll, cc_handle, mccl, cn_handle, mcnl):
        column_index = self.__find_column_index(the_filter.column_name, cn_handle, mcnl)
        query_col_coords = parse_data_coords([column_index], cc_handle, mccl)

        for row_index in row_indices:
            value = next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip()
            if is_missing_value(value):
                continue

            if the_filter.operator(fastnumbers.float(value), the_filter.query_value):
                yield row_index

    def __find_column_index(self, query_column_name, cn_handle, mcnl):
        col_coords = [[0, mcnl]]

        for col_index in range(self.num_columns):
            column_name = next(parse_data_values(col_index, mcnl + 1, col_coords, cn_handle)).rstrip()

            if query_column_name == column_name:
                return col_index

        raise Exception(f"A column named {query_column_name.decode()} could not be found for {self.data_file_path}.")

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

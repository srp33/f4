import fastnumbers
from itertools import islice
import operator
import os
from DataSetHelper import *
from DiscreteFilter import *
from NumericFilter import *

class DataSetParser:
    def __init__(self, data_file_path):
        self.data_file_path = data_file_path

        self.__num_rows = None
        self.__num_columns = None
        self.__total_datapoints = None

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

    @property
    def total_datapoints(self):
        """Return the total number of data points in the dataset."""
        if self.__total_datapoints == None:
            self.__total_datapoints = self.num_rows * self.num_columns
        return self.__total_datapoints

    def query_and_save(self, discrete_filters, numeric_filters, select_columns, out_file_path, out_file_type="tsv"):
        """
        Query the data file using zero or more filters.

        This function accepts filtering criteria, identifies matching rows and columns,
        and saves the output to a tab-separated-value (tsv) file. The input arguments must be
        of type DiscreteFilter or NumericFilter, respectively. Currently, the only
        supported output file type is tsv.

        Parameters
        ----------
        discrete_filters: list
            A list of DiscreteFilter objects. This list may be empty.
        numeric_filters: list
            A list of NumericFilter objects. This list may be empty.
        select_columns: list
            A list of strings that indicate the names of columns that should be selected.
            If this an empty list, all columns will be selected.
        out_file_path: str
            A path to a file that will store the output data.
        out_file_type: str
            The output file type. Currently, the only supported value is tsv.
        """
        # Prepare to parse data.
        data_handle = open_read_file(self.data_file_path)
        ll = read_int_from_file(self.data_file_path, ".ll")
        cc_handle = open_read_file(self.data_file_path, ".cc")
        mccl = read_int_from_file(self.data_file_path, ".mccl")

        # Find rows that match discrete filtering criteria.
        keep_row_indices = range(self.num_rows)
        for df in discrete_filters:
            keep_row_indices = self.filter_rows_discrete(keep_row_indices, df, data_handle, cc_handle, mccl, ll)

        # Find rows that match numeric filtering criteria.
        num_operator_dict = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq, "!=": operator.ne}
        for nf in numeric_filters:
            keep_row_indices = self.filter_rows_numeric(keep_row_indices, nf, num_operator_dict, data_handle, cc_handle, mccl, ll)

        # Read all column names.
        column_names = get_column_names(self.data_file_path)

        # By default, select all columns.
        if not select_columns or len(select_columns) == 0:
            select_column_indices = range(len(column_names))
        else:
            # This enables faster lookups.
            select_columns_set = set(select_columns)

            # Make sure select_columns are valid.
            nonexistent_columns = select_columns_set - set(column_names)
            if len(nonexistent_columns) > 0:
                raise Exception("Invalid select_columns value(s): {}".format(",".join(sorted(list(nonexistent_columns)))))

            # Pair column names with positions.
            select_columns_dict = { column_name: i for i, column_name in enumerate(column_names) if column_name in select_columns_set }

            # Specific the indices and names of columns we want to select (in the user-specified order).
            select_column_indices = [select_columns_dict[column_name] for column_name in select_columns]
            column_names = select_columns

        # Get the coords for each column to select
        select_column_coords = list(parse_data_coords(select_column_indices, cc_handle, mccl))

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

    ########################################################################
    # Treat these as private functions.
    ########################################################################

    def filter_rows_discrete(self, row_indices, the_filter, data_handle, cc_handle, mccl, ll):
        query_col_coords = list(parse_data_coords([the_filter.column_index], cc_handle, mccl))

        for row_index in row_indices:
            if next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip() in the_filter.values_set:
                yield row_index

    def filter_rows_numeric(self, row_indices, the_filter, operator_dict, data_handle, cc_handle, mccl, ll):
        if the_filter.operator not in operator_dict:
            raise Exception("Invalid operator: " + oper)

        query_col_coords = list(parse_data_coords([the_filter.column_index], cc_handle, mccl))

        for row_index in row_indices:
            value = next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip()
            if value == b"" or value == b"NA": # Is missing
                continue

            # See https://stackoverflow.com/questions/18591778/how-to-pass-an-operator-to-a-python-function
            if operator_dict[the_filter.operator](fastnumbers.float(value), the_filter.query_value):
                yield row_index

import f4py
import fastnumbers
from joblib import Parallel, delayed
import operator
import re

"""
This is a base class for all filters used in this package. It provides common class functions.
"""
class BaseFilter:
    def get_column_name_set(self):
        raise Exception("This function must be implemented by classes that inherit this class.")

    def check_types(self, column_index_dict, column_type_dict):
        pass

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        with f4py.Parser(data_file_path, fixed_file_extensions=[""], stats_file_extensions=[".ll"]) as parser:
            line_length = parser.get_stat(".ll")
            coords = column_coords_dict[column_index_dict[self.column_name]]
            data_file_handle = parser.get_file_handle("")

            passing_row_indices = set()

            for i in row_indices:
                if self.passes(parser._parse_row_value(i, coords, line_length, data_file_handle)):
                    passing_row_indices.add(i)

            return passing_row_indices

    def passes(self, value):
        raise Exception("This function must be implemented by classes that inherit this class.")

"""
This class is used to indicate when no filtering should be performed.
"""
class NoFilter(BaseFilter):
    def get_column_name_set(self):
        return set()

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return row_indices

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        return set(range(end_index))

class __SimpleBaseFilter(BaseFilter):
    def __init__(self, column_name, value):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        self.column_name = column_name.encode()
        self.value = value

    def get_column_name_set(self):
        return set([self.column_name])

#    def __str__(self):
#        return f"{type(self).__name__}____{self.column_name.decode()}____{self.value}"

class StringEqualsFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        if not value or type(value) != str:
            raise Exception("The value argument must be a string.")

        super().__init__(column_name, value.encode())

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.column_name.decode())
        index_column_type = column_type_dict[column_index_dict[self.column_name]]

        if index_column_type == "u":
            return f4py.IdentifierIndexer().filter(index_file_path, self, end_index, num_processes)
        else:
            return f4py.CategoricalIndexer().filter(index_file_path, self, end_index, num_processes)

    def passes(self, value):
        return value == self.value

class StringNotEqualsFilter(StringEqualsFilter):
    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        with f4py.Parser(data_file_path, fixed_file_extensions=[""], stats_file_extensions=[".nrow"]) as parser:
            num_rows = parser.get_num_rows()

        matching_indices = super().filter_indexed_column_values(data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)
        return set(range(num_rows)) - matching_indices

    def passes(self, value):
        return value != self.value

class __StringBasicFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        if not value or type(value) != str:
            raise Exception("The value argument must be a string.")

        super().__init__(column_name, value.encode())

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.column_name.decode())

        return f4py.CategoricalIndexer().filter(index_file_path, self, end_index, num_processes)

class StringGreaterThanOrEqualsFilter(__StringBasicFilter):
    def passes(self, value):
        return value >= self.value

class StringGreaterThanFilter(__StringBasicFilter):
    def passes(self, value):
        return value > self.value

class StringLessThanOrEqualsFilter(__StringBasicFilter):
    def passes(self, value):
        return value <= self.value

class StringLessThanFilter(__StringBasicFilter):
    def passes(self, value):
        return value < self.value

#class InFilter(__SimpleBaseFilter):
#    """
#    This class is used to construct a filter that identifies rows with any of a list of values in a particular column. It can be used on any column type.
#
#    Args:
#        column_name (str): The name of a column that should be evaluated. May not be an empty string.
#        values_list (list): A non-empty list of strings that indicates which values should be matched in the specified column. All values will be evaluated as strings. Values of other types will be ignored. Missing values (empty string or 'NA') are allowed.
#    """
#    def __init__(self, column_name, values_list):
#        super().__init__(column_name)
#
#        if not values_list or len([x for x in values_list if type(x) == str]) == 0:
#            raise Exception("The values_list argument must contain at least one string value.")
#
#        self._values_set = set([x.encode() for x in values_list if type(x) == str])
#
#    def passes(self, value):
#        return value in self._values_set

#class NotInFilter(InFilter):
#    """
#    This class is used to construct a filter that identifies rows without any of a list of values in a particular column. It can be used on any column type.
#
#    Args:
#        column_name (str): The name of a column that should be evaluated. May not be an empty string.
#        values_list (list): A non-empty list of strings that indicates which values should be matched in the specified column. All values will be evaluated as strings. Values of other types will be ignored. Missing values (empty string or 'NA') are allowed.
#    """
#    def __init__(self, column_name, values_list):
#        super().__init__(column_name, values_list)
#
#    def passes(self, value):
#        return value not in self._values_set

class StartsWithFilter(__StringBasicFilter):
    """
    This class is used to check whether values start with a particular string.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        query_string (str): The string to check for. Matches will be retained. May not be an empty string. Missing values will not be evaluated.
    """
    def passes(self, value):
        return value.startswith(self.value)

class EndsWithFilter(__StringBasicFilter):
    """
    This class is used to check whether values end with a particular string.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        query_string (str): The string to check for. Matches will be retained. May not be an empty string. Missing values will not be evaluated.
    """
    def passes(self, value):
        return value.endswith(self.value)

class LikeFilter(__StringBasicFilter):
    """
    This class is used to construct regular-expression based filters for querying any column type in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        regular_expression (str): Values in the specified column will be compared against this regular expression. Matches will be retained. Can be a raw string. May not be an empty string. Missing values will not be evaluated.
    """
    def __init__(self, column_name, regular_expression):
        super().__init__(column_name, regular_expression)

        self.value = re.compile(self.value.decode())

    def passes(self, value):
        return self.value.search(value.decode())

class NotLikeFilter(LikeFilter):
    """
    This class is used to construct regular-expression based filters for querying any column type.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        regular_expression (str): Values in the specified column will be compared against this regular expression. Matches will be retained. Can be a raw string. May not be an empty string. Missing values will not be evaluated.
    """
    def passes(self, value):
        return not self.value.search(value.decode())

class __NumericFilter(__SimpleBaseFilter):
    def __init__(self, column_name, oper, query_value):
        super().__init__(column_name, query_value)

        self.operator = oper

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        if end_index == 0:
            return set()

        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.column_name.decode())

        with f4py.IndexHelper._get_index_parser(index_file_path) as index_parser:
            line_length = index_parser.get_stat(".ll")
            coords = index_parser._parse_data_coords([0, 1])
            file_handle = index_parser.get_file_handle("")

            if self.operator == operator.eq:
                return f4py.IndexHelper._find_row_indices_for_range(index_parser, compression_level, coords[0], coords[1], self.value, self.value, self.get_conversion_function(), end_index, num_processes)
            else:
                if self.operator == operator.ne:
                    lower_position, upper_position = f4py.IndexHelper._find_bounds_for_range(index_parser, compression_level, coords[0], self.value, self.value, self.get_conversion_function(), end_index, num_processes)

                    lower_positions = (0, lower_position)
                    upper_positions = (upper_position, end_index)

                    lower_row_indices = f4py.IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], lower_positions, num_processes)
                    upper_row_indices = f4py.IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], upper_positions, num_processes)

                    return lower_row_indices | upper_row_indices
                else:
                    if self.operator == operator.gt:
                        positions = f4py.IndexHelper._find_positions_g(index_parser, line_length, coords[0], file_handle, self.value, end_index, self.operator, operator.le, self.get_conversion_function())
                    elif self.operator == operator.ge:
                        positions = f4py.IndexHelper._find_positions_g(index_parser, line_length, coords[0], file_handle, self.value, end_index, self.operator, operator.lt, self.get_conversion_function())
                    elif self.operator == operator.lt:
                        positions = f4py.IndexHelper._find_positions_l(index_parser, line_length, coords[0], file_handle, self.value, end_index, self.operator, operator.ge, self.get_conversion_function())
                    elif self.operator == operator.le:
                        positions = f4py.IndexHelper._find_positions_l(index_parser, line_length, coords[0], file_handle, self.value, end_index, self.operator, operator.gt, self.get_conversion_function())

                    return f4py.IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], positions, num_processes)

class FloatFilter(__NumericFilter):
    """
    This class is used to construct filters for querying based on a float column in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        operator (operator): The comparison operator to use.
        query_value (float): A float value to use for comparison.
    """
    def __init__(self, column_name, oper, query_value):
        q_type = type(query_value)
        if q_type != float:
            raise Exception("The query_value value must be a float.")

        super().__init__(column_name, oper, query_value)

    def check_types(self, column_index_dict, column_type_dict):
        if column_type_dict[column_index_dict[self.column_name]] != "f":
            raise Exception(f"A float filter may only be used with float columns, but {self.column_name.decode()} is not a float.")

    def get_conversion_function(self):
        return fastnumbers.fast_float

    def passes(self, value):
        return self.operator(fastnumbers.fast_float(value), self.value)

#    def __str__(self):
#        return f"{type(self).__name__}____{self.column_name.decode()}____{self.operator}____{self.value}"

class IntFilter(__NumericFilter):
    """
    This class is used to construct filters for querying based on an integer column in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        operator (operator): The comparison operator to use.
        query_value (int): An integer value to use for comparison.
    """
    def __init__(self, column_name, oper, query_value):
        q_type = type(query_value)
        if q_type != int:
            raise Exception("The query_value value must be an integer.")

        super().__init__(column_name, oper, query_value)

    def check_types(self, column_index_dict, column_type_dict):
        if column_type_dict[column_index_dict[self.column_name]] != "i":
            raise Exception(f"An integer filter may only be used with integer columns, but {self.column_name.decode()} is not an integer.")

    def get_conversion_function(self):
        return fastnumbers.fast_int

    def passes(self, value):
        return self.operator(fastnumbers.fast_int(value), self.value)

class HeadFilter(BaseFilter):
    def __init__(self, n, select_columns):
        self.n = n
        self.select_columns_set = set([x.encode() for x in select_columns])

    def get_column_name_set(self):
        return self.select_columns_set

    def _get_num_rows(self, data_file_path):
        with f4py.Parser(data_file_path, fixed_file_extensions=[""], stats_file_extensions=[".nrow"]) as parser:
            return parser.get_num_rows()

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return set(range(min(self._get_num_rows(data_file_path), self.n))) & row_indices

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        return set(range(min(self._get_num_rows(data_file_path), self.n)))

class TailFilter(HeadFilter):
    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        num_rows = self._get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows)) & row_indices

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        num_rows = self._get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows))

class __CompositeBaseFilter(BaseFilter):
    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2

    def check_types(self, column_index_dict, column_type_dict):
        self.filter1.check_types(column_index_dict, column_type_dict)
        self.filter2.check_types(column_index_dict, column_type_dict)

    def get_column_name_set(self):
        return self.filter1.get_column_name_set() | self.filter2.get_column_name_set()

#    def get_sub_filters(self):
#        return self.filter1.get_sub_filters() + self.filter2.get_sub_filters()

#    def get_sub_filter_row_indices(self, fltr_results_dict):
#        if len(self.filter1.get_sub_filters()) == 1:
#            row_indices_1 = fltr_results_dict[str(self.filter1)]
#        else:
#            row_indices_1 = self.filter1.filter_indexed_column_values_parallel(fltr_results_dict)
#
#        if len(self.filter2.get_sub_filters()) == 1:
#            row_indices_2 = fltr_results_dict[str(self.filter2)]
#        else:
#            row_indices_2 = self.filter2.filter_indexed_column_values_parallel(fltr_results_dict)
#
#        return row_indices_1, row_indices_2

class AndFilter(__CompositeBaseFilter):
    """
    This class is used to construct a filter with multiple sub-filters that must all evaluate to True.
    Order does matter; filter1 is applied first. Any rows that remain after filter1 has been applied
    will be sent to filter2.

    Args:
        filter1: The first filter to be evaluated.
        filter2: The second filter to be evaluated.
    """
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        row_indices_1 = self.filter1.filter_column_values(data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict)
        return self.filter2.filter_column_values(data_file_path, row_indices_1, column_index_dict, column_type_dict, column_coords_dict)

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        row_indices_1 = self.filter1.filter_indexed_column_values(data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)
        row_indices_2 = self.filter2.filter_indexed_column_values(data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)

        return row_indices_1 & row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 & row_indices_2

class OrFilter(__CompositeBaseFilter):
    """
    This class is used to construct a filter with multiple sub-filters. At least one must evaluate to True.
    Order does matter; filter1 is applied first. Any rows that did not pass after filter1 has been
    applied will be sent to filter2.

    Args:
        *args (list): A variable number of filters that should be evaluated. At least two filters must be specified.
    """
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        row_indices_1 = self.filter1.filter_column_values(data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict)
        row_indices_2 = self.filter2.filter_column_values(data_file_path, row_indices - row_indices_1, column_index_dict, column_type_dict, column_coords_dict)

        return row_indices_1 | row_indices_2

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        row_indices_1 = self.filter1.filter_indexed_column_values(data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)
        row_indices_2 = self.filter2.filter_indexed_column_values(data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)

        return row_indices_1 | row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 | row_indices_2

class __RangeFilter(__CompositeBaseFilter):
    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return AndFilter(self.filter1, self.filter2).filter_column_values(data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict)

class __NumericRangeFilter(__RangeFilter):
    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.filter1.column_name.decode())

        with f4py.IndexHelper._get_index_parser(index_file_path) as index_parser:
            coords = index_parser._parse_data_coords([0, 1])
            return f4py.IndexHelper._find_row_indices_for_range(index_parser, compression_level, coords[0], coords[1], self.filter1.value, self.filter2.value, self.get_conversion_function(), end_index, num_processes)

class FloatRangeFilter(__NumericRangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        if type(lower_bound_value) != float:
            raise Exception("The lower_bound_value must be a float.")
        if type(upper_bound_value) != float:
            raise Exception("The upper_bound_value must be a float.")
        if lower_bound_value > upper_bound_value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

        filter1 = FloatFilter(column_name, operator.ge, lower_bound_value)
        filter2 = FloatFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def get_conversion_function(self):
        return fastnumbers.fast_float

class IntRangeFilter(__NumericRangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        if type(lower_bound_value) != int:
            raise Exception("The lower_bound_value must be a int.")
        if type(upper_bound_value) != int:
            raise Exception("The upper_bound_value must be a int.")
        if lower_bound_value > upper_bound_value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

        filter1 = IntFilter(column_name, operator.ge, lower_bound_value)
        filter2 = IntFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def get_conversion_function(self):
        return fastnumbers.fast_int

class StringRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        if type(lower_bound_value) != str:
            raise Exception("The lower_bound_value must be a string.")
        if type(upper_bound_value) != str:
            raise Exception("The upper_bound_value must be a string.")
        if lower_bound_value > upper_bound_value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

        filter1 = StringGreaterThanOrEqualsFilter(column_name, lower_bound_value)
        filter2 = StringLessThanOrEqualsFilter(column_name, upper_bound_value)

        super().__init__(filter1, filter2)

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.filter1.column_name.decode())
        index_column_type = column_type_dict[column_index_dict[self.filter1.column_name]]

        if index_column_type == "u":
            return f4py.IdentifierIndexer().filter(index_file_path, self, end_index, num_processes)
        else:
            return f4py.CategoricalIndexer().double_filter(index_file_path, self.filter1, self.filter2, end_index, num_processes)

class FunnelFilter(__CompositeBaseFilter):
    pass

class StringIntRangeFunnelFilter(FunnelFilter):
    def __init__(self, string_column, string_value, int_column, lower_bound_value, upper_bound_value):
        if type(string_value) != str:
            raise Exception("The categorical value must be a string.")
        if type(lower_bound_value) != int:
            raise Exception("The lower_bound_value must be an integer.")
        if type(upper_bound_value) != int:
            raise Exception("The upper_bound_value must be an integer.")

        filter1 = StringEqualsFilter(string_column, string_value)
        filter2 = IntFilter(int_column, lower_bound_value, upper_bound_value)

        super().__init__(filter1, filter2)

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        raise Exception("Not implemented")

    def filter_indexed_column_values(self, data_file_path, compression_level, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        #funnel_indexer = f4py.IndexHelper._get_indexer(data_file_path, compression_level, b"", None, self)
        index_name = f4py.IndexHelper._get_index_name([self.filter1.column_name.decode(), self.filter2.column_name.decode()])
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, index_name)
        #funnel_indexer = f4py.FunnelIndexer(index_file_path, compression_level)

#        lower_index_column_type = column_type_dict[column_index_dict[self.filter1.column_name]]
#        upper_index_column_type = column_type_dict[column_index_dict[self.filter2.column_name]]

#        lower_indexer = f4py.IndexHelper._get_indexer(data_file_path, compression_level, self.filter1.column_name, lower_index_column_type, self.filter1)
#        upper_indexer = f4py.IndexHelper._get_indexer(data_file_path, compression_level, self.filter2.column_name, upper_index_column_type, self.filter2)

#        lower_positions = f4py.IndexHelper._find_positions(index_file_path, self.filter1, end_index, f4py.do_nothing)
#        upper_positions = f4py.IndexHelper._find_positions(lower_indexer.index_file_path, self.filter2, end_index, fastnumbers.fast_float)

#        lower_position = max(lower_positions[0], upper_positions[0])
#        upper_position = min(lower_positions[1], upper_positions[1])

#        return(f4py.IndexHelper._find_matching_row_indices(lower_indexer.index_file_path, (lower_position, upper_position)))
        print("got here")
        import sys
        sys.exit()

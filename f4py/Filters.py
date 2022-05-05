import f4py
import fastnumbers
from joblib import Parallel, delayed
import operator
import re

"""
This class is used to indicate that no filtering should be performed.
"""
class NoFilter:
    def check_types(self, column_index_dict, column_type_dict):
        pass

    def get_column_name_set(self):
        return set()

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return row_indices

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        return set(range(end_index))

class __SimpleBaseFilter(NoFilter):
    def __init__(self, column_name, value):
        self.check_argument(column_name, "column_name", str)
        self.column_name = column_name.encode()
        self.value = value

    def check_argument(self, x, argument_name, expected_value_type):
        if x == None:
            raise Exception(f"A value of None was specified for the {argument_name} argument of the {type(self).__name__} class.")

        if type(x) != expected_value_type:
            raise Exception(f"A variable of {expected_value_type.__name__} type is required for the {argument_name} argument of the {type(self).__name__} class, but the type was {type(x).__name__}.")

    def get_column_name_set(self):
        return set([self.column_name])

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

    def get_conversion_function(self):
        return f4py.do_nothing

#    def __str__(self):
#        return f"{type(self).__name__}____{self.column_name.decode()}____{self.value}"

class __OperatorFilter(__SimpleBaseFilter):
    def __init__(self, column_name, oper, value):
        super().__init__(column_name, value)

        self.oper = oper

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.column_name.decode())

        return f4py.IndexHelper._filter_using_operator(index_file_path, compression_level, self, end_index, num_processes)

    def check_column_types(self, column_index_dict, column_type_dict, expected_column_type, expected_column_type_description):
        if column_type_dict[column_index_dict[self.column_name]] != expected_column_type:
            raise Exception(f"A {type(self).__name__} may only be used with {expected_column_type_description} columns, and {self.column_name.decode()} is not a {expected_column_type_description}.")

    def passes(self, value):
        return self.oper(self.get_conversion_function()(value), self.value)

class StringFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self.check_argument(value, "value", str)
        super().__init__(column_name, oper, value.encode())

    def check_types(self, column_index_dict, column_type_dict):
        if column_type_dict[column_index_dict[self.column_name]] != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string.")

class FloatFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self.check_argument(value, "value", float)
        super().__init__(column_name, oper, value)

    def check_types(self, column_index_dict, column_type_dict):
        if column_type_dict[column_index_dict[self.column_name]] != "f":
            raise Exception(f"A float filter may only be used with float columns, but {self.column_name.decode()} is not a float.")

    def get_conversion_function(self):
        return fastnumbers.fast_float

class IntFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self.check_argument(value, "value", int)
        super().__init__(column_name, oper, value)

    def check_types(self, column_index_dict, column_type_dict):
        if column_type_dict[column_index_dict[self.column_name]] != "i":
            raise Exception(f"An integer filter may only be used with integer columns, but {self.column_name.decode()} is not an integer.")

    def get_conversion_function(self):
        return fastnumbers.fast_int

class __NonOperatorStringFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        self.check_argument(value, "value", str)
        super().__init__(column_name, value.encode())

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.column_name.decode())

        with f4py.IndexHelper._get_index_parser(index_file_path) as index_parser:
            line_length = index_parser.get_stat(".ll")
            coords = index_parser._parse_data_coords([0, 1])
            data_file_handle = index_parser.get_file_handle("")

            passing_row_indices = set()

            for i in range(end_index):
                if self.passes(index_parser._parse_row_value(i, coords[0], line_length, data_file_handle)):
                    passing_row_indices.add(fastnumbers.fast_int(index_parser._parse_row_value(i, coords[1], line_length, data_file_handle)))

            return passing_row_indices

class StartsWithFilter(__NonOperatorStringFilter):
    def passes(self, value):
        return value.startswith(self.value)

class EndsWithFilter(__NonOperatorStringFilter):
    def passes(self, value):
        return value.endswith(self.value)

class LikeFilter(__NonOperatorStringFilter):
    def __init__(self, column_name, regular_expression):
        super().__init__(column_name, regular_expression)

        self.value = re.compile(self.value.decode())

    def passes(self, value):
        return self.value.search(value.decode())

class NotLikeFilter(LikeFilter):
    def passes(self, value):
        return not self.value.search(value.decode())

class HeadFilter(NoFilter):
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

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        return set(range(min(self._get_num_rows(data_file_path), self.n)))

class TailFilter(HeadFilter):
    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        num_rows = self._get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows)) & row_indices

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        num_rows = self._get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows))

class __CompositeFilter(NoFilter):
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

class AndFilter(__CompositeFilter):
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

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        row_indices_1 = self.filter1.filter_indexed_column_values(data_file_path, compression_level, end_index, num_processes)
        row_indices_2 = self.filter2.filter_indexed_column_values(data_file_path, compression_level, end_index, num_processes)

        return row_indices_1 & row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 & row_indices_2

class OrFilter(__CompositeFilter):
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

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        row_indices_1 = self.filter1.filter_indexed_column_values(data_file_path, compression_level, end_index, num_processes)
        row_indices_2 = self.filter2.filter_indexed_column_values(data_file_path, compression_level, end_index, num_processes)

        return row_indices_1 | row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 | row_indices_2

class __RangeFilter(__CompositeFilter):
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

        if filter1.value > filter2.value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return AndFilter(self.filter1, self.filter2).filter_column_values(data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict)

    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, self.filter1.column_name.decode())

        with f4py.IndexHelper._get_index_parser(index_file_path) as index_parser:
            coords = index_parser._parse_data_coords([0, 1])
            return f4py.IndexHelper._find_row_indices_for_range(index_parser, compression_level, coords[0], coords[1], self.filter1.value, self.filter2.value, self.get_conversion_function(), end_index, num_processes)

    def get_conversion_function(self):
        return fastnumbers.fast_int

class FloatRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = FloatFilter(column_name, operator.ge, lower_bound_value)
        filter2 = FloatFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def get_conversion_function(self):
        return fastnumbers.fast_float

class IntRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = IntFilter(column_name, operator.ge, lower_bound_value)
        filter2 = IntFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def get_conversion_function(self):
        return fastnumbers.fast_int

class StringRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = StringFilter(column_name, operator.ge, lower_bound_value)
        filter2 = StringFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

#class FunnelFilter(__CompositeFilter):
#    pass
#
#class StringIntRangeFunnelFilter(FunnelFilter):
#    def __init__(self, string_column, string_value, int_column, lower_bound_value, upper_bound_value):
#        if type(string_value) != str:
#            raise Exception("The categorical value must be a string.")
#        if type(lower_bound_value) != int:
#            raise Exception("The lower_bound_value must be an integer.")
#        if type(upper_bound_value) != int:
#            raise Exception("The upper_bound_value must be an integer.")
#
#        filter1 = StringEqualsFilter(string_column, string_value)
#        filter2 = IntFilter(int_column, lower_bound_value, upper_bound_value)
#
#        super().__init__(filter1, filter2)
#
#    def filter_column_values(self, data_file_path, row_indices, column_index_dict, column_type_dict, column_coords_dict):
#        raise Exception("Not implemented")
#
#    def filter_indexed_column_values(self, data_file_path, compression_level, end_index, num_processes):
#        #funnel_indexer = f4py.IndexHelper._get_indexer(data_file_path, compression_level, b"", None, self)
#        index_name = f4py.IndexHelper._get_index_name([self.filter1.column_name.decode(), self.filter2.column_name.decode()])
#        index_file_path = f4py.IndexHelper._get_index_file_path(data_file_path, index_name)
#        #funnel_indexer = f4py.FunnelIndexer(index_file_path, compression_level)
#
##        lower_index_column_type = column_type_dict[column_index_dict[self.filter1.column_name]]
##        upper_index_column_type = column_type_dict[column_index_dict[self.filter2.column_name]]
#
##        lower_indexer = f4py.IndexHelper._get_indexer(data_file_path, compression_level, self.filter1.column_name, lower_index_column_type, self.filter1)
##        upper_indexer = f4py.IndexHelper._get_indexer(data_file_path, compression_level, self.filter2.column_name, upper_index_column_type, self.filter2)
#
##        lower_positions = f4py.IndexHelper._find_positions(index_file_path, self.filter1, end_index, f4py.do_nothing)
##        upper_positions = f4py.IndexHelper._find_positions(lower_indexer.index_file_path, self.filter2, end_index, fastnumbers.fast_float)
#
##        lower_position = max(lower_positions[0], upper_positions[0])
##        upper_position = min(lower_positions[1], upper_positions[1])
#
##        return(f4py.IndexHelper._find_matching_row_indices(lower_indexer.index_file_path, (lower_position, upper_position)))
#        print("got here")
#        import sys
#        sys.exit()

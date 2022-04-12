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

    def filter_column_values(self, parser, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        line_length = parser.get_stat(".ll")
        coords = column_coords_dict[column_index_dict[self.column_name]]
        data_file_handle = parser.get_file_handle("")

        passing_row_indices = set()

        for i in row_indices:
            if self.passes(parser._parse_row_value(i, coords).rstrip()):
                passing_row_indices.add(i)

        return passing_row_indices

    def filter_indexed_column_values(self, parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        index_column_type = column_type_dict[column_index_dict[self.column_name]]

        return f4py.IndexHelper._get_filter_indexer(parser.data_file_path, parser.compression_level, self.column_name, index_column_type, self).filter(self, end_index)

    def passes(self, value):
        raise Exception("This function must be implemented by classes that inherit this class.")

"""
This class is used to indicate when no filtering should be performed.
"""
class NoFilter(BaseFilter):
    def get_column_name_set(self):
        return set()

    def filter_column_values(self, parser, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return row_indices

    def filter_indexed_column_values(self, parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
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

class StringEqualsFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        if not value or type(value) != str:
            raise Exception("The value argument must be a string.")

        super().__init__(column_name, value.encode())

    def passes(self, value):
        return value == self.value

class StringNotEqualsFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        if not value or type(value) != str:
            raise Exception("The value argument must be a string.")

        super().__init__(column_name, value.encode())

    def passes(self, value):
        return value != self.value

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

class StartsWithFilter(__SimpleBaseFilter):
    """
    This class is used to check whether values start with a particular string.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        query_string (str): The string to check for. Matches will be retained. May not be an empty string. Missing values will not be evaluated.
    """
    def __init__(self, column_name, query_string):
        if type(query_string) != str:
            raise Exception("The query string must be a string.")

        super().__init__(column_name, query_string.encode())

    def passes(self, value):
        return value.startswith(self.value)

class EndsWithFilter(__SimpleBaseFilter):
    """
    This class is used to check whether values end with a particular string.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        query_string (str): The string to check for. Matches will be retained. May not be an empty string. Missing values will not be evaluated.
    """
    def __init__(self, column_name, query_string):
        if type(query_string) != str:
            raise Exception("The query string must be a string.")

        super().__init__(column_name, query_string.encode())

    def passes(self, value):
        return value.endswith(self.value)

class LikeFilter(__SimpleBaseFilter):
    """
    This class is used to construct regular-expression based filters for querying any column type in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        regular_expression (str): Values in the specified column will be compared against this regular expression. Matches will be retained. Can be a raw string. May not be an empty string. Missing values will not be evaluated.
    """
    def __init__(self, column_name, regular_expression):
        if type(regular_expression) != str:
            raise Exception("The regular expression must be a string.")

        super().__init__(column_name, re.compile(regular_expression))

    def passes(self, value):
        return self.value.search(value.decode())

class NotLikeFilter(__SimpleBaseFilter):
    """
    This class is used to construct regular-expression based filters for querying any column type.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        regular_expression (str): Values in the specified column will be compared against this regular expression. Matches will be retained. Can be a raw string. May not be an empty string. Missing values will not be evaluated.
    """
    def __init__(self, column_name, regular_expression):
        if type(regular_expression) != str:
            raise Exception("The regular expression must be a string.")

        super().__init__(column_name, re.compile(regular_expression))

    def passes(self, value):
        return not self.value.search(value.decode())

class NumericFilter(__SimpleBaseFilter):
    """
    This class is used to construct filters for querying based on a numeric column in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        operator (operator): The comparison operator to use.
        query_value (float or int): A numeric value to use for comparison.
    """
    def __init__(self, column_name, oper, query_value):
        q_type = type(query_value)
        if not q_type == float and not q_type == int:
            raise Exception("The query_value value must be a float or an integer.")

        super().__init__(column_name, query_value)

        self.operator = oper

    def check_types(self, column_index_dict, column_type_dict):
        if column_type_dict[column_index_dict[self.column_name]] == "c":
            raise Exception(f"A numeric filter may only be used with numeric columns, but {self.column_name.decode()} is not numeric (float or integer).")

    def passes(self, value):
        return self.operator(fastnumbers.fast_float(value), self.value)

class __CompositeBaseFilter(BaseFilter):
    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2

    def check_types(self, column_index_dict, column_type_dict):
        self.filter1.check_types(column_index_dict, column_type_dict)
        self.filter2.check_types(column_index_dict, column_type_dict)

    def get_column_name_set(self):
        return self.filter1.get_column_name_set() | self.filter2.get_column_name_set()

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

    def filter_column_values(self, parser, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        row_indices_1 = self.filter1.filter_column_values(parser, row_indices, column_index_dict, column_type_dict, column_coords_dict)
        return self.filter2.filter_column_values(parser, row_indices_1, column_index_dict, column_type_dict, column_coords_dict)

    def filter_indexed_column_values(self, parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        #if num_processes == 1:
        row_indices_1 = self.filter1.filter_indexed_column_values(parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)
        row_indices_2 = self.filter2.filter_indexed_column_values(parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)
        #else:
        #keep_row_indices_sets = Parallel(n_jobs=num_processes)(delayed(fltr.filter_indexed_column_values)(parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes) for fltr in [self.filter1, self.filter2])
        #keep_row_indices_sets = Parallel(n_jobs=2)(delayed(fltr.filter_indexed_column_values)(parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes) for fltr in [self.filter1, self.filter2])
        #row_indices_1 = keep_row_indices_sets[0]
        #row_indices_2 = keep_row_indices_sets[1]

        return row_indices_1 & row_indices_2

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

    def filter_column_values(self, parser, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        row_indices_1 = self.filter1.filter_column_values(parser, row_indices, column_index_dict, column_type_dict, column_coords_dict)
        row_indices_2 = self.filter2.filter_column_values(parser, row_indices - row_indices_1, column_index_dict, column_type_dict, column_coords_dict)

        return row_indices_1 | row_indices_2

    def filter_indexed_column_values(self, parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        row_indices_1 = self.filter1.filter_indexed_column_values(parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)
        row_indices_2 = self.filter2.filter_indexed_column_values(parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes)

        return row_indices_1 | row_indices_2

class NumericWithinFilter(__CompositeBaseFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        lower_type = type(lower_bound_value)
        if not lower_type == float and not lower_type == int:
            raise Exception("The lower_bound_value must be a float or an integer.")
        upper_type = type(upper_bound_value)
        if not upper_type == float and not upper_type == int:
            raise Exception("The upper_bound_value must be a float or an integer.")

        filter1 = NumericFilter(column_name, operator.ge, lower_bound_value)
        filter2 = NumericFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def filter_column_values(self, parser, row_indices, column_index_dict, column_type_dict, column_coords_dict):
        return AndFilter(self.filter1, self.filter2).filter_column_values(parser, row_indices, column_index_dict, column_type_dict, column_coords_dict)

    def filter_indexed_column_values(self, parser, column_index_dict, column_type_dict, column_coords_dict, end_index, num_processes):
        lower_index_column_type = column_type_dict[column_index_dict[self.filter1.column_name]]
        upper_index_column_type = column_type_dict[column_index_dict[self.filter2.column_name]]

        lower_indexer = f4py.IndexHelper._get_filter_indexer(parser.data_file_path, parser.compression_level, self.filter1.column_name, lower_index_column_type, self.filter1)
        upper_indexer = f4py.IndexHelper._get_filter_indexer(parser.data_file_path, parser.compression_level, self.filter2.column_name, upper_index_column_type, self.filter2)

        lower_positions = lower_indexer.find_positions(self.filter1, end_index)
        upper_positions = upper_indexer.find_positions(self.filter2, end_index)

        lower_position = max(lower_positions[0], upper_positions[0])
        upper_position = min(lower_positions[1], upper_positions[1])

        return(lower_indexer.find_matching_row_indices((lower_position, upper_position)))

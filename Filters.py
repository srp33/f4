import fastnumbers
from Helper import is_missing_value
import operator
import re

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

class In(BaseFilter):
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

class Like(BaseFilter):
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

class Numeric(BaseFilter):
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

class And(BaseFilter):
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

class Or(BaseFilter):
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

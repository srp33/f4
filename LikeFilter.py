import re

class LikeFilter:
    """
    This class is used to construct regular-expression based filters for querying any column type in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        regular_expression (str): Values in the specified column will be compared against this regular expression. Matches will be retained. Can be a raw string. May not be an empty string.
        negate (bool): Whether to use negation. In other words, this will match rows that do not contain the specified values in the specified column. Default: False.

    Attributes:
        column_name (str): The name (encoded string) of a column that should be evaluated.
        regular_expresssion (regular expression object): A compiled version of the regular expression.
        negate (bool): Whether to use negation.
    """
    def __init__(self, column_name, regular_expression, negate=False):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        if type(regular_expression) != str:
            raise Exception("The regular expression must be a string.")

        self.column_name = column_name.encode()
        self.regular_expression = re.compile(regular_expression)
        self.negate = negate

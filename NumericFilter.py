import operator

class NumericFilter:
    """
    This class is used to construct filters for querying based on a column in an F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        operator (operator): The comparison operator to use.
        query_value (float or int): A numeric value to use for comparison.

    Attributes:
        column_name (str): The name (encoded string) of a column that should be evaluated.
        operator (operator): The comparison operator to use.
        query_value (float or int): A numeric value to use for comparison.
    """
    def __init__(self, column_name, oper, query_value):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        self.column_name = column_name.encode()
        self.operator = oper
        self.query_value = query_value

class InFilter:
    """
    This class is used to construct a filter that identifies rows container any of a list of values in a particular column. It can be used on any column type.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        values_list (list): A non-empty list of strings that indicates which values should be matched in the specified column. All values will be evaluated as strings. Values of other types will be ignored. Missing values (empty string or 'NA') are allowed.
        negate (bool): Whether to use negation. In other words, this will match rows that do not contain the specified values in the specified column. Default: False.

    Attributes:
        column_name (str): The name (an encoded string) of a column that should be evaluated.
        values_set (set): A set of encoded strings that indicate which values should be matched in the specified column.
        negate (bool): Whether to use negation.
    """
    def __init__(self, column_name, values_list, negate=False):
        if not column_name or column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        if type(column_name) != str:
            raise Exception("The column name must be a string.")

        if not values_list or len([x for x in values_list if type(x) == str]) == 0:
            raise Exception("The values_list argument must contain at least one string value.")

        self.column_name = column_name.encode()
        self.values_set = set([x.encode() for x in values_list if type(x) == str])
        self.negate = negate

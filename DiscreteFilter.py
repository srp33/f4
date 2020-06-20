class DiscreteFilter:
    """
    This class is used to construct filters for querying based on a column of a F4 file.

    Args:
        column_name (str): The name of a column that should be evaluated. May not be an empty string.
        values_list (list): A non-empty list of strings that indicates which values should be matched in the specified column.

    Attributes:
        column_name (str): The name (an encoded string) of a column that should be evaluated.
        values_set (set): A set of encoded strings that indicate which values should be matched in the specified column.
    """
    def __init__(self, column_name, values_list):
        if column_name == "":
            raise Exception("An empty value is not supported for the column_name argument.")

        self.column_name = column_name.encode()
        self.values_set = set([x.encode() for x in values_list])

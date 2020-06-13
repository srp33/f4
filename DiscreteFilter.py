class DiscreteFilter:
    # Input the column index as an integer.
    # Input the values as a list of strings; will be converted to an encoded set.
    def __init__(self, column_index, values_list):
        self.column_index = column_index
        self.values_set = set([x.encode() for x in values_list])

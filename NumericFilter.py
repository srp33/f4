class NumericFilter:
    # Input the column index as an integer.
    # Operator must be <, <=, >, >=, ==, or !=.
    # Query value must numeric (float or int).
    def __init__(self, column_index, operator, query_value):
        self.column_index = column_index
        self.operator = operator
        self.query_value = query_value

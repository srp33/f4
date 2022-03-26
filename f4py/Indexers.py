import f4py
#from f4py.IndexHelpers import *
#from f4py.Parser import *
#from f4py.Utilities import *
from operator import itemgetter
import pynumparser
#import zstandard

class BaseIndexer():
    #TODO: Make private?
    def build(self, values_positions):
        raise Exception("This function must be implemented by classes that inherit this class.")

    #TODO: Make private?
    def filter(self, index_file_path, row_indices, fltr):
        raise Exception("This function must be implemented by classes that inherit this class.")

class CategoricalIndexer(BaseIndexer):
    #TODO: Can these be pushed to base class?
    def __init__(self, f4_file_path, compression_level):
        self.__f4_file_path = f4_file_path

    def build(self, values_positions):
        value_dict = {}
        for i in range(len(values_positions)):
            value = values_positions[i][0]
            row_index = values_positions[i][1]

            value_dict[value] = value_dict.setdefault(value, []) + [row_index]

        index_string = b""
        for value, row_indices in value_dict.items():
            row_indices_string = pynumparser.NumberSequence().encode(row_indices)
            index_string += (f"{value.decode()}\t{row_indices_string}\n").encode()

        return index_string

    def filter(self, index_file_path, row_indices, fltr):
        return row_indices

class IdentifierIndexer(BaseIndexer):
    def __init__(self, f4_file_path, compression_level):
        self.__f4_file_path = f4_file_path

    def build(self, values_positions):
        values_positions.sort(key=itemgetter(0))

        values = [x[0] for x in values_positions]
        positions = [str(x[1]).encode() for x in values_positions]

        values_max_length = f4py.get_max_string_length(values)
        positions_max_length = f4py.get_max_string_length(positions)

        values_fixed_width = f4py.format_column_items(values, values_max_length)
        positions_fixed_width = f4py.format_column_items(positions, positions_max_length)

        rows = []
        for i, value in enumerate(values_fixed_width):
            position = positions_fixed_width[i]
            rows.append(value + position)

        column_coords_string, rows_max_length = f4py.build_string_map(rows)

        f4py.Builder().save_meta_files(self.__f4_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

        return column_coords_string

    def filter(self, index_file_path, row_indices, fltr):
        parser = f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

        line_length = parser.get_stat(".ll")
        data_file_handle = parser.get_file_handle("")
        value_coords = parser._parse_data_coords([0])[0]
        position_coords = parser._parse_data_coords([1])[0]

        passing_row_indices = set()

#        #TODO: This is the brute-force search. Implement binary search.
        for i in row_indices:
            if fltr.passes(parser.parse_data_value(i, line_length, value_coords, data_file_handle).rstrip()):
                passing_row_indices.add(i)

        parser.close()

#def binary_search(list1, n):
#    low = 0
#    high = len(list1) - 1
#    mid = 0
#
#    while low <= high:
#        # for get integer result
#        mid = (high + low) // 2
#
#        # Check if n is present at mid
#        if list1[mid] < n:
#            low = mid + 1
#
#        # If n is greater, compare to the right of mid
#        elif list1[mid] > n:
#            high = mid - 1
#
#        # If n is smaller, compared to the left of mid
#        else:
#            return mid
#
#            # element was not present in the list, return -1
#    return -1

        #TODO: Refactor some of this logic to the base class?
        return passing_row_indices

class NumericIndexer(BaseIndexer):
    def __init__(self, f4_file_path, compression_level):
        self.__f4_file_path = f4_file_path

    def build(self, values_positions):
        for i in range(len(values_positions)):
            values_positions[i][0] = float(values_positions[i][0])
        values_positions.sort(key=itemgetter(0))

        values = [str(x[0]).encode() for x in values_positions]
        positions = [str(x[1]).encode() for x in values_positions]

        values_max_length = f4py.get_max_string_length(values)
        positions_max_length = f4py.get_max_string_length(positions)

        values_fixed_width = f4py.format_column_items(values, values_max_length)
        positions_fixed_width = f4py.format_column_items(positions, positions_max_length)

        rows = []
        for i, value in enumerate(values_fixed_width):
            position = positions_fixed_width[i]
            rows.append(value + position)

        column_coords_string, rows_max_length = f4py.build_string_map(rows)

        f4py.Builder().save_meta_files(self.__f4_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

        return column_coords_string

    def filter(self, index_file_path, row_indices, fltr):
        parser = f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

        line_length = parser.get_stat(".ll")
        data_file_handle = parser.get_file_handle("")
        value_coords = parser._parse_data_coords([0])[0]
        position_coords = parser._parse_data_coords([1])[0]

        passing_row_indices = set()

        #TODO: This is the brute-force search. Move to binary search.
        for i in row_indices:
            if fltr.passes(parser.parse_data_value(i, line_length, value_coords, data_file_handle).rstrip()):
                passing_row_indices.add(i)

        parser.close()

        return passing_row_indices

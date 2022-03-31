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
    #TODO: Is start_index necessary?
    def filter(self, index_file_path, fltr, start_index, end_index):
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

    def filter(self, index_file_path, fltr, start_index, end_index):
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

    def filter(self, index_file_path, fltr, start_index, end_index):
        if end_index == 0:
            return set()

        index_parser = f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

        line_length = index_parser.get_stat(".ll")
        data_file_handle = index_parser.get_file_handle("")
        value_coords = index_parser._parse_data_coords([0])[0]
        position_coords = index_parser._parse_data_coords([1])[0]

        matching_position = self.binary_search(index_parser, line_length, value_coords, data_file_handle, fltr.value, start_index, end_index)

        if matching_position != None:
            #TODO: Test this when IDs are not in order
            matching_row_index = int(index_parser.parse_data_value(matching_position, line_length, position_coords, data_file_handle).rstrip())
            return set([matching_row_index])

        return set()

    def binary_search(self, parser, line_length, value_coords, data_file_handle, value_to_find, l, r):
        mid = l + (r - l) // 2

        mid_value = parser.parse_data_value(mid, line_length, value_coords, data_file_handle).rstrip()

        if mid_value == value_to_find:
            # If element is present at the middle itself
            return mid
        elif mid_value > value_to_find:
            # If element is smaller than mid, then it can only be present in left subarray
            return self.binary_search(parser, line_length, value_coords, data_file_handle, value_to_find, l, mid-1)
        else:
            # Else the element can only be present in right subarray
            return self.binary_search(parser, line_length, value_coords, data_file_handle, value_to_find, mid+1, r)

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

    def filter(self, index_file_path, fltr, start_index, end_index):
        if end_index == 0:
            return set()

        index_parser = f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

        line_length = index_parser.get_stat(".ll")
        data_file_handle = index_parser.get_file_handle("")
        value_coords = index_parser._parse_data_coords([0])[0]
        position_coords = index_parser._parse_data_coords([1])[0]

        # if smallest value > query value: all are true
        smallest_value = float(index_parser.parse_data_value(0, line_length, value_coords, data_file_handle).rstrip())
        if smallest_value > fltr.value:
            return set(range(end_index))

        # if largest value < query value: all are false
        largest_value = float(index_parser.parse_data_value(end_index - 1, line_length, value_coords, data_file_handle).rstrip())
        if largest_value < fltr.value:
            return set()

        previous_query_position = end_index - 1
        query_position = end_index - 2

        # Work backwards from the end and find the position where all values are greater than the query value.
        while float(index_parser.parse_data_value(query_position, line_length, value_coords, data_file_handle).rstrip()) > fltr.value:
            previous_query_position = query_position
            query_position = query_position // 2

        # Find the value just lower than the query value.
        while query_position < previous_query_position:
            this_value = float(index_parser.parse_data_value(query_position, line_length, value_coords, data_file_handle).rstrip())
            next_value = float(index_parser.parse_data_value(query_position + 1, line_length, value_coords, data_file_handle).rstrip())

            # Start with the position one ahead of what you just found.
            if this_value < fltr.value and next_value >= fltr.value:
                query_position += 1
                break

            query_position += (previous_query_position - query_position) // 2

            if query_position + 1 == previous_query_position:
                query_position += 1
                break

        matching_row_indices = set()
        for i in range(query_position, end_index):
            matching_row_indices.add(int(index_parser.parse_data_value(i, line_length, position_coords, data_file_handle).rstrip()))

        return matching_row_indices
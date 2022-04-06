import f4py
#from f4py.IndexHelpers import *
#from f4py.Parser import *
#from f4py.Utilities import *
import operator
from operator import itemgetter
import pynumparser
#import zstandard

class BaseIndexer():
    def __init__(self, index_file_path, compression_level):
        self.index_file_path = index_file_path
        self.compression_level = compression_level

    #TODO: Make private?
    def build(self, values_positions):
        raise Exception("This function must be implemented by classes that inherit this class.")

    #TODO: Make private?
    def filter(self, fltr, end_index):
        raise Exception("This function must be implemented by classes that inherit this class.")

class CategoricalIndexer(BaseIndexer):
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

    def build(self, values_positions):
        value_dict = {}
        for i in range(len(values_positions)):
            value = values_positions[i][0]
            row_index = values_positions[i][1]

            value_dict[value] = value_dict.setdefault(value, []) + [row_index]

        index_string = b""
        for value, row_indices in value_dict.items():
            row_indices_string = pynumparser.NumberSequence(int).encode(row_indices)
            index_string += (f"{value.decode()}\t{row_indices_string}\n").encode()

        f4py.write_string_to_file(self.index_file_path, "", index_string)

        # TODO: Indicate whether the index is compressed.
        #write_string_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compression_level).encode())

    def filter(self, fltr, end_index):
        index_file = f4py.open_read_file(self.index_file_path, file_extension="")
        row_indices = set()

        while True:
            line_items = index_file.readline().rstrip(b"\n").split(b"\t")

            if len(line_items) < 2:
                break

            if line_items[0].rstrip() == fltr.value:
                row_indices = set(pynumparser.NumberSequence(int).parse(line_items[1].decode()))

        index_file.close()

        return row_indices

class IdentifierIndexer(BaseIndexer):
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

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

        f4py.write_string_to_file(self.index_file_path, "", column_coords_string)
        f4py.Builder().save_meta_files(self.index_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

        # TODO: Indicate whether the index is compressed.
        #write_string_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compression_level).encode())

    def filter(self, fltr, end_index):
        if end_index == 0:
            return set()

        index_parser = f4py.Parser(self.index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

        line_length = index_parser.get_stat(".ll")
        data_file_handle = index_parser.get_file_handle("")
        value_coords = index_parser._parse_data_coords([0])[0]
        position_coords = index_parser._parse_data_coords([1])[0]

        matching_position = self.binary_search(index_parser, line_length, value_coords, data_file_handle, fltr.value, 0, end_index)

        if matching_position == -1:
            return set()

        matching_row_index = int(index_parser.parse_data_value(matching_position, line_length, position_coords, data_file_handle).rstrip())
        return set([matching_row_index])

    def binary_search(self, parser, line_length, value_coords, data_file_handle, value_to_find, l, r):
        if r == -1:
            return -1

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
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

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

        f4py.write_string_to_file(self.index_file_path, "", column_coords_string)
        f4py.Builder().save_meta_files(self.index_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

        # TODO: Indicate whether the index is compressed.
        #write_string_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compression_level).encode())

    def filter(self, fltr, end_index):
        if end_index == 0:
            return set()

        return self.find_matching_row_indices(self.find_positions(fltr, end_index))

    def find_positions(self, fltr, end_index):
        index_parser = f4py.Parser(self.index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])
        line_length = index_parser.get_stat(".ll")
        data_file_handle = index_parser.get_file_handle("")
        value_coords = index_parser._parse_data_coords([0])[0]

        if fltr.operator == operator.gt:
            positions = self.find_positions_g(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.le)
        elif fltr.operator == operator.ge:
            positions = self.find_positions_g(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.lt)
        elif fltr.operator == operator.lt:
            positions = self.find_positions_l(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.ge)
        elif fltr.operator == operator.le:
            positions = self.find_positions_l(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.gt)

        index_parser.close()

        return positions

    def find_positions_g(self, index_parser, line_length, value_coords, data_file_handle, fltr, end_index, all_true_operator, all_false_operator):
        smallest_value = float(index_parser.parse_data_value(0, line_length, value_coords, data_file_handle).rstrip())
        if all_true_operator(smallest_value, fltr.value):
            #return range(end_index)
            return 0, end_index

        largest_value = float(index_parser.parse_data_value(end_index - 1, line_length, value_coords, data_file_handle).rstrip())
        if not all_true_operator(largest_value, fltr.value):
            #return []
            return 0, 0

        matching_position = self.search(index_parser, line_length, value_coords, data_file_handle, fltr.value, 0, end_index, all_false_operator)

        return matching_position + 1, end_index

    def find_positions_l(self, index_parser, line_length, value_coords, data_file_handle, fltr, end_index, all_true_operator, all_false_operator):
        smallest_value = float(index_parser.parse_data_value(0, line_length, value_coords, data_file_handle).rstrip())
        if not all_true_operator(smallest_value, fltr.value):
            return 0, 0

        largest_value = float(index_parser.parse_data_value(end_index - 1, line_length, value_coords, data_file_handle).rstrip())
        if all_true_operator(largest_value, fltr.value):
            #return range(end_index)
            return 0, end_index

        matching_position = self.search(index_parser, line_length, value_coords, data_file_handle, fltr.value, 0, end_index, all_true_operator)

        return 0, matching_position + 1

    def search(self, index_parser, line_length, value_coords, data_file_handle, value_to_find, l, r, search_operator):
        mid = l + (r - l) // 2

        mid_value = float(index_parser.parse_data_value(mid, line_length, value_coords, data_file_handle).rstrip())

        if search_operator(mid_value, value_to_find):
            next_value = index_parser.parse_data_value(mid + 1, line_length, value_coords, data_file_handle).rstrip()

            if next_value == b"":
                return mid
            elif not search_operator(float(next_value), value_to_find):
                return mid
            else:
                return self.search(index_parser, line_length, value_coords, data_file_handle, value_to_find, mid, r, search_operator)
        else:
            return self.search(index_parser, line_length, value_coords, data_file_handle, value_to_find, l, mid, search_operator)

    def find_matching_row_indices(self, positions):
        index_parser = f4py.Parser(self.index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

        line_length = index_parser.get_stat(".ll")
        data_file_handle = index_parser.get_file_handle("")
        position_coords = index_parser._parse_data_coords([1])[0]

        matching_row_indices = set()

        for i in range(positions[0], positions[1]):
            matching_row_indices.add(int(index_parser.parse_data_value(i, line_length, position_coords, data_file_handle).rstrip()))

        index_parser.close()

        return matching_row_indices

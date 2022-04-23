import f4py
import fastnumbers
from operator import itemgetter
import pynumparser

class BaseIndexer():
    def __init__(self, index_file_path, compression_level):
        self.index_file_path = index_file_path
        self.compression_level = compression_level

    def build(self, values_positions):
        conversion_function = self._get_conversion_function()

        for i in range(len(values_positions)):
            values_positions[i][0] = conversion_function(values_positions[i][0])
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

        f4py.write_str_to_file(self.index_file_path, "", column_coords_string)
        f4py.Builder()._save_meta_files(self.index_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

        # TODO: Indicate whether the index is compressed.
        #write_str_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compression_level).encode())

    def _get_conversion_function(self):
        return f4py.do_nothing

    def filter(self, fltr, end_index, num_processes=1):
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

        f4py.write_str_to_file(self.index_file_path, "", index_string)

        # TODO: Indicate whether the index is compressed.
        #write_str_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compression_level).encode())

    def filter(self, fltr, end_index, num_processes=1):
        index_file = f4py.open_read_file(self.index_file_path, file_extension="")
        row_indices = set()

        while True:
            line_items = index_file.readline().rstrip(b"\n").split(b"\t")

            if len(line_items) < 2:
                break

            if fltr.passes(line_items[0].rstrip()):
                row_indices = row_indices | set(pynumparser.NumberSequence(int).parse(line_items[1].decode()))

        index_file.close()

        return row_indices

class IdentifierIndexer(BaseIndexer):
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

    def filter(self, fltr, end_index, num_processes=1):
        if end_index == 0:
            return set()

        with f4py.Parser(self.index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as index_parser:
            line_length = index_parser.get_stat(".ll")
            data_file_handle = index_parser.get_file_handle("")
            value_coords = index_parser._parse_data_coords([0])[0]
            position_coords = index_parser._parse_data_coords([1])[0]

            matching_position = self.binary_search(index_parser, line_length, value_coords, data_file_handle, fltr.value, 0, end_index)

            if matching_position == -1:
                return set()

            matching_row_index = fastnumbers.fast_int(index_parser._parse_row_value(matching_position, position_coords, line_length, data_file_handle))

            return set([matching_row_index])

    #TODO: Move to IndexHelper?
    def binary_search(self, parser, line_length, value_coords, data_file_handle, value_to_find, l, r):
        if r == -1 or l > r:
            return -1

        mid = l + (r - l) // 2
        mid_value = parser._parse_row_value(mid, value_coords, line_length, data_file_handle)

        if mid_value == value_to_find:
            # If element is present at the middle itself
            return mid
        elif mid_value > value_to_find:
            return self.binary_search(parser, line_length, value_coords, data_file_handle, value_to_find, l, mid-1)
        else:
            # Else the element can only be present in right subarray
            return self.binary_search(parser, line_length, value_coords, data_file_handle, value_to_find, mid+1, r)

class FloatIndexer(BaseIndexer):
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

    def _get_conversion_function(self):
        return fastnumbers.fast_float

    def filter(self, fltr, end_index, num_processes=1):
        if end_index == 0:
            return set()

        positions = f4py.IndexHelper._find_positions(self.index_file_path, fltr, end_index, self._get_conversion_function())
        return f4py.IndexHelper._retrieve_matching_row_indices(self.index_file_path, positions, num_processes)

class IntIndexer(BaseIndexer):
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

    def _get_conversion_function(self):
        return fastnumbers.fast_int

    def filter(self, fltr, end_index, num_processes=1):
        if end_index == 0:
            return set()

        positions = f4py.IndexHelper._find_positions(self.index_file_path, fltr, end_index, self._get_conversion_function())
        return f4py.IndexHelper._retrieve_matching_row_indices(self.index_file_path, positions, num_processes)

class FunnelIndexer(BaseIndexer):
    def __init__(self, index_file_path, compression_level):
        super().__init__(index_file_path, compression_level)

    def build(self, values_positions):
        # Convert the second values to integers.
        for i in range(len(values_positions)):
            values_positions[i][1] = fastnumbers.fast_int(values_positions[i][1])

        # Sort by the first, then second key.
        values_positions.sort(key=itemgetter(0, 1))

        values_1 = [x[0] for x in values_positions]
        values_2 = [str(x[1]).encode() for x in values_positions]
        positions = [str(x[2]).encode() for x in values_positions]

        values_1_max_length = f4py.get_max_string_length(values_1)
        values_2_max_length = f4py.get_max_string_length(values_2)
        positions_max_length = f4py.get_max_string_length(positions)

        values_1_fixed_width = f4py.format_column_items(values_1, values_1_max_length)
        values_2_fixed_width = f4py.format_column_items(values_2, values_2_max_length)
        positions_fixed_width = f4py.format_column_items(positions, positions_max_length)

        rows = []
        for i, value_1 in enumerate(values_1_fixed_width):
            value_2 = values_2_fixed_width[i]
            position = positions_fixed_width[i]
            rows.append(value_1 + value_2 + position)

        column_coords_string, rows_max_length = f4py.build_string_map(rows)

        f4py.write_str_to_file(self.index_file_path, "", column_coords_string)
        f4py.Builder()._save_meta_files(self.index_file_path, [values_1_max_length, values_2_max_length, positions_max_length], rows_max_length + 1)

#    def filter(self, fltr, end_index, num_processes=1):
#        if end_index == 0:
#            return set()
#
#        positions = f4py.IndexHelper._find_positions(self.index_file_path, fltr, end_index, f4py.do_nothing)
#        return f4py.IndexHelper._retrieve_matching_row_indices(self.index_file_path, positions, num_processes)

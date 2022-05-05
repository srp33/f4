import f4py
import fastnumbers
from operator import itemgetter
import pynumparser

class StringIndexBuilder:
    def build(self, index_file_path, values_positions):
        for i in range(len(values_positions)):
            values_positions[i][0] = self._get_conversion_function()(values_positions[i][0])
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

        f4py.write_str_to_file(index_file_path, "", column_coords_string)
        f4py.Builder()._save_meta_files(index_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

    def _get_conversion_function(self):
        return f4py.decode_string

class IdentifierIndexBuilder(StringIndexBuilder):
    def _get_conversion_function(self):
        return f4py.do_nothing

class FloatIndexBuilder(StringIndexBuilder):
    def _get_conversion_function(self):
        return fastnumbers.fast_float

class IntIndexBuilder(StringIndexBuilder):
    def _get_conversion_function(self):
        return fastnumbers.fast_int

#class FunnelIndexBuilder(BaseIndexBuilder):
#    def __init__(self, index_file_path, compression_level):
#        super().__init__(index_file_path, compression_level)
#
#    def build(self, values_positions):
#        # Convert the second values to integers.
#        for i in range(len(values_positions)):
#            values_positions[i][1] = fastnumbers.fast_int(values_positions[i][1])
#
#        # Sort by the first, then second key.
#        values_positions.sort(key=itemgetter(0, 1))
#
#        values_1 = [x[0] for x in values_positions]
#        values_2 = [str(x[1]).encode() for x in values_positions]
#        positions = [str(x[2]).encode() for x in values_positions]
#
#        values_1_max_length = f4py.get_max_string_length(values_1)
#        values_2_max_length = f4py.get_max_string_length(values_2)
#        positions_max_length = f4py.get_max_string_length(positions)
#
#        values_1_fixed_width = f4py.format_column_items(values_1, values_1_max_length)
#        values_2_fixed_width = f4py.format_column_items(values_2, values_2_max_length)
#        positions_fixed_width = f4py.format_column_items(positions, positions_max_length)
#
#        rows = []
#        for i, value_1 in enumerate(values_1_fixed_width):
#            value_2 = values_2_fixed_width[i]
#            position = positions_fixed_width[i]
#            rows.append(value_1 + value_2 + position)
#
#        column_coords_string, rows_max_length = f4py.build_string_map(rows)
#
#        f4py.write_str_to_file(self.index_file_path, "", column_coords_string)
#        f4py.Builder()._save_meta_files(self.index_file_path, [values_1_max_length, values_2_max_length, positions_max_length], rows_max_length + 1)

#    def filter(self, fltr, end_index, num_processes=1):
#        if end_index == 0:
#            return set()
#
#        positions = f4py.IndexHelper._find_positions(self.index_file_path, fltr, end_index, f4py.do_nothing, num_processes=num_processes)
#        return f4py.IndexHelper._retrieve_matching_row_indices(self.index_file_path, positions, num_processes)

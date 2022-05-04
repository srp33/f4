import f4py
import fastnumbers
from operator import itemgetter
import pynumparser

#class BaseIndexer():
class StringIndexer():
    def build(self, index_file_path, values_positions):
        #TODO: Does this speed things up at all?
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

        f4py.write_str_to_file(index_file_path, "", column_coords_string)
        f4py.Builder()._save_meta_files(index_file_path, [values_max_length, positions_max_length], rows_max_length + 1)

    def _get_conversion_function(self):
        #return f4py.do_nothing
        return f4py.decode_string

#    def filter(self, index_file_path, fltr, end_index, num_processes=1):
#        raise Exception("This function must be implemented by classes that inherit this class.")

#class CategoricalIndexer(BaseIndexer):
#    def build(self, index_file_path, values_positions):
#        value_dict = {}
#        unique_values = set([x[0] for x in values_positions])
#        for value in unique_values:
#            value_dict[value] = []
#
#        for i in range(len(values_positions)):
#            value = values_positions[i][0]
#            row_index = values_positions[i][1]
#            value_dict[value].append(row_index)
#
#        index_string = b""
#        for value, row_indices in value_dict.items():
#            row_indices_string = pynumparser.NumberSequence(int).encode(row_indices)
#            index_string += (f"{value.decode()}\t{row_indices_string}\n").encode()
#
#        f4py.write_str_to_file(index_file_path, "", index_string)
#
#    def filter(self, index_file_path, fltr, end_index, num_processes=1):
#        with f4py.open_read_file(index_file_path, file_extension="") as index_file:
#            row_indices = set()
#
#            while True:
#                line_items = index_file.readline().rstrip(b"\n").split(b"\t")
#
#                if len(line_items) < 2:
#                    break
#
#                if fltr.passes(line_items[0].rstrip()):
#                    row_indices = row_indices | set(pynumparser.NumberSequence(int).parse(line_items[1].decode()))
#
#            return row_indices
#
#    def double_filter(self, index_file_path, filter1, filter2, end_index, num_processes=1):
#        with f4py.open_read_file(index_file_path, file_extension="") as index_file:
#            row_indices = set()
#
#            while True:
#                line_items = index_file.readline().rstrip(b"\n").split(b"\t")
#
#                if len(line_items) < 2:
#                    break
#
#                value = line_items[0].rstrip()
#                if filter1.passes(value) and filter2.passes(value):
#                    row_indices = row_indices | set(pynumparser.NumberSequence(int).parse(line_items[1].decode()))
#
#            return row_indices

class IdentifierIndexer(StringIndexer):
    def _get_conversion_function(self):
        return f4py.do_nothing

    def filter(self, index_file_path, query_value, end_index, num_processes=1):
        if end_index == 0:
            return set()

        with f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as index_parser:
            line_length = index_parser.get_stat(".ll")
            data_file_handle = index_parser.get_file_handle("")
            value_coords = index_parser._parse_data_coords([0])[0]
            position_coords = index_parser._parse_data_coords([1])[0]

            matching_position = f4py.IndexHelper._binary_identifier_search(index_parser, line_length, value_coords, data_file_handle, query_value, 0, end_index)

            if matching_position == -1:
                return set()

            matching_row_index = fastnumbers.fast_int(index_parser._parse_row_value(matching_position, position_coords, line_length, data_file_handle))

            return set([matching_row_index])

#class FloatIndexer(BaseIndexer):
class FloatIndexer(StringIndexer):
    def _get_conversion_function(self):
        return fastnumbers.fast_float

#class IntIndexer(BaseIndexer):
class IntIndexer(StringIndexer):
    def _get_conversion_function(self):
        return fastnumbers.fast_int

#class FunnelIndexer(BaseIndexer):
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

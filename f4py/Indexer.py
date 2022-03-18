from f4py.Parser import *
from f4py.Utilities import *
from operator import itemgetter
import pynumparser
import zstandard

class Indexer:
    def __init__(self, f4_file_path, index_columns, compression_level=22, verbose=False):
        self.__f4_file_path = f4_file_path
        self.__index_columns = index_columns
        self.__compression_level = compression_level
        self.__verbose = verbose

    def save(self, num_processes=1, num_rows_per_save=10):
        self._print_message(f"Building index for {self.__f4_file_path}.")

        num_rows = read_int_from_file(self.__f4_file_path, ".nrow")

        parser = None

        try:
            parser = Parser(self.__f4_file_path)

#            compressor = None
#            if self.__compression_level:
#                compressor = zstandard.ZstdCompressor(level=self.__compression_level)

            # Get information about index columns.
            index_columns, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(NoFilter(), self.__index_columns, get_types_for_select_columns=True)
            data_file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            for index_column in index_columns:
                index_column_type = column_type_dict[column_index_dict[index_column]]
                coords = column_coords_dict[column_index_dict[index_column]]

                values_positions = []
                for row_index in range(parser.get_num_rows()):
                    value = parser.parse_data_value(row_index, line_length, coords, data_file_handle)
                    values_positions.append([value, row_index])

                if index_column_type == "c":
                    index_string = _CategoricalIndexHelper().build(values_positions)
                elif index_column_type == "f":
                    index_string = _NumericIndexHelper().build(values_positions)
                else: # i
                    index_string = _IdentifierIndexHelper().build(values_positions)

                write_string_to_file(parser.data_file_path, f".idx_{column_index_dict[index_column]}", index_string)

            # Indicate whether the index is compressed.
            #write_string_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compression_level).encode())
        finally:
            if parser:
                parser.close()

    ##############################################
    # Non-public function
    ##############################################

    def _print_message(self, message):
        print_message(message, self.__verbose)

class __BaseIndexHelper():
    def build(self, values_positions):
        raise Exception("This function must be implemented by classes that inherit this class.")

class _CategoricalIndexHelper(__BaseIndexHelper):
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

class _NumericIndexHelper(__BaseIndexHelper):
    def build(self, values_positions):
        for i in range(len(values_positions)):
            values_positions[i][0] = float(values_positions[i][0])
        values_positions.sort(key=itemgetter(0))

        values = [str(x[0]).encode() for x in values_positions]
        positions = [str(x[1]).encode() for x in values_positions]

        values_max_length = get_max_string_length(values)
        positions_max_length = get_max_string_length(positions)

        values_fixed_width = format_column_items(values, values_max_length)
        positions_fixed_width = format_column_items(positions, positions_max_length)

        rows = []
        for i, value in enumerate(values_fixed_width):
            position = positions_fixed_width[i]
            rows.append(value + position)

        column_coords_string, rows_max_length = build_string_map(rows)
        return column_coords_string

class _IdentifierIndexHelper(__BaseIndexHelper):
    def build(self, values_positions):
        values_positions.sort(key=itemgetter(0))

        values = [x[0] for x in values_positions]
        positions = [str(x[1]).encode() for x in values_positions]

        values_max_length = get_max_string_length(values)
        positions_max_length = get_max_string_length(positions)

        values_fixed_width = format_column_items(values, values_max_length)
        positions_fixed_width = format_column_items(positions, positions_max_length)

        rows = []
        for i, value in enumerate(values_fixed_width):
            position = positions_fixed_width[i]
            rows.append(value + position)

        column_coords_string, rows_max_length = build_string_map(rows)
        return column_coords_string

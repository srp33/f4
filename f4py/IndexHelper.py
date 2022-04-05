import f4py
#from f4py.Parser import *
#from f4py.Utilities import *

#TODO: Move to Utilities.py?
class IndexHelper:
    def save(f4_file_path, index_column, compression_level=None, verbose=False):
        #TODO: Make sure index_column is valid.

        f4py.print_message(f"Saving index for {f4_file_path}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")

        parser = None

        try:
            parser = f4py.Parser(f4_file_path)

#            compressor = None
#            if self.__compression_level:
#                compressor = zstandard.ZstdCompressor(level=self.__compression_level)

            # Get information about index columns.
            ignore, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(f4py.NoFilter(), [index_column])
            data_file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            index_column_type = column_type_dict[column_index_dict[index_column.encode()]]
            coords = column_coords_dict[column_index_dict[index_column.encode()]]

            values_positions = []
            for row_index in range(parser.get_num_rows()):
                value = parser.parse_data_value(row_index, line_length, coords, data_file_handle)
                values_positions.append([value, row_index])

            index_file_path = IndexHelper.get_index_file_path(parser.data_file_path, index_column)

            if index_column_type == "c":
                f4py.CategoricalIndexer(index_file_path, compression_level).build(values_positions)
            elif index_column_type == "f":
                f4py.NumericIndexer(index_file_path, compression_level).build(values_positions)
            else: # i
                f4py.IdentifierIndexer(index_file_path, compression_level).build(values_positions)
        finally:
            if parser:
                parser.close()

    def get_filter_indexer(f4_file_path, compression_level, index_column, index_column_type, fltr):
        index_file_path = IndexHelper.get_index_file_path(f4_file_path, index_column.decode())

        if index_column_type == "i":
            if isinstance(fltr, f4py.StringEqualsFilter):
                return f4py.IdentifierIndexer(index_file_path, compression_level)
            else:
                raise Exception("TODO: Not yet supported.")

        if index_column_type == "c":
            return f4py.CategoricalIndexer(index_file_path, compression_level)

        if isinstance(fltr, f4py.NumericFilter) or isinstance(fltr, f4py.NumericWithinFilter):
            return f4py.NumericIndexer(index_file_path, compression_level)
        else:
            raise Exception("TODO: Not yet supported.")

    def get_index_file_path(f4_file_path, index_column):
        index_file_path_extension = f".idx_{index_column}"
        return f"{f4_file_path}{index_file_path_extension}"

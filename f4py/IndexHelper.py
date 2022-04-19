import f4py

class IndexHelper:
    def save_index(f4_file_path, index_column, compression_level=None, verbose=False):
        #TODO: Make sure index_column is valid.

        f4py.print_message(f"Saving index for {f4_file_path}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")

        with f4py.Parser(f4_file_path) as parser:
            #compressor = None
            #if compression_level:
            #    compressor = zstandard.ZstdCompressor(level=self.__compression_level)

            # Get information about index columns.
            f4py.print_message(f"Getting column meta information for {index_column} index for {f4_file_path}.", verbose)
            ignore, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(f4py.NoFilter(), [index_column])
            data_file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            index_column_type = column_type_dict[column_index_dict[index_column.encode()]]
            coords = column_coords_dict[column_index_dict[index_column.encode()]]

            values_positions = []
            f4py.print_message(f"Parsing values and positions for {index_column} index for {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value = parser._parse_row_value(row_index, coords, line_length, data_file_handle)
                values_positions.append([value, row_index])

            index_file_path = IndexHelper._get_index_file_path(parser.data_file_path, index_column)

            f4py.print_message(f"Building and saving index file for {index_column} index for {f4_file_path}.", verbose)
            if index_column_type == "c":
                f4py.CategoricalIndexer(index_file_path, compression_level).build(values_positions)
            elif index_column_type == "f":
                f4py.NumericIndexer(index_file_path, compression_level).build(values_positions)
            else: # i
                f4py.IdentifierIndexer(index_file_path, compression_level).build(values_positions)

        f4py.print_message(f"Done creating index file for {index_column} index for {f4_file_path}.", verbose)

    def save_indices(f4_file_path, index_columns, compression_level=None, verbose=False):
        for index_column in index_columns:
            f4py.IndexHelper.save_index(f4_file_path, index_column, compression_level=compression_level)

    def _get_filter_indexer(f4_file_path, compression_level, index_column, index_column_type, fltr):
        index_file_path = IndexHelper._get_index_file_path(f4_file_path, index_column.decode())

        if index_column_type == "u":
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

    def _get_index_file_path(f4_file_path, index_column):
        index_file_path_extension = f".idx_{index_column}"
        return f"{f4_file_path}{index_file_path_extension}"

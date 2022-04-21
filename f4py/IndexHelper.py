import f4py

class IndexHelper:
    def save_index(f4_file_path, index_column, compression_level=None, verbose=False):
        #TODO: Make sure index_column is valid.

        f4py.print_message(f"Saving index for {f4_file_path} and {index_column}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")

        with f4py.Parser(f4_file_path) as parser:
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

            f4py.print_message(f"Building index file for {index_column} index for {f4_file_path}.", verbose)
            if index_column_type == "c":
                f4py.CategoricalIndexer(index_file_path, compression_level).build(values_positions)
            elif index_column_type == "f" or index_column_type == "i":
                f4py.NumericIndexer(index_file_path, compression_level).build(values_positions)
            else: # u
                f4py.IdentifierIndexer(index_file_path, compression_level).build(values_positions)

        f4py.print_message(f"Done building index file for {index_column} index for {f4_file_path}.", verbose)

    def save_sequential_index(f4_file_path, index_column_1, index_column_2, compression_level=None, verbose=False):
        #TODO: Make sure index_column_1 and index_column_2 are valid.

        index_name = f"{index_column_1}____{index_column_2}"

        f4py.print_message(f"Saving index for {index_name} sequential index and {f4_file_path}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")

        with f4py.Parser(f4_file_path) as parser:
            # Get information about index columns.
            f4py.print_message(f"Getting column meta information for {index_name} sequential index and {f4_file_path}.", verbose)
            ignore, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(f4py.NoFilter(), [index_column_1, index_column_2])

            data_file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            index_column_1_type = column_type_dict[column_index_dict[index_column_1.encode()]]
            index_column_2_type = column_type_dict[column_index_dict[index_column_2.encode()]]
            coords_1 = column_coords_dict[column_index_dict[index_column_1.encode()]]
            coords_2 = column_coords_dict[column_index_dict[index_column_2.encode()]]

            values_positions = []
            f4py.print_message(f"Parsing values and positions for {index_name} sequential index and {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value_1 = parser._parse_row_value(row_index, coords_1, line_length, data_file_handle)
                value_2 = parser._parse_row_value(row_index, coords_2, line_length, data_file_handle)
                values_positions.append([value_1, value_2, row_index])

            # This needs to be a combined file path.
            index_file_path = IndexHelper._get_index_file_path(parser.data_file_path, index_name)

            f4py.print_message(f"Building sequential index file for {index_name} and {f4_file_path}.", verbose)
            if index_column_1_type == "c" and (index_column_2_type == "f" or index_column_2_type == "i"):
                f4py.CategoricalNumericSequentialIndexer(index_file_path, compression_level).build(values_positions)
            else:
                raise Exception("Sequential indices are currently only supported for categorical + numeric columns.")

        f4py.print_message(f"Done building sequential index file for {index_name} and {f4_file_path}.", verbose)

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

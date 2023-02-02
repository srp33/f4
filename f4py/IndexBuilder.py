import f4py

class IndexBuilder:
    #####################################################
    # Class (static) functions
    #####################################################
    # index_columns should be a list. Elements within it can be two-element lists.
    def build_indexes(f4_file_path, index_columns, verbose=False):
        if isinstance(index_columns, str):
            IndexBuilder._build_one_column_index(f4_file_path, index_columns, verbose)
        elif isinstance(index_columns, list):
            for index_column in index_columns:
                if isinstance(index_column, list):
                    if len(index_column) != 2:
                        raise Exception("If you pass a list as an index_column, it must have exactly two elements.")

                    IndexBuilder._build_two_column_index(f4_file_path, index_column[0], index_column[1], verbose)
                else:
                    if not isinstance(index_column, str):
                        raise Exception("When specifying an index column name, it must be a string.")

                    IndexBuilder._build_one_column_index(f4_file_path, index_column, verbose, f4py.do_nothing)
        else:
            raise Exception("When specifying index_columns, it must either be a string or a list.")

    # This function is specifically for the EndsWithFilter.
    def build_endswith_index(f4_file_path, index_column, verbose=False):
        IndexBuilder._build_one_column_index(f4_file_path, index_column, verbose, f4py.reverse_string)

    def _build_one_column_index(f4_file_path, index_column, verbose, custom_index_function):
        # TODO: Add logic to verify that index_column is valid. But where?
        f4py.print_message(f"Saving index for {f4_file_path} and {index_column}.", verbose)
        index_column_encoded = index_column.encode()

        with f4py.Parser(f4_file_path) as parser:
            f4py.print_message(f"Getting column meta information for {index_column} index for {f4_file_path}.", verbose)
            select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = parser._get_column_meta(set([index_column_encoded]), [])

            file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")
            index_column_type = column_type_dict[index_column_encoded]
            coords = column_coords_dict[index_column_encoded]
            values_positions = []
            decompressor = f4py.get_decompressor(decompression_type, decompressor)

            f4py.print_message(f"Parsing values and positions for {index_column} index for {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value = parser._parse_row_value(row_index, coords, line_length, file_handle, decompression_type, decompressor, bigram_size_dict, index_column_encoded)
                values_positions.append([value, row_index])
#            if len(column_decompression_dict) > 0:
                # recompression_dict = {0: {}}

 #               for row_index in range(parser.get_num_rows()):
#                    value = parser._parse_row_value(row_index, coords, line_length, file_handle, decompression_type, decompressor, bigram_size_dict, index_column_encoded)
                    #decompressed_value = f4py.decompress(value, column_decompression_dict[index_column_encoded], bigram_size)
                    # recompression_dict[0][decompressed_value] = value
                    #values_positions.append([decompressed_value, row_index])
#            else:
#                 for row_index in range(parser.get_num_rows()):
#                     value = parser._parse_row_value(row_index, coords, line_length, file_handle)
#                     values_positions.append([value, row_index])

            f4py.print_message(f"Building index file for {index_column} index for {f4_file_path}.", verbose)
            IndexBuilder._customize_values_positions(values_positions, [index_column_type], f4py.sort_first_column, custom_index_function)

            #if len(column_decompression_dict) > 0:
                # IndexBuilder._recompress_values(values_positions, [0], recompression_dict)

            index_file_path = IndexBuilder._get_index_file_path(parser.data_file_path, index_column, custom_index_function)
            IndexBuilder._save_index(values_positions, index_file_path)

        f4py.print_message(f"Done building index file for {index_column} index for {f4_file_path}.", verbose)

    # TODO: Combine this function with the above one and make it generic enough to handle indexes with more columns.
    def _build_two_column_index(f4_file_path, index_column_1, index_column_2, verbose):
        if not isinstance(index_column_1, str) or not isinstance(index_column_2, str):
            raise Exception("When specifying an index column name, it must be a string.")

        f4py.print_message(f"Saving index for {index_column_1} and {index_column_2} for {f4_file_path}.", verbose)

        index_name = "____".join([index_column_1, index_column_2])
        index_column_1_encoded = index_column_1.encode()
        index_column_2_encoded = index_column_2.encode()

        with f4py.Parser(f4_file_path) as parser:
            f4py.print_message(f"Getting column meta information for {index_name} index and {f4_file_path}.", verbose)
            select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = parser._get_column_meta(set([index_column_1_encoded, index_column_2_encoded]), [])
            # TODO: Add logic to verify that index_column_1 and index_column_2 are valid.

            file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")
            index_column_1_type = column_type_dict[index_column_1_encoded]
            index_column_2_type = column_type_dict[index_column_2_encoded]
            coords_1 = column_coords_dict[index_column_1_encoded]
            coords_2 = column_coords_dict[index_column_2_encoded]
            decompressor = f4py.get_decompressor(decompression_type, decompressor)

            values_positions = []
            f4py.print_message(f"Parsing values and positions for {index_name} index and {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value_1 = parser._parse_row_value(row_index, coords_1, line_length, file_handle, decompression_type, decompressor, bigram_size_dict, index_column_1_encoded)
                value_2 = parser._parse_row_value(row_index, coords_2, line_length, file_handle, decompression_type, decompressor, bigram_size_dict, index_column_2_encoded)

                values_positions.append([value_1, value_2, row_index])

            f4py.print_message(f"Building index file for {index_name} and {f4_file_path}.", verbose)
            IndexBuilder._customize_values_positions(values_positions, [index_column_1_type, index_column_2_type], f4py.sort_first_two_columns, f4py.do_nothing)

            index_file_path = IndexBuilder._get_index_file_path(parser.data_file_path, index_name)
            IndexBuilder._save_index(values_positions, index_file_path)

        f4py.print_message(f"Done building two-column index file for {index_name} and {f4_file_path}.", verbose)

    def _customize_values_positions(values_positions, column_types, sort_function, custom_index_function):
        # Iterate through each "column" except the last one (which has row_indices) and convert the data.
        for i in range(len(column_types)):
            conversion_function = f4py.get_conversion_function(column_types[i])

            # Iterate through each "row" in the data.
            for j in range(len(values_positions)):
                values_positions[j][i] = conversion_function(values_positions[j][i])
                values_positions[j][i] = custom_index_function(values_positions[j][i])

        # Sort the rows.
        sort_function(values_positions)

    # def _recompress_values(values_positions, column_indices, recompression_dict):
    #     for row_index in range(len(values_positions)):
    #         for column_index in column_indices:
    #             value = values_positions[row_index][column_index]
    #             if not isinstance(value, bytes):
    #                 value = str(value).encode()
    #
    #             compressed_value = recompression_dict[column_index][value]
    #             values_positions[row_index][column_index] = compressed_value

    def _save_index(values_positions, index_file_path):
        column_dict = {}
        for i in range(len(values_positions[0])):
            column_dict[i] = [x[i] if isinstance(x[i], bytes) else str(x[i]).encode() for x in values_positions]

        max_lengths = []
        for i in range(len(values_positions[0])):
            max_lengths.append(f4py.get_max_string_length(column_dict[i]))

        for i in range(len(values_positions[0])):
            column_dict[i] = f4py.format_column_items(column_dict[i], max_lengths[i])

        rows = []
        for row_num in range(len(column_dict[0])):
            row_value = b""

            for col_num in sorted(column_dict.keys()):
                row_value += column_dict[col_num][row_num]

            rows.append(row_value)

        column_coords_string, rows_max_length = f4py.build_string_map(rows)
        f4py.write_str_to_file(index_file_path, column_coords_string)

        column_start_coords = f4py.get_column_start_coords(max_lengths)
        column_coords_string, max_column_coord_length = f4py.build_string_map(column_start_coords)
        f4py.write_str_to_file(index_file_path + ".cc", column_coords_string)
        f4py.write_str_to_file(index_file_path + ".mccl", str(max_column_coord_length).encode())

        # Find and save the line length.
        f4py.write_str_to_file(index_file_path + ".ll", str(rows_max_length + 1).encode())

    def _get_index_file_path(data_file_path, index_name, custom_index_function=f4py.do_nothing):
        index_file_path_extension = f".idx_{index_name}"

        if custom_index_function != f4py.do_nothing:
            index_file_path_extension = f"{index_file_path_extension}_{custom_index_function.__name__}"

        return f"{data_file_path}{index_file_path_extension}"
import f4py
import fastnumbers
from itertools import chain
from joblib import Parallel, delayed
import math
import operator

class IndexHelper:
    # index_columns should be a list. Elements within it can be two-element lists.
    def build_indexes(f4_file_path, index_columns, compression_level=None, verbose=False):
        if isinstance(index_columns, str):
            IndexHelper._build_one_column_index(f4_file_path, index_columns, compression_level, verbose)
        elif isinstance(index_columns, list):
            for index_column in index_columns:
                if isinstance(index_column, list):
                    if len(index_column) != 2:
                        raise Exception("If you pass a list as an index_column, it must have exactly two elements.")

                    IndexHelper._build_two_column_index(f4_file_path, index_column[0], index_column[1], compression_level, verbose)
                else:
                    if not isinstance(index_column, str):
                        raise Exception("When specifying an index column name, it must be a string.")

                    IndexHelper._build_one_column_index(f4_file_path, index_column, compression_level, verbose, f4py.do_nothing)
        else:
            raise Exception("When specifying index_columns, it must either be a string or a list.")

    # This function is specifically for the EndsWithFilter.
    def build_endswith_index(f4_file_path, index_column, compression_level=None, verbose=False):
        IndexHelper._build_one_column_index(f4_file_path, index_column, compression_level, verbose, f4py.reverse_string)

    def _build_one_column_index(f4_file_path, index_column, compression_level, verbose, custom_index_function):
        f4py.print_message(f"Saving index for {f4_file_path} and {index_column}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")

        with f4py.Parser(f4_file_path) as parser:
            f4py.print_message(f"Getting column meta information for {index_column} index for {f4_file_path}.", verbose)
            ignore, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(f4py.NoFilter(), [index_column])
            #TODO: Add logic to verify that index_column is valid.

            file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            index_column_type = column_type_dict[column_index_dict[index_column.encode()]]
            coords = column_coords_dict[column_index_dict[index_column.encode()]]

            values_positions = []
            f4py.print_message(f"Parsing values and positions for {index_column} index for {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value = parser._parse_row_value(row_index, coords, line_length, file_handle)
                values_positions.append([value, row_index])

            f4py.print_message(f"Building index file for {index_column} index for {f4_file_path}.", verbose)
            IndexHelper._customize_values_positions(values_positions, [index_column_type], f4py.sort_first_column, custom_index_function)

            index_file_path = IndexHelper._get_index_file_path(parser.data_file_path, index_column, custom_index_function)
            IndexHelper._save_index(values_positions, index_file_path)

        f4py.print_message(f"Done building index file for {index_column} index for {f4_file_path}.", verbose)

    # TODO: Combine this function with the above one and make it generic enough to handle indexes with more columns.
    def _build_two_column_index(f4_file_path, index_column_1, index_column_2, compression_level, verbose):
        if not isinstance(index_column_1, str) or not isinstance(index_column_1, str):
            raise Exception("When specifying an index column name, it must be a string.")

        f4py.print_message(f"Saving index for {index_column_1} and {index_column_2} for {f4_file_path}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")
        index_name = "____".join([index_column_1, index_column_2])

        with f4py.Parser(f4_file_path) as parser:
            f4py.print_message(f"Getting column meta information for {index_name} index and {f4_file_path}.", verbose)
            ignore, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(f4py.NoFilter(), [index_column_1, index_column_2])
            #TODO: Add logic to verify that index_column is valid.

            file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            index_column_1_type = column_type_dict[column_index_dict[index_column_1.encode()]]
            index_column_2_type = column_type_dict[column_index_dict[index_column_2.encode()]]
            coords_1 = column_coords_dict[column_index_dict[index_column_1.encode()]]
            coords_2 = column_coords_dict[column_index_dict[index_column_2.encode()]]

            values_positions = []
            f4py.print_message(f"Parsing values and positions for {index_name} index and {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value_1 = parser._parse_row_value(row_index, coords_1, line_length, file_handle)
                value_2 = parser._parse_row_value(row_index, coords_2, line_length, file_handle)
                values_positions.append([value_1, value_2, row_index])

            f4py.print_message(f"Building index file for {index_name} and {f4_file_path}.", verbose)
            IndexHelper._customize_values_positions(values_positions, [index_column_1_type, index_column_2_type], f4py.sort_first_two_columns, f4py.do_nothing)

            index_file_path = IndexHelper._get_index_file_path(parser.data_file_path, index_name)
            IndexHelper._save_index(values_positions, index_file_path)

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

    def _save_index(values_positions, index_file_path):
        column_dict = {}
        for i in range(len(values_positions[0])):
            column_dict[i] = [str(x[i]).encode() for x in values_positions]

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
        f4py.write_str_to_file(index_file_path, "", column_coords_string)

        f4py.Builder()._save_meta_files(index_file_path, max_lengths, rows_max_length + 1)

    def _get_two_column_index_name(filter1, filter2):
        return "____".join([filter1.column_name.decode(), filter2.column_name.decode()])

    def _get_index_file_path(data_file_path, index_name, custom_index_function=f4py.do_nothing):
        index_file_path_extension = f".idx_{index_name}"

        if custom_index_function != f4py.do_nothing:
            index_file_path_extension = f"{index_file_path_extension}_{custom_index_function.__name__}"

        return f"{data_file_path}{index_file_path_extension}"

    def _get_index_parser(index_file_path):
        return f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"])

    def _get_identifier_row_index(index_parser, query_value, end_index, num_processes=1):
        if end_index == 0:
            return -1

        line_length = index_parser.get_stat(".ll")
        file_handle = index_parser.get_file_handle("")
        value_coords = index_parser._parse_data_coords([0])[0]
        position_coords = index_parser._parse_data_coords([1])[0]

        matching_position = IndexHelper._binary_identifier_search(index_parser, line_length, value_coords, file_handle, query_value, 0, end_index)

        if matching_position == -1:
            return -1

        matching_row_index = fastnumbers.fast_int(index_parser._parse_row_value(matching_position, position_coords, line_length, file_handle))

        return matching_row_index

    # Searches for a single matching value.
    def _binary_identifier_search(parser, line_length, value_coords, file_handle, value_to_find, l, r):
        if r == -1 or l > r:
            return -1

        mid = l + (r - l) // 2
        mid_value = parser._parse_row_value(mid, value_coords, line_length, file_handle)

        if mid_value == value_to_find:
            # If element is present at the middle itself
            return mid
        elif mid_value > value_to_find:
            return IndexHelper._binary_identifier_search(parser, line_length, value_coords, file_handle, value_to_find, l, mid-1)
        else:
            # Else the element can only be present in right subarray
            return IndexHelper._binary_identifier_search(parser, line_length, value_coords, file_handle, value_to_find, mid+1, r)

    def _filter_using_operator(index_file_path, compression_level, fltr, end_index, num_processes):
        if end_index == 0:
            return set()

        with IndexHelper._get_index_parser(index_file_path) as index_parser:
            line_length = index_parser.get_stat(".ll")
            coords = index_parser._parse_data_coords([0, 1])
            file_handle = index_parser.get_file_handle("")

            if fltr.oper == operator.eq:
                return IndexHelper._find_row_indices_for_range(index_parser, compression_level, coords[0], coords[1], fltr, fltr, end_index, num_processes)
            else:
                if fltr.oper == operator.ne:
                    lower_position, upper_position = IndexHelper._find_bounds_for_range(index_parser, compression_level, coords[0], fltr, fltr, end_index, num_processes)

                    lower_positions = (0, lower_position)
                    upper_positions = (upper_position, end_index)

                    lower_row_indices = IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], lower_positions, num_processes)
                    upper_row_indices = IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], upper_positions, num_processes)

                    return lower_row_indices | upper_row_indices
                else:
                    if fltr.oper == operator.gt:
                        positions = IndexHelper._find_positions_g(index_parser, line_length, coords[0], file_handle, fltr, 0, end_index, operator.le)
                    elif fltr.oper == operator.ge:
                        positions = IndexHelper._find_positions_g(index_parser, line_length, coords[0], file_handle, fltr, 0, end_index, operator.lt)
                    elif fltr.oper == operator.lt:
                        positions = IndexHelper._find_positions_l(index_parser, line_length, coords[0], file_handle, fltr, 0, end_index, fltr.oper)
                    elif fltr.oper == operator.le:
                        positions = IndexHelper._find_positions_l(index_parser, line_length, coords[0], file_handle, fltr, 0, end_index, fltr.oper)

                    return IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], positions, num_processes)

    def _find_positions_g(index_parser, line_length, value_coords, file_handle, fltr, start_index, end_index, all_false_operator):
        smallest_value = index_parser._parse_row_value(start_index, value_coords, line_length, file_handle)
        if smallest_value == b"":
            return start_index, end_index

        if not all_false_operator(fltr.get_conversion_function()(smallest_value), fltr.value):
            return start_index, end_index

        largest_value = index_parser._parse_row_value(end_index - 1, value_coords, line_length, file_handle)
        if largest_value == b"":
            return start_index, start_index

        matching_position = IndexHelper._search(index_parser, line_length, value_coords, file_handle, fltr, 0, end_index, all_false_operator)

        return matching_position + 1, end_index

    def _find_positions_l(index_parser, line_length, value_coords, file_handle, fltr, start_index, end_index, all_true_operator):
        smallest_value = index_parser._parse_row_value(start_index, value_coords, line_length, file_handle)
        if smallest_value == b"":
            return start_index, start_index

        if not all_true_operator(fltr.get_conversion_function()(smallest_value), fltr.value):
            return start_index, start_index

        largest_value = index_parser._parse_row_value(end_index - 1, value_coords, line_length, file_handle)
        if largest_value == b"":
            return start_index, end_index

        if all_true_operator(fltr.get_conversion_function()(largest_value), fltr.value):
            return start_index, end_index

        matching_position = IndexHelper._search(index_parser, line_length, value_coords, file_handle, fltr, 0, end_index, all_true_operator)

        return start_index, matching_position + 1

    #TODO: It should be feasible to combine this function with _search_with_filter
    #      to avoid duplicating similar code.
    def _search(index_parser, line_length, value_coords, file_handle, fltr, l, r, search_operator):
        mid = l + (r - l) // 2

        conversion_function = fltr.get_conversion_function()
        mid_value = conversion_function(index_parser._parse_row_value(mid, value_coords, line_length, file_handle))

        if search_operator(mid_value, fltr.value):
            next_value = index_parser._parse_row_value(mid + 1, value_coords, line_length, file_handle)

            # TODO: Does this work if we have a blank data value? Modify to be like search_with_filter?
            if next_value == b"":
                return mid
            elif not search_operator(conversion_function(next_value), fltr.value):
                return mid
            else:
                return IndexHelper._search(index_parser, line_length, value_coords, file_handle, fltr, mid, r, search_operator)
        else:
            return IndexHelper._search(index_parser, line_length, value_coords, file_handle, fltr, l, mid, search_operator)

    def _search_with_filter(index_parser, line_length, value_coords, file_handle, left_index, right_index, overall_end_index, fltr):
        mid_index = (left_index + right_index) // 2

        if mid_index == 0:
            return 0

        conversion_function = fltr.get_conversion_function()
        mid_value = conversion_function(index_parser._parse_row_value(mid_index, value_coords, line_length, file_handle))

        if fltr.passes(mid_value):
            if mid_index == right_index:
                return mid_index

            next_index = mid_index + 1

            if next_index == overall_end_index:
                return next_index

            next_value = conversion_function(index_parser._parse_row_value(next_index, value_coords, line_length, file_handle))

            if fltr.passes(next_value):
                return IndexHelper._search_with_filter(index_parser, line_length, value_coords, file_handle, next_index, right_index, overall_end_index, fltr)
            else:
                return mid_index + 1
        else:
            if left_index == mid_index:
                return mid_index

            return IndexHelper._search_with_filter(index_parser, line_length, value_coords, file_handle, left_index, mid_index, overall_end_index, fltr)

    def _find_matching_row_indices(index_file_path, position_coords, positions):
        # To make this paralellizable, we pass just a file path rather than index_parser.
        with IndexHelper._get_index_parser(index_file_path) as index_parser:
            line_length = index_parser.get_stat(".ll")
            file_handle = index_parser.get_file_handle("")

            matching_row_indices = set()
            for i in range(positions[0], positions[1]):
                matching_row_indices.add(fastnumbers.fast_int(index_parser._parse_row_value(i, position_coords, line_length, file_handle)))

            return matching_row_indices

    def _retrieve_matching_row_indices(index_parser, position_coords, positions, num_processes):
        # This is a rough threshold for determine whether it is worth the overhead to parallelize.
        num_indices = positions[1] - positions[0]

        if num_processes == 1 or num_indices < 100:
            return IndexHelper._find_matching_row_indices(index_parser.data_file_path, position_coords, positions)
        else:
            chunk_size = math.ceil(num_indices / num_processes)
            position_chunks = []
            for i in range(positions[0], positions[1], chunk_size):
                position_chunks.append((i, min(positions[1], i + chunk_size)))

            return set(chain.from_iterable(Parallel(n_jobs = num_processes)(delayed(IndexHelper._find_matching_row_indices)(index_parser.data_file_path, position_coords, position_chunk) for position_chunk in position_chunks)))

    def _find_bounds_for_range(index_parser, compression_level, value_coords, filter1, filter2, end_index, num_processes, start_index=0):
        line_length = index_parser.get_stat(".ll")
        file_handle = index_parser.get_file_handle("")

        lower_positions = IndexHelper._find_positions_g(index_parser, line_length, value_coords, file_handle, filter1, start_index, end_index, operator.lt)
        upper_positions = IndexHelper._find_positions_l(index_parser, line_length, value_coords, file_handle, filter2, lower_positions[0], lower_positions[1], operator.le)

        lower_position = max(lower_positions[0], upper_positions[0])
        upper_position = min(lower_positions[1], upper_positions[1])

        return lower_position, upper_position

    def _find_row_indices_for_range(index_parser, compression_level, value_coords, position_coords, filter1, filter2, end_index, num_processes):
        lower_position, upper_position = IndexHelper._find_bounds_for_range(index_parser, compression_level, value_coords, filter1, filter2, end_index, num_processes)

        return IndexHelper._retrieve_matching_row_indices(index_parser, position_coords, (lower_position, upper_position), num_processes)

    def _get_passing_row_indices(fltr, parser, line_length, coords_value, coords_position, file_handle, start_index, end_index):
        passing_row_indices = set()

        for i in range(start_index, end_index):
            if fltr.passes(parser._parse_row_value(i, coords_value, line_length, file_handle)):
                passing_row_indices.add(fastnumbers.fast_int(parser._parse_row_value(i, coords_position, line_length, file_handle)))

        return passing_row_indices

    def _get_passing_row_indices_for_with_filter(index_file_path, fltr, end_index, num_processes):
        with f4py.IndexHelper._get_index_parser(index_file_path) as index_parser:
            line_length = index_parser.get_stat(".ll")
            coords = index_parser._parse_data_coords([0, 1])
            file_handle = index_parser.get_file_handle("")

            lower_range = f4py.IndexHelper._find_positions_g(index_parser, line_length, coords[0], file_handle, fltr, 0, end_index, operator.lt)

            if lower_range[0] == end_index:
                return set()

            upper_position = IndexHelper._search_with_filter(index_parser, line_length, coords[0], file_handle, lower_range[0], lower_range[1], end_index, fltr)

            return f4py.IndexHelper._retrieve_matching_row_indices(index_parser, coords[1], (lower_range[0], upper_position), num_processes)

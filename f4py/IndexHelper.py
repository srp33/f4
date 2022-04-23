import f4py
import fastnumbers
from itertools import chain
from joblib import Parallel, delayed
import math #TODO: keep?
import operator

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
            elif index_column_type == "f":
                f4py.FloatIndexer(index_file_path, compression_level).build(values_positions)
            elif index_column_type == "i":
                f4py.IntIndexer(index_file_path, compression_level).build(values_positions)
            else: # u
                for i in range(len(values_positions)):
                    values_positions[i][0] = values_positions[i][0].decode()
                f4py.IdentifierIndexer(index_file_path, compression_level).build(values_positions)

        f4py.print_message(f"Done building index file for {index_column} index for {f4_file_path}.", verbose)

    def save_indices(f4_file_path, index_columns, compression_level=None, verbose=False):
        for index_column in index_columns:
            f4py.IndexHelper.save_index(f4_file_path, index_column, compression_level=compression_level)

    def save_funnel_index(f4_file_path, index_column_1, index_column_2, compression_level=None, verbose=False):
        #TODO: Make sure index_column_1 and index_column_2 are valid.

        index_name = IndexHelper._get_index_name([index_column_1, index_column_2])

        f4py.print_message(f"Saving index for {index_name} funnel index and {f4_file_path}.", verbose)

        num_rows = f4py.read_int_from_file(f4_file_path, ".nrow")

        with f4py.Parser(f4_file_path) as parser:
            # Get information about index columns.
            f4py.print_message(f"Getting column meta information for {index_name} funnel index and {f4_file_path}.", verbose)
            ignore, column_index_dict, column_type_dict, column_coords_dict = parser._get_column_meta(f4py.NoFilter(), [index_column_1, index_column_2])

            data_file_handle = parser.get_file_handle("")
            line_length = parser.get_stat(".ll")

            index_column_1_type = column_type_dict[column_index_dict[index_column_1.encode()]]
            index_column_2_type = column_type_dict[column_index_dict[index_column_2.encode()]]
            coords_1 = column_coords_dict[column_index_dict[index_column_1.encode()]]
            coords_2 = column_coords_dict[column_index_dict[index_column_2.encode()]]

            values_positions = []
            f4py.print_message(f"Parsing values and positions for {index_name} funnel index and {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value_1 = parser._parse_row_value(row_index, coords_1, line_length, data_file_handle)
                value_2 = parser._parse_row_value(row_index, coords_2, line_length, data_file_handle)
                values_positions.append([value_1, value_2, row_index])

            # This needs to be a combined file path.
            index_file_path = IndexHelper._get_index_file_path(parser.data_file_path, index_name)

            f4py.print_message(f"Building funnel index file for {index_name} and {f4_file_path}.", verbose)
            if index_column_1_type == "c" and index_column_2_type == "i":
                f4py.FunnelIndexer(index_file_path, compression_level).build(values_positions)
            else:
                raise Exception("Funnel indices are currently only supported for categorical + numeric columns.")

        f4py.print_message(f"Done building funnel index file for {index_name} and {f4_file_path}.", verbose)

    def _get_index_name(column_names):
        index_name = "____".join(column_names)

    def _get_index_file_path(f4_file_path, index_name):
        index_file_path_extension = f".idx_{index_name}"
        return f"{f4_file_path}{index_file_path_extension}"

    def _find_positions(index_file_path, fltr, end_index, conversion_function):
        with f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as index_parser:
            line_length = index_parser.get_stat(".ll")
            data_file_handle = index_parser.get_file_handle("")
            value_coords = index_parser._parse_data_coords([0])[0]

            if fltr.operator == operator.gt:
                positions = f4py.IndexHelper._find_positions_g(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.le, conversion_function)
            elif fltr.operator == operator.ge:
                positions = f4py.IndexHelper._find_positions_g(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.lt, conversion_function)
            elif fltr.operator == operator.lt:
                positions = f4py.IndexHelper._find_positions_l(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.ge, conversion_function)
            elif fltr.operator == operator.le:
                positions = f4py.IndexHelper._find_positions_l(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, fltr.operator, operator.gt, conversion_function)

            return positions

    def _find_positions_g(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, all_true_operator, all_false_operator, conversion_function):
        smallest_value = conversion_function(index_parser._parse_row_value(0, value_coords, line_length, data_file_handle))
        if all_true_operator(smallest_value, fltr.value):
            return 0, end_index

        largest_value = conversion_function(index_parser._parse_row_value(end_index - 1, value_coords, line_length, data_file_handle))
        if not all_true_operator(largest_value, fltr.value):
            return 0, 0

        matching_position = f4py.IndexHelper._search(index_parser, line_length, value_coords, data_file_handle, fltr.value, 0, end_index, all_false_operator, conversion_function)

        return matching_position + 1, end_index

    def _find_positions_l(index_parser, line_length, value_coords, data_file_handle, fltr, end_index, all_true_operator, all_false_operator, conversion_function):
        smallest_value = conversion_function(index_parser._parse_row_value(0, value_coords, line_length, data_file_handle))
        if not all_true_operator(smallest_value, fltr.value):
            return 0, 0

        largest_value = conversion_function(index_parser._parse_row_value(end_index - 1, value_coords, line_length, data_file_handle), conversion_function)
        if all_true_operator(largest_value, fltr.value):
            return 0, end_index

        matching_position = f4py.IndexHelper._search(index_parser, line_length, value_coords, data_file_handle, fltr.value, 0, end_index, all_true_operator, conversion_function)

        return 0, matching_position + 1

    def _search(index_parser, line_length, value_coords, data_file_handle, value_to_find, l, r, search_operator, conversion_function):
        mid = l + (r - l) // 2

        mid_value = conversion_function(index_parser._parse_row_value(mid, value_coords, line_length, data_file_handle))

        if search_operator(mid_value, value_to_find):
            next_value = index_parser._parse_row_value(mid + 1, value_coords, line_length, data_file_handle)

            if next_value == b"":
                return mid
            elif not search_operator(conversion_function(next_value), value_to_find):
                return mid
            else:
                return f4py.IndexHelper._search(index_parser, line_length, value_coords, data_file_handle, value_to_find, mid, r, search_operator, conversion_function)
        else:
            return f4py.IndexHelper._search(index_parser, line_length, value_coords, data_file_handle, value_to_find, l, mid, search_operator, conversion_function)

    def _retrieve_matching_row_indices(index_file_path, positions, num_processes):
        # This is a rough threshold for determine whether it is worth the overhead to parallelize.
        num_indices = positions[1] - positions[0]
        if num_processes == 1 or num_indices < 100:
            return f4py.IndexHelper._find_matching_row_indices(index_file_path, positions)
        else:
            chunk_size = math.ceil(num_indices / num_processes)
            position_chunks = []
            for i in range(positions[0], positions[1], chunk_size):
                position_chunks.append((i, min(positions[1], i + chunk_size)))

            return set(chain.from_iterable(Parallel(n_jobs = num_processes)(delayed(f4py.IndexHelper._find_matching_row_indices)(index_file_path, position_chunk) for position_chunk in position_chunks)))

    def _find_matching_row_indices(index_file_path, positions):
        with f4py.Parser(index_file_path, fixed_file_extensions=["", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as index_parser:
            line_length = index_parser.get_stat(".ll")
            data_file_handle = index_parser.get_file_handle("")
            position_coords = index_parser._parse_data_coords([1])[0]

            matching_row_indices = set()
            for i in range(positions[0], positions[1]):
                matching_row_indices.add(fastnumbers.fast_int(index_parser._parse_row_value(i, position_coords, line_length, data_file_handle)))

            return matching_row_indices

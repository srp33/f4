#from joblib import Parallel, delayed
from Helper import *
from Parser import *
import zstandard

class Indexer:
    def __init__(self, f4_file_path, index_columns, compress=True, verbose=False):
        self.__f4_file_path = f4_file_path
        self.__index_columns = [x.encode() for x in index_columns]
        self.__compress = compress
        self.__verbose = verbose

    def save(self, num_processes=1):
        self.__print_message(f"Building index for {self.__f4_file_path}.")

        num_rows = read_int_from_file(self.__f4_file_path, ".nrow")

        parser = Parser(self.__f4_file_path)

        try:
            compressor = None
            if self.__compress:
                compressor = zstandard.ZstdCompressor(level=1)

            # Find coordinates of index columns.
            index_column_indices = parser.get_column_indices(self.__index_columns)
            index_column_coords = parser._parse_data_coords(index_column_indices)

            # Store index column data.
            max_line_length = 0
            with open(f"{self.__f4_file_path}.idx", 'wb') as index_file:
                for row_index in range(num_rows):
                    row = b"".join(parser.parse_row_values(row_index, index_column_coords))

                    if self.__compress:
                        row = compressor.compress(row)
                    else:
                        row += b"\n"

                    max_line_length = max([max_line_length, len(row)])
                    index_file.write(row)

            # Save the length of the longest row. Row lengths may vary when compression is used.
            write_string_to_file(f"{self.__f4_file_path}", ".idx.ll", str(max_line_length).encode())

            # Indicate the number of index rows and columns.
            write_string_to_file(f"{self.__f4_file_path}", ".idx.nrow", str(num_rows).encode())
            write_string_to_file(f"{self.__f4_file_path}", ".idx.ncol", str(len(self.__index_columns)).encode())

            # Calculate and save the index column coordinates and max length of these coordinates.
            index_column_sizes = []
            for coord in index_column_coords:
                index_column_sizes.append(coord[1] - coord[0])
            index_column_start_coords = get_column_start_coords(index_column_sizes)
            column_coords_string, max_column_coord_length = build_string_map(index_column_start_coords)
            write_string_to_file(self.__f4_file_path, ".idx.cc", column_coords_string)
            write_string_to_file(self.__f4_file_path, ".idx.mccl", str(max_column_coord_length).encode())

            # Save the index column names and max length of these names.
            index_column_names_string, max_col_name_length = build_string_map(self.__index_columns)
            write_string_to_file(self.__f4_file_path, ".idx.cn", index_column_names_string)
            write_string_to_file(self.__f4_file_path, ".idx.mcnl", str(max_col_name_length).encode())

            # Save the index column types.
            index_column_types = [parser.get_column_type_from_index(i).encode() for i in index_column_indices]
            index_column_types_string, max_col_type_length = build_string_map(index_column_types)
            write_string_to_file(self.__f4_file_path, ".idx.ct", index_column_types_string)

            # Indicate whether the index is compressed.
            write_string_to_file(self.__f4_file_path, ".idx.cmp", str(self.__compress).encode())
        finally:
            parser.close()

    def __print_message(self, message):
        print(message, self.__verbose)

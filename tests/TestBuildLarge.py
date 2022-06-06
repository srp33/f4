import cProfile
import glob
import os
import f4py
import pstats
from pstats import SortKey
import time

def build(in_file_path, tall_or_wide, num_processes, num_cols_per_chunk, num_rows_per_save, index_columns, compression_level, use_compression_dict):
    out_file_prefix = tall_or_wide + "_"
    print2(tall_or_wide)

    if index_columns:
        out_file_prefix += "indexed_"
        print2("Yes")
    else:
        out_file_prefix += "notindexed_"
        print2("No")

    out_file_prefix += f"{compression_level}_"
    print2(f"{compression_level}")

    if use_compression_dict:
        out_file_prefix += "cmpd"
        print2("Yes")
    else:
        out_file_prefix += "nocmpd"
        print2("No")

    f4_file_path = f"data/{out_file_prefix}.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    start = time.time()

    verbose = False
    f4py.Builder(verbose=verbose).convert_delimited_file(in_file_path, f4_file_path, index_columns, delimiter="\t", compression_level=compression_level, build_compression_dictionary=use_compression_dict, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=f"/tmp/{out_file_prefix}")
    #f4py.Builder(verbose=verbose).convert_delimited_file(in_file_path, f4_file_path, index_columns, delimiter="\t", compression_level=compression_level, build_compression_dictionary=use_compression_dict, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=f"/tmp/{out_file_prefix}", cache_dir_path=f"/tmp/{out_file_prefix}")

    if index_columns != None:
        f4py.IndexHelper.build_endswith_index(f4_file_path, index_columns[0], verbose=verbose)

    end = time.time()
    elapsed = f"{round(end - start, 3)}"

    print2(f"{get_output_size(f4_file_path)}")
    print2(f"{num_processes}")
    print2(f"{elapsed}", end="\n")

def print2(output, end="\t"):
    print(output, end=end)

def get_output_size(f4_file_path):
    total_size = 0
    for file_path in glob.glob(f"{f4_file_path}*"):
        total_size += os.path.getsize(file_path)
    return total_size

print(f"Shape\tIndexed\tCompression_Level\tUse_Compression_Dict\tOverall_Size\tNum_Processes\tElapsed_Seconds")

#for num_processes in [1, 2, 4, 8, 16, 32]:
for num_processes in [32]:
    #build("data/tall.tsv", "tall", num_processes, 51, 10001, index_columns=None, compression_level=None, use_compression_dict=False)
    #build("data/wide.tsv", "wide", num_processes, 50001, 51, index_columns=None, compression_level=None, use_compression_dict=False)

    build("data/tall.tsv", "tall", num_processes, 51, 10001, index_columns=["Discrete100", "Numeric900"], compression_level=None, use_compression_dict=False)
    #build("data/wide.tsv", "wide", num_processes, 50001, 51, index_columns=["Discrete100000", "Numeric900000"], compression_level=None, use_compression_dict=False)

    #for compression_level in [1, 22]:
    #    for use_compression_dict in [False, True]:
    #        build("data/tall.tsv", "tall", num_processes, 51, 10001, index_columns=["Discrete100", "Numeric900"], compression_level=compression_level, use_compression_dict=use_compression_dict)
    #        build("data/wide.tsv", "wide", num_processes, 50001, 51, index_columns=["Discrete100000", "Numeric900000"], compression_level=compression_level, use_compression_dict=use_compression_dict)

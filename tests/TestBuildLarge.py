import cProfile
import glob
import os
import f4py
import pstats
from pstats import SortKey
import time

def build(in_file_path, tall_or_wide, num_processes, num_cols_per_chunk, num_rows_per_save, index_columns, compression_level):
    out_file_prefix = tall_or_wide + "_"

    if index_columns:
        out_file_prefix += "indexed_"
    else:
        out_file_prefix += "notindexed_"

    if compression_level:
        out_file_prefix += "compressed"
    else:
        out_file_prefix += "notcompressed"

    f4_file_path = f"data/{out_file_prefix}.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    start = time.time()
    f4py.Builder(verbose=False).convert_delimited_file(in_file_path, f4_file_path, index_columns, delimiter="\t", compression_level=compression_level, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=f"/tmp/{out_file_prefix}", cache_dir_path=f"/tmp/{out_file_prefix}")
    #f4py.Builder(verbose=True).convert_delimited_file(in_file_path, f4_file_path, index_columns, delimiter="\t", compression_level=compression_level, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=f"/tmp/{out_file_prefix}", cache_dir_path=f"/tmp/{out_file_prefix}")
    end = time.time()
    elapsed = f"{round(end - start, 3)}"

    output = f"{tall_or_wide}\t"
    if index_columns:
        output += "Yes\t"
    else:
        output += "No\t"
    if compression_level:
        output += "Yes\t"
    else:
        output += "No\t"

    output += f"{num_processes}\t{elapsed}"

    print(output)

print(f"Shape\tIndexed\tCompressed\tNum_Processes\tElapsed_Seconds")

#for num_processes in [1, 2, 4, 8, 16, 32]:
for num_processes in [30]:
    build("data/tall.tsv", "tall", num_processes, 51, 10001, index_columns=None, compression_level=None)
    build("data/wide.tsv", "wide", num_processes, 50001, 51, index_columns=None, compression_level=None)

    build("data/tall.tsv", "tall", num_processes, 51, 10001, index_columns=["Discrete100", "Numeric900"], compression_level=None)
    build("data/wide.tsv", "wide", num_processes, 50001, 51, index_columns=["Discrete100000", "Numeric900000"], compression_level=None)

    build("data/tall.tsv", "tall", num_processes, 51, 10001, index_columns=["Discrete100", "Numeric900"], compression_level=22)
    build("data/wide.tsv", "wide", num_processes, 50001, 51, index_columns=["Discrete100000", "Numeric900000"], compression_level=22)

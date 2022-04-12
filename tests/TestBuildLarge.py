import glob
import os
import f4py

def run_test(in_file_path, out_file_prefix, num_processes, num_cols_per_chunk, index_columns, compression_level):
    f4_file_path = f"data/{out_file_prefix}.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4py.Builder(verbose=True).convert_delimited_file(in_file_path, f4_file_path, delimiter="\t", compression_level=compression_level, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk)

    if index_columns:
        for index_column in index_columns:
            f4py.IndexHelper.save_index(f4_file_path, index_column, compression_level=compression_level)

#run_test("data/tall.tsv", "tall", 20, 51, index_columns=None, compression_level=None) #3:52
#run_test("data/tall.tsv.gz", "tall", 20, 51, index_columns=None, compression_level=None) #6:28

#run_test("data/wide.tsv", "wide", 20, 50001, index_columns=None, compression_level=None) #5:08
#run_test("data/wide.tsv.gz", "wide", 20, 50001, index_columns=None, compression_level=None) #7:21

#run_test("data/tall.tsv", "tall_indexed", 20, 51, index_columns=["Discrete100", "Numeric900"], compression_level=None) #4:10
#run_test("data/wide.tsv", "wide_indexed", 20, 50001, index_columns=["Discrete100", "Numeric900"], compression_level=None) #5:11

run_test("data/tall.tsv", "tall_indexed_compressed", 20, 51, index_columns=["Discrete100", "Numeric900"], compression_level=22) #3:40
#run_test("data/wide.tsv", "wide_indexed_compressed", 20, 50001, index_columns=["Discrete100", "Numeric900"], compression_level=22) #4:13

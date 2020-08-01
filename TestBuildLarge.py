import glob
import os
from Builder import *
from Parser import *

def run_test(in_file_path, out_file_prefix, num_processes, num_cols_per_chunk, index_columns, compress):
    f4_file_path = f"data/{out_file_prefix}.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    Builder(in_file_path, f4_file_path, index_columns=index_columns, compress=compress, verbose=True).convert(num_processes, num_cols_per_chunk)

##run_test("test_data.tsv", "test", 4, 3, index_columns=None, compress=False)

#run_test("tall.tsv", "tall", 20, 51, index_columns=None, compress=False) #3:11
#run_test("tall.tsv.gz", "tall", 20, 51, index_columns=None, compress=False) #5:47

#run_test("wide.tsv", "wide", 20, 50001, index_columns=None, compress=False) #4:06
#run_test("wide.tsv.gz", "wide", 20, 50001, index_columns=None, compress=False) #6:30

#run_test("tall.tsv", "tall_indexed", 20, 51, index_columns=["Discrete100", "Numeric900"], compress=False) #3:15
#run_test("wide.tsv", "wide_indexed", 20, 50001, index_columns=["Discrete100", "Numeric900"], compress=False) #4:10

#run_test("tall.tsv", "tall_indexed_compressed", 20, 51, index_columns=["Discrete100", "Numeric900"], compress=True) #3:40
#run_test("wide.tsv", "wide_indexed_compressed", 20, 50001, index_columns=["Discrete100", "Numeric900"], compress=True) #4:13

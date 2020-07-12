import glob
import os
from Builder import *
from Parser import *

def run_test(in_file_path, out_file_prefix, num_processes, num_cols_per_chunk):
    f4_file_path = f"data/{out_file_prefix}.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    convert_delimited_file_to_f4(in_file_path, f4_file_path, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk)

##run_test("test_data.tsv", "test", 4, 3)

#run_test("tall.tsv", "tall", 20, 51) #3:13
#run_test("tall.tsv.gz", "tall", 20, 51) #5:47

#run_test("wide.tsv", "wide", 20, 50001) #4:20
#run_test("wide.tsv.gz", "wide", 20, 50001) #6:30

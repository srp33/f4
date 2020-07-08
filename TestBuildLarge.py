import glob
import os
from Builder import *
from Parser import *

def run_test(in_file_path, num_processes, lines_per_chunk):
    f4_file_path = "data/tall.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    convert_delimited_file_to_f4(in_file_path, f4_file_path, num_processes=num_processes, lines_per_chunk=lines_per_chunk)

#run_test("tall.tsv", 20, 1000) #3:09
#run_test("tall.tsv.gz", 20, 1000) #4:26

run_test("wide.tsv", 4, 10)
#run_test("wide.tsv.gz", 20, 10)

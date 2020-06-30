import glob
import os
from Builder import *
from Parser import *

def run_test(in_file_path):
    f4_file_path = "data/test2.f4"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    convert_delimited_file_to_f4(in_file_path, f4_file_path, num_processes=20)

#run_test("tall.tsv") #3:09
#run_test("tall.tsv.gz") #4:26

import glob
import operator
import os
from Builder import *
from Parser import *

def run_test(f4_file_prefix, num_processes, lines_per_chunk):
    parser = Parser(f"data/{f4_file_prefix}.f4")

    fltr = AndFilter(OrFilter(LikeFilter("Discrete100", r"^A"), LikeFilter("Discrete100", r"Z$")), NumericFilter("Numeric900", operator.ge, 0.1))
    parser.query_and_save(fltr, ["ID", "Discrete1", "Discrete100", "Numeric1", "Numeric900"], f"data/{f4_file_prefix}.tsv", out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

#run_test("tall", 4, 1000)
#run_test("wide", 4, 10)

import glob
import operator
import os
from Builder import *
from Parser import *

def run_test(f4_file_path):
    convert_delimited_file_to_f4(in_file_path, f4_file_path, num_processes=20)

parser = Parser("data/test2.f4")

filters = [CategoricalFilter("Discrete1", ["AA", "BB", "CC"]), CategoricalFilter("Discrete100", ["XX", "YY", "ZZ"]), NumericFilter("Numeric1", operator.ge, 0.1), NumericFilter("Numeric900", operator.ge, 0.1)]

# >= 0.1:
#if value.startswith(b"A") or value.endswith(b"Z"):
parser.query_and_save(filters, ["ID", "Discrete1", "Discrete100", "Numeric1", "Numeric900"], "/tmp/f4.tsv", out_file_type="tsv", num_processes=20, lines_per_chunk=1000)

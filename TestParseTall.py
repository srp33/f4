import glob
import operator
import os
from Builder import *
from Parser import *

parser = Parser("data/tall.f4")
num_processes = 4
lines_per_chunk = 1000
#lines_per_chunk = 10000

fltr = AndFilter(OrFilter(LikeFilter("Discrete100", r"^A"), LikeFilter("Discrete100", r"Z$")), NumericFilter("Numeric900", operator.ge, 0.1))

parser.query_and_save(fltr, ["ID", "Discrete1", "Discrete100", "Numeric1", "Numeric900"], "data/tall_query.tsv", out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

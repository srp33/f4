import cProfile
import glob
import operator
import os
import pstats
from pstats import SortKey
from f4py.Builder import *
from f4py.Parser import *

def run_test(f4_file_prefix, num_processes, lines_per_chunk):
    parser = Parser(f"data/{f4_file_prefix}.f4")
    fltr = f4py.AndFilter(f4py.OrFilter(f4py.StartsWithFilter("Discrete100", "A"), f4py.EndsWithFilter("Discrete100", "Z")), f4py.NumericFilter("Numeric900", operator.ge, 0.1))
    parser.query_and_save(fltr, ["Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"], f"data/{f4_file_prefix}_filtered.tsv", out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

#def temp():
#    #run_tall_test(4, 10)
#    run_wide_test(1, 10)
#profile = cProfile.Profile()
#profile.runcall(temp)
#ps = pstats.Stats(profile)
#ps.print_stats()

#run_test("tall", 1, 10000) #3.329, 3.8
#run_test("tall", 4, 10000) #2.156
#run_test("wide", 1, 10) #0.412, 0.197
#run_test("wide", 4, 10) #0.598, 0.188

#run_test("tall_indexed", 1, 10000) #1.248
#run_test("tall_indexed", 4, 10000) #
#run_test("wide_indexed", 1, 10) #0.177
#run_test("wide_indexed", 4, 10) #0.177

#run_test("tall_indexed_compressed", 1, 10000) #1.283
#run_test("tall_indexed_compressed", 4, 10000) #
#run_test("wide_indexed_compressed", 1, 10) #0.175
run_test("wide_indexed_compressed", 4, 10) #0.181

import cProfile
import glob
import operator
import os
import pstats
from pstats import SortKey
from f4py.Builder import *
from f4py.Parser import *

def run_tall_test(num_processes, lines_per_chunk):
    f4_file_prefix = "tall"

    parser = Parser(f"data/{f4_file_prefix}.f4")
    fltr = AndFilter(OrFilter(StartsWithFilter("Discrete100", "A"), EndsWithFilter("Discrete100", "Z")), NumericFilter("Numeric900", operator.ge, 0.1))
    parser.query_and_save(fltr, ["Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"], f"data/{f4_file_prefix}_filtered.tsv", out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

def run_wide_test(num_processes, lines_per_chunk):
    f4_file_prefix = "wide"

    parser = Parser(f"data/{f4_file_prefix}.f4")
    fltr = AndFilter(OrFilter(StartsWithFilter("Discrete100", "A"), EndsWithFilter("Discrete100", "Z")), NumericFilter("Numeric900", operator.ge, 0.1))
    parser.query_and_save(fltr, ["Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"], f"data/{f4_file_prefix}_filtered.tsv", out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

#def temp():
#    #run_tall_test(4, 10)
#    run_wide_test(1, 10)
#profile = cProfile.Profile()
#profile.runcall(temp)
#ps = pstats.Stats(profile)
#ps.print_stats()

#run_tall_test(1, 10000) #3.329
#run_tall_test(4, 10000) #2.156
run_wide_test(1, 10) #0.412
#run_wide_test(4, 10) #0.598

#run_test("tall_indexed", 4, 10) #3.536
#run_test("wide_indexed", 4, 10) #3.176

#run_test("tall_indexed_compressed", 4, 10) #5.251
#run_test("wide_indexed_compressed", 4, 10) #3.921

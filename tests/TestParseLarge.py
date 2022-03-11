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

    #TODO:
    #fltr = AndFilter(OrFilter(LikeFilter("Discrete100", r"^A"), LikeFilter("Discrete100", r"Z$")), NumericFilter("Numeric900", operator.ge, 0.1))
    fltr = AndFilter(OrFilter(StartsWithFilter("Discrete100", "A"), EndsWithFilter("Discrete100", "Z")), NumericFilter("Numeric900", operator.ge, 0.1))

    parser.query_and_save(fltr, ["Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"], f"data/{f4_file_prefix}_filtered.tsv", out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

#def temp():
#    run_test("tall", 4, 10) #3.849
#profile = cProfile.Profile()
#profile.runcall(temp)
#ps = pstats.Stats(profile)
#ps.print_stats()

run_test("tall", 4, 10) #3.849
#run_test("wide", 4, 10) #3.248

#run_test("tall_indexed", 4, 10) #3.536
#run_test("wide_indexed", 4, 10) #3.176

#run_test("tall_indexed_compressed", 4, 10) #5.251
#run_test("wide_indexed_compressed", 4, 10) #3.921

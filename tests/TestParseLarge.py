import cProfile
import f4py
import glob
import operator
import os
import pstats
from pstats import SortKey
import sys
import time

def run_test(description, tall_or_wide, select_columns, discrete_filter1, discrete_filter2, float_filter, indexed, compressed, num_processes, lines_per_chunk, expected_size):
    f4_file_path = f"data/{tall_or_wide}_"
    if indexed:
        f4_file_path += "indexed_"
    else:
        f4_file_path += "notindexed_"
    if compressed:
        f4_file_path += "compressed.f4"
    else:
        f4_file_path += "notcompressed.f4"

    out_file_path = f"/tmp/{os.path.basename(f4_file_path)}"

    start = time.time()

    fltr = f4py.AndFilter(f4py.OrFilter(discrete_filter1, discrete_filter2), float_filter)

    with f4py.Parser(f4_file_path) as parser:
        parser.query_and_save(fltr, select_columns, out_file_path, out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

    file_size = os.path.getsize(out_file_path)
    if file_size != expected_size:
        print(f"ERROR: Size of {out_file_path} was {file_size}, but it was expected to be {expected_size}.")
        sys.exit()

    end = time.time()
    elapsed = f"{round(end - start, 3)}"

    output = f"{description}\t{tall_or_wide}\t"
    if indexed:
        output += "Yes\t"
    else:
        output += "No\t"
    if compressed:
        output += "Yes\t"
    else:
        output += "No\t"

    output += f"{num_processes}\t{elapsed}"

    print(output)

def run_tests(description, tall_discrete_filter1, tall_discrete_filter2, tall_float_filter, wide_discrete_filter1, wide_discrete_filter2, wide_float_filter, num_processes, tall_expected_size, wide_expected_size):
    run_test(description, "tall", tall_select_columns, tall_discrete_filter1, tall_discrete_filter2, tall_float_filter, False, False, num_processes, 10000, tall_expected_size)
    run_test(description, "wide", wide_select_columns, wide_discrete_filter1, wide_discrete_filter2, wide_float_filter, False, False, num_processes, 10, wide_expected_size)

    run_test(description, "tall", tall_select_columns, tall_discrete_filter1, tall_discrete_filter2, tall_float_filter, True, False, num_processes, 10000, tall_expected_size)
    run_test(description, "wide", wide_select_columns, wide_discrete_filter1, wide_discrete_filter2, wide_float_filter, True, False, num_processes, 10, wide_expected_size)

    run_test(description, "tall", tall_select_columns, tall_discrete_filter1, tall_discrete_filter2, tall_float_filter, True, True, num_processes, 10000, tall_expected_size)
    run_test(description, "wide", wide_select_columns, wide_discrete_filter1, wide_discrete_filter2, wide_float_filter, True, True, num_processes, 10, wide_expected_size)

tall_select_columns = ["ID", "Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"]
wide_select_columns = ["ID"] + [f"Discrete{i}" for i in range(100, 100001, 100)] + [f"Numeric{i}" for i in range(100, 900001, 100)]

#########################################################
# Debugging code
#########################################################

#fltr = f4py.StartsWithFilter("Discrete100", "1")
#fltr = f4py.StartsWithFilter("Discrete100", "A")
#fltr = f4py.StartsWithFilter("Discrete100", "B")
#fltr = f4py.StartsWithFilter("Discrete100", "Z")
#fltr = f4py.StartsWithFilter("Discrete100", "ZZ")
#fltr = f4py.StartsWithFilter("Discrete100", "ZZZ")

#f4_file_path = "data/tall_"
#f4_file_path += "indexed_"
#f4_file_path += "notcompressed.f4"

#start = time.time()

#with f4py.Parser(f4_file_path) as parser:
#    parser.query_and_save(fltr, tall_select_columns, "/tmp/1", out_file_type="tsv", num_processes=8, lines_per_chunk=1000)

#end = time.time()
#print(f"{round(end - start, 3)} seconds")
#import sys
#sys.exit()

#########################################################
# Performance profiling code
#########################################################

#def temp():
#    run_test("tall", tall_select_columns, "Discrete100", "Numeric900", True, True, 8, 10000)
#profile = cProfile.Profile()
#profile.runcall(temp)
#ps = pstats.Stats(profile)
#ps.print_stats()

print(f"Description\tShape\tIndexed\tCompressed\tNum_Processes\tElapsed_Seconds")

#for num_processes in [1, 2, 4, 8, 16, 32]:
for num_processes in [1, 4, 8]:
#for num_processes in [8]:
    tall_discrete_filter1 = f4py.StartsWithFilter("Discrete100", "A")
    wide_discrete_filter1 = f4py.StartsWithFilter("Discrete100000", "A")

    tall_discrete_filter2 = f4py.EndsWithFilter("Discrete100", "Z")
    wide_discrete_filter2 = f4py.EndsWithFilter("Discrete100000", "Z")

    tall_float_filter = f4py.FloatFilter("Numeric900", operator.ge, 0.1)
    wide_float_filter = f4py.FloatFilter("Numeric900000", operator.ge, 0.1)

    run_tests("EndsStartsWith", tall_discrete_filter1, tall_discrete_filter2, tall_float_filter, wide_discrete_filter1, wide_discrete_filter2, wide_float_filter, num_processes, 7613257, 7177264)

    tall_discrete_filter1 = f4py.StringFilter("Discrete100", operator.eq, "AA")
    wide_discrete_filter1 = f4py.StringFilter("Discrete100000", operator.eq, "AA")

    tall_discrete_filter2 = f4py.StringFilter("Discrete100", operator.eq, "ZZ")
    wide_discrete_filter2 = f4py.StringFilter("Discrete100000", operator.eq, "ZZ")

    run_tests("AA_ZZ", tall_discrete_filter1, tall_discrete_filter2, tall_float_filter, wide_discrete_filter1, wide_discrete_filter2, wide_float_filter, num_processes, 289122, 342803)

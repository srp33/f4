import cProfile
import f4py
import glob
import operator
import os
import pstats
from pstats import SortKey
import time

def run_test(tall_or_wide, indexed, compressed, num_processes, lines_per_chunk):
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

    parser = f4py.Parser(f4_file_path)

    #fltr = f4py.OrFilter(f4py.NumericFilter("Numeric900", operator.ge, 0.1), f4py.NoFilter())
    #fltr = f4py.OrFilter(f4py.StartsWithFilter("Discrete100", "A"), f4py.EndsWithFilter("Discrete100", "Z"))
    fltr = f4py.AndFilter(f4py.OrFilter(f4py.StartsWithFilter("Discrete100", "A"), f4py.EndsWithFilter("Discrete100", "Z")), f4py.NumericFilter("Numeric900", operator.ge, 0.1))

    parser.query_and_save(fltr, ["Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"], out_file_path, out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

    end = time.time()
    elapsed = f"{round(end - start, 3)}"

    output = f"{tall_or_wide}\t"
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

#def temp():
#    #run_tall_test(4, 10)
#    run_wide_test(1, 10)
#profile = cProfile.Profile()
#profile.runcall(temp)
#ps = pstats.Stats(profile)
#ps.print_stats()

print(f"Shape\tIndexed\tCompressed\tNum_Processes\tElapsed_Seconds")

run_test("tall", False, False, 1, 10000)
run_test("tall", False, False, 4, 10000)
run_test("tall", False, False, 16, 10000)
run_test("wide", False, False, 1, 10)
run_test("wide", False, False, 4, 10)
run_test("wide", False, False, 16, 10)

run_test("tall", True, False, 1, 10000)
run_test("tall", True, False, 4, 10000)
run_test("tall", True, False, 16, 10000)
run_test("wide", True, False, 1, 10)
run_test("wide", True, False, 4, 10)
run_test("wide", True, False, 16, 10)

run_test("tall", True, True, 1, 10000)
run_test("tall", True, True, 4, 10000)
run_test("tall", True, True, 16, 10000)
run_test("wide", True, True, 1, 10)
run_test("wide", True, True, 4, 10)
run_test("wide", True, True, 16, 10)

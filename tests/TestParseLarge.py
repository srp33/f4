import cProfile
import f4py
import glob
import operator
import os
import pstats
from pstats import SortKey
import sys
import time

def run_test(tall_or_wide, select_columns, discrete_filter_column, numeric_filter_column, indexed, compressed, num_processes, lines_per_chunk):
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

    #fltr = f4py.OrFilter(f4py.NumericFilter(numeric_filter_column, operator.ge, 0.1), f4py.NoFilter())
    #fltr = f4py.OrFilter(f4py.StartsWithFilter(discrete_filter_column, "A"), f4py.EndsWithFilter(discrete_filter_column, "Z"))
    fltr = f4py.AndFilter(f4py.OrFilter(f4py.StartsWithFilter(discrete_filter_column, "A"), f4py.EndsWithFilter(discrete_filter_column, "Z")), f4py.NumericFilter(numeric_filter_column, operator.ge, 0.1))

    with f4py.Parser(f4_file_path) as parser:
        parser.query_and_save(fltr, select_columns, out_file_path, out_file_type="tsv", num_processes=num_processes, lines_per_chunk=lines_per_chunk)

    file_size = os.path.getsize(out_file_path)
    if tall_or_wide == "tall":
        expected_size = 7613257
    else:
        expected_size = 7177264
    if file_size != expected_size:
        print("Error: output file size was invalid!")
        sys.exit()

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

tall_select_columns = ["ID", "Discrete100", "Numeric100", "Numeric200", "Numeric300", "Numeric400", "Numeric500", "Numeric600", "Numeric700", "Numeric800", "Numeric900"]
wide_select_columns = ["ID"] + [f"Discrete{i}" for i in range(100, 100001, 100)] + [f"Numeric{i}" for i in range(100, 900001, 100)]

#def temp():
#    run_test("tall", tall_select_columns, "Discrete100", "Numeric900", True, True, 8, 10000)
#profile = cProfile.Profile()
#profile.runcall(temp)
#ps = pstats.Stats(profile)
#ps.print_stats()

print(f"Shape\tIndexed\tCompressed\tNum_Processes\tElapsed_Seconds")

#for num_processes in [1, 2, 4, 8, 16, 32]:
for num_processes in [8]:
    run_test("tall", tall_select_columns, "Discrete100", "Numeric900", False, False, num_processes, 10000)
    run_test("wide", wide_select_columns, "Discrete100000", "Numeric900000", False, False, num_processes, 10)

    run_test("tall", tall_select_columns, "Discrete100", "Numeric900", True, False, num_processes, 10000)
    run_test("wide", wide_select_columns, "Discrete100000", "Numeric900000", True, False, num_processes, 10)

    run_test("tall", tall_select_columns, "Discrete100", "Numeric900", True, True, num_processes, 10000)
    run_test("wide", wide_select_columns, "Discrete100000", "Numeric900000", True, True, num_processes, 10)

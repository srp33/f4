import f4py
import glob
import gzip
import operator
import os
import sys

def get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    else:
        return open(file_path, 'rb')

def read_file_into_lists(file_path, delimiter=b"\t"):
    out_items = []

    the_file = get_delimited_file_handle(file_path)

    for line in the_file:
        out_items.append(line.rstrip(b"\n").split(delimiter))

    the_file.close()

    return out_items

def check_results(description, actual_lists, expected_lists):
    check_result(description, "Number of rows", len(actual_lists), len(expected_lists), False)

    for i in range(len(actual_lists)):
        actual_row = actual_lists[i]
        expected_row = expected_lists[i]

        check_result(description, f"Number of columns in row {i}", len(actual_row), len(expected_row), False)

        for j in range(len(actual_row)):
            check_result(description, f"row {i}, column {j}", actual_row[j], expected_row[j], False)

    pass_test(description)

def check_result(description, test, actual, expected, show_confirmation=True):
    if actual != expected:
        fail_test(f"A test failed for '{description}' and '{test}'.\n  Actual: {actual}\n  Expected: {expected}")
        sys.exit(1)
    elif show_confirmation:
        pass_test(f"'{description}' and '{test}'")

def pass_test(message):
    print(f"PASS: {message}")

def fail_test(message):
    print(f"FAIL: {message}")
    sys.exit(1)

def run_small_tests(in_file_path, f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_level=None, index_columns=None):
    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path}")
    print(f"num_processes: {num_processes}")
    print(f"num_cols_per_chunk: {num_cols_per_chunk}")
    print(f"lines_per_chunk: {lines_per_chunk}")
    print(f"compression_level: {compression_level}")
    print(f"index_columns: {index_columns}")
    print("-------------------------------------------------------")

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4py.Builder().convert_delimited_file(in_file_path, f4_file_path, compression_level=compression_level, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, index_columns=index_columns)

    try:
        parser = Parser("bogus_file_path")
        fail_test("Invalid file path.")
    except:
        pass_test("Invalid file path.")

    parser = f4py.Parser(f4_file_path)

    check_result("Parser properties", "Number of rows", parser.get_num_rows(), 5)
    check_result("Parser properties", "Number of columns", parser.get_num_cols(), 9)

    check_result("Column types", "ID column", parser.get_column_type_from_name("ID"), "s")
    check_result("Column types", "FloatA column", parser.get_column_type_from_name("FloatA"), "f")
    check_result("Column types", "FloatB column", parser.get_column_type_from_name("FloatB"), "f")
    check_result("Column types", "OrdinalA column", parser.get_column_type_from_name("OrdinalA"), "s")
    check_result("Column types", "OrdinalB column", parser.get_column_type_from_name("OrdinalB"), "s")
    check_result("Column types", "IntA column", parser.get_column_type_from_name("IntA"), "i")
    check_result("Column types", "IntB column", parser.get_column_type_from_name("IntB"), "i")
    check_result("Column types", "CategoricalA column", parser.get_column_type_from_name("CategoricalA"), "s")
    check_result("Column types", "CategoricalB column", parser.get_column_type_from_name("CategoricalB"), "s")

    parser.query_and_save(f4py.NoFilter(), [], out_file_path, num_processes=num_processes, lines_per_chunk=lines_per_chunk)
    check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

    parser.query_and_save(f4py.NoFilter(), ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","CategoricalA","CategoricalB"], out_file_path, num_processes=num_processes)
    check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

    parser.query_and_save(f4py.NoFilter(), ["ID"], out_file_path, num_processes=num_processes)
    check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"E"],[b"A"],[b"B"],[b"C"],[b"D"]])

    parser.query_and_save(f4py.NoFilter(), ["CategoricalB"], out_file_path, num_processes=num_processes)
    check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"CategoricalB"],[b"Brown"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])

    parser.query_and_save(f4py.NoFilter(), ["FloatA", "CategoricalB"], out_file_path, num_processes=num_processes)
    check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"CategoricalB"],[b"9.9", b"Brown"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])

    try:
        parser.query_and_save(f4py.NoFilter(), ["ID", "InvalidColumn"], out_file_path, num_processes=num_processes)
        fail_test("Invalid column name in select.")
    except:
        pass_test("Invalid column name in select.")

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(f4py.StringFilter("ID", operator.ne, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.StringFilter("ID", operator.ge, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string >= filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])

    parser.query_and_save(f4py.StringFilter("ID", operator.gt, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string > filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])

    parser.query_and_save(f4py.StringFilter("ID", operator.le, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string <= filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(f4py.StringFilter("ID", operator.lt, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string < filter", read_file_into_lists(out_file_path), [[b"FloatA"]])

    parser.query_and_save(f4py.IntRangeFilter("IntA", -100, 100), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within -100 and 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])

    parser.query_and_save(f4py.IntFilter("IntA", operator.eq, 7), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Int equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    parser.query_and_save(f4py.IntFilter("IntA", operator.eq, 5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Int equals filter - one match", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"], [b"4.4"]])

    parser.query_and_save(f4py.IntFilter("IntA", operator.ne, 5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Int not equals filter - two matches", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.eq, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Float equals filter - one match", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.eq, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Float equals filter - two matches", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ne, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Float not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.AndFilter(f4py.IntFilter("IntA", operator.eq, 7), f4py.FloatFilter("FloatA", operator.ne, 1.1)), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Two numeric filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    fltr = f4py.AndFilter(
             f4py.OrFilter(
               f4py.StringFilter("OrdinalA", operator.eq, "Med"),
               f4py.StringFilter("OrdinalA", operator.eq, "High")
             ),
             f4py.OrFilter(
               f4py.OrFilter(
                 f4py.StringFilter("IntB", operator.eq, "44"),
                 f4py.StringFilter("IntB", operator.eq, "99")
               ),
               f4py.StringFilter("IntB", operator.eq, "77")
             )
           )
    #TODO: The following filter is just for debugging.
    or_1 = f4py.OrFilter(
       f4py.StringFilter("OrdinalA", operator.eq, "Med"),
       f4py.StringFilter("OrdinalA", operator.eq, "High")
     )
    or_2 = f4py.OrFilter(
               f4py.OrFilter(
                 f4py.IntFilter("IntB", operator.eq, 44),
                 f4py.IntFilter("IntB", operator.eq, 99)
               ),
               f4py.IntFilter("IntB", operator.eq, 77)
             )
    fltr = f4py.AndFilter(or_1, or_2)
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Nested or filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"],[b"4.4"]])

    fltr = f4py.AndFilter(
             f4py.OrFilter(
               f4py.OrFilter(
                 f4py.StringFilter("OrdinalA", operator.eq, "Low"),
                 f4py.StringFilter("OrdinalA", operator.eq, "Med")
               ),
               f4py.StringFilter("OrdinalA", operator.eq, "High")
             ),
             f4py.FloatFilter("FloatB", operator.le, 44.4)
           )
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Numeric filters and string filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.LikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Like filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(f4py.NotLikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("NotLike filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.StartsWithFilter("CategoricalB", "Yell"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StartsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(f4py.EndsWithFilter("CategoricalB", "ow"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("EndsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", -9.9, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within -9.9 and 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 2.2, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 2.2 and 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 4.4, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 4.4 and 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 1.1, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 1.1 and 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 2.2, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 2.2 and 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"]])
    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 100.0, 1000.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 100 and 1000", read_file_into_lists(out_file_path), [[b"FloatA"]])

    parser.query_and_save(f4py.IntRangeFilter("IntA", -100, 100), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within -100 and 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.IntRangeFilter("IntA", 5, 8), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 5 and 8", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.IntRangeFilter("IntA", -8, -5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within -8 and -5", read_file_into_lists(out_file_path), [[b"FloatA"]])
    parser.query_and_save(f4py.IntRangeFilter("IntA", 5, 7), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 5 and 7", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.IntRangeFilter("IntA", 6, 8), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 6 and 8", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"]])
    parser.query_and_save(f4py.IntRangeFilter("IntA", 5, 5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 5 and 5", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"4.4"]])
    parser.query_and_save(f4py.IntRangeFilter("IntA", 6, 6), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 6 and 6", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])

    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "High", "Medium"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "High", "Low"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Low", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"]])
    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "Low", "Medium"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within Low and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "A", "Z"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "A", "B"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"]])

    fltr = f4py.AndFilter(f4py.StringRangeFilter("OrdinalA", "High", "Low"), f4py.IntRangeFilter("IntA", 5, 6))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StringRangeFilter and IntRangeFilter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"]])
    fltr = f4py.AndFilter(f4py.StringRangeFilter("OrdinalA", "High", "Low"), f4py.FloatRangeFilter("FloatA", 0.0, 5.0))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StringRangeFilter and IntRangeFilter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"]])

    parser.head(3, ["FloatA"], out_file_path)
    check_results("Head filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"]])
    parser.tail(3, ["FloatA"], out_file_path)
    check_results("Tail filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])

    try:
        parser.query_and_save(FloatFilter("InvalidColumn", operator.eq, 1), ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("Invalid column name in float filter.")
    except:
        pass_test("Invalid column name in float filter.")

    try:
        parser.query_and_save(FloatFilter(2, operator.eq, 1), ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("Non-string column name in float filter.")
    except:
        pass_test("Non-string column name in float filter.")

    try:
        parser.query_and_save(StringFilter("CategoricalA", operator.eq, None), ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("None value to equals filter.")
    except:
        pass_test("None value to equals filter.")

    try:
        parser.query_and_save(StringFilter("CategoricalA", operator.eq, 1), ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("Non-string value to equals filter.")
    except:
        pass_test("Non-string value to equals filter.")

    try:
        parser.query_and_save(FloatFilter("FloatA", operator.eq, "2"), ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("Non-number specified in float filter.")
    except:
        pass_test("Non-number specified in float filter.")

    try:
        parser.query_and_save(FloatFilter("OrdinalA", operator.eq, 2), ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("Non-float column specified for float filter.")
    except:
        pass_test("Non-float column specified for float filter.")

    try:
        parser.query_and_save("abc", ["FloatA"], out_file_path, num_processes=num_processes)
        fail_test("Non-filter is passed as a filter.")
    except:
        pass_test("Non-filter is passed as a filter.")

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = A", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "B"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = B", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "D"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = D", read_file_into_lists(out_file_path), [[b"FloatA"], [b"4.4"]])
    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "E"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = E", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 9.9", read_file_into_lists(out_file_path), [[b"FloatA"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 100", read_file_into_lists(out_file_path), [[b"FloatA"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 100", read_file_into_lists(out_file_path), [[b"FloatA"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 0", read_file_into_lists(out_file_path), [[b"FloatA"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 1.1", read_file_into_lists(out_file_path), [[b"FloatA"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 0", read_file_into_lists(out_file_path), [[b"FloatA"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])

    parser.query_and_save(f4py.StringFilter("OrdinalA", operator.eq, "Low"), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Categorical filter OrdinalA = Low", read_file_into_lists(out_file_path), [[b"ID"], [b"E"], [b"A"]])
    parser.query_and_save(f4py.StringFilter("OrdinalA", operator.eq, "Med"), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Categorical filter OrdinalA = Med", read_file_into_lists(out_file_path), [[b"ID"], [b"C"], [b"D"]])
    parser.query_and_save(f4py.StringFilter("OrdinalA", operator.eq, "High"), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Categorical filter OrdinalA = High", read_file_into_lists(out_file_path), [[b"ID"], [b"B"]])

    fltr = f4py.AndFilter(
             f4py.OrFilter(
               f4py.OrFilter(
                 f4py.StringFilter("ID", operator.eq, "A"),
                 f4py.StringFilter("ID", operator.eq, "B"),
               ),
               f4py.StringFilter("ID", operator.eq, "C"),
             ),
             f4py.FloatFilter("FloatA", operator.ge, 2.0)
           )
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter using two index columns", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"]])

#    f4py.IndexHelper.save_funnel_index(f4_file_path, "OrdinalA", "IntA", compression_level=compression_level)
#    fltr = f4py.StringIntRangeFunnelFilter("OrdinalA", "Low", "IntA", 5, 6)
    #parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    #check_results("Filter using basic funnel index - categorical + integer", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"1.1"]])

#    fltr = f4py.AndFilter(
#             f4py.OrFilter(
#               f4py.OrFilter(
#                 f4py.StringEqualsFilter("ID", "A"),
#                 f4py.StringEqualsFilter("ID", "B"),
#               ),
#               f4py.StringEqualsFilter("ID", "C"),
#             ),
#             f4py.FloatFilter("FloatA", operator.ge, 2)
#           )
#    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
#    check_results("Filter using categorical-int sequential index", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"], [b"2.2"]])

def run_medium_tests(num_processes):
    in_file_path = "/data/medium.tsv"
    f4_file_path = "/data/medium.f4"
    out_file_path = "/tmp/f4_out.tsv"

    with open("/data/medium.tsv") as medium_file:
        medium_lines = medium_file.read().rstrip("\n").split("\n")
        medium_ID = [[line.split("\t")[0].encode()] for line in medium_lines]
        medium_Discrete1 = [[line.split("\t")[1].encode()] for line in medium_lines]
        medium_Numeric1 = [[line.split("\t")[2].encode()] for line in medium_lines]

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4py.Builder().convert_delimited_file(in_file_path, f4_file_path, compression_level=None, num_processes=num_processes, num_cols_per_chunk=1)

    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path} - no indexing")
    print("-------------------------------------------------------")

    run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Discrete1, medium_Numeric1, num_processes)

    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path} - with indexing")
    print("-------------------------------------------------------")

    f4py.IndexHelper.save_indices(f4_file_path, ["ID", "Discrete1", "Numeric1"], compression_level=None)

    run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Discrete1, medium_Numeric1, num_processes)

def run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Discrete1, medium_Numeric1, num_processes):
    parser = f4py.Parser(f4_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row1"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row1", read_file_into_lists(out_file_path), [[b"Discrete1"],[b"MY"]])
    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row33"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row33", read_file_into_lists(out_file_path), [[b"Discrete1"],[b"CV"]])
    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row91"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row91", read_file_into_lists(out_file_path), [[b"Discrete1"],[b"AB"]])
    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row100"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row100", read_file_into_lists(out_file_path), [[b"Discrete1"],[b"UG"]])

    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", 0.0, 1.0), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = 0.0-1.0", read_file_into_lists(out_file_path), medium_ID)
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", 0.85, 0.90), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = 0.85-0.90", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[1], medium_ID[19], medium_ID[84], medium_ID[100]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", -0.90, -0.85), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = -0.90--0.85", read_file_into_lists(out_file_path), [medium_ID[0]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", float(medium_Numeric1[1][0].decode()), float(medium_Numeric1[1][0].decode())), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = row 1", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[1]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", float(medium_Numeric1[2][0].decode()), float(medium_Numeric1[2][0].decode())), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = row 2", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[2]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", float(medium_Numeric1[3][0].decode()), float(medium_Numeric1[3][0].decode())), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = row 3", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[3]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", float(medium_Numeric1[98][0].decode()), float(medium_Numeric1[98][0].decode())), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = row 98", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[98]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", float(medium_Numeric1[99][0].decode()), float(medium_Numeric1[99][0].decode())), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = row 99", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[99]])
    parser.query_and_save(f4py.FloatRangeFilter("Numeric1", float(medium_Numeric1[100][0].decode()), float(medium_Numeric1[100][0].decode())), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatWithin = row 100", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[100]])

    parser.query_and_save(f4py.StringFilter("Discrete1", operator.eq, medium_Discrete1[1][0].decode()), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter Discrete1 = row 1", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[1]])
    parser.query_and_save(f4py.StringFilter("Discrete1", operator.eq, medium_Discrete1[2][0].decode()), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter Discrete1 = row 2", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[2]])
    parser.query_and_save(f4py.StringFilter("Discrete1", operator.eq, medium_Discrete1[3][0].decode()), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter Discrete1 = row 3", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[3]])
    parser.query_and_save(f4py.StringFilter("Discrete1", operator.eq, medium_Discrete1[98][0].decode()), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter Discrete1 = row 98", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[98]])
    parser.query_and_save(f4py.StringFilter("Discrete1", operator.eq, medium_Discrete1[99][0].decode()), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter Discrete1 = row 99", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[99]])
    parser.query_and_save(f4py.StringFilter("Discrete1", operator.eq, medium_Discrete1[100][0].decode()), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Filter Discrete1 = row 100", read_file_into_lists(out_file_path), [medium_ID[0], medium_ID[100]])

# Basic small tests
f4_file_path = "/data/small.f4"
out_file_path = "/tmp/small_out.tsv"
run_small_tests("/data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1)
run_small_tests("/data/small.tsv.gz", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1)
run_small_tests("/data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2)
run_small_tests("/data/small.tsv.gz", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2)

# Small tests with compression
f4_file_path = "/data/small_compressed.f4"
out_file_path = "/tmp/small_compressed_out.tsv"
run_small_tests("/data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_level = 22)
run_small_tests("/data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, compression_level = 22)

# Small tests with indexing
f4_file_path = "/data/small_indexing.f4"
out_file_path = "/tmp/small_indexing_out.tsv"
index_columns = ["ID", "CategoricalB", "FloatA", "FloatB", "IntA", "IntB", "OrdinalA"]
run_small_tests("/data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, index_columns = index_columns)
run_small_tests("/data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, index_columns = index_columns)

# Medium tests
run_medium_tests(num_processes=1)
run_medium_tests(num_processes=2)

print("All tests passed!!")

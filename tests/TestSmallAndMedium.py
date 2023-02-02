import f4py
import glob
import gzip
from io import TextIOWrapper, BytesIO
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

def read_string_into_lists(s, delimiter="\t"):
    out_items = []

    for line in s.split("\n"):
        out_items.append([x.encode() for x in line.split(delimiter)])

    return out_items

def check_results(description, actual_lists, expected_lists):
    #print(actual_lists)
    #print(expected_lists)
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

def run_small_tests(in_file_path, f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_type=None, index_columns=None):
    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path}")
    print(f"num_processes: {num_processes}")
    print(f"num_cols_per_chunk: {num_cols_per_chunk}")
    print(f"lines_per_chunk: {lines_per_chunk}")
    print(f"compression_type: {compression_type}")
    print(f"index_columns: {index_columns}")
    print("-------------------------------------------------------")

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4py.Builder().convert_delimited_file(in_file_path, f4_file_path, compression_type=compression_type, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, index_columns=index_columns)

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
    #print(out_file_path, in_file_path)
    check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))
    os.unlink(out_file_path)

    parser.query_and_save(f4py.NoFilter(), ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","CategoricalA","CategoricalB"], out_file_path, num_processes=num_processes)
    check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))
    os.unlink(out_file_path)

    parser.query_and_save(f4py.NoFilter(), ["ID"], out_file_path, num_processes=num_processes)
    check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"E"],[b"A"],[b"B"],[b"C"],[b"D"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.NoFilter(), ["CategoricalB"], out_file_path, num_processes=num_processes)
    check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"CategoricalB"],[b"Brown"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.NoFilter(), ["FloatA", "CategoricalB"], out_file_path, num_processes=num_processes)
    check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"CategoricalB"],[b"9.9", b"Brown"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])
    os.unlink(out_file_path)

    try:
        parser.query_and_save(f4py.NoFilter(), ["ID", "InvalidColumn"], out_file_path, num_processes=num_processes)
        fail_test("Invalid column name in select.")
    except:
        pass_test("Invalid column name in select.")

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.ne, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"],[b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.ge, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string >= filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.gt, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string > filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.le, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string <= filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.lt, "A"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter by ID using string < filter", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", -100, 100), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within -100 and 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntFilter("IntA", operator.eq, 7), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Int equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntFilter("IntA", operator.eq, 5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Int equals filter - one match", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntFilter("IntA", operator.ne, 5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Int not equals filter - two matches", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.eq, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Float equals filter - one match", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.eq, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Float equals filter - two matches", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ne, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Float not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"],[b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.AndFilter(f4py.IntFilter("IntA", operator.eq, 7), f4py.FloatFilter("FloatA", operator.ne, 1.1)), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Two numeric filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])
    os.unlink(out_file_path)

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
    os.unlink(out_file_path)

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
    os.unlink(out_file_path)

    parser.query_and_save(f4py.LikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Like filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.NotLikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("NotLike filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StartsWithFilter("CategoricalB", "Yell"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StartsWith - CategoricalB - Yell", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StartsWithFilter("CategoricalB", "B"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StartsWith - CategoricalB - B", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StartsWithFilter("CategoricalB", "Or"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StartsWith - CategoricalB - Or", read_file_into_lists(out_file_path), [[b"FloatA"],[b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StartsWithFilter("CategoricalB", "Gr"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StartsWith - CategoricalB - Gr", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.EndsWithFilter("CategoricalB", "ow"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("EndsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.EndsWithFilter("CategoricalB", "own"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("EndsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.EndsWithFilter("CategoricalB", "x"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("EndsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", -9.9, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within -9.9 and 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 2.2, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 2.2 and 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 4.4, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 4.4 and 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 1.1, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 1.1 and 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 2.2, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 2.2 and 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatRangeFilter("FloatA", 100.0, 1000.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("FloatA within 100 and 1000", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", -100, 100), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within -100 and 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", 5, 8), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 5 and 8", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", -8, -5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within -8 and -5", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", 5, 7), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 5 and 7", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"4.4"]])

    parser.query_and_save(f4py.IntRangeFilter("IntA", 6, 8), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 6 and 8", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", 5, 5), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 5 and 5", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.IntRangeFilter("IntA", 6, 6), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("IntA within 6 and 6", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "High", "Medium"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "High", "Low"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Low", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "Low", "Medium"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within Low and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "A", "Z"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringRangeFilter("OrdinalA", "A", "B"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    fltr = f4py.AndFilter(f4py.StringRangeFilter("OrdinalA", "High", "Low"), f4py.IntRangeFilter("IntA", 5, 6))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StringRangeFilter and IntRangeFilter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"]])
    os.unlink(out_file_path)

    fltr = f4py.AndFilter(f4py.StringRangeFilter("OrdinalA", "High", "Low"), f4py.FloatRangeFilter("FloatA", 0.0, 5.0))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("StringRangeFilter and IntRangeFilter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.head(3, ["FloatA"], out_file_path)
    check_results("Head filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.tail(3, ["FloatA"], out_file_path)
    check_results("Tail filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

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
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "B"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = B", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "D"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = D", read_file_into_lists(out_file_path), [[b"FloatA"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "E"), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = E", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 9.9", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.gt, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA > 100", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.ge, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA >= 100", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 0", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 1.1", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.lt, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA < 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 0.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 0", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 1.1), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 2.2), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 4.4), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 9.9), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.FloatFilter("FloatA", operator.le, 100.0), ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter FloatA <= 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("OrdinalA", operator.eq, "Low"), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Categorical filter OrdinalA = Low", read_file_into_lists(out_file_path), [[b"ID"], [b"E"], [b"A"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("OrdinalA", operator.eq, "Med"), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Categorical filter OrdinalA = Med", read_file_into_lists(out_file_path), [[b"ID"], [b"C"], [b"D"]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("OrdinalA", operator.eq, "High"), ["ID"], out_file_path, num_processes=num_processes)
    check_results("Categorical filter OrdinalA = High", read_file_into_lists(out_file_path), [[b"ID"], [b"B"]])
    os.unlink(out_file_path)

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
    os.unlink(out_file_path)

    fltr = f4py.AndFilter(f4py.StringFilter("CategoricalB", operator.eq, "Yellow"), f4py.IntRangeFilter("IntB", 0, 50))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter using string/int-range two-column index", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    os.unlink(out_file_path)

    fltr = f4py.AndFilter(f4py.StringFilter("CategoricalB", operator.eq, "Yellow"), f4py.IntRangeFilter("IntB", 0, 25))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter using string/int-range two-column index", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    fltr = f4py.AndFilter(f4py.StringFilter("CategoricalB", operator.eq, "Brown"), f4py.IntRangeFilter("IntB", 50, 100))
    parser.query_and_save(fltr, ["FloatA"], out_file_path, num_processes=num_processes)
    check_results("Filter using string/int-range two-column index", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"]])
    os.unlink(out_file_path)

    # Clean up data files
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

def run_medium_tests(num_processes):
    in_file_path = "data/medium.tsv"
    f4_file_path = "data/medium.f4"
    out_file_path = "/tmp/f4_out.tsv"

    with open("data/medium.tsv") as medium_file:
        medium_lines = [line for line in medium_file.read().rstrip("\n").split("\n")]
        medium_ID = [[line.split("\t")[0].encode()] for line in medium_lines]
        medium_Categorical1 = [[line.split("\t")[1].encode()] for line in medium_lines]
        medium_Discrete1 = [[line.split("\t")[11].encode()] for line in medium_lines]
        medium_Numeric1 = [[line.split("\t")[21]] for line in medium_lines]

        for i in range(1, len(medium_Numeric1)):
            medium_Numeric1[i][0] = float(medium_Numeric1[i][0])

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4py.Builder().convert_delimited_file(in_file_path, f4_file_path, compression_type=None, num_processes=num_processes, num_cols_per_chunk=1)

    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path} - no indexing")
    print("-------------------------------------------------------")

    run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Categorical1, medium_Discrete1, medium_Numeric1, num_processes)

    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path} - with indexing")
    print("-------------------------------------------------------")

    f4py.IndexBuilder.build_indexes(f4_file_path, ["ID", "Categorical1", "Discrete1", "Numeric1"])
    run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Categorical1, medium_Discrete1, medium_Numeric1, num_processes)

    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path} - custom indexing")
    print("-------------------------------------------------------")

    f4py.IndexBuilder.build_endswith_index(f4_file_path, "Discrete1")
    run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Categorical1, medium_Discrete1, medium_Numeric1, num_processes)

    # Clean up data files
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

def run_medium_tests2(f4_file_path, out_file_path, medium_ID, medium_Categorical1, medium_Discrete1, medium_Numeric1, num_processes):
    parser = f4py.Parser(f4_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row1"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row1", read_file_into_lists(out_file_path), [[b"Discrete1"], medium_Discrete1[1]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row33"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row33", read_file_into_lists(out_file_path), [[b"Discrete1"], medium_Discrete1[33]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row91"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row91", read_file_into_lists(out_file_path), [[b"Discrete1"], medium_Discrete1[91]])
    os.unlink(out_file_path)

    parser.query_and_save(f4py.StringFilter("ID", operator.eq, "Row100"), ["Discrete1"], out_file_path, num_processes=num_processes)
    check_results("Filter ID = Row100", read_file_into_lists(out_file_path), [[b"Discrete1"], medium_Discrete1[100]])
    os.unlink(out_file_path)

    run_string_test("Categorical1", "A", "A", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)
    run_string_test("Categorical1", "D", "D", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)
    run_string_test("Categorical1", "A", "D", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)
    run_string_test("Categorical1", "B", "C", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)
    run_string_test("Categorical1", "A", "C", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)
    run_string_test("Categorical1", "B", "D", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)
    run_string_test("Categorical1", "B", "Z", parser, medium_ID, medium_Categorical1, out_file_path, num_processes)

    run_string_test("Discrete1", "AA", "AA", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)
    run_string_test("Discrete1", "PM", "PM", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)
    run_string_test("Discrete1", "AA", "ZZ", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)
    run_string_test("Discrete1", "FA", "SZ", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)

    run_endswith_test("M", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)
    run_endswith_test("PM", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)
    run_endswith_test("ZZZZ", parser, medium_ID, medium_Discrete1, out_file_path, num_processes)

    run_float_test(0.0, 1.0, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)
    run_float_test(0.85, 0.9, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)
    run_float_test(-0.9, -0.85, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)
    run_float_test(-0.5, 0.0, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)
    run_float_test(-0.5, 0.5, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)
    run_float_test(-1000.0, 1000.0, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)
    run_float_test(0.5, 0.5, parser, medium_ID, medium_Numeric1, out_file_path, num_processes)

def run_string_test(column_name, lower_bound, upper_bound, parser, medium_ID, filter_values, out_file_path, num_processes):
    parser.query_and_save(f4py.StringRangeFilter(column_name, lower_bound, upper_bound), ["ID"], out_file_path, num_processes=num_processes)
    indices = [i for i in range(len(filter_values)) if filter_values[i][0] == column_name.encode() or (filter_values[i][0] >= lower_bound.encode() and filter_values[i][0] <= upper_bound.encode())]
    matches = [medium_ID[i] for i in indices]
    actual = read_file_into_lists(out_file_path)
    check_results(f"Filter {column_name} = {lower_bound} <> {upper_bound} = {len(matches) - 1} matches", read_file_into_lists(out_file_path), matches)
    os.unlink(out_file_path)

def run_endswith_test(value, parser, medium_ID, filter_values, out_file_path, num_processes):
    column_name = "Discrete1"
    parser.query_and_save(f4py.EndsWithFilter(column_name, value), ["ID"], out_file_path, num_processes=num_processes)
    indices = [i for i in range(len(filter_values)) if filter_values[i][0] == column_name.encode() or filter_values[i][0].endswith(value.encode())]
    matches = [medium_ID[i] for i in indices]
    check_results(f"EndsWith filter - {column_name} - {value} = {len(matches) - 1} matches", read_file_into_lists(out_file_path), matches)
    os.unlink(out_file_path)

def run_float_test(lower_bound, upper_bound, parser, medium_ID, medium_Numeric1, out_file_path, num_processes):
    column_name = "Numeric1"
    parser.query_and_save(f4py.FloatRangeFilter(column_name, lower_bound, upper_bound), ["ID"], out_file_path, num_processes=num_processes)
    indices = [i for i in range(len(medium_Numeric1)) if isinstance(medium_Numeric1[i][0], str) or (medium_Numeric1[i][0] >= lower_bound and medium_Numeric1[i][0] <= upper_bound)]
    matches = [medium_ID[i] for i in indices]
    check_results(f"Filter FloatWithin = {lower_bound} <> {upper_bound} = {len(matches) - 1} matches", read_file_into_lists(out_file_path), matches)
    os.unlink(out_file_path)

# Basic small tests
f4_file_path = "data/small.f4"
out_file_path = "/tmp/small_out.tsv"
run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1)
#run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2)

# Basic small tests (with gzipped files)
run_small_tests("data/small.tsv.gz", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1)
#run_small_tests("data/small.tsv.gz", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2)

# Make sure we print to standard out properly (this code does not work inside a function).
f4py.Builder().convert_delimited_file("data/small.tsv", f4_file_path)
old_stdout = sys.stdout
sys.stdout = TextIOWrapper(BytesIO(), sys.stdout.encoding)
parser = f4py.Parser(f4_file_path)
parser.query_and_save(f4py.NoFilter(), [], out_file_path=None, num_processes=1, lines_per_chunk=10)
sys.stdout.seek(0)
out = sys.stdout.read()
sys.stdout.close()
sys.stdout = old_stdout
check_results("No filters, select all columns - std out", read_string_into_lists(out), read_file_into_lists("data/small.tsv"))

index_columns = ["ID", "CategoricalB", "FloatA", "FloatB", "IntA", "IntB", "OrdinalA", ["CategoricalB", "IntB"]]

## Small tests with indexing
run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, index_columns = index_columns)
#run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, index_columns = index_columns)

## Small tests with dictionary-based compression
run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_type = "dictionary")
#run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, compression_type = "dictionary")

## Small tests with dictionary-based compression (and indexing)
run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_type = "dictionary", index_columns = index_columns)
#run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, compression_type = "dictionary", index_columns = index_columns)

## Small tests with z-standard compression
run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_type = "zstd")
#run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, compression_type = "zstd")

## Small tests with z-standard compression (and indexing)
run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1, compression_type = "zstd", index_columns = index_columns)
#run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2, compression_type = "zstd", index_columns = index_columns)

# Clean up data files
for file_path in glob.glob(f"{f4_file_path}*"):
    os.unlink(file_path)

# Medium tests
run_medium_tests(num_processes=1)
#run_medium_tests(num_processes=2)

print("All tests passed!!")

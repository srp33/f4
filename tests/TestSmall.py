import f4py
#from f4py.Builder import *
#from f4py.Filters import *
#from f4py.Parser import *
#from f4py.IndexHelper import *
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

def run_all_tests(in_file_path, num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1):
    print("-------------------------------------------------------")
    print(f"Running all tests for {in_file_path}, {num_processes}, {num_cols_per_chunk}, {lines_per_chunk}")
    print("-------------------------------------------------------")

    f4_file_path = "/data/small.f4"
    out_file_path = "/tmp/f4_out.tsv"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4py.Builder().convert_delimited_file(in_file_path, f4_file_path, compression_level=None, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk)

    try:
        parser = Parser("bogus_file_path")
        fail_test("Invalid file path.")
    except:
        pass_test("Invalid file path.")

    parser = f4py.Parser(f4_file_path)

    check_result("Parser properties", "Number of rows", parser.get_num_rows(), 5)
    check_result("Parser properties", "Number of columns", parser.get_num_cols(), 9)

    check_result("Column types", "ID column", parser.get_column_type_from_name("ID"), "i")
    check_result("Column types", "FloatA column", parser.get_column_type_from_name("FloatA"), "f")
    check_result("Column types", "FloatB column", parser.get_column_type_from_name("FloatB"), "f")
    check_result("Column types", "OrdinalA column", parser.get_column_type_from_name("OrdinalA"), "c")
    check_result("Column types", "OrdinalB column", parser.get_column_type_from_name("OrdinalB"), "c")
    check_result("Column types", "IntA column", parser.get_column_type_from_name("IntA"), "i")
    check_result("Column types", "IntB column", parser.get_column_type_from_name("IntB"), "i")
    check_result("Column types", "CategoricalA column", parser.get_column_type_from_name("CategoricalA"), "c")
    check_result("Column types", "CategoricalB column", parser.get_column_type_from_name("CategoricalB"), "c")

    parser.query_and_save(f4py.NoFilter(), [], out_file_path, num_processes=num_processes, lines_per_chunk=lines_per_chunk)
    check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

    parser.query_and_save(f4py.NoFilter(), ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","CategoricalA","CategoricalB"], out_file_path)
    check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

    parser.query_and_save(f4py.NoFilter(), ["ID"], out_file_path)
    check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"9"],[b"1"],[b"2"],[b"3"],[b"4"]])

    parser.query_and_save(f4py.NoFilter(), ["CategoricalB"], out_file_path)
    check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"CategoricalB"],[b"Brown"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])

    parser.query_and_save(f4py.NoFilter(), ["FloatA", "CategoricalB"], out_file_path)
    check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"CategoricalB"],[b"9.9", b"Brown"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])

    try:
        parser.query_and_save(f4py.NoFilter(), ["ID", "InvalidColumn"], out_file_path)
        fail_test("Invalid column name in select.")
    except:
        pass_test("Invalid column name in select.")

    parser.query_and_save(f4py.NumericFilter("ID", operator.eq, 1), ["FloatA"], out_file_path)
    check_results("Filter by ID using Numeric filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(f4py.StringEqualsFilter("ID", "1"), ["FloatA"], out_file_path)
    check_results("Filter by ID using equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(f4py.StringNotEqualsFilter("ID", "1"), ["FloatA"], out_file_path)
    check_results("Filter by ID using not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"],[b"4.4"],])

    parser.query_and_save(f4py.AndFilter(f4py.NumericFilter("FloatA", operator.ne, 1.1), f4py.NumericFilter("IntA", operator.eq, 7)), ["FloatA"], out_file_path)
    check_results("Two Numeric filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    fltr = f4py.AndFilter(
             f4py.OrFilter(
               f4py.StringEqualsFilter("OrdinalA", "Med"),
               f4py.StringEqualsFilter("OrdinalA", "High")
             ),
             f4py.OrFilter(
               f4py.OrFilter(
                 f4py.StringEqualsFilter("IntB", "44"),
                 f4py.StringEqualsFilter("IntB", "99")
               ),
               f4py.StringEqualsFilter("IntB", "77")
             )
           )
    parser.query_and_save(fltr, ["FloatA"], out_file_path)
    check_results("Nested or filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"],[b"4.4"]])

    fltr = f4py.AndFilter(
             f4py.OrFilter(
               f4py.OrFilter(
                 f4py.StringEqualsFilter("OrdinalA", "Low"),
                 f4py.StringEqualsFilter("OrdinalA", "Med")
               ),
               f4py.StringEqualsFilter("OrdinalA", "High")
             ),
             f4py.NumericFilter("FloatB", operator.le, 44.4)
           )
    parser.query_and_save(fltr, ["FloatA"], out_file_path)
    check_results("Numeric filters and string filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.LikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path)
    check_results("Like filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(f4py.NotLikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path)
    check_results("NotLike filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.AndFilter(f4py.LikeFilter("FloatB", r"^\d\d\.\d$"), f4py.LikeFilter("FloatB", r"88")), ["FloatA"], out_file_path)
    check_results("Like filter on numerical columns", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    parser.query_and_save(f4py.StartsWithFilter("CategoricalB", "Yell"), ["FloatA"], out_file_path)
    check_results("StartsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(f4py.EndsWithFilter("CategoricalB", "ow"), ["FloatA"], out_file_path)
    check_results("EndsWith filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(f4py.OrFilter(f4py.LikeFilter("FloatB", r"^x$"), f4py.StringEqualsFilter("FloatB", "88.8")), ["FloatA"], out_file_path)
    check_results("Or filter simple", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    parser.query_and_save(f4py.OrFilter(f4py.OrFilter(f4py.LikeFilter("FloatB", r"^x$"), f4py.StringEqualsFilter("FloatB", "88.8")), f4py.NumericFilter("FloatB", operator.eq, 44.4)), ["FloatA"], out_file_path)
    check_results("Or filter multiple", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(f4py.AndFilter(f4py.OrFilter(f4py.LikeFilter("FloatB", r"^x$"), f4py.OrFilter(f4py.StringEqualsFilter("FloatB", "88.8"), f4py.NumericFilter("FloatB", operator.eq, 44.4))), f4py.AndFilter(f4py.NumericFilter("FloatA", operator.ge, 2.2), f4py.AndFilter(f4py.NumericFilter("FloatA", operator.le, 10), f4py.OrFilter(f4py.StringEqualsFilter("FloatA", "2.2"), f4py.LikeFilter("FloatA", r"2\.2"))))), ["FloatA"], out_file_path)
    check_results("Nested filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    try:
        parser.query_and_save(NumericFilter("InvalidColumn", operator.eq, 1), ["FloatA"], out_file_path)
        fail_test("Invalid column name in Numeric filter.")
    except:
        pass_test("Invalid column name in Numeric filter.")

    try:
        parser.query_and_save(NumericFilter(2, operator.eq, 1), ["FloatA"], out_file_path)
        fail_test("Non-string column name in Numeric filter.")
    except:
        pass_test("Non-string column name in Numeric filter.")

    try:
        parser.query_and_save(EqualsFilter("CategoricalA", None), ["FloatA"], out_file_path)
        fail_test("None value to equals filter.")
    except:
        pass_test("None value to equals filter.")

    try:
        parser.query_and_save(EqualsFilter("CategoricalA", 1), ["FloatA"], out_file_path)
        fail_test("Non-string value to equals filter.")
    except:
        pass_test("Non-string value to equals filter.")

    #try:
    #    parser.query_and_save(InFilter("CategoricalA", []), ["FloatA"], out_file_path)
    #    fail_test("Empty values list in In filter.")
    #except:
    #    pass_test("Empty values list in In filter.")

    #try:
    #    parser.query_and_save(InFilter("CategoricalA", [2, 3.3]), ["FloatA"], out_file_path)
    #    fail_test("No string specified in In filter.")
    #except:
    #    pass_test("No string specified in In filter.")

    #try:
    #    parser.query_and_save(InFilter(2, ["A"]), ["FloatA"], out_file_path)
    #    fail_test("Non-string column name in In filter.")
    #except:
    #    pass_test("Non-string column name in In filter.")

    try:
        parser.query_and_save(NumericFilter("FloatA", operator.eq, "2"), ["FloatA"], out_file_path)
        fail_test("Non-number specified in Numeric filter.")
    except:
        pass_test("Non-number specified in Numeric filter.")

    try:
        parser.query_and_save(NumericFilter("OrdinalA", operator.eq, 2), ["FloatA"], out_file_path)
        fail_test("Non-numeric column specified for Numeric filter.")
    except:
        pass_test("Non-numeric column specified for Numeric filter.")

    try:
        parser.query_and_save("abc", ["FloatA"], out_file_path)
        fail_test("Non-filter is passed as a filter.")
    except:
        pass_test("Non-filter is passed as a filter.")

    # Test ability to query based on index columns.
    f4py.Builder().convert_delimited_file(in_file_path, f4_file_path, compression_level=None, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk)
    f4py.IndexHelper.save(f4_file_path, "ID", compression_level=None)
    f4py.IndexHelper.save(f4_file_path, "FloatA", compression_level=None)
    f4py.IndexHelper.save(f4_file_path, "OrdinalA", compression_level=None)

    parser = f4py.Parser(f4_file_path)

    parser.query_and_save(f4py.StringEqualsFilter("ID", "1"), ["FloatA"], out_file_path)
    check_results("Indexed filter ID = 1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    parser.query_and_save(f4py.StringEqualsFilter("ID", "2"), ["FloatA"], out_file_path)
    check_results("Indexed filter ID = 2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    parser.query_and_save(f4py.StringEqualsFilter("ID", "4"), ["FloatA"], out_file_path)
    check_results("Indexed filter ID = 4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"4.4"]])
    parser.query_and_save(f4py.StringEqualsFilter("ID", "9"), ["FloatA"], out_file_path)
    check_results("Indexed filter ID = 9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])

    parser.query_and_save(f4py.NumericFilter("FloatA", operator.ge, 0), ["FloatA"], out_file_path)
    check_results("Indexed filter FloatA > 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.NumericFilter("FloatA", operator.ge, 1.1), ["FloatA"], out_file_path)
    check_results("Indexed filter FloatA > 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    parser.query_and_save(f4py.NumericFilter("FloatA", operator.ge, 2.2), ["FloatA"], out_file_path)
    check_results("Indexed filter FloatA > 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    parser.query_and_save(f4py.NumericFilter("FloatA", operator.ge, 4.4), ["FloatA"], out_file_path)
    check_results("Indexed filter FloatA > 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    parser.query_and_save(f4py.NumericFilter("FloatA", operator.ge, 100), ["FloatA"], out_file_path)
    check_results("Indexed filter FloatA > 100", read_file_into_lists(out_file_path), [[b"FloatA"]])

    # TODO: Make it work for >=, <, <=, and ==.

    fltr = f4py.AndFilter(
             f4py.OrFilter(
               f4py.OrFilter(
                 f4py.StringEqualsFilter("ID", "1"),
                 f4py.StringEqualsFilter("ID", "2"),
               ),
               f4py.StringEqualsFilter("ID", "3"),
             ),
             f4py.NumericFilter("FloatA", operator.ge, 2)
           )
    parser.query_and_save(fltr, ["FloatA"], out_file_path)
    check_results("Filter using two index columns", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"], [b"2.2"]])

    # TODO: Add some more (simple) tests for indexed columns.
    #         Simplify the above test first?
    #       Make sure we can find values in the first or last row.

    # Test ability to query when the data are compressed.
    #Builder().convert_delimited_file(in_file_path, f4_file_path, compression_level=22, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk)
    #Indexer(f4_file_path, ["ID", "FloatA", "OrdinalA"], compression_level=22).save(num_processes)
    #parser = Parser(f4_file_path)
    #parser.query_and_save(AndFilter(InFilter("ID", ["1", "2", "3"]), NumericFilter("FloatA", operator.ge, 2)), ["FloatA"], out_file_path)
    #check_results("Filter using two index columns (compressed)", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"], [b"2.2"]])

    #pass_test("Completed all tests succesfully!!")

run_all_tests("/data/small.tsv", num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1)
#TODO:
#run_all_tests("/data/small.tsv.gz", num_processes = 1, num_cols_per_chunk = 1, lines_per_chunk = 1)
#run_all_tests("/data/small.tsv", num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2)
#run_all_tests("/data/small.tsv.gz", num_processes = 2, num_cols_per_chunk = 2, lines_per_chunk = 2)

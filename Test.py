import glob
import gzip
import os
import sys
from Builder import *
from Parser import *

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

def run_all_tests(in_file_path):
    print("--------------------------------------")
    print(f"Running all tests for {in_file_path}")
    print("--------------------------------------")

    f4_file_path = "data/test_data.f4"
    out_file_path = "/tmp/f4_out.tsv"
    #num_processes = 1
    #num_cols_per_chunk = 1
    #lines_per_chunk = 1
    num_processes = 2
    num_cols_per_chunk = 2
    lines_per_chunk = 2

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    convert_delimited_file_to_f4(in_file_path, f4_file_path, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk)

    try:
        parser = Parser("bogus_file_path")
        fail_test("Invalid file path.")
    except:
        pass_test("Invalid file path.")

    parser = Parser(f4_file_path)

#    check_result("Parser properties", "Number of rows", parser.get_num_rows(), 4)
#    check_result("Parser properties", "Number of columns", parser.get_num_columns(), 9)

    check_result("Column types", "ID column", parser.get_column_type("ID"), "i")
    check_result("Column types", "FloatA column", parser.get_column_type("FloatA"), "f")
    check_result("Column types", "FloatB column", parser.get_column_type("FloatB"), "f")
    check_result("Column types", "OrdinalA column", parser.get_column_type("OrdinalA"), "c")
    check_result("Column types", "OrdinalB column", parser.get_column_type("OrdinalB"), "c")
    check_result("Column types", "IntA column", parser.get_column_type("IntA"), "i")
    check_result("Column types", "IntB column", parser.get_column_type("IntB"), "i")
    check_result("Column types", "CategoricalA column", parser.get_column_type("CategoricalA"), "c")
    check_result("Column types", "CategoricalB column", parser.get_column_type("CategoricalB"), "c")

    parser.query_and_save(KeepAll(), [], out_file_path, num_processes=num_processes, lines_per_chunk=lines_per_chunk)
    check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

    parser.query_and_save(KeepAll(), ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","CategoricalA","CategoricalB"], out_file_path)
    check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

    parser.query_and_save(KeepAll(), ["ID"], out_file_path)
    check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"1"],[b"2"],[b"3"],[b"4"]])

    parser.query_and_save(KeepAll(), ["CategoricalB"], out_file_path)
    check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"CategoricalB"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])

    parser.query_and_save(KeepAll(), ["FloatA", "CategoricalB"], out_file_path)
    check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"CategoricalB"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])

    try:
        parser.query_and_save(KeepAll(), ["ID", "InvalidColumn"], out_file_path)
        fail_test("Invalid column name in select.")
    except:
        pass_test("Invalid column name in select.")

    parser.query_and_save(NumericFilter("ID", operator.eq, 1), ["FloatA"], out_file_path)
    check_results("Filter by ID using Numeric filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(InFilter("ID", ["1"]), ["FloatA"], out_file_path)
    check_results("Filter by ID using In filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

    parser.query_and_save(InFilter("ID", ["1"], negate=True), ["FloatA"], out_file_path)
    check_results("Filter by ID using In filter with negation", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"],[b"4.4"],])

    parser.query_and_save(AndFilter(NumericFilter("FloatA", operator.ne, 1.1), NumericFilter("IntA", operator.eq, 7)), ["FloatA"], out_file_path)
    check_results("Two Numeric filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    parser.query_and_save(AndFilter(InFilter("OrdinalA", ["Med", "High"]), InFilter("IntB", ["44", "99", "77"])), ["FloatA"], out_file_path)
    check_results("Two In filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(AndFilter(InFilter("OrdinalA", ["Low","Med","High"]), NumericFilter("FloatB", operator.le, 44.4)), ["FloatA"], out_file_path)
    check_results("Numeric filters and In filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(LikeFilter("CategoricalB", r"ow$"), ["FloatA"], out_file_path)
    check_results("Like filter on categorical column", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"2.2"]])

    parser.query_and_save(LikeFilter("CategoricalB", r"ow$", negate=True), ["FloatA"], out_file_path)
    check_results("Like filter on categorical column with negation", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(AndFilter(LikeFilter("FloatB", r"^\d\d\.\d$"), LikeFilter("FloatB", r"88")), ["FloatA"], out_file_path)
    check_results("Like filter on categorical columns", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])

    parser.query_and_save(OrFilter(LikeFilter("FloatB", r"^x$"), InFilter("FloatB", ["88.8"]), NumericFilter("FloatB", operator.eq, 44.4)), ["FloatA"], out_file_path)
    check_results("Or filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"4.4"]])

    parser.query_and_save(AndFilter(OrFilter(LikeFilter("FloatB", r"^x$"), InFilter("FloatB", ["88.8"]), NumericFilter("FloatB", operator.eq, 44.4)), AndFilter(NumericFilter("FloatA", operator.ge, 2.2), NumericFilter("FloatA", operator.le, 10), OrFilter(InFilter("FloatA", ["2.2"]), LikeFilter("FloatA", r"2\.2")))), ["FloatA"], out_file_path)
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
        parser.query_and_save(InFilter("CategoricalA", []), ["FloatA"], out_file_path)
        fail_test("Empty values list in In filter.")
    except:
        pass_test("Empty values list in In filter.")

    try:
        parser.query_and_save(InFilter("CategoricalA", [2, 3.3]), ["FloatA"], out_file_path)
        fail_test("No string specified in In filter.")
    except:
        pass_test("No string specified in In filter.")

    try:
        parser.query_and_save(InFilter(2, ["A"]), ["FloatA"], out_file_path)
        fail_test("Non-string column name in In filter.")
    except:
        pass_test("Non-string column name in In filter.")

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
    convert_delimited_file_to_f4(in_file_path, f4_file_path, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, index_columns=["ID", "FloatA", "OrdinalA"])
    parser = Parser(f4_file_path)
    parser.query_and_save(AndFilter(InFilter("ID", ["1", "2", "3"]), NumericFilter("FloatA", operator.ge, 2)), ["FloatA"], out_file_path)
    check_results("Filter using two index columns", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"], [b"2.2"]])

    pass_test("Completed all tests succesfully!!")

if not os.path.exists("data"):
    os.mkdir("data")

run_all_tests("test_data.tsv")
run_all_tests("test_data.tsv.gz")

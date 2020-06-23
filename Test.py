import glob
import sys
from Builder import *
from Parser import *

def read_file_into_lists(file_path, delimiter=b"\t"):
    out_items = []

    with open(file_path, "rb") as the_file:
        for line in the_file:
            out_items.append(line.rstrip(b"\n").split(delimiter))

    return out_items

def check_results(description, actual_lists, expected_lists):
    check_result(description, "Number of rows", len(actual_lists), len(expected_lists))

    for i in range(len(actual_lists)):
        actual_row = actual_lists[i]
        expected_row = expected_lists[i]

        check_result(description, f"Number of columns in row {i}", len(actual_row), len(expected_row))

        for j in range(len(actual_row)):
            check_result(description, f"row {i}, column {j}", actual_row[j], expected_row[j])

    pass_test(description)

def check_result(description, test, actual, expected):
    if actual != expected:
        fail_test(f"A test failed for '{description}' and '{test}'.\n  Actual: {actual}\n  Expected: {expected}")
        sys.exit(1)

def pass_test(message):
    print(f"PASS: {message}")

def fail_test(message):
    print(f"FAIL: {message}")
    sys.exit(1)

in_file_path = "test_data.tsv"
f4_file_path = "output/test_data.f4"
out_file_path = "/tmp/f4_out.tsv"

# Clean up output files if they already exist
for file_path in glob.glob(f"{f4_file_path}*"):
    os.unlink(file_path)

convert_delimited_file_to_f4(in_file_path, f4_file_path)

parser = Parser(f4_file_path)

try:
    parser = Parser("bogus_file_path")
    fail_test("It should fail with an invalid file path.")
except:
    pass_test("It should fail with an invalid file path.")

check_result("Parser properties", "Number of rows", parser.num_rows, 4)
check_result("Parser properties", "Number of columns", parser.num_columns, 9)

check_result("Column types", "ID column", parser.get_column_type("ID"), "i")
check_result("Column types", "FloatA column", parser.get_column_type("FloatA"), "f")
check_result("Column types", "FloatB column", parser.get_column_type("FloatB"), "f")
check_result("Column types", "OrdinalA column", parser.get_column_type("OrdinalA"), "d")
check_result("Column types", "OrdinalB column", parser.get_column_type("OrdinalB"), "d")
check_result("Column types", "IntA column", parser.get_column_type("IntA"), "i")
check_result("Column types", "IntB column", parser.get_column_type("IntB"), "i")
check_result("Column types", "DiscreteA column", parser.get_column_type("DiscreteA"), "d")
check_result("Column types", "DiscreteB column", parser.get_column_type("DiscreteB"), "d")

check_result("Column unique values", "ID column", parser.does_column_have_unique_values("ID"), True)
check_result("Column unique values", "FloatA column", parser.does_column_have_unique_values("FloatA"), False)
check_result("Column unique values", "FloatB column", parser.does_column_have_unique_values("FloatB"), True)
check_result("Column unique values", "OrdinalA column", parser.does_column_have_unique_values("OrdinalA"), False)
check_result("Column unique values", "OrdinalB column", parser.does_column_have_unique_values("OrdinalB"), False)
check_result("Column unique values", "IntA column", parser.does_column_have_unique_values("IntA"), False)
check_result("Column unique values", "IntB column", parser.does_column_have_unique_values("IntB"), False)
check_result("Column unique values", "DiscreteA column", parser.does_column_have_unique_values("DiscreteA"), False)
check_result("Column unique values", "DiscreteB column", parser.does_column_have_unique_values("DiscreteB"), False)

parser.query_and_save([], [], out_file_path)
check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

parser.query_and_save([], ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","DiscreteA","DiscreteB"], out_file_path)
check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

parser.query_and_save([], ["ID"], out_file_path)
check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"1"],[b"2"],[b"3"],[b"4"]])

parser.query_and_save([], ["DiscreteB"], out_file_path)
check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"DiscreteB"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])

parser.query_and_save([], ["FloatA", "DiscreteB"], out_file_path)
check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"DiscreteB"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])

try:
    parser.query_and_save([], ["ID", "InvalidColumn"], out_file_path)
    fail_test("An exception should have been raised for an invalid column name.")
except:
    pass_test("Should raise exception when invalid column name specified in select.")

parser.query_and_save([NumericFilter("ID", operator.eq, 1)], ["FloatA"], out_file_path)
check_results("Filter by ID", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])

parser.query_and_save([DiscreteFilter("ID", ["1","4"])], ["FloatA"], out_file_path)
check_results("Filter by IDs", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"],[b"4.4"]])

parser.query_and_save([NumericFilter("FloatA", operator.ne, 1.1), NumericFilter("IntA", operator.eq, 5)], ["FloatA"], out_file_path)
check_results("Two numeric filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"4.4"]])

parser.query_and_save([DiscreteFilter("OrdinalA", ["Med", "High"]), DiscreteFilter("DiscreteB", ["Yellow", "Brown"])], ["FloatA"], out_file_path)
check_results("Two discrete filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"]])

parser.query_and_save([DiscreteFilter("ID", ["1","2","4"]), NumericFilter("FloatB", operator.le, 44.4)], ["FloatA"], out_file_path)
check_results("Numeric and discrete filters", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"4.4"]])

try:
    parser.query_and_save([NumericFilter("InvalidColumn", operator.eq, 1)], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised for an invalid column name.")
except:
    pass_test("Should raise exception when invalid column name specified in filter.")

try:
    parser.query_and_save([NumericFilter(2, operator.eq, 1)], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised for a non-string column name in numeric filter.")
except:
    pass_test("Should raise exception should have been raised for a non-string column name in numeric filter.")

try:
    parser.query_and_save([DiscreteFilter("DiscreteA", [])], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised for an empty values list.")
except:
    pass_test("Should raise exception when an empty list is specified.")

try:
    parser.query_and_save([DiscreteFilter("DiscreteA", [2, 3.3])], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised when no string specified.")
except:
    pass_test("Should raise exception when no string specified.")

try:
    parser.query_and_save([DiscreteFilter(2, ["A"])], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised for a non-string column name in discrete filter.")
except:
    pass_test("Should raise exception should have been raised for a non-string column name in discrete filter.")

try:
    parser.query_and_save([NumericFilter("FloatA", operator.eq, "2")], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised when a non-number was specified.")
except:
    pass_test("Should raise exception when a non-number was specified.")

try:
    parser.query_and_save(["abc"], ["FloatA"], out_file_path)
    fail_test("An exception should have been raised when a non-filter is passed.")
except:
    pass_test("Should raise exception when a non-filter is passed.")

print("PASS: Completed all tests succesfully!!")

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
    f4_file_path = "data/test_data.f4"
    out_file_path = "/tmp/f4_out.tsv"

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    convert_delimited_file_to_f4(in_file_path, f4_file_path)

if not os.path.exists("data"):
    os.mkdir("data")

#run_all_tests("test_data.tsv")
#run_all_tests("test_data.tsv.gz")
run_all_tests("tall.tsv.gz")

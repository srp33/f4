import glob
import sys
from DataSetBuilder import *
from DataSetParser import *

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
            check_result(description, f"row {i}, column{j}", actual_row[j], expected_row[j])

    print(f"{description} - passed!")

def check_result(description, test, actual, expected):
    if actual != expected:
        print(f"A test did not pass for '{description}' and '{test}'.\n  Actual: {actual}\n  Expected: {expected}")
        sys.exit(1)

in_file_path = "test_data.tsv"
f4_file_path = "output/test_data.f4"
out_file_path = "/tmp/f4_out.tsv"

# Clean up output files if they already exist
for file_path in glob.glob(f"{f4_file_path}*"):
    os.unlink(file_path)

convert_delimited_file_to_f4(in_file_path, f4_file_path)

parser = DataSetParser(f4_file_path)

parser.query_and_save([], [], [], out_file_path)
check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

parser.query_and_save([], [], ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","DiscreteA","DiscreteB"], out_file_path)
check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

parser.query_and_save([], [], ["ID"], out_file_path)
check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"1"],[b"2"],[b"3"],[b"4"]])

parser.query_and_save([], [], ["DiscreteB"], out_file_path)
check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"DiscreteB"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])

parser.query_and_save([], [], ["FloatA", "DiscreteB"], out_file_path)
check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"DiscreteB"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])

#TODO: On line 45, check the parser properties (num rows, etc.).
#TODO: Specify invalid select column(s) and verify exception thrown.
#TODO: out_file_type="tsv"
#TODO: Add function to get type of a given column. Should be able to use F4 methodology for this.

#ID	FloatA	FloatB	OrdinalA	OrdinalB	IntA	IntB	DiscreteA	DiscreteB
#1	1.1	99.9	Low	High	5	99	Red	Yellow
#2	2.2	22.2	High	Low	8	44	Red	Yellow
#3	2.2	88.8	Med	Med	7	77	Orange	Brown
#4	4.4	44.4	Med	Med	5	44	Brown	Orange

#parser1.query([], [], [], [], [], query_file_path)
#checkResultFile("Default values", query_file_path, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser1.query([DiscreteFilter(3, ["Med"])], [NumericFilter(1, ">", 0)], [2, 4], [], [], query_file_path)
#checkResultFile("Standard mixed query", query_file_path, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

#parser1.query([], [], [2, 4], [], [], query_file_path)
#checkResultFile("Missing query", query_file_path, [[b'Sample', b'FloatB', b'TempB'], [b'1', b'11.1', b'High'], [b'2', b'22.2', b'Low'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

#parser1.query([], [NumericFilter(1, "<", 0)], [2, 4], [], [], query_file_path)
#checkResultFile("No matching rows", query_file_path, [[b'Sample', b'FloatB', b'TempB']])

#parser1.query([DiscreteFilter(4, ["Low", "Med"])], [], [], [], [], query_file_path)
#checkResultFile("Composite discrete filter", query_file_path, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser1.query([DiscreteFilter(4, ["Low", "High"])], [NumericFilter(1, ">", 0)], [1, 4], [], [], query_file_path)
#checkResultFile("First and last columns and rows", query_file_path, [[b'Sample', b'FloatA', b'TempB'], [b'1', b'1.1', b'High'], [b'2', b'2.2', b'Low']])

#parser2.query([], [NumericFilter(2, ">", 35)], [1, 4], [], [], query_file_path)
#checkResultFile("Filter based on int column", query_file_path, [[b'Sample', b'IntA', b'ColorB'], [b'4', b'4', b'Brown'], [b'5', b'5', b'Orange']])

#parser1.query([], [NumericFilter(0, ">=", 3)], [], [], [], query_file_path)
#checkResultFile("Query by sample ID", query_file_path, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser1.query([], [NumericFilter(0, "==", 3)], [], [], [], query_file_path)
#checkResultFile("Equals", query_file_path, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med']])

#parser1.query([], [NumericFilter(0, "!=", 3)], [], [], [], query_file_path)
#checkResultFile("Not equals", query_file_path, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser12.query([DiscreteFilter(3, ["Med"]), DiscreteFilter(8, ["Yellow"])], [NumericFilter(1, ">", 0), NumericFilter(2, ">", 0)], [2, 4], ["2"], [], query_file_path)
#checkResultFile("Query merged data + group", query_file_path, [[b'Sample', b'1__FloatB', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'3', b'33.3', b'Med', b'3', b'33', b'Red', b'Yellow']])

#parser12.query([], [NumericFilter(0, "==", 1)], [], [], [], query_file_path)
#checkResultFile("Missing values coming through properly in merged output", query_file_path, [[b'Sample', b'1__FloatA', b'1__FloatB', b'1__TempA', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'1', b'1.1', b'11.1', b'Low', b'High', b'', b'', b'', b'']])

#parser_genes1.query([], [], [4], [], ["Glycolysis / Gluconeogenesis [kegg]"], query_file_path)
#checkResultFile("Select columns by pathway 1", query_file_path, [[b'Sample', b'ENSG1 (PGM1)', b'ENSG2 (PGM2)', b'ENSG3 (PFKP)', b'ENSG4 (PHPT1)'], [b'1', b'5.2', b'3.8', b'1', b'2'], [b'2', b'6.4', b'9.2', b'1', b'2']])

#parser_genes2.query([], [], [], [], ["Gene expression of MAFbx by FOXO ( Insulin receptor signaling (Mammal) ) [inoh]"], query_file_path)
#checkResultFile("Select columns by pathway 2", query_file_path, [[b'Sample', b'FOXO4', b'FOXO6'], [b'1', b'9', b'8'], [b'2', b'6', b'5']])

#parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"], query_file_path)
#checkResultFile("Merge genes", query_file_path, [[b'Sample', b'Genes1__ENSG1 (PGM1)', b'Genes1__ENSG2 (PGM2)', b'Genes1__ENSG3 (PFKP)', b'Genes1__ENSG5 (PMM1)', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

#parser1.query([DiscreteFilter(3, ["Med"])], [NumericFilter(1, ">", 0)], [2, 4], [], [], query_file_path)
#checkResultFile("Apply aliases to individual dataset", query_file_path, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

#parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"], query_file_path)
#checkResultFile("Apply aliases to merged dataset", query_file_path, [[b'Sample', b'Genes1__ENSG1 (PGM1)', b'Genes1__ENSG2 (PGM2)', b'Genes1__ENSG3 (PFKP)', b'Genes1__ENSG5 (PMM1)', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

#checkResult("Check sample column options", parser1.search_variable_options(0, search_str=None, max_discrete_options=100), ['1', '2', '3', '4'])
#checkResult("Check sample column options2", parser1.search_variable_options(0, search_str="1", max_discrete_options=100), ['1'])
#checkResult("Check discrete column options2", parser1.search_variable_options(3, search_str=None, max_discrete_options=100), ['High', 'Low', 'Med'])
#checkResult("Check discrete column options2 - max", parser1.search_variable_options(3, search_str=None, max_discrete_options=3), ['High', 'Low', 'Med'])
#checkResult("Check discrete column options2 - beyond max", parser1.search_variable_options(3, search_str=None, max_discrete_options=2), ['High', 'Low'])
#checkResult("Check discrete column options - search", parser1.search_variable_options(3, search_str="d", max_discrete_options=2), ['Med'])

print("All tests passed!!!")

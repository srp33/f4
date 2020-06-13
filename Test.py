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

    print(f"{description} - passed!")

def check_result(description, test, actual, expected):
    if actual != expected:
        print(f"A test did not pass for {description} and {test}.\n  Actual: {actual}.\n  Expected: {expected}.")
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
check_results("No filters, all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))

#TODO: Same as above but explicitly specify all the columns.
#TODO: Specify invalid select column(s) and verify exception thrown.

#num_rows, sample_temp = parser.save_sample_indices_matching_filters([DiscreteFilter(12335, ["F"])], [NumericFilter(1, ">", 11.4)])

#ID	FloatA	FloatB	OrdinalA	OrdinalB	IntA	IntB	DiscreteA	DiscreteB
#1	1.1	11.1	Low	High	9	99	Red	Yellow
#2	2.2	22.2	High	Low	8	88	Red	Yellow
#3	3.3	33.3	Med	Med	7	77	Orange	Brown
#4	4.4	44.4	Med	Med	6	66	Brown	Orange

#parser.query_and_save([], [], [], out_file_path, out_file_type="tsv")



#print(parser.id)
#print(parser.title)
#print(parser.num_samples)
#print(parser.num_features)
#print(parser.total_datapoints)
#print(parser.get_variable_meta(0, max_discrete_options=100))
#print(parser.get_variable_meta(1, max_discrete_options=100))
#print(parser.get_variable_meta(12334, max_discrete_options=100))
#print(parser.get_variable_meta(12335, max_discrete_options=100))
#print(parser.search_variable_options(0, search_str="A", max_discrete_options=100))
#print(parser.search_variable_options(12335, search_str="F", max_discrete_options=100))
#num_rows, sample_temp = parser.save_sample_indices_matching_filters([DiscreteFilter(12335, ["F"])], [NumericFilter(1, ">", 11.4)])
#print(num_rows)
#num_cols, col_temp, col_temp2 = parser.save_column_indices_to_select([1,2,3], ["Metadata"], ["Metabolic pathways [kegg]"])
#print(num_rows)
#print(num_rows * num_cols)
#parser.build_output_file(sample_temp, col_temp, col_temp2, "/Applications/tmp/1", "tsv")
#parser.clean_up()

#tsv_file_path_2 = "TestData/Test2/data.tsv"
#tsv_file_path_genes_1 = "TestData/Genes1/data.tsv"
#tsv_file_path_genes_2 = "TestData/Genes2/data.tsv"
#fwf_file_path_1 = "{}/1.fwf".format(tmp_dir)
#fwf_file_path_2 = "{}/2.fwf".format(tmp_dir)
#fwf_file_path_genes_1 = "{}/Genes1.fwf".format(tmp_dir)
#fwf_file_path_genes_2 = "{}/Genes2.fwf".format(tmp_dir)
#merged_file_path = "{}/12.fwf".format(tmp_dir)
#merged_genes_file_path = "{}/Genes12.fwf".format(tmp_dir)
#query_file_path = "{}/output.tsv".format(tmp_dir)
#parser1 = DataSetParser(fwf_file_path_1)
#parser2 = DataSetParser(fwf_file_path_2)
#parser12 = DataSetParser(merged_file_path)
#parser_genes1 = DataSetParser(fwf_file_path_genes_1)
#parser_genes2 = DataSetParser(fwf_file_path_genes_2)
#parser_genes12 = DataSetParser(merged_genes_file_path)

#convert_tsv_to_fwf(tsv_file_path_1, fwf_file_path_1)
#convert_tsv_to_fwf(tsv_file_path_2, fwf_file_path_2)
#convert_tsv_to_fwf(tsv_file_path_genes_1, fwf_file_path_genes_1)
#convert_tsv_to_fwf(tsv_file_path_genes_2, fwf_file_path_genes_2)

#merge_fwf_files([fwf_file_path_1, fwf_file_path_2], merged_file_path)
#merge_fwf_files([fwf_file_path_genes_1, fwf_file_path_genes_2], merged_genes_file_path)

#build_metadata("TestData/Test1", fwf_file_path_1)
#build_metadata("TestData/Test2", fwf_file_path_2)
#build_metadata("TestData/Genes1", fwf_file_path_genes_1)
#build_metadata("TestData/Genes2", fwf_file_path_genes_2)
#build_metadata("TestData/Test1", merged_file_path)
#build_metadata("TestData/Genes1", merged_genes_file_path)

#checkResult("ID", parser1.id, "Test1")
#checkResult("Title 1", parser1.title, "This is the title1")
#checkResult("featureDescription 1", parser1.featureDescription, "gene")
#checkResult("featureDescriptionPlural 1", parser1.featureDescriptionPlural, "genes")
#checkResult("ID", parser2.id, "Test2")
#checkResult("Title 2", parser2.title, "This is the title2")
#checkResult("Num samples 1", parser1.num_samples, 4)
#checkResult("Num samples 2", parser2.num_samples, 4)
#checkResult("Num samples 3", parser12.num_samples, 5)
#checkResult("Num features 1", parser1.num_features, 5)
#checkResult("Num features 2", parser2.num_features, 5)
#checkResult("Num features 3", parser12.num_features, 9)
#checkResult("Num datapoints 1", parser1.total_datapoints, 20)
#checkResult("Num datapoints 2", parser2.total_datapoints, 20)
#checkResult("Num datapoints 3", parser12.total_datapoints, 45)

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

#checkResult("1 group", len(parser1.get_groups()), 1)
#checkResult("1 group - search - no match", len(parser1.search_group("data", "blah")), 0)
#checkResult("1 group - search - match", len(parser1.search_group("data", "Float")), 2)
#checkResult("2 groups", len(parser12.get_groups()), 2)
#checkResult("2 groups - group 1 values", parser12.search_group("1"), [(1, 'FloatA'), (2, 'FloatB'), (3, 'TempA'), (4, 'TempB')])
#checkResult("2 groups - group 1 values - search", parser12.search_group("1", "F"), [(1, 'FloatA'), (2, 'FloatB')])
#checkResult("2 groups - group 1 values - search - max", parser12.search_group("1", "F", 2), [(1, 'FloatA'), (2, 'FloatB')])
#checkResult("2 groups - group 1 values - search - beyond max", parser12.search_group("1", "F", 1), [(1, 'FloatA')])
#checkResult("2 groups - group 2 values - search", parser12.search_group("2", "olor"), [(7, 'ColorA'), (8, 'ColorB')])

#checkResult("No pathways 1", len(parser1.get_pathways()), 0)
#checkResult("No pathways 12", len(parser12.get_pathways()), 0)
#checkResult("Pathways genes1", len(parser_genes1.get_pathways()), 31)
#checkResult("Pathways genes12", len(parser_genes12.get_pathways()), 57)
#checkResult("Pathways genes12 - element", parser_genes12.get_pathways()[0], ('AKT phosphorylates targets in the nucleus [reactome]', 2))

#checkResult("Get sample column meta", parser1.get_variable_meta(0), (4, ['1', '2', '3', '4']))
#checkResult("Get num column meta", parser1.get_variable_meta(1), (1.1, 4.4))
#checkResult("Get discrete column meta", parser1.get_variable_meta(3), (3, ['High', 'Low', 'Med']))
#checkResult("Get discrete column meta - beyond max", parser1.get_variable_meta(3, max_discrete_options=2), (3, None))

#checkResult("Check sample column options", parser1.search_variable_options(0, search_str=None, max_discrete_options=100), ['1', '2', '3', '4'])
#checkResult("Check sample column options2", parser1.search_variable_options(0, search_str="1", max_discrete_options=100), ['1'])
#checkResult("Check discrete column options2", parser1.search_variable_options(3, search_str=None, max_discrete_options=100), ['High', 'Low', 'Med'])
#checkResult("Check discrete column options2 - max", parser1.search_variable_options(3, search_str=None, max_discrete_options=3), ['High', 'Low', 'Med'])
#checkResult("Check discrete column options2 - beyond max", parser1.search_variable_options(3, search_str=None, max_discrete_options=2), ['High', 'Low'])
#checkResult("Check discrete column options - search", parser1.search_variable_options(3, search_str="d", max_discrete_options=2), ['Med'])

#parser1.clean_up(max_age_seconds=0)
#parser1.save_sample_indices_matching_filters([], [])
#checkResult("Clean up", parser1.clean_up(max_age_seconds=0), 1)

print("All tests passed!!!")

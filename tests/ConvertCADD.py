import f4py
import glob
import os
import sys

delimited_file_path = sys.argv[1]
out_file_prefix = sys.argv[2]
num_processes = int(sys.argv[3])
num_cols_per_chunk = int(sys.argv[4])
num_rows_per_save = int(sys.argv[5])
index_columns = sys.argv[6].split(",")
compression_level = int(sys.argv[7])
use_training_dict = sys.argv[8] == "True"
tmp_dir_path = sys.argv[9]
cache_dir_path = sys.argv[10]
if cache_dir_path == "":
    cache_dir_path = None

import fastnumbers
import gzip

#import zstandard
##with gzip.open("data/cadd_head_medium.tsv.gz") as delim_file:
#with gzip.open("data/cadd_head_small.tsv.gz") as delim_file:
##with open("data/cadd_head_small.f4") as delim_file:
#    file_lines = delim_file.read().rstrip().split(b"\n")[1:]
#    #file_lines = [x.encode() for x in delim_file.read().rstrip().split("\n")][1:]
#
#    #training_list = file_lines[:10]
#    training_list = file_lines[:900]
##    training_list = file_lines[:10000]
#
#    #training_set = set()
#    #for line in file_lines:
#    #    line_items = line.split(b"\t")
#    #    for x in line_items:
#    #        if not fastnumbers.isint(x.decode()) and not fastnumbers.isfloat(x.decode()):
#    #            training_set.add(x)
#    #training_list = sorted(list(training_set))
#
#    #training_dict = zstandard.train_dictionary(300, training_list, level=compression_level, threads=num_processes)
#    #training_dict = zstandard.train_dictionary(4809, training_list, level=compression_level, threads=num_processes)
#    #compression_level = 22
#    training_dict = zstandard.train_dictionary(100000000, training_list, level=compression_level, threads=num_processes)
#    #training_dict = zstandard.train_dictionary(len(training_dict), training_list, level=compression_level, threads=num_processes)
#    compressor = zstandard.ZstdCompressor(dict_data = training_dict, level = compression_level)
#    decompressor = zstandard.ZstdDecompressor(dict_data=training_dict)
#
#compressed_lines = []
#decompressed_length = 0
#compressed_length = 0
#for line in file_lines:
#    compressed_line = compressor.compress(line)
#
#    decompressed_length += len(line)
#    compressed_length += len(compressed_line)
#
#    compressed_lines.append(compressed_line)
#
#print("training list length")
#tll = 0
#for x in training_list:
#    tll += len(x)
#print(tll)
#print("training dict length")
#print(len(training_dict.as_bytes()))
#print("decompressed length")
#print(decompressed_length)
#print("compressed length")
#print(compressed_length)
#
#with open("/tmp/compression_test.tsv", "wb") as comp_file:
#    for line in compressed_lines:
#        comp_file.write(line + b"\n")
##############################################################
##############################################################

#with gzip.open(delimited_file_path) as delim_file:
#    file_lines = delim_file.read().rstrip().split(b"\n")[1:]
#
#    training_set = set()
#    for line in file_lines:
#        line_items = line.split(b"\t")
#        for x in line_items:
#            if not fastnumbers.isint(x.decode()) and not fastnumbers.isfloat(x.decode()):
#                training_set.add(x)
#                #if len(training_set) > 100:
#                    #with open("/tmp/training_set", "w") as tmp_file:
#                    #    for x in sorted(list(training_set)):
#                    #        tmp_file.write(f"{x.decode()}\n")
#                    #sys.exit()
#    print(training_set)


#https://stackoverflow.com/questions/7591258/fast-n-gram-calculation/32266204#32266204
#https://raw.githubusercontent.com/Lucas-C/dotfiles_and_notes/master/languages/python/utf8_iterator.py
#sys.exit()


f4_file_path = f"{out_file_prefix}.f4"

# Clean up data files if they already exist
for file_path in glob.glob(f"{f4_file_path}*"):
    os.unlink(file_path)

###f4py.Builder(verbose=True).convert_delimited_file(delimited_file_path, f4_file_path, compression_level=None, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=tmp_dir_path, cache_dir_path=cache_dir_path)
f4py.Builder(verbose=True).convert_delimited_file(delimited_file_path, f4_file_path, index_columns=None, delimiter="\t", compression_level=compression_level, build_compression_dictionary=use_training_dict, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=tmp_dir_path, cache_dir_path=cache_dir_path)
#f4py.Builder(verbose=True).convert_delimited_file(delimited_file_path, f4_file_path, index_columns=index_columns, delimiter="\t", compression_level=compression_level, build_compression_dictionary=use_training_dict, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_save=num_rows_per_save, tmp_dir_path=tmp_dir_path, cache_dir_path=cache_dir_path)

#f4py.Parser(f4_file_path).head(n=10000, out_file_path="/tmp/test.tsv")

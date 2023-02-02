#! /bin/bash

set -o errexit

#######################################################
# Build the Docker image
#######################################################

#docker build --platform linux/x86_64 -t srp33/f4_test .

#######################################################
# Run detailed functional tests on small file
#######################################################

mkdir -p data

rm -rf f4py
cp -r ../f4py .

dockerCommand="docker run -i -t --rm --platform linux/x86_64 --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"
#dockerCommand="docker run --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"

#$dockerCommand bash -c "time python3 BuildTsv.py 10 10 10 10000 data/medium.tsv"

#TODO: zstandard compression
#        No longer create .zstd file. Instead, use .cmpr. Add logic to determine the compression_type from it.
#        Why are we inverting in get_decompression_dict?
#        Record line indices and starts positions in a msgpack file? Check how variable the line sizes are.
#TODO: Calculate .nrow and .ncol so you don't have to save them in files?
#TODO: Does it work if we do not include a newline character after the last line (no compression)?
#TODO: Address remaining TODO items in the code, remove unnecessary commented code.
#TODO: Combine all information into a single file.
#        Use this spec? https://tools.ietf.org/id/draft-kunze-bagit-16.html
#TODO: Combine these TODO items with the TODO file.
#TODO: Integrate f4py into the analysis paper tests.
#TODO: Save for separate paper:
#        Filters.py - Can we filter without decompressing by converting self.value in constructor?
#          Looks like we can do it for some filter classes (String, StartsWith, EndsWith, Head, Tail), but not others.
#          If you don't do it, remove select_compression_dict as a parameter from filter_column_values().
#        Compress values in index files using our dictionary compression technique.
#        Use more options for compression type and only store compression dictionary when more than 256 combinations (?).
#        Store individual, serialized compression dictionaries on one line, using .cc file to indicate where each starts and ends.
#        Do compression at the bigram level.
#        Do bit-packing (see to01() function in bitarray module). Also https://wiki.python.org/moin/BitManipulation
#        Pandas integration
#          Instead of And, Or, FloatRange, etc. classes, use the same syntax that they use?
python3 TestSmallAndMedium.py
#$dockerCommand python3 TestSmallAndMedium.py
#$dockerCommand python3

#######################################################
# Create large test files and do tests
#######################################################

#$dockerCommand bash -c "time python3 BuildTsv.py 100 900 1000000 data/tall.tsv"
#$dockerCommand bash -c "time python3 BuildTsv.py 100000 900000 1000 data/wide.tsv"
#$dockerCommand bash -c "time gzip -k data/tall.tsv"
#$dockerCommand bash -c "time gzip -k data/wide.tsv"

mkdir -p results

#$dockerCommand bash -c "python3 TestBuildLarge.py" | tee results/Large_Build.tsv
#$dockerCommand bash -c "python3 TestParseLarge.py" | tee results/Large_Parse.tsv

############################################################
# Test CADD files.
############################################################

#TODO: Remove this stuff? Run this stuff within the container.

#wget -O data/whole_genome_SNVs_inclAnno.tsv.gz https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/whole_genome_SNVs_inclAnno.tsv.gz
#wget -O data/whole_genome_SNVs_inclAnno.tsv.gz.tbi https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/whole_genome_SNVs_inclAnno.tsv.gz.tbi

#zcat data/whole_genome_SNVs_inclAnno.tsv.gz | head -n 2 | tail -n +2 | cut -c2- | gzip > data/cadd.tsv.gz
#zcat data/whole_genome_SNVs_inclAnno.tsv.gz | tail -n +3 | gzip >> data/cadd.tsv.gz

# The full-sized CADD file has 12221577961 lines total. We will make some smaller ones for testing.

#zcat data/cadd.tsv.gz | head -n 1001 > /tmp/cadd_head_small.tsv
#zcat data/cadd.tsv.gz | head -n 10000001 > /tmp/cadd_head_medium.tsv

#sed -r 's/1\t([0-9])/X\t\1/g' /tmp/cadd_head_small.tsv > /tmp/cadd_head_small_X.tsv
#sed -r 's/1\t([0-9])/X\t\1/g' /tmp/cadd_head_medium.tsv > /tmp/cadd_head_medium_X.tsv

#tail -n +2 /tmp/cadd_head_small_X.tsv > /tmp/cadd_head_small_X2.tsv
#tail -n +2 /tmp/cadd_head_medium_X.tsv > /tmp/cadd_head_medium_X2.tsv

#cat /tmp/cadd_head_small.tsv /tmp/cadd_head_small_X2.tsv > data/cadd_head_small.tsv
#cat /tmp/cadd_head_medium.tsv /tmp/cadd_head_medium_X2.tsv > data/cadd_head_medium.tsv

#gzip data/cadd_head_small.tsv
#gzip data/cadd_head_medium.tsv

##$dockerCommand bash -c "python3 ConvertCADD.py 'data/cadd_head_small.tsv.gz' 'data/cadd_head_small' 32 5 100 Chrom,Pos,Consequence,ConsScore 1 False /tmp/cadd_small ''"
#$dockerCommand bash -c "python3 ConvertCADD.py 'data/cadd_head_small.tsv.gz' 'data/cadd_head_small' 1 5 100 Chrom,Pos,Consequence,ConsScore 1 True /tmp/cadd_small ''"
#$dockerCommand bash -c "python3 ConvertCADD.py 'data/cadd_head_small.tsv.gz' 'data/cadd_head_small' 32 5 100 Chrom,Pos,Consequence,ConsScore 1 True /tmp/cadd_small ''"
##$dockerCommand bash -c "python3 ConvertCADD.py 'data/cadd_head_medium.tsv.gz' 'data/cadd_head_medium' 32 5 100 Chrom,Pos,Consequence,ConsScore 1 False /tmp/cadd_medium ''"
#$dockerCommand bash -c "python3 ConvertCADD.py 'data/cadd_head_medium.tsv.gz' 'data/cadd_head_medium' 32 5 100 Chrom,Pos,Consequence,ConsScore 1 True /tmp/cadd_medium ''"
##$dockerCommand bash -c "python3 ConvertCADD.py 'data/cadd_head_medium.tsv.gz' 'data/cadd_head_medium' 32 5 100 Chrom,Pos,Consequence,ConsScore 11 True /tmp/cadd_medium ''"

#mkdir -p /tmp/cadd
#python3 ConvertCADD.py "data/cadd.tsv.gz" "data/cadd" 28 5 100000 Chrom,Pos,Consequence,ConsScore /tmp/cadd
# 19814003803 lines according to wc -l in data/cadd.f4


#python3 ConvertTsvToFixedWidthFile2.py data/cadd.tsv.gz data/cadd.fwf2
#python3 ConvertTsvToFixedWidthFile2.py data/cadd.tsv.gz /tmp/1.fwf2

#rm -f data/whole_genome_SNVs_inclAnno.tsv.gz data/whole_genome_SNVs_inclAnno.tsv.gz.tbi

#python3 F4/Builder.py data/cadd.tsv.gz data/cadd.f4 "\t" 30

# 12,221,577,960 rows in CADD file (excluding header).
# 134 columns

rm -rf f4py

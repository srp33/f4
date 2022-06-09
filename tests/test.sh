#! /bin/bash

set -o errexit

#######################################################
# Build the Docker image
#######################################################

docker build -t srp33/f4_test .

#######################################################
# Run detailed functional tests on small file
#######################################################

mkdir -p data

rm -rf f4py
cp -r ../f4py .

dockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"
#dockerCommand="docker run --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"

#$dockerCommand bash -c "time python3 BuildTsv.py 1 1 100 data/medium.tsv"

$dockerCommand python3 TestSmallAndMedium.py

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

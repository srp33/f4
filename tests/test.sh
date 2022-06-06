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

#$dockerCommand python3 TestSmallAndMedium.py

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
# Download, parse, and query CADD files.
############################################################

#wget -O TestData/whole_genome_SNVs_inclAnno.tsv.gz https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/whole_genome_SNVs_inclAnno.tsv.gz
#wget -O TestData/whole_genome_SNVs_inclAnno.tsv.gz.tbi https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/whole_genome_SNVs_inclAnno.tsv.gz.tbi

#zcat TestData/whole_genome_SNVs_inclAnno.tsv.gz | head -n 2 | tail -n +2 | cut -c2- | gzip > TestData/cadd.tsv.gz
#zcat TestData/whole_genome_SNVs_inclAnno.tsv.gz | tail -n +3 | gzip >> TestData/cadd.tsv.gz

# The cadd file has 12221577961 lines total.
#mkdir -p /tmp/cadd
#python3 ConvertCADD.py "TestData/cadd.tsv.gz" "TestData/cadd" 28 5 100000 Chrom,Pos,Consequence,ConsScore /tmp/cadd
# 19814003803 lines according to wc -l in TestData/cadd.f4
#zcat TestData/cadd.tsv.gz | head -n 101 | gzip > TestData/cadd_head_small.tsv.gz
#zcat TestData/cadd.tsv.gz | head -n 10001 | gzip > TestData/cadd_head_medium.tsv.gz
#zcat TestData/cadd.tsv.gz | tail -n 100 | gzip > TestData/cadd_tail_small.tsv.gz
#zcat TestData/cadd.tsv.gz | tail -n 100000000 | gzip > TestData/cadd_tail_medium.tsv.gz
#python3 ConvertCADD.py "TestData/cadd_head_small.tsv.gz" "TestData/cadd_head_small" 28 5 100 Chrom,Pos,Consequence,ConsScore /tmp/cadd ""
#python3 ConvertCADD.py "TestData/cadd_head_medium.tsv.gz" "TestData/cadd_head_medium" 28 5 1000 Chrom,Pos,Consequence,ConsScore /tmp/cadd ""
#python3 ConvertCADD.py "TestData/cadd_tail_small.tsv.gz" "TestData/cadd_head_small" 28 5 100 Chrom,Pos,Consequence,ConsScore /tmp/cadd ""
#python3 ConvertCADD.py "TestData/cadd_tail_medium.tsv.gz" "TestData/cadd_head_medium" 28 5 1000 Chrom,Pos,Consequence,ConsScore /tmp/cadd ""

#python3 ConvertTsvToFixedWidthFile2.py TestData/cadd.tsv.gz TestData/cadd.fwf2
#python3 ConvertTsvToFixedWidthFile2.py TestData/cadd.tsv.gz /tmp/1.fwf2

#rm -f TestData/whole_genome_SNVs_inclAnno.tsv.gz TestData/whole_genome_SNVs_inclAnno.tsv.gz.tbi

#python3 F4/Builder.py TestData/cadd.tsv.gz TestData/cadd.f4 "\t" 30

# 12,221,577,960 rows in CADD file (excluding header).
# 134 columns


rm -rf f4py

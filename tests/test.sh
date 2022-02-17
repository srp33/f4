#! /bin/bash

set -o errexit

#######################################################
# Build the Docker image
#######################################################

cp ../src/*.py .

docker build -t srp33/f4 .

#######################################################
# Run detailed functional tests on small file
#######################################################

dockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox --workdir=/sandbox srp33/f4"
#dockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) --workdir=/sandbox srp33/f4"

$dockerCommand python3 TestSmall.py

#######################################################
# Create large test files and do tests
#######################################################

#$dockerCommand bash -c "time python3 BuildTsv.py 100 900 1000000 data/tall.tsv; time python3 TestBuildLarge.py"

#time $dockerCommand python3 BuildTsv.py 100 900 1000000 data/tall.tsv
#time $dockerCommand python3 BuildTsv.py 100000 900000 1000 data/wide.tsv
#time $dockerCommand python3 TestBuildLarge.py
#time $dockerCommand python3 TestParseLarge.py

rm Builder.py Helper.py Indexer.py Parser.py

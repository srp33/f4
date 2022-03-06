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

dockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data --workdir=/sandbox srp33/f4_test"

#$dockerCommand python3 TestSmall.py

#######################################################
# Create large test files and do tests
#######################################################

#time $dockerCommand python3 BuildTsv.py 100 900 1000000 data/tall.tsv
#time $dockerCommand python3 BuildTsv.py 100000 900000 1000 data/wide.tsv
#time $dockerCommand python3 TestBuildLarge.py

$dockerCommand bash -c "time python3 TestParseLarge.py"

rm -rf f4py
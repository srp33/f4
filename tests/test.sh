#! /bin/bash

set -o errexit

#######################################################
# Build the Docker image
#######################################################

#docker build --platform linux/x86_64 -t srp33/f4_test .

#######################################################
# Run preparatory steps
#######################################################

mkdir -p data

rm -rf f4py
cp -r ../f4py .

dockerCommand="docker run -i -t --rm --platform linux/x86_64 --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"
#dockerCommand="docker run --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"

#$dockerCommand bash -c "time python3 build_tsv.py 10 10 10 10000 data/medium.tsv"

#TODO: Add a parameter to ignore lines starting with a specified comment character (default = "#").
#TODO: Modify some function names to be private.
#TODO: Integrate f4py into the analysis paper tests. Check speed and optimize more, if needed.
#TODO: Address remaining TODO items in the code, remove unnecessary commented code.
#TODO: Combine all information into a single file.
#        Use this spec? https://tools.ietf.org/id/draft-kunze-bagit-16.html
#TODO: Try potential speed improvements:
#        - Python 3.11
#        - Try Nuitka? https://nuitka.net (compiles your Python code to C, is supposed to achieve speedups of 3x or greater).
#        - Parallelize the process of generating the output file (save to temp file)? This *might* help when doing a lot of decompression.
#TODO: Run this script from beginning to end as a final check.

#######################################################
# Run tests
#######################################################

python3 test.py
#$dockerCommand python3 test.py
#$dockerCommand python3

#######################################################
# Clean up
#######################################################

rm -rf f4py

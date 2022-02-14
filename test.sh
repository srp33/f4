#! /bin/bash

currentDir=$(pwd)
tmpDir=/tmp/f4_docker

mkdir -p $tmpDir
rm -rf $tmpDir/*

cp Dockerfile $tmpDir/
cd $tmpDir

docker build -t srp33/f4 .

cd $currentDir

dockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox --workdir=/sandbox srp33/f4"

$dockerCommand python3 TestSmall.py
#time $dockerCommand python3 TestBuildLarge.py
#time $dockerCommand python3 TestParseLarge.py

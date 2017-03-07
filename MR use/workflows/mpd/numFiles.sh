#!/bin/bash

set -eo pipefail

sourceDir=$1
echo "Source Dir: $sourceDir"

# Get just the filenames on hdfs
files=( `hdfs dfs -ls $sourceDir | sed '1d' | wc -l` )
echo "numFiles=$files"


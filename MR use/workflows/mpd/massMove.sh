#!/bin/bash

set -eo pipefail

sourceDir=$1
echo "Source Dir: $sourceDir"
destDir=$2
echo "Destination Dir: $destDir"
numberToKeep=$3
echo "Number of files being kept: $numberToKeep"

# Get just the filenames on hdfs
# Note the -1 in the numberToKeep.  This is because we're actually removing the header printout
files=( `hdfs dfs -ls $sourceDir | sed '1d' | sort -r -k6 | tail -n +$((numberToKeep+1))| awk '{print $8}'` )
echo "List of files being moved"
printf '\t%s\n' "${files[@]}"

for file in "${files[@]}"; do
        echo "File is: $file"
        filename=`basename $file`
        echo "Filename is: $filename"
        hdfs dfs -mv "$file" "$destDir"
done
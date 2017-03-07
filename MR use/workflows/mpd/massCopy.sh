#!/bin/bash

set -eo pipefail

sourceDir=$1
echo "Source Dir: $sourceDir"
destDir=$2
echo "Destination Dir: $destDir"
fileSuffix=$3
echo "File Suffix: $fileSuffix"

# Get just the filenames on hdfs
files=( `hdfs dfs -ls $sourceDir | sed '1d' | awk '{print $8}'` )
echo "Files is: $files"

for file in "${files[@]}"; do
        echo "File is: $file"
        filename=`basename $file`
        echo "Filename is: $filename"
        echo "Final destination is $destDir/$filename.$fileSuffix"
        hdfs dfs -cp "$file" "$destDir/$filename.$fileSuffix"
done
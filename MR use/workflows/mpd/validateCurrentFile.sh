#!/bin/bash

set -eo pipefail

defaultValue=0

sourceDir=$1
echo "Source Dir: $sourceDir"
destDir=$2
echo "Destination Dir: $destDir"
numberToKeep=$3
echo "Number of files being kept: $numberToKeep"

# Get just the filenames on hdfs
filesCount=( `hdfs dfs -ls $sourceDir | sed '1d' | wc -l` )

if [ $filesCount == $defaultValue ]
then
    echo "message=There is no current MPD file available to process this job. Please refer to the HDFS directory /common/enterprise_services/data/ingest/mpd/currentFile."
    exit 1
else
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

    # Get the count details of the given source directory
    sizeInBytes=( `hadoop fs -count $sourceDir | awk '{print $3}'` )
    echo "message=There are no records in the current file to compare with previous file. Please refer to the HDFS directory /common/enterprise_services/data/ingest/mpd/currentFile."
    exit 1
fi

#!/bin/bash -x

set -eo pipefail

cluster_name=$1
keytab=$2
hadoop_home=$3
current_file=$4
previous_file=$5
output_dir=$6
queue=$7
username=$8
jar_loc=$9
REALM=${cluster_name^^}.TARGET.COM
echo $REALM

full_name=${username}@${REALM}

echo "starting kinit"
kinit -kt $keytab $full_name
klist;

echo "Using Hive script directory $script_dir"

echo $hadoop_home

export HADOOP_HOME=$hadoop_home

#move to /tmp to avoid any home_dir issues we've seen occasionally
cd /tmp
rm -rf /tmp/mpd
mkdir -p /tmp/mpd
cd /tmp/mpd
hadoop fs -get ${jar_loc}

jar_name=`basename ${jar_loc}`

echo "${HADOOP_TOKEN_FILE_LOCATION}"
hadoop jar ${jar_name} com.target.mpd.delta.mapreduce.MpdDeltaFilter ${current_file} ${previous_file} ${output_dir} ${keytab} ${full_name} ${queue}

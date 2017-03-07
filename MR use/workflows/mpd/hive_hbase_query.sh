#!/bin/bash -x

cluster_name=$1
script_dir=$2
query=$3
keytab=$7
hadoop_home=$8
hive_home=$9
hcat_home=${10}
tez_home=${11}
REALM=${cluster_name^^}.TARGET.COM
hadoop_user=${12}

echo $REALM

echo "starting kinit"
kinit -kt $keytab SVESERV@$REALM
klist;

echo "Using Hive script directory $script_dir"

echo $hadoop_home
echo $hive_home
echo $hcat_home
echo $tez_home

export HADOOP_HOME=$hadoop_home
export HIVE_HOME=$hive_home
export HCAT_HOME=$hcat_home
export HCAT_LIB_JARS=$HCAT_HOME/hive-hcatalog-core.jar,$HCAT_HOME/hive-hcatalog-server-extensions.jar,$HCAT_HOME/hive-hcatalog-pig-adapter.jar,$HCAT_HOME/hive-hcatalog-streaming.jar,$HIVE_HOME/lib/hive-metastore.jar,$HIVE_HOME/lib/libthrift-0.9.0.jar,$HIVE_HOME/lib/hive-exec.jar,$HIVE_HOME/lib/libfb303-0.9.0.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$HIVE_HOME/lib/hive-cli.jar,$HIVE_HOME/lib/hive-common.jar,$HIVE_HOME/lib/hive-serde.jar
export TEZ_HOME=$tez_home

export HADOOP_CLASSPATH=$HCAT_HOME/*:$HIVE_HOME/lib/*:$HIVE_HOME/conf:$TEZ_HOME/conf:$TEZ_HOME/conf/*:$HADOOP_CLASSPATH
export HADOOP_USER_NAME=$hadoop_user

echo "Getting Hive script directory $script_dir"
rm -rf "/tmp/mpd/queries/$query"
#move to /tmp to avoid any home_dir issues we've seen occasionally
cd /tmp
mkdir -p /tmp/mpd/queries
cd /tmp/mpd/queries
hdfs dfs -get "$script_dir/$query"

echo "Running script $query"
echo "$4"
echo "$5"
echo "$6"

echo "${HADOOP_TOKEN_FILE_LOCATION}"
hive --hiveconf "$4" --hiveconf "$5" --hiveconf "$6" --hiveconf "mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION}" --hiveconf "tez.credentials.path=${HADOOP_TOKEN_FILE_LOCATION}" -f "/tmp/mpd/queries/$query"

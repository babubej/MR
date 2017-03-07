SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.reduce.tasks=-1;

SET hive.execution.engine=tez;
SET mapreduce.job.queuename=etl;
SET tez.queue.name=etl;

SET mapreduce.task.io.sort.mb=1535;

USE esv;

SELECT
  CONCAT("itemsCount=",count(t1.departmentid))
FROM
  inStoreLocation t1
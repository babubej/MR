mpd_diff
========

Performs a diff between the most recent file and the last file we ran on. We always just take the most recent file's results.

The job is a MapReduce running on the LittleRed and BigRed clusters, where one input is the current file and the second is the previous file. These go to a single reducer which either discards records where both exists, or takes the current file's set.

Output is taken by a bulk loader route downstream.

The job is managed and coordinated by Oozie, and the bulk loader watches the output directory in HDFS for new files.

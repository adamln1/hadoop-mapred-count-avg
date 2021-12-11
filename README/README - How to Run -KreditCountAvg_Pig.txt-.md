# Apache Pig

## Prerequisites
- Installed Hadoop 3.3.0 with JDK 1.8.0 (Java 8)
- All environment variables has been set up for both Hadoop and Java
- Activated Spark master & worker with: `start-all.sh`
- Has Kredit.csv file inside Hadoop File System (HDFS)
	- Ex: `/Public/data/input/kredit.csv`
- started pig with command: `pig`

## Running The Script
1. copy the script inside `KreditCountAvg_Pig.txt`
2. paste into the terminal where pig has been activated

```
> grunt> df = LOAD '/lab/kredit/input/kredit_nohead.csv' USING PigStorage(',') AS (occupation:int, salary:int, installment:int, tenor:int, usia:int, ketr:chararray);

> grunt> group_ketr = GROUP df BY ketr;

> grunt> count_avg_per_ketr = foreach group_ketr GENERATE group, (chararray)COUNT(df.ketr) AS total, (chararray)AVG(df.salary) AS average;

> grunt> DUMP count_avg_per_ketr;
```
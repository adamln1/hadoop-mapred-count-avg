# Apache Spark Map Reduce

## Prerequisites
- Installed Hadoop 3.3.0 with JDK 1.8.0 (Java 8)
- Installed Apache Spark 3.2.0
- All environment variables has been set up for both Hadoop and Java
- All environment variables has been set up for Apache Spark
- Activated Spark master & worker with: `start-all.sh`
- Has Kredit.csv file inside Hadoop File System (HDFS)
	- Ex: `/Public/data/input/kredit.csv`

- Assuming the local directory (not hdfs) of pyspark file is in 
	- `/home/user/tugas2/KreditCountAvg.py`

- we have to get into the same directory (level) of KreditCountAvg.py
	- `user@ubuntu: ~/$ cd tugas2` --> `user@ubuntu: ~/tugas2/$`


## Running The Script
1. Run
```
spark-submit <relative_path_to_py_file> <input_folder_in_hdfs> <output_folder_in_hdfs> <spark_context_uri>
```
- Ex: 	`spark-submit KreditCountAvg.py /Public/data/input /Public/data/output/1 spark://ubuntu.localdomain:7077`
	- Note:  
		- `/Public/data/output/1` should not exist before running
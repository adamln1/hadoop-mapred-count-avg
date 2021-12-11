# Hadoop Map Reduce

## Prerequisites
- Installed Hadoop 3.3.0 with JDK 1.8.0 (Java 8)
- All environment variables has been set up for both Hadoop and Java
- Activated Hadoop master & worker with: `${HADOOP_HOME}/sbin/start-all.sh`
- Has Kredit.csv file inside Hadoop File System (HDFS)
	- Ex: `/Public/data/input/kredit.csv`

- Assuming the local directory (not hdfs) of JAVA file is in 
	- `/home/user/tugas2/KreditCountAvg.java`

- we have to get into the same directory (level) of KreditCountAvg.java
	- `user@ubuntu: ~/$ cd tugas2` --> `user@ubuntu: ~/tugas2/$

- inside tugas2 directory, prepare a folder to store class files
	- `user@ubuntu: ~/tugas2/$ mkdir classes`

## Running The Script

1. Compile Java file via terminal to create class file in ./classes
```
javac -classpath ${HADOOP_CLASSPATH} -d <relative_path_to_classes_directory> <relative_path_to_java_file>
```
- Ex: `javac -classpath ${HADOOP_CLASSPATH} -d ./classes ./KreditCountAvg.java`


2. Generate JAR file using compiled classes in ./classes
```
jar -cvf <target_filename>.jar -C <relative_path_to_classes> <relative_target_directory>
```
- Ex: `jar -cvf KreditCountAvg.jar -C ./classes .`



3. Run generated JAR file inside hadoop file system
```
hadoop jar <relative_path_to_jar_file> <application class name> <input_path_in_hdfs> <output_path_in_hdfs>
```
- Ex: `hadoop jar ./KreditCountAvg.jar KreditCountAvg /Public/data/input /Public/data/output/1`
	- Note: `/Public/data/output/1` should not exist in hdfs, otherwise use another directory (i.e. `/Public/data/output/2`)
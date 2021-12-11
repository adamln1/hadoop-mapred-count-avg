import re, sys
import csv
import subprocess
import timeit

from pyspark.context import SparkContext

def read_file_and_map(filepath, sc):
    tmp_doc = sc.textFile(filepath)
    header = tmp_doc.first()
    tmp_doc = tmp_doc.filter(lambda row: row != header)
    terms = tmp_doc.flatMap(lambda row: [row.split(",")]).map(lambda col: (col[5], (1,int(col[1]))))
    # print("TERMS-PHASE 1: MAPPING>>>>", terms.collect())
    # print(50*"=")
    return terms

def listdir_in_hdfs(folder):
    args = "hdfs dfs -ls "+folder+" | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dirs = s_output.split()
    return all_dirs

def map_and_create_terms(hdfs_folder, sc):
    supported_formats = ('.csv', '.txt', '.tsv')
    file_count = 0
    all_terms = {} # placeholder for a variable
    folder = listdir_in_hdfs(hdfs_folder)
    for path in folder:
        path = path.decode('utf-8')
        if (path.endswith(supported_formats)):
            if (file_count == 0):
                file_count+=1
                all_terms = read_file_and_map(path, sc)
            else:
                file_count+=1
                all_terms = all_terms.union(read_file_and_map(path))
        else:
            pass
    return all_terms

def reduce_terms(all_terms):
    result = all_terms.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
    # print("TERMS-PHASE-2: REDUCE>>> ", result.collect())
    # print(50*"=")
    return result

def count_and_compute_avg(result):
    result = result.collect()
    print(70*"=")
    print("\tKETR\t\t|\tCOUNT\t|\tAVG SALARY")
    print(70*"-")
    for tup in result:
        key = tup[0]
        count = tup[1][0]
        sumsalary = tup[1][1]
        avg = int(sumsalary/count)
        print(f'\t{key}\t\t|\t{count}\t|\t{avg}')
    print(70*"=")        

def main():
    args = sys.argv[1:]
    input_folder = args[0]
    output_folder = args[1]
    sc_uri = args[2]
    sc = SparkContext(sc_uri, 'main')
    start = timeit.default_timer()
    terms = map_and_create_terms(input_folder, sc)
    result = reduce_terms(terms)
    stop_job = timeit.default_timer()
    # writing collected into a file in hdfs:
    result.saveAsTextFile(output_folder)
    # Printing result in terminal
    count_and_compute_avg(result)
    stop_overall = timeit.default_timer()
    print('>> Job Run time (sec): ', stop_job - start)
    print('>> Overall Run time (sec): ', stop_overall - start)  
    
if __name__ == "__main__":
    main()
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class KreditCountAvg extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, SalaryCountWritable> {
        // Variable-variable
        
        static enum Counters { INPUT_CATEGORIES }
        
        private Text ketr = new Text();
        private SalaryCountWritable salarycount = new SalaryCountWritable(0,1);

        private long numRecords = 0;
        private String inputFile;

        public void map(LongWritable key, Text value, OutputCollector<Text, SalaryCountWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            // CREATE MAPPER LOGIC HERE
            
            // 1. Skip header line
            if (line.startsWith("OCCUPATION,SALARY")) {
                return;
            }

            // 2. Map values
            String[] lineSplit = line.split(",");
            int tempSalary = Integer.valueOf(lineSplit[1]);
            String tempKetr = lineSplit[5];
            
            ketr.set(tempKetr);
            salarycount.set(tempSalary, 1);
            output.collect(ketr, salarycount);
            
            // 5. report
            reporter.incrCounter(Counters.INPUT_CATEGORIES, 1);

            ++numRecords;
            reporter.setStatus("Processed " + numRecords + " line of data from input file:" + inputFile); 
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, SalaryCountWritable, Text, SalaryCountWritable> {
        public void reduce(Text key, Iterator<SalaryCountWritable> values, OutputCollector<Text, SalaryCountWritable> output, Reporter reporter) throws IOException {
            // CREATE REDUCER LOGIC HERE
            int sumsalary = 0;
            int count = 0;
            int[] salarycount;

            while (values.hasNext()) {
                salarycount = values.next().get();
                sumsalary += salarycount[0];
                count += salarycount[1];
            }
            output.collect(key, new SalaryCountWritable(sumsalary, count));
        }
    }

    public static class SalaryCountWritable implements Writable {

        private int count;
        private int salary;
        
        public SalaryCountWritable() {}

        public SalaryCountWritable(int salary, int count) {
            this.salary = salary;
            this.count = count;
        }
    
        public int[] get() {
            int[] arr={salary,count};
            return arr;
        }

        public void set(int salary, int count) {
            this.salary = salary;
            this.count = count;
        }

        public void readFields(DataInput in) throws IOException {
            this.salary = in.readInt();
            this.count = in.readInt();
        }

        public void write(DataOutput out) throws IOException { 
            out.writeInt(salary);
            out.writeInt(count);
        }

        public String toString() {
           return String.valueOf(salary) + '\t' + String.valueOf(count);
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), KreditCountAvg.class);
        conf.setJobName("KreditCountAvg");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(SalaryCountWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            if ("-skip".equals(args[i])) {
                DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
                //conf.setBoolean("wordcount.skip.patterns", true);
            } else {
                other_args.add(args[i]);
            }
        }

        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

        long startJobTime = System.currentTimeMillis();
        JobClient.runJob(conf);
        long stopJobTime = System.currentTimeMillis();
        long jobRuntime = stopJobTime - startJobTime;

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(other_args.get(1), "part-00000"));

        System.out.println("\n\n======================RESULT======================");
        System.out.println("\tKETR\t|\tCOUNT\t|\tAVG SLRY\t");
        System.out.println("==================================================");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        while(reader.ready()) {
            // line --> key:'xxxx'|val: 'yyy1,yyy2' 
            String[] line = reader.readLine().split("\t");
            // String[] sc = line[1].split(",");
            String key;
            int sal, count;
            key = line[0];
            sal = Integer.parseInt(line[1]);
            count = Integer.parseInt(line[2]);
            Integer avg = sal/count;
            System.out.println(String.format("\t%s\t|\t%d\t|\t%d\t", key, count, avg));
        }
        System.out.println("--------------------------------------------------\t");
        System.out.println(">>> Elapsed Job Runtime (ms):" + Long.toString(jobRuntime));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        long startOverallTime = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new KreditCountAvg(), args);
        long stopOverallTime = System.currentTimeMillis();
        long overallRuntime = stopOverallTime - startOverallTime;
        System.out.println(">>> Elapsed Overall Runtime (ms):"+ Long.toString(overallRuntime));
        System.exit(res);
    }
}
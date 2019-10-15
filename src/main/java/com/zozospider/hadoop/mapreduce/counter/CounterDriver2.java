package com.zozospider.hadoop.mapreduce.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 打印出不同字段数的累计行数 (不输出数据).
 */
public class CounterDriver2 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/Counter2/
     * total 8
     * -rw-r--r--  1 user  staff   119B 10 15 21:03 f1
     * ➜  input cat Counter2/f1
     * abc abc love
     * qq
     * qq
     * qq who love
     * see ok book
     * see ok book
     * see ok book
     * see ok book
     * enough book love me
     * enough book love me
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/Counter2/
     * ls: /Users/user/other/tmp/MapReduce/output/Counter2/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/Counter2", "/Users/user/other/tmp/MapReduce/output/Counter2"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // Map Join 不需要 Reduce 阶段, 所以将 ReduceTask 个数设置为 0
        job.setNumReduceTasks(0);

        // 2 设置 Jar, Mapper 类
        job.setJarByClass(CounterDriver2.class);
        job.setMapperClass(CounterMapper2.class);

        // 3 设置最终的 KEYOUT, VALUEOUT
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/Counter2/
     * total 0
     * -rw-r--r--  1 user  staff     0B 10 15 21:19 _SUCCESS
     * -rw-r--r--  1 user  staff     0B 10 15 21:19 part-m-00000
     * ➜  output cat Counter2/part-m-00000
     * ➜  output
     */

    /**
     * 2019-10-15 21:19:54,090 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-15 21:19:54,359 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-15 21:19:54,360 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-15 21:19:54,772 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-15 21:19:54,776 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-15 21:19:54,791 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-15 21:19:54,836 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-15 21:19:54,941 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1006683688_0001
     * 2019-10-15 21:19:55,128 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-15 21:19:55,130 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1006683688_0001
     * 2019-10-15 21:19:55,130 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-15 21:19:55,136 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 21:19:55,138 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-15 21:19:55,181 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-15 21:19:55,182 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1006683688_0001_m_000000_0
     * 2019-10-15 21:19:55,211 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 21:19:55,219 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-15 21:19:55,219 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-15 21:19:55,223 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/Counter2/f1:0+119
     * 2019-10-15 21:19:55,252 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-15 21:19:55,253 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1006683688_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-15 21:19:55,261 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-15 21:19:55,261 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1006683688_0001_m_000000_0 is allowed to commit now
     * 2019-10-15 21:19:55,263 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1006683688_0001_m_000000_0' to file:/Users/user/other/tmp/MapReduce/output/Counter2/_temporary/0/task_local1006683688_0001_m_000000
     * 2019-10-15 21:19:55,264 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-15 21:19:55,264 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1006683688_0001_m_000000_0' done.
     * 2019-10-15 21:19:55,264 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1006683688_0001_m_000000_0
     * 2019-10-15 21:19:55,264 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-15 21:19:56,148 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1006683688_0001 running in uber mode : false
     * 2019-10-15 21:19:56,149 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-15 21:19:56,151 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1006683688_0001 completed successfully
     * 2019-10-15 21:19:56,159 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 18
     * 	File System Counters
     * 		FILE: Number of bytes read=292
     * 		FILE: Number of bytes written=276095
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=10
     * 		Map output records=0
     * 		Input split bytes=119
     * 		Spilled Records=0
     * 		Failed Shuffles=0
     * 		Merged Map outputs=0
     * 		GC time elapsed (ms)=0
     * 		Total committed heap usage (bytes)=128974848
     * 	fields_length
     * 		len-1=2
     * 		len-3=6
     * 		len-4=2
     * 	File Input Format Counters
     * 		Bytes Read=119
     * 	File Output Format Counters
     * 		Bytes Written=8
     *
     * Process finished with exit code 0
     */

}
